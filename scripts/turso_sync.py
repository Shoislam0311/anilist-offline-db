#!/usr/bin/env python3
"""
AniList Offline Database - Turso Sync (delta-disciplined for the free tier).

Free caps: 5GB storage / 500M rows read / 10M rows written per month.
A full re-seed writes ~1-2M rows, so it fits ONCE (bootstrap), but routine
syncs push only touched IDs:
  - data/touched_full.json     -> full anime rows + all scoped related rows
  - data/touched_counters.json -> full anime rows (counters live there too)

Writes per daily rails run: ~300-500 anime + scoped rows. Far under budget.

Write-only mode (--write-only, used by the daily workflow): makes ZERO
remote SELECTs. No sqlite_master probes, no COUNT(*) gates, no per-chunk
table checks — blind CREATE IF NOT EXISTS once, then INSERT OR REPLACE /
DELETE only. Local SELECTs are free (SQLite file, unbilled).

Env required: TURSO_URL, TURSO_AUTH_TOKEN  (exit 2 if missing)
Usage:
  python3 scripts/turso_sync.py                  # delta (default)
  python3 scripts/turso_sync.py --write-only     # daily rails: no remote reads
  python3 scripts/turso_sync.py --bootstrap      # full copy (first run only)
"""

import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

CHUNK = 2000       # ids per scoped pass (small rows: few round trips)
BATCH = 1000       # rows per executemany for small tables
ANIME_BATCH = 10   # full raw_json rows (~330KB each): ~3MB bodies survive
# slow links without timeouts (25 was timing out against Turso free tier)
TIME_BUDGET_DEFAULT = 100  # minutes; stop gracefully, resume next run

PROGRESS_FILE = "turso_sync_progress.json"

# association/detail tables keyed by anime_id (delete scoped rows, re-insert)
SCOPED_TABLES = [
    "anime_titles", "anime_descriptions", "anime_genres", "anime_tags",
    "anime_studios", "anime_characters", "character_voice_actors",
    "anime_staff", "relations", "recommendations", "airing_schedule",
    "external_links", "streaming_episodes", "rankings", "trends",
    "reviews", "statistics",
]

SKIP_TABLES = {"anime_fts", "characters_fts", "sqlite_sequence"}


def connect_remote():
    url = (os.environ.get("TURSO_URL", "") or "").strip()
    token = (os.environ.get("TURSO_AUTH_TOKEN", "") or "").strip()
    # file: URLs run token-free (local tests, no remote touched)
    if url.startswith("file:") or url.endswith(".db"):
        if not url:
            print("ERROR: TURSO_URL / TURSO_AUTH_TOKEN not set", file=sys.stderr)
            sys.exit(2)
        try:
            import libsql_experimental as libsql
        except ImportError:
            print("ERROR: pip install libsql-experimental", file=sys.stderr)
            sys.exit(2)
        return libsql.connect(database=url)
    if not url or not token:
        print("ERROR: TURSO_URL / TURSO_AUTH_TOKEN not set", file=sys.stderr)
        sys.exit(2)
    try:
        import libsql_experimental as libsql
    except ImportError:
        print("ERROR: pip install libsql-experimental", file=sys.stderr)
        sys.exit(2)
    return libsql.connect(database=url, auth_token=token)


def ensure_schema(rconn, blind=False):
    from db_utils import DB_SCHEMA, SCOPED_INDEXES
    if blind:
        # Write-only path: zero remote reads. CREATE IF NOT EXISTS is a
        # write; safe to replay every run without probing sqlite_master.
        for stmt in DB_SCHEMA.split(";"):
            stmt = stmt.strip()
            if stmt:
                rconn.execute(stmt)
        for idx_sql in SCOPED_INDEXES:
            try:
                rconn.execute(idx_sql)
            except Exception:
                pass
        try:
            rconn.commit()
        except Exception:
            pass
        return False
    existing = {r[0] for r in rconn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    created = False
    if "anime" not in existing:
        print("Remote empty: creating schema...")
        for stmt in DB_SCHEMA.split(";"):
            stmt = stmt.strip()
            if stmt:
                rconn.execute(stmt)
        rconn.commit()
        created = True
    # Always: scoped DELETE/SELECT by anime_id must be indexed, or each one
    # full-scans hundred-thousand-row tables (slow sync + phantom row reads).
    for idx_sql in SCOPED_INDEXES:
        try:
            rconn.execute(idx_sql)
        except Exception:
            pass
    try:
        rconn.commit()
    except Exception:
        pass
    return created


def local_columns(lconn, table):
    return [r[1] for r in lconn.execute(f"PRAGMA table_info({table})").fetchall()]


def remote_has_table(rconn, table):
    return bool(rconn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,)).fetchall())


def replace_rows(rconn, table, cols, rows, batch_size=BATCH):
    if not rows:
        return 0
    placeholders = ", ".join(["?"] * len(cols))
    colnames = ", ".join([f'"{c}"' for c in cols])
    sql = f'INSERT OR REPLACE INTO "{table}" ({colnames}) VALUES ({placeholders})'
    done = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        try:
            _with_retry(lambda: rconn.executemany(sql, batch))
            done += len(batch)
        except Exception as e:
            # Deterministic poison (e.g. FK on a dangling ref) after transient
            # retries: fall back to row-by-row, skipping only poison rows.
            # Re-inserted rows REPLACE identically — never duplicates.
            skipped = 0
            for row in batch:
                try:
                    _with_retry(lambda: rconn.execute(sql, _t(list(row))), tries=3)
                    done += 1
                except Exception:
                    skipped += 1
            print(f"  {table}: batch aborted ({str(e)[:100]}); "
                  f"row-level fallback saved {done} total, skipped {skipped}")
    rconn.commit()
    return done


def _t(params):
    # libsql execute() requires a tuple, not a list
    return tuple(params) if isinstance(params, list) else params


def delete_scoped(rconn, table, ids):
    ids = list(ids)
    for i in range(0, len(ids), CHUNK):
        chunk = ids[i:i + CHUNK]
        q = ", ".join(["?"] * len(chunk))
        _with_retry(lambda: (rconn.execute(
            f'DELETE FROM "{table}" WHERE anime_id IN ({q})', _t(chunk)), rconn.commit()))


def copy_where(lconn, rconn, table, where, params, batch_size=BATCH,
               skip_remote_check=False):
    cols = local_columns(lconn, table)
    if not cols:
        return 0
    if not skip_remote_check and not remote_has_table(rconn, table):
        return 0
    colnames = ", ".join([f'"{c}"' for c in cols])
    rows = lconn.execute(f"SELECT {colnames} FROM {table} WHERE {where}", _t(params)).fetchall()
    return replace_rows(rconn, table, cols, [tuple(r) for r in rows], batch_size=batch_size)


def _chunks(ids, n=None):
    ids = [i for i in ids if i is not None]
    n = n or CHUNK
    return [ids[i:i + n] for i in range(0, len(ids), n)]


def _sync_shared_for_chunk(lconn, rconn, chunk, write_only=False):
    """Shared entities first: scoped rows FK-reference these."""
    count = 0
    q = ", ".join(["?"] * len(chunk))
    refs = {
        "genres": ("SELECT id FROM genres WHERE id IN "
                   "(SELECT genre_id FROM anime_genres WHERE anime_id IN (%s))" % q, chunk),
        "studios": ("SELECT id FROM studios WHERE id IN "
                    "(SELECT studio_id FROM anime_studios WHERE anime_id IN (%s))" % q, chunk),
        "characters": ("SELECT id FROM characters WHERE id IN "
                       "(SELECT character_id FROM anime_characters WHERE anime_id IN (%s))" % q, chunk),
        "staff": ("SELECT id FROM staff WHERE id IN "
                  "(SELECT staff_id FROM anime_staff WHERE anime_id IN (%s))" % q, chunk),
    }
    for table, (sql, params) in refs.items():
        try:
            ref_ids = [r[0] for r in lconn.execute(sql, _t(params)).fetchall()]
        except Exception:
            continue
        for c2 in _chunks(ref_ids):
            q2 = ", ".join(["?"] * len(c2))
            count += copy_where(lconn, rconn, table, f"id IN ({q2})", c2,
                                skip_remote_check=write_only)
    try:
        tag_names = [r[0] for r in lconn.execute(
            f"SELECT DISTINCT tag_name FROM anime_tags WHERE anime_id IN ({q})", _t(chunk)).fetchall()]
        for c2 in _chunks(tag_names):
            q2 = ", ".join(["?"] * len(c2))
            count += copy_where(lconn, rconn, "tags", f"name IN ({q2})", c2,
                                skip_remote_check=write_only)
        va_ids = [r[0] for r in lconn.execute(
            f"""SELECT DISTINCT voice_actor_id FROM character_voice_actors
                WHERE anime_id IN ({q})""", _t(chunk)).fetchall()]
        for c2 in _chunks(va_ids):
            q2 = ", ".join(["?"] * len(c2))
            count += copy_where(lconn, rconn, "voice_actors", f"id IN ({q2})", c2,
                                skip_remote_check=write_only)
    except Exception:
        pass
    return count


def _deadline_ok(started_at, max_minutes):
    import time
    return (time.monotonic() - started_at) < max_minutes * 60


def load_progress(data_dir):
    import json as _json
    path = os.path.join(data_dir, PROGRESS_FILE)
    try:
        with open(path) as f:
            return _json.load(f)
    except Exception:
        return {}


def save_progress(data_dir, progress):
    import json as _json
    try:
        with open(os.path.join(data_dir, PROGRESS_FILE), "w") as f:
            _json.dump(progress, f)
    except Exception:
        pass


def mark_complete(rconn, total_anime):
    rconn.execute(
        "CREATE TABLE IF NOT EXISTS sync_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    rconn.execute(
        "INSERT OR REPLACE INTO sync_state (key, value) VALUES ('bootstrap_complete', '1')")
    rconn.execute(
        "INSERT OR REPLACE INTO sync_state (key, value) VALUES ('total_anime', ?)",
        (str(total_anime),))
    try:
        from datetime import datetime, timezone
        rconn.execute(
            "INSERT OR REPLACE INTO sync_state (key, value) VALUES ('completed_at', ?)",
            (datetime.now(timezone.utc).isoformat(),))
    except Exception:
        pass
    rconn.commit()


WORKERS = 1  # SEQUENTIAL writes only. Turso (single-writer, free-tier
# throttled) times out under parallel writers: 6 lanes just contend on the
# write lock, burn the budget in retries, and finish slower than one lane.
# Rails deltas are small (<=1000 IDs) — one steady lane wins every time.

_TRANSIENT_HINTS = ("busy", "locked", "timeout", "timed out", "connection",
                    "reset by peer", "unavailable", "try again", "429",
                    "500", "502", "503", "504")


def _is_transient(e):
    return any(t in str(e).lower() for t in _TRANSIENT_HINTS)


def _with_retry(fn, tries=4):
    import time as _t
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if not _is_transient(e) or i == tries - 1:
                raise
            _t.sleep(min(2 ** i, 8))
    raise last


def _open_remote():
    import libsql_experimental as libsql
    url = (os.environ.get("TURSO_URL", "") or "").strip()
    token = (os.environ.get("TURSO_AUTH_TOKEN", "") or "").strip()
    if url.startswith("file:") or url.endswith(".db"):
        return libsql.connect(database=url)
    return libsql.connect(database=url, auth_token=token)


def _open_local(db_path):
    from db_utils import connect_db
    return connect_db(db_path)


def _close_quietly(conn):
    try:
        conn.close()
    except Exception:
        pass


def _worker_anime_chunk(idx, chunk, db_path, write_only=False):
    """Sync anime rows for one disjoint chunk. Own connections; returns result tuple."""
    lconn = _open_local(db_path)
    rconn = _open_remote()
    try:
        q = ", ".join(["?"] * len(chunk))
        n = copy_where(lconn, rconn, "anime", f"id IN ({q})", chunk,
                       batch_size=ANIME_BATCH, skip_remote_check=write_only)
        return ("anime", idx, n, 0, None)
    except Exception as e:
        return ("anime", idx, 0, 0, e)
    finally:
        _close_quietly(lconn)
        _close_quietly(rconn)


def _worker_related_chunk(idx, chunk, db_path, write_only=False):
    """Sync shared entities + scoped rows for one disjoint chunk."""
    lconn = _open_local(db_path)
    rconn = _open_remote()
    anime_n = related_n = 0
    try:
        q = ", ".join(["?"] * len(chunk))
        related_n += _sync_shared_for_chunk(lconn, rconn, chunk,
                                            write_only=write_only)
        for table in SCOPED_TABLES:
            if not write_only and not remote_has_table(rconn, table):
                continue
            cols = local_columns(lconn, table)
            if not cols or "anime_id" not in cols:
                continue
            _with_retry(lambda: delete_scoped(rconn, table, chunk))
            colnames = ", ".join([f'"{c}"' for c in cols])
            rows = lconn.execute(
                f"SELECT {colnames} FROM {table} WHERE anime_id IN ({q})", _t(chunk)).fetchall()
            related_n += replace_rows(rconn, table, cols, [tuple(r) for r in rows])
        return ("related", idx, anime_n, related_n, None)
    except Exception as e:
        return ("related", idx, anime_n, related_n, e)
    finally:
        _close_quietly(lconn)
        _close_quietly(rconn)


def _namespaced_sets(progress, tag):
    """Progress bookkeeping per sync call (full/counters/bootstrap share one file).
    Migrates the legacy bare keys (bootstrap-era: anime_done int, related_done list)."""
    ns = progress.setdefault("chunks", {}).setdefault(tag, {})
    if not ns and tag == "bootstrap":
        legacy_a = progress.get("anime_done", 0)
        anime = set(range(legacy_a)) if isinstance(legacy_a, int) else set(legacy_a or [])
        related = set(progress.get("related_done", []) or [])
        if anime or related:
            ns["anime"] = sorted(anime)
            ns["related"] = sorted(related)
    return set(ns.get("anime", [])), set(ns.get("related", []))


def _mark_done(progress, tag, kind, idx):
    ns = progress.setdefault("chunks", {}).setdefault(tag, {})
    key = "anime" if kind == "anime" else "related"
    done = set(ns.get(key, []))
    done.add(idx)
    ns[key] = sorted(done)


def sync_anime_ids(db_path, ids, with_related: bool,
                   data_dir=None, max_minutes=None, progress=None, tag="sync",
                   write_only=False):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    from functools import partial
    stats = {"anime": 0, "related": 0}
    chunks = _chunks(ids)
    if not chunks:
        return stats
    started_at = time.monotonic()
    progress = progress or {}
    anime_done, related_done = _namespaced_sets(progress, tag)
    stopped = False

    def run_phase(kind, pending):
        nonlocal stopped
        if not pending or stopped:
            return
        phase_started = time.monotonic()
        phase_rows = dict(stats)
        print(f"Phase {kind}: {len(pending)} chunk(s), {WORKERS} lane(s)...",
              flush=True)
        worker = partial(_worker_anime_chunk, write_only=write_only) \
            if kind == "anime" else partial(
            _worker_related_chunk, write_only=write_only)
        ex = ThreadPoolExecutor(max_workers=WORKERS)
        try:
            futs = {ex.submit(worker, idx, chunks[idx], db_path): idx for idx in pending}
            completed = 0
            for fut in as_completed(futs):
                try:
                    _, idx, a_n, r_n, err = fut.result()
                except Exception as e:
                    print(f"  chunk worker crashed, will resume: {str(e)[:120]}")
                    continue
                if err is None:
                    stats["anime"] += a_n
                    stats["related"] += r_n
                    (anime_done if kind == "anime" else related_done).add(idx)
                    _mark_done(progress, tag, kind, idx)
                    completed += 1
                    if data_dir and completed % 2 == 0:
                        save_progress(data_dir, progress)
                else:
                    print(f"  chunk {idx} ({kind}) failed, will resume: {str(err)[:120]}")
                if max_minutes and not _deadline_ok(started_at, max_minutes):
                    stopped = True
                    break
        finally:
            ex.shutdown(wait=False, cancel_futures=True)
        phase_secs = time.monotonic() - phase_started
        wrote = (stats["anime"] - phase_rows["anime"]
                 + stats["related"] - phase_rows["related"])
        rate = wrote / phase_secs if phase_secs > 0 else 0
        print(f"Phase {kind} done: {completed}/{len(pending)} chunks, "
              f"{wrote} rows in {phase_secs:.0f}s ({rate:.1f} rows/s)",
              flush=True)

    # Pass 1 (barrier): ALL anime rows first — relations FK-reference anime
    # that may live in another worker's chunk.
    run_phase("anime", [n for n in range(len(chunks)) if n not in anime_done])
    if data_dir:
        save_progress(data_dir, progress)
    # Pass 2: shared entities + scoped rows per chunk.
    if with_related:
        run_phase("related", [n for n in range(len(chunks)) if n not in related_done])
    progress["incomplete"] = stopped or any(
        n not in anime_done for n in range(len(chunks))) or (
        with_related and any(n not in related_done for n in range(len(chunks))))
    if not progress["incomplete"]:
        progress.pop("incomplete", None)
    if data_dir:
        save_progress(data_dir, progress)
    return stats


def read_touched(data_dir, name):
    path = os.path.join(data_dir, name)
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            return [i for i in json.load(f) if isinstance(i, int)]
    except Exception:
        return []


def _read_manifest_ids(base_dir):
    """IDs the fetcher promised to push (rail_manifest.json pushIds).
    Falls back to the union of rail membership on older manifests.
    None = no manifest (older/manual runs) — cross-check skipped."""
    try:
        with open(os.path.join(base_dir, "docs", "api", "rail_manifest.json")) as f:
            m = json.load(f)
        if m.get("pushIds"):
            return set(m["pushIds"])
        ids = set()
        for rail in (m.get("rails") or {}).values():
            ids.update(rail.get("ids") or [])
        return ids
    except Exception:
        return None


def _save_sync_report(base_dir, write_only, full_ids, only_counters,
                      total_stats, sync_secs, incomplete):
    """Machine-readable twin of the SYNC SUMMARY block. Committed under
    docs/api/ so the repo history shows exactly what every daily run
    pushed — the answer to 'what rows does Turso have from this run'
    without ever reading the remote."""
    try:
        from datetime import datetime, timezone
        report = {
            "completedAt": datetime.now(timezone.utc).isoformat(),
            "mode": "write-only" if write_only else "delta",
            "idsPushed": len(full_ids) + len(only_counters),
            "ids": sorted(set(full_ids) | set(only_counters)),
            "animeRows": total_stats.get("anime", 0),
            "relatedRows": total_stats.get("related", 0),
            "seconds": round(sync_secs, 1),
            "incomplete": incomplete,
        }
        out = os.path.join(base_dir, "docs", "api", "sync_report.json")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w") as f:
            json.dump(report, f)
        print(f"Sync report written: {out} ({report['idsPushed']} IDs)")
    except Exception as e:
        print(f"Sync report skipped: {str(e)[:120]}")


def main():
    from db_utils import connect_db, get_db_path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")
    db_path = get_db_path(data_dir)
    if not os.path.exists(db_path):
        print(f"ERROR: local DB not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    bootstrap = "--bootstrap" in sys.argv
    write_only = "--write-only" in sys.argv
    # Rails-only limitation: refuse to push the whole catalog from a daily
    # run (that was the budget blowout). Bootstrap is the only full path.
    RAIL_ROW_CAP = 1000
    max_minutes = TIME_BUDGET_DEFAULT
    for i, a in enumerate(sys.argv[1:]):
        if a.startswith("--max-minutes"):
            if "=" in a:
                max_minutes = int(a.split("=", 1)[1])
            elif i + 2 < len(sys.argv):
                max_minutes = int(sys.argv[i + 2])
        if a.startswith("--max-rows"):
            if "=" in a:
                RAIL_ROW_CAP = int(a.split("=", 1)[1])
            elif i + 2 < len(sys.argv):
                RAIL_ROW_CAP = int(sys.argv[i + 2])
    rconn = connect_remote()
    created = ensure_schema(rconn, blind=write_only)

    if not write_only:
        # Self-healing gate: if the remote already holds the full dataset
        # (e.g. manual file upload), stamp the completion flag so serving flips
        # on automatically. No-op when already flagged or when counts differ.
        # Skipped in --write-only (it is a remote read, banned on daily runs).
        try:
            local_total = connect_db(db_path).execute("SELECT COUNT(*) FROM anime").fetchone()[0]
            remote_total = rconn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
            if local_total > 10000 and local_total == remote_total:
                mark_complete(rconn, local_total)
                print(f"Completeness verified locally ({local_total} rows): flag live.")
        except Exception as e:
            print(f"Completeness check skipped: {str(e)[:120]}")

    lconn = connect_db(db_path)
    try:
        if bootstrap or created:
            ids = [r[0] for r in lconn.execute("SELECT id FROM anime ORDER BY id").fetchall()]
            progress = load_progress(data_dir)
            print(f"Bootstrap: pushing {len(ids)} anime + related rows "
                  f"({WORKERS} workers, budget {max_minutes} min, resume-safe)...")
            stats = sync_anime_ids(db_path, ids, with_related=True,
                                   data_dir=data_dir, max_minutes=max_minutes,
                                   progress=progress, tag="bootstrap")
            if progress.get("incomplete"):
                print(f"Bootstrap INCOMPLETE within budget: {stats} — progress saved, "
                      f"resume continues next run. Remote stays ungated (invisible).")
                return
            mark_complete(rconn, len(ids))
            try:
                os.remove(os.path.join(data_dir, PROGRESS_FILE))
            except OSError:
                pass
            print(f"Bootstrap done: {stats} — remote gated LIVE.")
            return

        full_ids = read_touched(data_dir, "touched_full.json")
        counter_ids = read_touched(data_dir, "touched_counters.json")
        # No-double-write verification (local, zero remote reads): touched
        # files must already be unique; any dupes are collapsed here and
        # reported, so one anime row is written exactly once per run.
        # Idempotency itself comes from INSERT OR REPLACE / ON CONFLICT
        # upserts — re-running the same manifest can never duplicate rows.
        dupe_full = len(full_ids) - len(set(full_ids))
        dupe_cnt = len(counter_ids) - len(set(counter_ids))
        full_ids = sorted(set(full_ids))
        counter_ids = sorted(set(counter_ids))
        if dupe_full or dupe_cnt:
            print(f"Verify: collapsed {dupe_full} full-list + {dupe_cnt} "
                  f"counters-list duplicate IDs (each row written once)")
        else:
            print(f"Verify: touched lists unique "
                  f"({len(full_ids)} full + {len(counter_ids)} counters)")
        # Cross-check against the rail manifest the fetcher wrote: the push
        # set must equal exactly what the rails produced — nothing added,
        # nothing dropped.
        manifest_ids = _read_manifest_ids(base_dir)
        if manifest_ids is not None:
            if set(full_ids) == manifest_ids:
                print(f"Verify: push set == rail manifest "
                      f"({len(full_ids)} IDs, exact match)")
            else:
                missing = len(manifest_ids - set(full_ids))
                extra = len(set(full_ids) - manifest_ids)
                print(f"Verify WARNING: push set differs from rail manifest "
                      f"(missing={missing}, extra={extra})")
        # counters-only ids ride along as full anime rows (counters live there)
        only_counters = [i for i in counter_ids if i not in set(full_ids)]
        if write_only and len(full_ids) + len(only_counters) > RAIL_ROW_CAP:
            print(f"ERROR: write-only cap exceeded "
                  f"({len(full_ids) + len(only_counters)} > {RAIL_ROW_CAP}): "
                  f"refusing run that would rewrite the catalog. "
                  f"Daily runs must stay rails-only.", file=sys.stderr)
            sys.exit(1)
        import time as _time
        sync_started = _time.monotonic()
        total_stats = {"anime": 0, "related": 0}
        progress = load_progress(data_dir)
        if full_ids:
            print(f"Delta: {len(full_ids)} full + {len(only_counters)} counters-only"
                  f"{' (write-only: zero remote reads)' if write_only else ''}...")
            s = sync_anime_ids(db_path, full_ids, with_related=True,
                               data_dir=data_dir, max_minutes=max_minutes,
                               progress=progress, tag="delta-full",
                               write_only=write_only)
            total_stats["anime"] += s["anime"]
            total_stats["related"] += s["related"]
        if only_counters:
            s = sync_anime_ids(db_path, only_counters, with_related=False,
                               data_dir=data_dir, max_minutes=max_minutes,
                               progress=progress, tag="delta-counters",
                               write_only=write_only)
            total_stats["anime"] += s["anime"]
        sync_secs = _time.monotonic() - sync_started
        incomplete = bool(progress.get("incomplete"))
        print(f"Delta done: {total_stats} (monthly write budget: 10M rows)")
        # SYNC SUMMARY block: the workflow prints this verbatim so every run
        # shows what happened without opening Turso's dashboard.
        print("=" * 60)
        print(f"SYNC SUMMARY: mode={'write-only' if write_only else 'delta'} "
              f"ids={len(full_ids) + len(only_counters)} "
              f"anime_rows={total_stats['anime']} related_rows={total_stats['related']} "
              f"secs={sync_secs:.0f} incomplete={incomplete} cap={RAIL_ROW_CAP}")
        print("=" * 60)
        _save_sync_report(base_dir, write_only, full_ids, only_counters,
                          total_stats, sync_secs, incomplete)
    finally:
        lconn.close()


if __name__ == "__main__":
    main()
