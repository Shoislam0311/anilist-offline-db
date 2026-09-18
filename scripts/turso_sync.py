#!/usr/bin/env python3
"""
AniList Offline Database - Turso Sync (delta-disciplined for the free tier).

Free caps: 5GB storage / 500M rows read / 10M rows written per month.
A full re-seed writes ~1-2M rows, so it fits ONCE (bootstrap), but routine
syncs push only touched IDs:
  - data/touched_full.json     -> full anime rows + all scoped related rows
  - data/touched_counters.json -> full anime rows (counters live there too)

Writes per daily run: ~15-30k. Per weekly incremental: similar. Far under budget.

Schema is created remotely on first run (DB_SCHEMA, minus FTS virtual tables
which are rebuilt locally only; Turso reads use LIKE on indexed columns).

Env required: TURSO_URL, TURSO_AUTH_TOKEN  (exit 2 if missing)
Usage:
  python3 scripts/turso_sync.py            # delta (default)
  python3 scripts/turso_sync.py --bootstrap  # full copy (first run only)
"""

import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

CHUNK = 2000       # ids per scoped pass (small rows: few round trips)
BATCH = 1000       # rows per executemany for small tables
ANIME_BATCH = 25   # full raw_json rows (~330KB each): keep HTTP bodies ~8MB
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


def ensure_schema(rconn):
    from db_utils import DB_SCHEMA, SCOPED_INDEXES
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


def copy_where(lconn, rconn, table, where, params, batch_size=BATCH):
    cols = local_columns(lconn, table)
    if not cols or not remote_has_table(rconn, table):
        return 0
    colnames = ", ".join([f'"{c}"' for c in cols])
    rows = lconn.execute(f"SELECT {colnames} FROM {table} WHERE {where}", _t(params)).fetchall()
    return replace_rows(rconn, table, cols, [tuple(r) for r in rows], batch_size=batch_size)


def _chunks(ids, n=None):
    ids = [i for i in ids if i is not None]
    n = n or CHUNK
    return [ids[i:i + n] for i in range(0, len(ids), n)]


def _sync_shared_for_chunk(lconn, rconn, chunk):
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
            count += copy_where(lconn, rconn, table, f"id IN ({q2})", c2)
    try:
        tag_names = [r[0] for r in lconn.execute(
            f"SELECT DISTINCT tag_name FROM anime_tags WHERE anime_id IN ({q})", _t(chunk)).fetchall()]
        for c2 in _chunks(tag_names):
            q2 = ", ".join(["?"] * len(c2))
            count += copy_where(lconn, rconn, "tags", f"name IN ({q2})", c2)
        va_ids = [r[0] for r in lconn.execute(
            f"""SELECT DISTINCT voice_actor_id FROM character_voice_actors
                WHERE anime_id IN ({q})""", _t(chunk)).fetchall()]
        for c2 in _chunks(va_ids):
            q2 = ", ".join(["?"] * len(c2))
            count += copy_where(lconn, rconn, "voice_actors", f"id IN ({q2})", c2)
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


WORKERS = 6  # parallel chunk lanes; disjoint ID sets, so no write conflicts

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


def _worker_anime_chunk(idx, chunk, db_path):
    """Sync anime rows for one disjoint chunk. Own connections; returns result tuple."""
    lconn = _open_local(db_path)
    rconn = _open_remote()
    try:
        q = ", ".join(["?"] * len(chunk))
        n = copy_where(lconn, rconn, "anime", f"id IN ({q})", chunk,
                       batch_size=ANIME_BATCH)
        return ("anime", idx, n, 0, None)
    except Exception as e:
        return ("anime", idx, 0, 0, e)
    finally:
        _close_quietly(lconn)
        _close_quietly(rconn)


def _worker_related_chunk(idx, chunk, db_path):
    """Sync shared entities + scoped rows for one disjoint chunk."""
    lconn = _open_local(db_path)
    rconn = _open_remote()
    anime_n = related_n = 0
    try:
        q = ", ".join(["?"] * len(chunk))
        related_n += _sync_shared_for_chunk(lconn, rconn, chunk)
        for table in SCOPED_TABLES:
            if not remote_has_table(rconn, table):
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
                   data_dir=None, max_minutes=None, progress=None, tag="sync"):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
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
        worker = _worker_anime_chunk if kind == "anime" else _worker_related_chunk
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


def main():
    from db_utils import connect_db, get_db_path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")
    db_path = get_db_path(data_dir)
    if not os.path.exists(db_path):
        print(f"ERROR: local DB not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    bootstrap = "--bootstrap" in sys.argv
    max_minutes = TIME_BUDGET_DEFAULT
    for i, a in enumerate(sys.argv[1:]):
        if a.startswith("--max-minutes"):
            if "=" in a:
                max_minutes = int(a.split("=", 1)[1])
            elif i + 2 < len(sys.argv):
                max_minutes = int(sys.argv[i + 2])
    rconn = connect_remote()
    created = ensure_schema(rconn)

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
        # counters-only ids ride along as full anime rows (counters live there)
        only_counters = [i for i in counter_ids if i not in set(full_ids)]
        total_stats = {"anime": 0, "related": 0}
        progress = load_progress(data_dir)
        if full_ids:
            print(f"Delta: {len(full_ids)} full + {len(only_counters)} counters-only...")
            s = sync_anime_ids(db_path, full_ids, with_related=True,
                               data_dir=data_dir, max_minutes=max_minutes,
                               progress=progress, tag="delta-full")
            total_stats["anime"] += s["anime"]
            total_stats["related"] += s["related"]
        if only_counters:
            s = sync_anime_ids(db_path, only_counters, with_related=False,
                               data_dir=data_dir, max_minutes=max_minutes,
                               progress=progress, tag="delta-counters")
            total_stats["anime"] += s["anime"]
        print(f"Delta done: {total_stats} (monthly write budget: 10M rows)")
    finally:
        lconn.close()


if __name__ == "__main__":
    main()
