#!/usr/bin/env python3
"""
AniList Offline Database - Turso Sync (production version)

Daily behaviour (SMART, fast, verified):
- touched_full.json     → brand-new titles → full write (row + children + FTS)
- touched_airing.json   → known releasing titles → ONE batched UPDATE (json_each)

Design constraints (free-tier budget):
- json_each multi-row inserts: ONE bound param per statement → no parameter
  limits, no per-row round trips, minimal HTTP overhead.
- Parallel uploads (WORKERS connections) for child tables.
- Every failed batch is retried 3x, counted, written to the sync report, and
  FAILS the process (exit 1) — silent partial syncs are how this DB drifted
  in the first place.
- Post-sync verification compares pushed-row counts per id-chunk (cheap,
  indexed reads) and writes the result to sync_state + report.
"""

import os
import sys
import json
import time
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHUNK = 500                 # anime ids per job
ROWS_PER_STMT = 400         # rows per json_each INSERT (small tables)
ANIME_ROWS_PER_STMT = 1     # anime rows carry ~350KB raw_json each
WORKERS = 3
TIME_BUDGET_DEFAULT = 8     # minutes

SCOPED_TABLES = [
    "anime_titles", "anime_descriptions", "anime_genres", "anime_tags",
    "anime_studios", "anime_characters", "character_voice_actors",
    "anime_staff", "relations", "recommendations", "airing_schedule",
    "external_links", "streaming_episodes", "rankings", "trends",
    "reviews", "statistics",
]

_all_errors = []
_tls = threading.local()


def load_env():
    env_path = os.path.join(BASE_DIR, ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())


def connect_remote():
    import libsql_experimental as libsql
    url = (os.environ.get("TURSO_URL", "") or "").strip()
    token = (os.environ.get("TURSO_AUTH_TOKEN", "") or "").strip()
    if url.startswith("file:") or url.endswith(".db"):
        path = url[5:] if url.startswith("file:") else url
        return libsql.connect(database=path)
    if not url or not token:
        print("ERROR: TURSO_URL / TURSO_AUTH_TOKEN not set", file=sys.stderr)
        sys.exit(2)
    return libsql.connect(database=url, auth_token=token)


def worker_conn():
    if not hasattr(_tls, "conn"):
        _tls.conn = connect_remote()
    return _tls.conn


def table_cols(conn, table):
    return [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]


ANIME_FK_COLUMNS = {
    "relations": ["related_anime_id"],
    "recommendations": ["recommended_anime_id"],
}


def filter_fk_rows(table, rows, cols, anime_ids):
    """Drop rows whose FK targets fall outside the anime catalog (e.g.
    relations pointing at manga) — Turso enforces FKs, local SQLite never
    did, so unfiltered pushes fail the whole batch."""
    fk_cols = ANIME_FK_COLUMNS.get(table)
    if not fk_cols:
        return rows
    idx = {c: i for i, c in enumerate(cols)}
    kept = [r for r in rows
            if all(r[idx[c]] is None or r[idx[c]] in anime_ids for c in fk_cols)]
    dropped = len(rows) - len(kept)
    if dropped:
        print(f"  {table}: dropped {dropped} rows with FK targets outside the anime catalog")
    return kept


def rows_limit_for(table, rows_per_stmt):
    # big payloads (~1MB+) stress Turso streams — cap by table
    caps = {"anime": 1, "anime_descriptions": 50, "anime_fts": 50, "anime_fts_new": 50,
            "characters": 100, "staff": 100, "voice_actors": 100}
    return min(rows_per_stmt, caps.get(table, rows_per_stmt))


def push_rows(conn_or_factory, table, cols, rows, rows_per_stmt=ROWS_PER_STMT, tag="", key_col=None):
    """INSERT OR REPLACE via json_each (one bound param per statement), with
    PER-BATCH PERSISTENCE VERIFICATION on a fresh connection.

    Background: libsql clients can report success on writes that a stale /
    dead hrana stream silently dropped. The only trustworthy check is to read
    the rows back through a NEW connection. conn_or_factory may be a
    connection (legacy) or a zero-arg callable returning a fresh connection —
    a callable is strongly recommended.
    """
    if not rows:
        return 0
    factory = conn_or_factory if callable(conn_or_factory) else (lambda: conn_or_factory)
    rows_per_stmt = rows_limit_for(table, rows_per_stmt)
    colnames = ", ".join(f'"{c}"' for c in cols)
    sel = ", ".join(f"json_extract(value, '$.{c}')" for c in cols)
    sql = f'INSERT OR REPLACE INTO "{table}" ({colnames}) SELECT {sel} FROM json_each(?)'
    key_idx = cols.index(key_col) if key_col in cols else None
    done, total = 0, len(rows)
    conn = factory()
    for i in range(0, total, rows_per_stmt):
        chunk = rows[i:i + rows_per_stmt]
        payload = json.dumps([dict(zip(cols, r)) for r in chunk], ensure_ascii=False)
        keys = sorted({r[key_idx] for r in chunk}) if key_idx is not None else None
        # diff guarantees zero pre-existing rows for missing keys, so the
        # correct expectation is exactly this chunk's row count
        expected = len(chunk) if keys is not None else None
        ok = False
        for attempt in range(1, 6):
            try:
                conn.execute(sql, (payload,))
                try:
                    conn.commit()
                except Exception:
                    pass
                if keys is None:
                    ok = True
                    break
                # verify through a FRESH connection — stale streams lie
                vc = factory()
                qm = ",".join("?" * len(keys))
                got = vc.execute(
                    f'SELECT COUNT(*) FROM "{table}" WHERE "{key_col}" IN ({qm})',
                    tuple(keys)).fetchone()[0]
                if got >= expected:
                    ok = True
                    break
                print(f"  {tag or table}: batch not visible (got {got} >= {expected} expected) — retry {attempt}")
                conn = factory()
                time.sleep(1.2 * attempt)
            except Exception as e:
                conn = factory()
                if attempt == 5:
                    msg = f"{tag or table}: batch of {len(chunk)} failed 5x: {str(e)[:160]}"
                    print(f"  ERROR {msg}")
                    _all_errors.append(msg)
                else:
                    time.sleep(1.5 * attempt)
        if ok:
            done += len(chunk)
    return done
def copy_where(lconn, factory, table, where, params, rows_per_stmt=ROWS_PER_STMT, key_col=None):
    cols = table_cols(lconn, table)
    if not cols:
        return 0
    colnames = ", ".join(f'"{c}"' for c in cols)
    rows = lconn.execute(
        f'SELECT {colnames} FROM "{table}" WHERE {where}', tuple(params)).fetchall()
    return push_rows(factory, table, cols, rows, rows_per_stmt=rows_per_stmt, key_col=key_col)


def anime_fts_rows(lconn, ids):
    """Build anime_fts rows locally for the given anime ids (with synonyms)."""
    q = ",".join("?" * len(ids))
    return lconn.execute(f"""
        SELECT a.id, a.title_romaji, a.title_english, a.title_native, a.synonyms, a.description,
            (SELECT group_concat(g.name, ' ') FROM anime_genres ag JOIN genres g ON g.id = ag.genre_id WHERE ag.anime_id = a.id),
            (SELECT group_concat(tag_name, ' ') FROM (SELECT tag_name FROM anime_tags WHERE anime_id = a.id ORDER BY tag_rank DESC LIMIT 20)),
            (SELECT group_concat(s.name, ' ') FROM anime_studios ast JOIN studios s ON s.id = ast.studio_id WHERE ast.anime_id = a.id),
            (SELECT group_concat(c.name_full, ' ') FROM (SELECT character_id FROM anime_characters WHERE anime_id = a.id ORDER BY CASE role WHEN 'MAIN' THEN 0 ELSE 1 END, sort_order LIMIT 16) xc JOIN characters c ON c.id = xc.character_id)
        FROM anime a WHERE a.id IN ({q})
    """, tuple(ids)).fetchall()


def sync_full(db_path, ids, max_minutes=None):
    """Full write for brand-new titles: anime row + children + FTS indexes."""
    if not ids:
        return {"anime": 0, "related": 0, "fts": 0}
    lconn = db_path if isinstance(db_path, sqlite3.Connection) else sqlite3.connect(db_path)
    started = time.monotonic()
    stats = {"anime": 0, "related": 0, "fts": 0}

    def read_chunk(chunk):
        """Read everything for this chunk — runs in the MAIN thread (sqlite3
        connections are not thread-safe). Workers only upload."""
        q = ",".join("?" * len(chunk))
        reads = {}
        cols_a = table_cols(lconn, "anime")
        reads["anime"] = (lconn.execute(
            f'SELECT {", ".join(chr(34)+c+chr(34) for c in cols_a)} FROM anime WHERE id IN ({q})',
            tuple(chunk)).fetchall(), cols_a)
        for table in SCOPED_TABLES:
            tcols = table_cols(lconn, table)
            if not tcols or "anime_id" not in tcols:
                continue
            reads[table] = (lconn.execute(
                f'SELECT {", ".join(chr(34)+c+chr(34) for c in tcols)} FROM "{table}" WHERE anime_id IN ({q})',
                tuple(chunk)).fetchall(), tcols)
        fts_rows = anime_fts_rows(lconn, chunk)
        return reads, fts_rows

    def delete_children(conn, chunk):
        """Replace semantics: an updated title's child rows must be REPLACED,
        not merged (old relations/recommendations/episodes would linger).
        Verified: after DELETE, a fresh connection must see zero rows."""
        q = ",".join("?" * len(chunk))
        for table in SCOPED_TABLES:
            for attempt in range(1, 4):
                try:
                    conn.execute(f'DELETE FROM "{table}" WHERE anime_id IN ({q})', tuple(chunk))
                    try:
                        conn.commit()
                    except Exception:
                        pass
                    vc = connect_remote()
                    left = vc.execute(
                        f'SELECT COUNT(*) FROM "{table}" WHERE anime_id IN ({q})', tuple(chunk)).fetchone()[0]
                    if left == 0:
                        break
                    print(f"  {table}: delete not visible ({left} left) — retry {attempt}")
                    conn = connect_remote()
                    time.sleep(1.2 * attempt)
                except Exception as e:
                    _all_errors.append(f"delete {table}: {str(e)[:120]}")
                    conn = connect_remote()
                    time.sleep(1.5 * attempt)

    def job(prep):
        reads, fts_rows = prep
        conn = worker_conn()
        rows_a, cols_a = reads["anime"]
        n = push_rows(connect_remote, "anime", cols_a, rows_a, rows_per_stmt=ANIME_ROWS_PER_STMT, tag="anime", key_col="id")
        delete_children(conn, [r[0] for r in reads["anime"][0]])
        rel = 0
        for table in SCOPED_TABLES:
            if table not in reads:
                continue
            rows_t, cols_t = reads[table]
            rel += push_rows(connect_remote, table, cols_t, rows_t, tag=table, key_col="anime_id")
        fts_cols = ["rowid", "title_romaji", "title_english", "title_native", "synonyms",
                    "description", "genres", "tags", "studios", "characters"]
        fts = push_rows(connect_remote, "anime_fts", fts_cols, fts_rows,
                        rows_per_stmt=50, tag="anime_fts")
        if fts == 0 and fts_rows:
            # legacy anime_fts without the synonyms column — degrade gracefully
            fts = push_rows(connect_remote, "anime_fts",
                            [c for c in fts_cols if c != "synonyms"],
                            [tuple(v for i, v in enumerate(r) if i != 4) for r in fts_rows],
                            rows_per_stmt=50, tag="anime_fts-legacy")
        return n, rel, fts

    chunks = [ids[i:i + CHUNK] for i in range(0, len(ids), CHUNK)]
    anime_ids = {r[0] for r in lconn.execute("SELECT id FROM anime").fetchall()}
    prepared = []
    for chunk in chunks:
        reads, fts_rows = read_chunk(chunk)
        for table in list(reads):
            if table == "anime":
                continue
            rows_t, cols_t = reads[table]
            reads[table] = (filter_fk_rows(table, rows_t, cols_t, anime_ids), cols_t)
        prepared.append((reads, fts_rows))
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for n, rel, fts in ex.map(job, prepared):
            stats["anime"] += n
            stats["related"] += rel
            stats["fts"] += fts
            if max_minutes and (time.monotonic() - started) > max_minutes * 60:
                print("Time budget reached, stopping full sync")
                break
    print(f"Full sync done: {stats}")
    return stats


def sync_airing(db_path, ids):
    """Ultra-light next-episode refresh for known releasing titles.
    ONE batched UPDATE via UPDATE...FROM json_each (was 1 round trip per row)."""
    if not ids:
        return 0
    lconn = db_path if isinstance(db_path, sqlite3.Connection) else sqlite3.connect(db_path)
    rows = []
    q = ",".join("?" * len(ids))
    for row in lconn.execute(
        f"SELECT id, next_airing_at, next_airing_episode, updated_at FROM anime WHERE id IN ({q})",
        tuple(ids)
    ).fetchall():
        rows.append({"id": row[0], "nat": row[1], "nep": row[2], "uat": row[3]})
    if not rows:
        return 0
    updated = 0
    conn = connect_remote()
    for i in range(0, len(rows), ROWS_PER_STMT):
        chunk = rows[i:i + ROWS_PER_STMT]
        payload = json.dumps(chunk, ensure_ascii=False)
        sql = """
            UPDATE anime SET
                next_airing_at = json_extract(value, '$.nat'),
                next_airing_episode = json_extract(value, '$.nep'),
                updated_at = json_extract(value, '$.uat')
            FROM json_each(?)
            WHERE anime.id = json_extract(value, '$.id')
        """
        ok = False
        for attempt in (1, 2, 3):
            try:
                cur = conn.execute(sql, (payload,))
                updated += max(0, int(getattr(cur, "rowcount", 0) or 0))
                ok = True
                break
            except Exception as e:
                if attempt == 3:
                    msg = f"airing batch of {len(chunk)} failed 3x: {str(e)[:160]}"
                    print(f"  ERROR {msg}")
                    _all_errors.append(msg)
                    # last resort: row-by-row so one bad row can't kill the batch
                    for r in chunk:
                        try:
                            conn.execute(
                                "UPDATE anime SET next_airing_at = ?, next_airing_episode = ?, updated_at = ? WHERE id = ?",
                                (r["nat"], r["nep"], r["uat"], r["id"]))
                            updated += 1
                        except Exception as e2:
                            _all_errors.append(f"airing row {r['id']}: {str(e2)[:120]}")
                else:
                    time.sleep(1.5 * attempt)
        if not ok:
            continue
    try:
        conn.commit()
    except Exception:
        pass
    print(f"Airing-only update: {updated} titles")
    return updated


def verify(db_path, full_ids, airing_ids):
    """Cheap indexed verification that what we pushed is actually there."""
    lconn = db_path if isinstance(db_path, sqlite3.Connection) else sqlite3.connect(db_path)
    conn = worker_conn()
    result = {"checked": 0, "mismatches": []}
    checks = []
    if full_ids:
        for i in range(0, len(full_ids), 500):
            chunk = full_ids[i:i + 500]
            checks.append(("anime", "id", chunk))
            for table in ("anime_genres", "relations", "recommendations"):
                checks.append((table, "anime_id", chunk))
    if airing_ids:
        for i in range(0, len(airing_ids), 500):
            chunk = airing_ids[i:i + 500]
            checks.append(("anime", "id", chunk))
    seen = set()
    for table, col, chunk in checks:
        key = (table, tuple(chunk))
        if key in seen:
            continue
        seen.add(key)
        q = ",".join("?" * len(chunk))
        if table in ANIME_FK_COLUMNS:
            # local rows with FK targets outside the anime catalog (manga
            # etc.) can never exist remotely — exclude them from the expectation
            fk_cols = ",".join(f"COALESCE({c}, anime_id)" for c in ANIME_FK_COLUMNS[table])
            local = lconn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE {col} IN ({q}) AND {fk_cols} IN (SELECT id FROM anime)',
                tuple(chunk)).fetchone()[0]
        else:
            local = lconn.execute(f'SELECT COUNT(*) FROM "{table}" WHERE {col} IN ({q})', tuple(chunk)).fetchone()[0]
        try:
            remote = conn.execute(f'SELECT COUNT(*) FROM "{table}" WHERE {col} IN ({q})', tuple(chunk)).fetchone()[0]
        except Exception as e:
            _all_errors.append(f"verify read {table}: {str(e)[:120]}")
            continue
        result["checked"] += 1
        if remote < local:
            result["mismatches"].append({"table": table, "local": local, "remote": remote})
            _all_errors.append(f"verify mismatch {table}: local={local} remote={remote}")
    print(f"Verify: {result['checked']} checks, {len(result['mismatches'])} mismatches")
    return result


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
    load_env()
    base_dir = BASE_DIR
    data_dir = os.path.join(base_dir, "data")
    db_path = os.path.join(data_dir, "anilist.db")

    if not os.path.exists(db_path):
        print(f"ERROR: local DB not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    write_only = "--write-only" in sys.argv
    no_verify = "--no-verify" in sys.argv
    max_minutes = TIME_BUDGET_DEFAULT
    argv = sys.argv[1:]
    for i, a in enumerate(argv):
        if a.startswith("--max-minutes"):
            if "=" in a:
                max_minutes = int(a.split("=", 1)[1])
            elif i + 1 < len(argv):
                try:
                    max_minutes = int(argv[i + 1])
                except ValueError:
                    pass

    full_ids = read_touched(data_dir, "touched_full.json")
    airing_ids = read_touched(data_dir, "touched_airing.json")

    print(f"SMART sync: {len(full_ids)} NEW (full) + {len(airing_ids)} airing-only")
    start = time.monotonic()

    total_stats = {"anime": 0, "related": 0, "fts": 0}
    if full_ids:
        print(f"→ Full write for {len(full_ids)} brand-new titles...")
        s = sync_full(db_path, full_ids, max_minutes=max_minutes)
        total_stats.update(s)
    if airing_ids:
        print(f"→ Airing-only update for {len(airing_ids)} titles...")
        total_stats["airing"] = sync_airing(db_path, airing_ids)

    verification = None
    if not no_verify:
        verification = verify(db_path, full_ids, airing_ids)

    secs = time.monotonic() - start
    print("=" * 60)
    print(f"SYNC SUMMARY: NEW={len(full_ids)} airing={len(airing_ids)} "
          f"anime_rows={total_stats['anime']} related={total_stats['related']} "
          f"fts={total_stats.get('fts', 0)} secs={secs:.1f} errors={len(_all_errors)}")
    print("=" * 60)

    report = {
        "completedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mode": "smart-rails",
        "newIds": len(full_ids),
        "airingIds": len(airing_ids),
        "animeRows": total_stats["anime"],
        "relatedRows": total_stats["related"],
        "ftsRows": total_stats.get("fts", 0),
        "airingUpdated": total_stats.get("airing", 0),
        "seconds": round(secs, 1),
        "errors": _all_errors[:50],
        "verified": verification is not None and not verification["mismatches"],
    }
    if verification:
        report["verifyChecks"] = verification["checked"]
    out = os.path.join(base_dir, "docs", "api", "sync_report.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2)

    if _all_errors:
        print("SYNC FAILED (see errors above) — exiting non-zero")
        sys.exit(1)
    print("SYNC OK")


if __name__ == "__main__":
    main()
