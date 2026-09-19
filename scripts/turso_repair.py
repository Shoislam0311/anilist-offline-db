#!/usr/bin/env python3
"""
Turso repair/backfill — brings the remote Turso DB to full parity with the
local SQLite DB. Idempotent: diffs per table by anime_id (and rowid for FTS),
so re-running only pushes what is missing.

Why this exists: the original bootstrap silently dropped failed batches
(swallowed exceptions), leaving anime_genres empty, relations/studios/
recommendations/character_voice_actors partial, and ~156 anime missing —
while sync_state still claimed bootstrap_complete=1.

What it does:
  1. Push missing `anime` rows (with raw_json) + their child rows.
  2. Diff every scoped child table by anime_id and push missing anime's rows.
  3. Rebuild anime_fts (adds `synonyms` column for search quality) and create
     staff_fts; fill missing characters_fts rowids.
  4. Verify remote vs local counts per table; update sync_state; exit 1 on
     any mismatch or failed batch (never silent).

Usage:
  python scripts/turso_repair.py                 # full diff + backfill + FTS + verify
  python scripts/turso_repair.py --verify-only   # counts only
  python scripts/turso_repair.py --skip-fts      # data only
Requires: TURSO_URL / TURSO_AUTH_TOKEN (or .env in repo root) and data/anilist.db.
"""

import os
import sys
import json
import time
import sqlite3
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "anilist.db")

SCOPED_TABLES = [
    "anime_titles", "anime_descriptions", "anime_genres", "anime_tags",
    "anime_studios", "anime_characters", "character_voice_actors",
    "anime_staff", "relations", "recommendations", "airing_schedule",
    "external_links", "streaming_episodes", "rankings", "trends",
    "reviews", "statistics",
]

# dimension tables referenced by FKs — must be pushed BEFORE scoped children
DIMENSION_TABLES = ["genres", "tags", "studios", "characters", "staff", "voice_actors"]
# child columns that FK-reference anime(id); rows pointing at manga (not in
# an anime-only DB) must be dropped or Turso rejects the batch
ANIME_FK_COLUMNS = {
    "relations": ["related_anime_id"],
    "recommendations": ["recommended_anime_id"],
}


def filter_fk_rows(lconn, table, rows, cols, anime_ids):
    fk_cols = ANIME_FK_COLUMNS.get(table)
    if not fk_cols:
        return rows
    idx = {c: i for i, c in enumerate(cols)}
    keep = []
    for r in rows:
        if all(r[idx[c]] is None or r[idx[c]] in anime_ids for c in fk_cols):
            keep.append(r)
    dropped = len(rows) - len(keep)
    if dropped:
        print(f"  {table}: dropped {dropped} rows with FK targets outside the anime catalog (manga etc.)")
    return keep

ROWS_PER_STMT = 400          # rows per json_each INSERT (small tables)
ANIME_ROWS_PER_STMT = 1      # anime rows carry ~350KB raw_json each
WORKERS = int(os.environ.get('REPAIR_WORKERS', '1'))  # concurrent writers get silently dropped by Turso — keep 1

_all_errors = []


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


def new_remote():
    # one connection per worker thread
    return connect_remote()


def table_cols(conn, table):
    return [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]


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
def fetch_ids(rconn, sql):
    try:
        return {r[0] for r in rconn.execute(sql).fetchall()}
    except Exception as e:
        _all_errors.append(f"read failed ({sql[:60]}): {str(e)[:120]}")
        return set()


def diff_anime(lconn, rconn, stats):
    t0 = time.monotonic()
    local_ids = {r[0] for r in lconn.execute("SELECT id FROM anime").fetchall()}
    remote_ids = fetch_ids(rconn, "SELECT id FROM anime")
    missing = sorted(local_ids - remote_ids)
    print(f"anime: local={len(local_ids)} remote={len(remote_ids)} missing={len(missing)}")
    if not missing:
        stats["anime"] = 0
        return
    cols = table_cols(lconn, "anime")
    colsel = ", ".join(f'"{c}"' for c in cols)
    pushed = 0
    for i in range(0, len(missing), 500):
        chunk = missing[i:i + 500]
        q = ",".join("?" * len(chunk))
        rows = lconn.execute(
            f"SELECT {colsel} FROM anime WHERE id IN ({q})",
            tuple(chunk)).fetchall()
        pushed += push_rows(lambda: new_remote(), "anime", cols, rows, rows_per_stmt=ANIME_ROWS_PER_STMT, tag="anime", key_col="id")
        # children for the new anime (all scoped tables, these ids)
        for table in SCOPED_TABLES:
            tcols = table_cols(lconn, table)
            if not tcols or "anime_id" not in tcols:
                continue
            crows = lconn.execute(
                f'SELECT {", ".join(chr(34)+c+chr(34) for c in tcols)} FROM "{table}" WHERE anime_id IN ({q})',
                tuple(chunk)).fetchall()
            crows = filter_fk_rows(lconn, table, crows, tcols, local_ids)
            push_rows(lambda: new_remote(), table, tcols, crows, tag=f"{table}+new", key_col="anime_id")
    stats["anime"] = pushed
    print(f"  pushed {pushed} anime rows in {time.monotonic() - t0:.1f}s")


def diff_dimensions(lconn, rconn, stats):
    """Push dimension rows missing remotely (FK parents for scoped tables)."""
    t0 = time.monotonic()
    total = 0
    for table in DIMENSION_TABLES:
        tcols = table_cols(lconn, table)
        if not tcols or "id" not in tcols:
            continue
        local_ids = {r[0] for r in lconn.execute(f'SELECT id FROM "{table}"').fetchall()}
        remote_ids = fetch_ids(rconn, f'SELECT id FROM "{table}"')
        missing = sorted(local_ids - remote_ids)
        stats[table] = {"missing": len(missing)}
        print(f"{table}: local={len(local_ids)} remote={len(remote_ids)} missing={len(missing)}")
        for i in range(0, len(missing), 500):
            chunk = missing[i:i + 500]
            q = ",".join("?" * len(chunk))
            rows = lconn.execute(
                f'SELECT {", ".join(chr(34)+c+chr(34) for c in tcols)} FROM "{table}" WHERE id IN ({q})',
                tuple(chunk)).fetchall()
            total += push_rows(lambda: new_remote(), table, tcols, rows, rows_per_stmt=200, tag=table, key_col="id")
    print(f"dimension rows pushed: {total} in {time.monotonic() - t0:.1f}s")
    stats["dimensionRowsPushed"] = total


def diff_children(lconn, rconn, stats):
    t0 = time.monotonic()
    jobs = []
    for table in SCOPED_TABLES:
        tcols = table_cols(lconn, table)
        if not tcols or "anime_id" not in tcols:
            continue
        local_ids = {r[0] for r in lconn.execute(f'SELECT DISTINCT anime_id FROM "{table}"').fetchall()}
        remote_ids = fetch_ids(rconn, f'SELECT DISTINCT anime_id FROM "{table}"')
        missing = sorted(local_ids - remote_ids)
        stats[table] = {"missingIds": len(missing), "localIds": len(local_ids), "remoteIds": len(remote_ids)}
        print(f"{table}: local_anime={len(local_ids)} remote_anime={len(remote_ids)} missing={len(missing)}")
        for i in range(0, len(missing), 500):
            jobs.append((table, tcols, missing[i:i + 500]))
    if not jobs:
        return

    import threading
    _tls = threading.local()

    def _tls_conn():
        if not hasattr(_tls, "conn"):
            _tls.conn = new_remote()
        return _tls.conn

    # pre-read ALL job rows in the MAIN thread (sqlite3 connections are
    # not thread-safe); workers only upload.
    anime_ids = {r[0] for r in lconn.execute("SELECT id FROM anime").fetchall()}
    read_jobs = []
    for table, tcols, ids in jobs:
        q = ",".join("?" * len(ids))
        rows = lconn.execute(
            f'SELECT {", ".join(chr(34)+c+chr(34) for c in tcols)} FROM "{table}" WHERE anime_id IN ({q})',
            tuple(ids)).fetchall()
        rows = filter_fk_rows(lconn, table, rows, tcols, anime_ids)
        if rows:
            read_jobs.append((table, tcols, rows))

    def run_job(job):
        table, tcols, rows = job
        conn = _tls_conn()
        return push_rows(new_remote, table, tcols, rows, tag=table, key_col="anime_id")

    total = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for n in ex.map(run_job, read_jobs):
            total += n
    stats["childRowsPushed"] = total
    print(f"child rows pushed: {total} in {time.monotonic() - t0:.1f}s")


def rebuild_fts(lconn, rconn, stats):
    t0 = time.monotonic()
    # ---- anime_fts: rebuild with a `synonyms` column (search-quality fix) ----
    print("rebuilding anime_fts (with synonyms)...")
    rconn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS anime_fts_new USING fts5(
            title_romaji, title_english, title_native, synonyms, description,
            genres, tags, studios, characters, tokenize='porter unicode61')
    """)
    rows = lconn.execute("""
        SELECT a.id, a.title_romaji, a.title_english, a.title_native, a.synonyms, a.description,
            (SELECT group_concat(g.name, ' ') FROM anime_genres ag JOIN genres g ON g.id = ag.genre_id WHERE ag.anime_id = a.id),
            (SELECT group_concat(tag_name, ' ') FROM (SELECT tag_name FROM anime_tags WHERE anime_id = a.id ORDER BY tag_rank DESC LIMIT 20)),
            (SELECT group_concat(s.name, ' ') FROM anime_studios ast JOIN studios s ON s.id = ast.studio_id WHERE ast.anime_id = a.id),
            (SELECT group_concat(c.name_full, ' ') FROM (SELECT character_id FROM anime_characters WHERE anime_id = a.id ORDER BY CASE role WHEN 'MAIN' THEN 0 ELSE 1 END, sort_order LIMIT 16) xc JOIN characters c ON c.id = xc.character_id)
        FROM anime a
    """).fetchall()
    n = push_rows(lambda: new_remote(), "anime_fts_new",
                  ["rowid", "title_romaji", "title_english", "title_native", "synonyms",
                   "description", "genres", "tags", "studios", "characters"],
                  rows, rows_per_stmt=50, tag="anime_fts_new")
    rconn.execute("DROP TABLE IF EXISTS anime_fts")
    rconn.execute("ALTER TABLE anime_fts_new RENAME TO anime_fts")
    try:
        rconn.commit()
    except Exception:
        pass
    stats["anime_fts_rows"] = n
    print(f"  anime_fts rebuilt: {n} rows in {time.monotonic() - t0:.1f}s")

    # ---- characters_fts: fill missing rowids ----
    t1 = time.monotonic()
    local_cids = {r[0] for r in lconn.execute("SELECT id FROM characters").fetchall()}
    remote_fids = fetch_ids(rconn, "SELECT rowid FROM characters_fts")
    missing = sorted(local_cids - remote_fids)
    print(f"characters_fts: local={len(local_cids)} remote={len(remote_fids)} missing={len(missing)}")
    if missing:
        rows = lconn.execute("""
            SELECT c.id, c.name_full, c.name_native, c.name_alternative, c.description
            FROM characters c ORDER BY c.id
        """).fetchall()
        by_id = {r[0]: r for r in rows}
        todo = [by_id[i] for i in missing if i in by_id]
        n = push_rows(lambda: new_remote(), "characters_fts",
                      ["rowid", "name_full", "name_native", "name_alternative", "description"],
                      todo, rows_per_stmt=200, tag="characters_fts")
        stats["characters_fts_rows"] = n
    print(f"  characters_fts filled in {time.monotonic() - t1:.1f}s")

    # ---- staff_fts: create + fill (new index for Staff search) ----
    t2 = time.monotonic()
    rconn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS staff_fts USING fts5(
            name_full, name_native, name_alternative, description, tokenize='porter unicode61')
    """)
    local_sids = {r[0] for r in lconn.execute("SELECT id FROM staff").fetchall()}
    remote_sids = fetch_ids(rconn, "SELECT rowid FROM staff_fts")
    missing = sorted(local_sids - remote_sids)
    print(f"staff_fts: local={len(local_sids)} remote={len(remote_sids)} missing={len(missing)}")
    if missing:
        rows = lconn.execute("""
            SELECT s.id, s.name_full, s.name_native, s.name_alternative, s.description
            FROM staff s ORDER BY s.id
        """).fetchall()
        by_id = {r[0]: r for r in rows}
        todo = [by_id[i] for i in missing if i in by_id]
        n = push_rows(lambda: new_remote(), "staff_fts",
                      ["rowid", "name_full", "name_native", "name_alternative", "description"],
                      todo, rows_per_stmt=200, tag="staff_fts")
        stats["staff_fts_rows"] = n
    try:
        rconn.commit()
    except Exception:
        pass
    print(f"  staff_fts filled in {time.monotonic() - t2:.1f}s")


NO_PK_TABLES = {
    "anime_titles": ["anime_id", "language", "title"],
    "anime_descriptions": ["anime_id", "language", "description"],
    "anime_genres": ["anime_id", "genre_id"],
    "anime_tags": ["anime_id", "tag_name"],
    "anime_studios": ["anime_id", "studio_id"],
    "anime_characters": ["edge_id"],
    "character_voice_actors": ["character_id", "voice_actor_id", "anime_id", "language"],
    "anime_staff": ["edge_id"],
}


LOGICAL_UNIQUE = {
    "relations": ["anime_id", "related_anime_id", "relation_type"],
    "recommendations": ["anime_id", "recommended_anime_id"],
    "airing_schedule": ["anime_id", "episode"],
    "rankings": ["anime_id", "rank_id"],
    "trends": ["anime_id", "date"],
    "reviews": ["id"],
    "external_links": ["id"],
    "streaming_episodes": ["id"],
}


def dedup_nopk_tables(lconn, rconn):
    """Tables without a primary key can accumulate duplicate rows if a
    retried batch lands twice. Server-side dedup, only when counts exceed."""
    all_dedup = dict(NO_PK_TABLES)
    all_dedup.update(LOGICAL_UNIQUE)
    for table, group_cols in all_dedup.items():
        try:
            r = rconn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            l = lconn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            gc = ", ".join(f'"{c}"' for c in group_cols)
            # always dedup logically-unique tables: duplicate pairs can exist
            # even when the total is below local (e.g. after a dropped DELETE)
            must = table in LOGICAL_UNIQUE or r > l
            if must:
                print(f"  dedup {table}: remote={r} local={l}")
                rconn.execute(
                    f'DELETE FROM "{table}" WHERE rowid NOT IN '
                    f'(SELECT MIN(rowid) FROM "{table}" GROUP BY {gc})')
                try:
                    rconn.commit()
                except Exception:
                    pass
        except Exception as e:
            _all_errors.append(f"dedup {table}: {str(e)[:120]}")


def rebuild_tables(lconn, rconn_factory, tables, stats):
    """DELETE all remote rows and re-push the complete local table.
    Needed when the remote holds PARTIAL per-anime rows (old bootstrap wrote
    fewer child rows per anime than the raw_json rebuild produces)."""
    t0 = time.monotonic()
    anime_ids = {r[0] for r in lconn.execute("SELECT id FROM anime").fetchall()}
    for table in tables:
        tcols = table_cols(lconn, table)
        if not tcols:
            continue
        t0t = time.monotonic()
        for attempt in range(1, 6):
            dc = rconn_factory()
            dc.execute(f'DELETE FROM "{table}"')
            try:
                dc.commit()
            except Exception:
                pass
            vc = rconn_factory()
            left = vc.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            if left == 0:
                break
            print(f"  {table}: DELETE not visible ({left} rows left) — retry {attempt}")
            time.sleep(1.5 * attempt)
        total_local = 0
        pushed = 0
        batch = 5000
        last = 0
        while True:
            rows = lconn.execute(
                f'SELECT {", ".join(chr(34)+c+chr(34) for c in tcols)} FROM "{table}" '
                f'ORDER BY rowid LIMIT {batch} OFFSET {last}').fetchall()
            if not rows:
                break
            # advance by the RAW count — filtering happens after pagination,
            # otherwise OFFSET drifts and the loop re-reads forever
            last += len(rows)
            rows = filter_fk_rows(lconn, table, rows, tcols, anime_ids)
            total_local += len(rows)
            pushed += push_rows(rconn_factory, table, tcols, rows,
                                tag=f"{table}#rebuild", key_col="anime_id" if "anime_id" in tcols else None)
        stats[table] = {"deleted": True, "pushed": pushed, "local": total_local}
        print(f"  {table}: rebuilt pushed={pushed} local={total_local} in {time.monotonic() - t0t:.0f}s")


def verify(lconn, rconn):
    print("\n== VERIFY (remote vs local counts) ==")
    tables = ["anime"] + DIMENSION_TABLES + SCOPED_TABLES
    bad = []
    for t in tables:
        try:
            r = rconn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        except Exception as e:
            bad.append((t, -1, "read-error"))
            print(f"  {t}: REMOTE READ FAILED {str(e)[:80]}")
            continue
        if t in ANIME_FK_COLUMNS:
            # local rows with FK targets outside the anime catalog (manga
            # etc.) legitimately cannot exist remotely — exclude them
            fk = ANIME_FK_COLUMNS[t]
            fk_where = " AND ".join(
                f"COALESCE({c}, anime_id) IN (SELECT id FROM anime)" for c in fk)
            l = lconn.execute(
                f'SELECT COUNT(*) FROM "{t}" WHERE anime_id IN (SELECT id FROM anime) AND {fk_where}').fetchone()[0]
        else:
            l = lconn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        flag = "OK " if r >= l else "MISMATCH"
        print(f"  [{flag}] {t}: remote={r} local={l}")
        if r < l:
            bad.append((t, r, l))
    # FTS sanity
    for fts, src in (("anime_fts", "anime"), ("characters_fts", "characters"), ("staff_fts", "staff")):
        try:
            r = rconn.execute(f'SELECT COUNT(*) FROM "{fts}"').fetchone()[0]
            l = lconn.execute(f'SELECT COUNT(*) FROM "{src}"').fetchone()[0]
            flag = "OK " if r >= l * 0.99 else "MISMATCH"
            print(f"  [{flag}] {fts}: remote={r} local_src={l}")
            if r < l * 0.99:
                bad.append((fts, r, l))
        except Exception as e:
            print(f"  [WARN ] {fts}: {str(e)[:80]}")
    return bad


def update_sync_state(rconn, lconn):
    total = lconn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
    now = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
    for k, v in (("bootstrap_complete", "1"), ("total_anime", str(total)), ("last_repair_at", now)):
        try:
            rconn.execute(
                "INSERT INTO sync_state (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (k, v))
        except Exception as e:
            try:
                rconn.execute("UPDATE sync_state SET value = ? WHERE key = ?", (v, k))
            except Exception:
                _all_errors.append(f"sync_state {k}: {str(e)[:100]}")
    try:
        rconn.commit()
    except Exception:
        pass
    print(f"sync_state updated: total_anime={total}")


def main():
    load_env()
    verify_only = "--verify-only" in sys.argv
    skip_fts = "--skip-fts" in sys.argv

    if not os.path.exists(DB_PATH):
        print(f"ERROR: local DB not found at {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    lconn = sqlite3.connect(DB_PATH)
    lconn.execute("PRAGMA query_only = 1")
    rconn = connect_remote()

    t0 = time.monotonic()
    stats = {}
    rebuild = None
    argv = sys.argv[1:]
    for i, a in enumerate(argv):
        if a == "--rebuild-tables":
            if i + 1 < len(argv) and not argv[i + 1].startswith("--"):
                rebuild = argv[i + 1]
            else:
                rebuild = ",".join(SCOPED_TABLES)
    if not verify_only:
        if rebuild:
            tables = [t for t in rebuild.split(",") if t in SCOPED_TABLES]
            print(f"REBUILD MODE for: {tables}")
            rebuild_tables(lconn, lambda: new_remote(), tables, stats)
        diff_dimensions(lconn, rconn, stats)
        diff_anime(lconn, rconn, stats)
        diff_children(lconn, rconn, stats)
        if not skip_fts:
            rebuild_fts(lconn, rconn, stats)

    if not verify_only:
        dedup_nopk_tables(lconn, rconn)
    bad = verify(lconn, rconn)
    if not verify_only:
        update_sync_state(rconn, lconn)

    secs = time.monotonic() - t0
    print("\n== REPAIR SUMMARY ==")
    print(json.dumps(stats, indent=2, default=str)[:2000])
    print(f"seconds={secs:.1f} errors={len(_all_errors)} mismatches={len(bad)}")
    if _all_errors:
        print("ERRORS:")
        for e in _all_errors[:20]:
            print("  -", e)
    if bad or _all_errors:
        sys.exit(1)
    print("REPAIR OK")


if __name__ == "__main__":
    main()
