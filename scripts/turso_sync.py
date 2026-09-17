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

CHUNK = 500
BATCH = 200

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
    url = os.environ.get("TURSO_URL", "")
    token = os.environ.get("TURSO_AUTH_TOKEN", "")
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
    from db_utils import DB_SCHEMA
    existing = {r[0] for r in rconn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if "anime" in existing:
        return False
    print("Remote empty: creating schema...")
    for stmt in DB_SCHEMA.split(";"):
        stmt = stmt.strip()
        if stmt:
            rconn.execute(stmt)
    rconn.commit()
    return True


def local_columns(lconn, table):
    return [r[1] for r in lconn.execute(f"PRAGMA table_info({table})").fetchall()]


def remote_has_table(rconn, table):
    return bool(rconn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,)).fetchall())


def replace_rows(rconn, table, cols, rows):
    if not rows:
        return 0
    placeholders = ", ".join(["?"] * len(cols))
    colnames = ", ".join([f'"{c}"' for c in cols])
    sql = f'INSERT OR REPLACE INTO "{table}" ({colnames}) VALUES ({placeholders})'
    for i in range(0, len(rows), BATCH):
        rconn.executemany(sql, rows[i:i + BATCH])
    rconn.commit()
    return len(rows)


def _t(params):
    # libsql execute() requires a tuple, not a list
    return tuple(params) if isinstance(params, list) else params


def delete_scoped(rconn, table, ids):
    ids = list(ids)
    for i in range(0, len(ids), CHUNK):
        chunk = ids[i:i + CHUNK]
        q = ", ".join(["?"] * len(chunk))
        rconn.execute(f'DELETE FROM "{table}" WHERE anime_id IN ({q})', _t(chunk))
    rconn.commit()


def copy_where(lconn, rconn, table, where, params):
    cols = local_columns(lconn, table)
    if not cols or not remote_has_table(rconn, table):
        return 0
    colnames = ", ".join([f'"{c}"' for c in cols])
    rows = lconn.execute(f"SELECT {colnames} FROM {table} WHERE {where}", _t(params)).fetchall()
    return replace_rows(rconn, table, cols, [tuple(r) for r in rows])


def _chunks(ids, n=CHUNK):
    ids = [i for i in ids if i is not None]
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


def sync_anime_ids(lconn, rconn, ids, with_related: bool):
    stats = {"anime": 0, "related": 0}
    chunks = _chunks(ids)
    if not chunks:
        return stats
    # Pass 1: anime rows for ALL chunks (relations FK-reference other anime,
    # which may live in a later chunk — especially on bootstrap).
    for chunk in chunks:
        q = ", ".join(["?"] * len(chunk))
        stats["anime"] += copy_where(lconn, rconn, "anime", f"id IN ({q})", chunk)
    if not with_related:
        return stats
    # Pass 2: shared entities, then scoped detail rows per chunk.
    for chunk in chunks:
        q = ", ".join(["?"] * len(chunk))
        stats["related"] += _sync_shared_for_chunk(lconn, rconn, chunk)
        for table in SCOPED_TABLES:
            if not remote_has_table(rconn, table):
                continue
            cols = local_columns(lconn, table)
            if not cols or "anime_id" not in cols:
                continue
            delete_scoped(rconn, table, chunk)
            colnames = ", ".join([f'"{c}"' for c in cols])
            rows = lconn.execute(
                f"SELECT {colnames} FROM {table} WHERE anime_id IN ({q})", _t(chunk)).fetchall()
            stats["related"] += replace_rows(rconn, table, cols, [tuple(r) for r in rows])
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

    bootstrap = len(sys.argv) > 1 and sys.argv[1] == "--bootstrap"
    rconn = connect_remote()
    created = ensure_schema(rconn)

    lconn = connect_db(db_path)
    try:
        if bootstrap or created:
            ids = [r[0] for r in lconn.execute("SELECT id FROM anime ORDER BY id").fetchall()]
            print(f"Bootstrap: pushing {len(ids)} anime + related rows...")
            stats = sync_anime_ids(lconn, rconn, ids, with_related=True)
            print(f"Bootstrap done: {stats}")
            return

        full_ids = read_touched(data_dir, "touched_full.json")
        counter_ids = read_touched(data_dir, "touched_counters.json")
        # counters-only ids ride along as full anime rows (counters live there)
        only_counters = [i for i in counter_ids if i not in set(full_ids)]
        total_stats = {"anime": 0, "related": 0}
        if full_ids:
            print(f"Delta: {len(full_ids)} full + {len(only_counters)} counters-only...")
            s = sync_anime_ids(lconn, rconn, full_ids, with_related=True)
            total_stats["anime"] += s["anime"]
            total_stats["related"] += s["related"]
        if only_counters:
            s = sync_anime_ids(lconn, rconn, only_counters, with_related=False)
            total_stats["anime"] += s["anime"]
        print(f"Delta done: {total_stats} (monthly write budget: 10M rows)")
    finally:
        lconn.close()


if __name__ == "__main__":
    main()
