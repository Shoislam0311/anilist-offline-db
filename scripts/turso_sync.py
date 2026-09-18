#!/usr/bin/env python3
"""
AniList Offline Database - Turso Sync (SMART minimal version)

Daily behaviour:
- touched_full.json     → brand-new titles → full write
- touched_airing.json   → already-known releasing titles → only next episode UPDATE
"""

import os
import sys
import json
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

CHUNK = 500
BATCH = 1000
ANIME_BATCH = 50
TIME_BUDGET_DEFAULT = 10          # minutes
PROGRESS_FILE = "turso_sync_progress.json"

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
    if url.startswith("file:") or url.endswith(".db"):
        import libsql_experimental as libsql
        return libsql.connect(database=url)
    if not url or not token:
        print("ERROR: TURSO_URL / TURSO_AUTH_TOKEN not set", file=sys.stderr)
        sys.exit(2)
    import libsql_experimental as libsql
    return libsql.connect(database=url, auth_token=token)


def connect_db(path):
    from db_utils import connect_db as _c
    return _c(path)


def ensure_schema(rconn, blind=False):
    from db_utils import DB_SCHEMA, SCOPED_INDEXES
    if blind:
        for stmt in DB_SCHEMA.split(";"):
            stmt = stmt.strip()
            if stmt:
                try:
                    rconn.execute(stmt)
                except Exception:
                    pass
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
    return False


def local_columns(lconn, table):
    return [r[1] for r in lconn.execute(f"PRAGMA table_info({table})").fetchall()]


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
            rconn.executemany(sql, batch)
            done += len(batch)
        except Exception as e:
            print(f"  {table} batch error: {str(e)[:80]}")
    try:
        rconn.commit()
    except Exception:
        pass
    return done


def copy_where(lconn, rconn, table, where, params, batch_size=BATCH):
    cols = local_columns(lconn, table)
    if not cols:
        return 0
    colnames = ", ".join([f'"{c}"' for c in cols])
    rows = lconn.execute(f"SELECT {colnames} FROM {table} WHERE {where}", tuple(params)).fetchall()
    return replace_rows(rconn, table, cols, [tuple(r) for r in rows], batch_size=batch_size)


def _chunks(ids, n=None):
    ids = [i for i in ids if i is not None]
    n = n or CHUNK
    return [ids[i:i + n] for i in range(0, len(ids), n)]


def sync_anime_ids(db_path, ids, with_related=True, max_minutes=None, write_only=False):
    """Full write for brand-new titles only."""
    if not ids:
        return {"anime": 0, "related": 0}

    lconn = connect_db(db_path)
    rconn = connect_remote()
    stats = {"anime": 0, "related": 0}
    started = time.monotonic()

    for chunk in _chunks(ids):
        if max_minutes and (time.monotonic() - started) > max_minutes * 60:
            print("Time budget reached, stopping full sync")
            break

        q = ", ".join(["?"] * len(chunk))
        n = copy_where(lconn, rconn, "anime", f"id IN ({q})", chunk, batch_size=ANIME_BATCH)
        stats["anime"] += n

        if with_related:
            for table in SCOPED_TABLES:
                cols = local_columns(lconn, table)
                if not cols or "anime_id" not in cols:
                    continue
                # simple delete + insert
                try:
                    rconn.execute(f'DELETE FROM "{table}" WHERE anime_id IN ({q})', tuple(chunk))
                except Exception:
                    pass
                n2 = copy_where(lconn, rconn, table, f"anime_id IN ({q})", chunk)
                stats["related"] += n2

    try:
        rconn.commit()
    except Exception:
        pass
    lconn.close()
    print(f"Full sync done: {stats}")
    return stats


def update_next_airing_on_turso(db_path, ids):
    """Ultra-light UPDATE for already-known releasing titles."""
    if not ids:
        return 0

    lconn = connect_db(db_path)
    rconn = connect_remote()

    # Ensure columns exist
    for col in ("next_airing_at", "next_airing_episode"):
        try:
            rconn.execute(f"ALTER TABLE anime ADD COLUMN {col} INTEGER")
        except Exception:
            pass

    q = ", ".join(["?"] * len(ids))
    rows = lconn.execute(f"""
        SELECT id, next_airing_at, next_airing_episode, updated_at
        FROM anime WHERE id IN ({q})
    """, tuple(ids)).fetchall()

    updated = 0
    for row in rows:
        try:
            rconn.execute("""
                UPDATE anime SET
                    next_airing_at = ?,
                    next_airing_episode = ?,
                    updated_at = ?
                WHERE id = ?
            """, (row[1], row[2], row[3], row[0]))
            updated += 1
        except Exception as e:
            print(f"  airing update failed {row[0]}: {e}")

    try:
        rconn.commit()
    except Exception:
        pass
    lconn.close()
    print(f"Airing-only update: {updated} titles")
    return updated


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
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")
    db_path = os.path.join(data_dir, "anilist.db")

    if not os.path.exists(db_path):
        print(f"ERROR: local DB not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    write_only = "--write-only" in sys.argv
    max_minutes = TIME_BUDGET_DEFAULT
    for i, a in enumerate(sys.argv[1:]):
        if a.startswith("--max-minutes"):
            if "=" in a:
                max_minutes = int(a.split("=", 1)[1])
            elif i + 2 <= len(sys.argv):
                max_minutes = int(sys.argv[i + 2])

    rconn = connect_remote()
    ensure_schema(rconn, blind=write_only)

    full_ids = read_touched(data_dir, "touched_full.json")
    airing_ids = read_touched(data_dir, "touched_airing.json")
    counter_ids = read_touched(data_dir, "touched_counters.json")

    print(f"SMART sync: {len(full_ids)} NEW (full) + {len(airing_ids)} airing-only")

    total_stats = {"anime": 0, "related": 0}
    start = time.monotonic()

    # 1. Full write only for brand-new titles
    if full_ids:
        print(f"→ Full write for {len(full_ids)} brand-new titles...")
        s = sync_anime_ids(db_path, full_ids, with_related=True,
                           max_minutes=max_minutes, write_only=write_only)
        total_stats["anime"] += s["anime"]
        total_stats["related"] += s["related"]

    # 2. Ultra-light next-episode update
    if airing_ids:
        print(f"→ Airing-only update for {len(airing_ids)} titles...")
        n = update_next_airing_on_turso(db_path, airing_ids)
        total_stats["anime"] += n

    secs = time.monotonic() - start
    print("=" * 60)
    print(f"SYNC SUMMARY: NEW={len(full_ids)} airing={len(airing_ids)} "
          f"anime_rows={total_stats['anime']} related={total_stats['related']} "
          f"secs={secs:.1f}")
    print("=" * 60)

    # write report
    try:
        report = {
            "completedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "mode": "smart-rails",
            "newIds": len(full_ids),
            "airingIds": len(airing_ids),
            "animeRows": total_stats["anime"],
            "relatedRows": total_stats["related"],
            "seconds": round(secs, 1),
        }
        out = os.path.join(base_dir, "docs", "api", "sync_report.json")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w") as f:
            json.dump(report, f, indent=2)
    except Exception as e:
        print(f"Report skipped: {e}")


if __name__ == "__main__":
    main()
