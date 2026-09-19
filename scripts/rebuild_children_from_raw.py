#!/usr/bin/env python3
"""
Rebuild child tables from raw_json — the authoritative per-anime source.

Why: the original fetcher's table upserts silently failed for large parts of
the catalog (relations covers only ~7k of 14.7k anime, recommendations ~9k,
characters ~12k) while anime.raw_json (byte-identical AniList responses)
stored everything. This script replays the fetcher's own upsert_* functions
over every raw_json so the local DB becomes complete, then turso_repair.py
pushes the diff to Turso.

Idempotent: child upserts are DELETE-then-INSERT per anime. Anime rows and
raw_json are untouched (no network needed).

Usage: python scripts/rebuild_children_from_raw.py [--db data/anilist.db]
"""

import os
import sys
import json
import time
import sqlite3
import contextlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "anilist.db")

from db_utils import (  # noqa: E402
    upsert_anime_titles, upsert_anime_descriptions,
    upsert_genres, upsert_tags, upsert_studios, upsert_characters,
    upsert_staff, upsert_relations, upsert_recommendations, upsert_airing_schedule,
    upsert_external_links, upsert_streaming_episodes, upsert_statistics,
    upsert_rankings, upsert_trends, upsert_reviews,
)


def main():
    db = DB_PATH
    for i, a in enumerate(sys.argv[1:]):
        if a == "--db" and i + 1 < len(sys.argv):
            db = sys.argv[i + 1]
    if not os.path.exists(db):
        print(f"ERROR: DB not found at {db}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")

    total = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
    print(f"rebuilding child tables for {total} anime from raw_json...")

    t0 = time.monotonic()
    done = 0
    errors = 0
    cur = conn.execute("SELECT id, raw_json FROM anime ORDER BY id")
    while True:
        batch = cur.fetchmany(200)
        if not batch:
            break
        for aid, raw in batch:
            if not raw:
                continue
            try:
                media = json.loads(raw)
                if media.get("type") and media.get("type") != "ANIME":
                    continue
                aid2 = media.get("id") or aid
                upsert_anime_titles(conn, aid2, media.get("title", {}) or {})
                upsert_anime_descriptions(conn, aid2, media.get("description", "") or "", media.get("synonyms", []) or [])
                upsert_genres(conn, aid2, media.get("genres", []) or [])
                upsert_tags(conn, aid2, media.get("tags", []) or [])
                upsert_studios(conn, aid2, media.get("studios", {}) or {})
                upsert_characters(conn, aid2, media.get("characters", {}) or {})
                with contextlib.suppress(Exception):
                    upsert_staff(conn, aid2, media.get("staff", {}) or {})
                upsert_relations(conn, aid2, media.get("relations", {}) or {})
                upsert_recommendations(conn, aid2, media.get("recommendations", {}) or {})
                with contextlib.suppress(Exception):
                    upsert_rankings(conn, aid2, media.get("rankings", []) or [])
                with contextlib.suppress(Exception):
                    upsert_trends(conn, aid2, media.get("trends", {}) or {})
                with contextlib.suppress(Exception):
                    upsert_reviews(conn, aid2, media.get("reviews", {}) or {})
                with contextlib.suppress(Exception):
                    upsert_statistics(conn, aid2, media.get("stats", {}) or {})
                sched = media.get("airingSchedule") or {}
                if isinstance(sched, dict) and sched.get("edges"):
                    upsert_airing_schedule(conn, aid2, sched.get("edges") or [])
                elif media.get("nextAiringEpisode"):
                    upsert_airing_schedule(conn, aid2, [media["nextAiringEpisode"]])
                upsert_external_links(conn, aid2, media.get("externalLinks", []) or [])
                upsert_streaming_episodes(conn, aid2, media.get("streamingEpisodes", []) or [])
            except Exception as e:
                errors += 1
                if errors <= 10:
                    print(f"  ERROR anime {aid}: {str(e)[:140]}")
        done += len(batch)
        conn.commit()
        elapsed = time.monotonic() - t0
        rate = done / max(elapsed, 0.001)
        print(f"  {done}/{total} ({elapsed:.0f}s, {rate:.0f}/s)", flush=True)

    print("\n== rebuilt counts ==")
    for t in ["anime_titles", "anime_genres", "anime_tags", "anime_studios",
              "anime_characters", "character_voice_actors", "anime_staff",
              "relations", "recommendations", "airing_schedule",
              "external_links", "streaming_episodes", "rankings", "trends",
              "reviews", "statistics"]:
        n = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        d = conn.execute(f'SELECT COUNT(DISTINCT anime_id) FROM "{t}"').fetchone()[0]
        print(f"  {t}: rows={n} distinct_anime={d}")
    print(f"\ndone in {time.monotonic() - t0:.0f}s, errors={errors}")
    if errors > total * 0.01:
        sys.exit(1)


if __name__ == "__main__":
    main()
