#!/usr/bin/env python3
"""
AniList Offline Database - API Generator
Pre-builds JSON data shards for the GitHub Pages GraphQL API.
Creates a static API that mirrors graphql.anilist.co with zero rate limits.
"""

import os
import sys
import json
import gzip
import shutil
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_utils import connect_db, get_db_path


def generate_anime_shards(conn, output_dir: str, shard_size: int = 200):
    shard_dir = os.path.join(output_dir, "shards")
    os.makedirs(shard_dir, exist_ok=True)

    # Remove stale shards from previous (larger) runs so the directory
    # never serves ghost entries. See: shrinking DB between runs.
    import glob as _glob
    for old in _glob.glob(os.path.join(shard_dir, "shard_*.json*")):
        try:
            os.remove(old)
        except OSError:
            pass

    total = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
    total_shards = (total + shard_size - 1) // shard_size

    print(f"Generating {total_shards} shards for {total} anime...")

    for shard_idx in range(total_shards):
        offset = shard_idx * shard_size
        rows = conn.execute("""
            SELECT a.id, a.title_romaji, a.title_english, a.title_native,
                   a.description, a.cover_large, a.cover_color, a.banner_image,
                   a.episodes, a.duration, a.status, a.format, a.season, a.season_year,
                   a.average_score, a.mean_score, a.popularity, a.favourites, a.trending,
                   a.next_airing_episode, a.next_airing_at, a.start_date, a.end_date,
                   a.source, a.hashtag, a.country_of_origin, a.is_adult,
                   a.created_at, a.updated_at, a.id_mal
            FROM anime a
            ORDER BY a.id
            LIMIT ? OFFSET ?
        """, (shard_size, offset)).fetchall()

        columns = ["id", "title_romaji", "title_english", "title_native",
                    "description", "cover_large", "cover_color", "banner_image",
                    "episodes", "duration", "status", "format", "season", "season_year",
                    "average_score", "mean_score", "popularity", "favourites", "trending",
                    "next_airing_episode", "next_airing_at", "start_date", "end_date",
                    "source", "hashtag", "country_of_origin", "is_adult",
                    "created_at", "updated_at", "id_mal"]

        anime_list = []
        for row in rows:
            anime = dict(zip(columns, row))
            anime_id = anime["id"]

            anime["genres"] = [r[0] for r in conn.execute(
                "SELECT g.name FROM genres g JOIN anime_genres ag ON g.id = ag.genre_id WHERE ag.anime_id=?",
                (anime_id,)
            )]

            anime["tags"] = [{"name": r[0], "rank": r[1]} for r in conn.execute(
                "SELECT tag_name, tag_rank FROM anime_tags WHERE anime_id=?", (anime_id,)
            )]

            anime["studios"] = [{"name": r[0], "isMain": bool(r[1])} for r in conn.execute(
                """SELECT s.name, ast.is_main FROM studios s
                   JOIN anime_studios ast ON s.id = ast.studio_id WHERE ast.anime_id=?""",
                (anime_id,)
            )]

            anime["characters"] = [{"id": r[0], "name": r[1], "nameNative": r[2],
                                    "image": r[3], "role": r[4]} for r in conn.execute(
                """SELECT c.id, c.name_full, c.name_native, c.image_large, ac.role
                   FROM characters c JOIN anime_characters ac ON c.id = ac.character_id
                   WHERE ac.anime_id=? ORDER BY ac.sort_order LIMIT 25""", (anime_id,)
            )]

            anime["relations"] = [{
                "relationType": r[0], "id": r[1],
                "title": {"romaji": r[2], "english": r[3], "native": r[4]},
                "coverImage": {"large": r[5], "color": None},
                "bannerImage": r[6], "format": r[7], "status": r[8],
                "episodes": r[9], "averageScore": r[10], "meanScore": r[11],
                "popularity": r[12], "source": r[13], "isAdult": bool(r[14]) if r[14] else False,
                "type": "ANIME",
            } for r in conn.execute(
                """SELECT r.relation_type, r.related_anime_id,
                   a.title_romaji, a.title_english, a.title_native,
                   a.cover_large, a.banner_image, a.format, a.status, a.episodes,
                   a.average_score, a.mean_score, a.popularity, a.source, a.is_adult
                   FROM relations r LEFT JOIN anime a ON r.related_anime_id = a.id
                   WHERE r.anime_id=?""", (anime_id,)
            )]

            anime["recommendations"] = [{
                "id": r[0],
                "title": {"romaji": r[1], "english": r[2], "native": r[3]},
                "coverImage": {"large": r[4], "color": None},
                "bannerImage": r[5], "format": r[6], "status": r[7],
                "episodes": r[8], "averageScore": r[9], "meanScore": r[10],
                "popularity": r[11], "rating": r[12],
                "type": "ANIME",
            } for r in conn.execute(
                """SELECT r.recommended_anime_id,
                   a.title_romaji, a.title_english, a.title_native,
                   a.cover_large, a.banner_image, a.format, a.status, a.episodes,
                   a.average_score, a.mean_score, a.popularity, r.rating
                   FROM recommendations r LEFT JOIN anime a ON r.recommended_anime_id = a.id
                   WHERE r.anime_id=? ORDER BY r.rating DESC LIMIT 10""", (anime_id,)
            )]

            anime["airingSchedule"] = [{"episode": r[0], "airingAt": r[1]} for r in conn.execute(
                "SELECT episode, airing_at FROM airing_schedule WHERE anime_id=? ORDER BY episode",
                (anime_id,)
            )]

            anime_list.append(anime)

        shard_path = os.path.join(shard_dir, f"shard_{shard_idx:04d}.json")
        with open(shard_path, "w", encoding="utf-8") as f:
            json.dump(anime_list, f, ensure_ascii=False)

        gz_path = shard_path + ".gz"
        with open(shard_path, "rb") as f_in:
            with gzip.open(gz_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        if shard_idx % 10 == 0:
            print(f"  Generated shard {shard_idx + 1}/{total_shards}")

    print(f"Generated {total_shards} shards in {shard_dir}")
    return total_shards


def generate_search_index(conn, output_dir: str):
    print("Generating search index...")

    rows = conn.execute("""
        SELECT a.id, a.title_romaji, a.title_english, a.title_native,
               a.average_score, a.popularity, a.episodes, a.status,
               a.format, a.season, a.season_year, a.cover_large, a.is_adult,
               GROUP_CONCAT(DISTINCT g.name) as genres
        FROM anime a
        LEFT JOIN anime_genres ag ON a.id = ag.anime_id
        LEFT JOIN genres g ON ag.genre_id = g.id
        GROUP BY a.id
        ORDER BY a.popularity DESC
    """).fetchall()

    search_index = []
    for row in rows:
        search_index.append({
            "id": row[0],
            "romaji": row[1],
            "english": row[2],
            "native": row[3],
            "score": row[4],
            "popularity": row[5],
            "episodes": row[6],
            "status": row[7],
            "format": row[8],
            "season": row[9],
            "year": row[10],
            "cover": row[11],
            "adult": bool(row[12]),
            "genres": (row[13] or "").split(",") if row[13] else []
        })

    index_path = os.path.join(output_dir, "search_index.json")
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(search_index, f, ensure_ascii=False)

    gz_path = index_path + ".gz"
    with open(index_path, "rb") as f_in:
        with gzip.open(gz_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    print(f"Search index: {len(search_index)} entries")
    return len(search_index)


def generate_metadata(conn, output_dir: str):
    print("Generating metadata...")

    stats = {}
    stats["totalAnime"] = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
    stats["totalCharacters"] = conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]
    stats["totalStudios"] = conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0]

    genres = [r[0] for r in conn.execute("SELECT name FROM genres ORDER BY name")]
    stats["genres"] = genres

    formats = [r[0] for r in conn.execute(
        "SELECT DISTINCT format FROM anime WHERE format IS NOT NULL ORDER BY format"
    )]
    stats["formats"] = formats

    statuses = [r[0] for r in conn.execute(
        "SELECT DISTINCT status FROM anime WHERE status IS NOT NULL ORDER BY status"
    )]
    stats["statuses"] = statuses

    seasons = [r[0] for r in conn.execute(
        "SELECT DISTINCT season FROM anime WHERE season IS NOT NULL ORDER BY season"
    )]
    stats["seasons"] = seasons

    years = [r[0] for r in conn.execute(
        "SELECT DISTINCT season_year FROM anime WHERE season_year IS NOT NULL ORDER BY season_year DESC"
    )]
    stats["years"] = years

    shard_size = 200
    stats["totalShards"] = (stats["totalAnime"] + shard_size - 1) // shard_size
    stats["shardSize"] = shard_size

    shard_start_ids = []
    for shard_idx in range(stats["totalShards"]):
        offset = shard_idx * shard_size
        first_id = conn.execute(
            "SELECT id FROM anime ORDER BY id LIMIT 1 OFFSET ?",
            (offset,)
        ).fetchone()
        shard_start_ids.append(first_id[0] if first_id else 0)
    stats["shardStartIds"] = shard_start_ids

    from datetime import datetime, timezone
    stats["generatedAt"] = datetime.now(timezone.utc).isoformat()

    meta_path = os.path.join(output_dir, "metadata.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)

    print(f"Metadata: {stats['totalAnime']} anime, {stats['totalShards']} shards")
    return stats


def generate_sample_data(conn, output_dir: str):
    print("Generating sample data...")

    top_popular = conn.execute("""
        SELECT a.id, a.title_romaji, a.title_english, a.cover_large,
               a.average_score, a.popularity, a.episodes, a.status, a.format
        FROM anime a
        WHERE a.is_adult = 0
        ORDER BY a.popularity DESC
        LIMIT 50
    """).fetchall()

    top_rated = conn.execute("""
        SELECT a.id, a.title_romaji, a.title_english, a.cover_large,
               a.average_score, a.popularity, a.episodes, a.status, a.format
        FROM anime a
        WHERE a.is_adult = 0 AND a.average_score IS NOT NULL
        ORDER BY a.average_score DESC
        LIMIT 50
    """).fetchall()

    sample = {
        "topPopular": [dict(zip(
            ["id", "romaji", "english", "cover", "score", "popularity", "episodes", "status", "format"],
            r
        )) for r in top_popular],
        "topRated": [dict(zip(
            ["id", "romaji", "english", "cover", "score", "popularity", "episodes", "status", "format"],
            r
        )) for r in top_rated]
    }

    sample_path = os.path.join(output_dir, "sample.json")
    with open(sample_path, "w", encoding="utf-8") as f:
        json.dump(sample, f, indent=2, ensure_ascii=False)

    print(f"Sample data: {len(top_popular)} popular + {len(top_rated)} rated")


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")
    db_path = get_db_path(data_dir)
    api_dir = os.path.join(base_dir, "docs", "api")

    if not os.path.exists(db_path):
        print(f"Error: Database not found at {db_path}")
        sys.exit(1)

    os.makedirs(api_dir, exist_ok=True)

    conn = connect_db(db_path)

    try:
        generate_anime_shards(conn, api_dir)
        generate_search_index(conn, api_dir)
        generate_metadata(conn, api_dir)
        generate_sample_data(conn, api_dir)
        print(f"\nAPI data generated in {api_dir}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
