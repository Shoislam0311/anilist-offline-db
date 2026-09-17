#!/usr/bin/env python3
"""
AniList Offline Database - API Generator (EXACT parity, anime-only)
Outputs AniList-exact Media JSON shards: camelCase, FuzzyDate objects,
full relations/recommendations/characters/staff/studios/tags/rankings/trends.
Prefers stored raw_json (byte-identical to graphql.anilist.co at scrape time).
"""

import os
import sys
import json
import gzip
import shutil

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_utils import connect_db, get_db_path, build_exact_media


def _load_json(text, default):
    try:
        return json.loads(text) if text else default
    except Exception:
        return default


def _hydrate_media(conn, anime_id: int) -> dict:
    """Exact Media object. raw_json first, then rebuild + hydrate relations."""
    m = build_exact_media(conn, anime_id)
    if m is None:
        return None

    # If raw_json existed it already has full nested data — just ensure anime-only defaults.
    if conn.execute("SELECT raw_json FROM anime WHERE id=?", (anime_id,)).fetchone()[0]:
        m.setdefault("type", "ANIME")
        m.setdefault("isFavourite", False)
        if not m.get("siteUrl"):
            m["siteUrl"] = f"https://anilist.co/anime/{anime_id}"
        return m

    # Fallback hydration from normalized tables (old DBs without raw_json)
    m["genres"] = [r[0] for r in conn.execute(
        "SELECT g.name FROM genres g JOIN anime_genres ag ON g.id=ag.genre_id WHERE ag.anime_id=?", (anime_id,))]
    m["synonyms"] = _load_json(conn.execute(
        "SELECT synonyms FROM anime WHERE id=?", (anime_id,)).fetchone()[0], [])
    m["tags"] = [{
        "id": r[0], "name": r[1], "description": r[2], "category": r[3], "rank": r[4],
        "isGeneralSpoiler": bool(r[5]), "isMediaSpoiler": bool(r[6]), "isAdult": bool(r[7]),
    } for r in conn.execute(
        """SELECT t.id, t.name, t.description, t.category, at2.tag_rank,
                  t.is_general_spoiler, t.is_media_spoiler, t.is_adult
           FROM tags t JOIN anime_tags at2 ON t.name=at2.tag_name WHERE at2.anime_id=?""", (anime_id,))]
    m["studios"] = {"edges": [{
        "id": r[0], "isMain": bool(r[1]), "favouriteOrder": r[2],
        "node": {"id": r[3], "name": r[4], "isAnimationStudio": bool(r[5]),
                 "siteUrl": r[6] or (f"https://anilist.co/studio/{r[3]}" if r[3] else None),
                 "favourites": r[7]},
    } for r in conn.execute(
        """SELECT ast.edge_id, ast.is_main, ast.favourite_order,
                  s.id, s.name, s.is_animation_studio, s.site_url, s.favourites
           FROM anime_studios ast JOIN studios s ON ast.studio_id=s.id WHERE ast.anime_id=?""", (anime_id,))]}
    return m


def generate_anime_shards(conn, output_dir: str, shard_size: int = 50):
    shard_dir = os.path.join(output_dir, "shards")
    os.makedirs(shard_dir, exist_ok=True)

    import glob as _glob
    for old in _glob.glob(os.path.join(shard_dir, "shard_*.json*")):
        try:
            os.remove(old)
        except OSError:
            pass

    ids = [r[0] for r in conn.execute("SELECT id FROM anime WHERE COALESCE(type,'ANIME')='ANIME' ORDER BY id")]
    total = len(ids)
    total_shards = (total + shard_size - 1) // shard_size
    print(f"Generating {total_shards} EXACT shards for {total} anime...")

    for shard_idx in range(total_shards):
        chunk = ids[shard_idx * shard_size:(shard_idx + 1) * shard_size]
        anime_list = []
        for aid in chunk:
            m = _hydrate_media(conn, aid)
            if m:
                anime_list.append(m)
        shard_path = os.path.join(shard_dir, f"shard_{shard_idx:04d}.json")
        with open(shard_path, "w", encoding="utf-8") as f:
            json.dump(anime_list, f, ensure_ascii=False)
        with open(shard_path, "rb") as f_in:
            with gzip.open(shard_path + ".gz", "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        if shard_idx % 10 == 0:
            print(f"  shard {shard_idx + 1}/{total_shards}")
    print(f"Generated {total_shards} shards in {shard_dir}")
    return total_shards


def generate_search_index(conn, output_dir: str):
    print("Generating search index (exact filter fields)...")
    rows = conn.execute("""
        SELECT a.id, a.id_mal, a.title_romaji, a.title_english, a.title_native,
               a.synonyms, a.average_score, a.mean_score, a.popularity, a.trending, a.favourites,
               a.episodes, a.duration, a.chapters, a.volumes,
               a.status, a.format, a.season, a.season_year,
               a.start_year, a.start_month, a.start_day,
               a.end_year, a.end_month, a.end_day,
               a.source, a.country_of_origin, a.hashtag, a.is_adult,
               a.cover_large, a.updated_at,
               GROUP_CONCAT(DISTINCT g.name) as genres
        FROM anime a
        LEFT JOIN anime_genres ag ON a.id = ag.anime_id
        LEFT JOIN genres g ON ag.genre_id = g.id
        WHERE COALESCE(a.type,'ANIME')='ANIME'
        GROUP BY a.id
        ORDER BY a.popularity DESC
    """).fetchall()
    tag_map = {}
    for aid, tname in conn.execute("SELECT anime_id, tag_name FROM anime_tags"):
        tag_map.setdefault(aid, []).append(tname)

    def fuzzy(y, mo, d):
        if not y:
            return None
        return y * 10000 + (mo or 0) * 100 + (d or 0)

    index = []
    for r in rows:
        (aid, idmal, romaji, eng, native, syn, avg, mean, pop, trend, fav,
         eps, dur, ch, vol, status, fmt, season, syear,
         syy, smo, sdd, eyy, emo, edd,
         source, country, hashtag, adult, cover, updated, genres) = r
        index.append({
            "id": aid, "idMal": idmal,
            "romaji": romaji, "english": eng, "native": native,
            "synonyms": _load_json(syn, []),
            "score": avg, "meanScore": mean, "popularity": pop or 0,
            "trending": trend or 0, "favourites": fav or 0,
            "episodes": eps, "duration": dur, "chapters": ch, "volumes": vol,
            "status": status, "format": fmt, "season": season, "year": syear,
            "startDate": fuzzy(syy, smo, sdd), "endDate": fuzzy(eyy, emo, edd),
            "source": source, "country": country, "hashtag": hashtag,
            "adult": bool(adult), "cover": cover, "updatedAt": updated,
            "genres": (genres or "").split(",") if genres else [],
            "tags": tag_map.get(aid, []),
        })
    path = os.path.join(output_dir, "search_index.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False)
    with open(path, "rb") as f_in:
        with gzip.open(path + ".gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"Search index: {len(index)} entries")
    return len(index)


def generate_metadata(conn, output_dir: str):
    print("Generating metadata...")
    stats = {}
    stats["type"] = "ANIME"
    stats["totalAnime"] = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE COALESCE(type,'ANIME')='ANIME'").fetchone()[0]
    stats["totalCharacters"] = conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]
    stats["totalStaff"] = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='staff'").fetchone()[0] \
        and conn.execute("SELECT COUNT(*) FROM staff").fetchone()[0] or 0
    stats["totalStudios"] = conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0]
    stats["genres"] = [r[0] for r in conn.execute("SELECT name FROM genres ORDER BY name")]
    stats["tags"] = [r[0] for r in conn.execute("SELECT name FROM tags ORDER BY name LIMIT 2000")]
    stats["formats"] = [r[0] for r in conn.execute(
        "SELECT DISTINCT format FROM anime WHERE format IS NOT NULL ORDER BY format")]
    stats["statuses"] = [r[0] for r in conn.execute(
        "SELECT DISTINCT status FROM anime WHERE status IS NOT NULL ORDER BY status")]
    stats["seasons"] = [r[0] for r in conn.execute(
        "SELECT DISTINCT season FROM anime WHERE season IS NOT NULL ORDER BY season")]
    stats["sources"] = [r[0] for r in conn.execute(
        "SELECT DISTINCT source FROM anime WHERE source IS NOT NULL ORDER BY source")]
    stats["years"] = [r[0] for r in conn.execute(
        "SELECT DISTINCT season_year FROM anime WHERE season_year IS NOT NULL ORDER BY season_year DESC")]
    shard_size = 50  # exact full-embed Media ~300KB each -> 50/shard ~= 15MB (under jsDelivr 20MB free CDN limit)
    stats["totalShards"] = (stats["totalAnime"] + shard_size - 1) // shard_size
    stats["shardSize"] = shard_size
    stats["shardStartIds"] = []
    for i in range(stats["totalShards"]):
        row = conn.execute(
            "SELECT id FROM anime WHERE COALESCE(type,'ANIME')='ANIME' ORDER BY id LIMIT 1 OFFSET ?",
            (i * shard_size,)).fetchone()
        stats["shardStartIds"].append(row[0] if row else 0)
    from datetime import datetime, timezone
    stats["generatedAt"] = datetime.now(timezone.utc).isoformat()
    stats["schema"] = "anilist-exact-anime-v2"
    with open(os.path.join(output_dir, "metadata.json"), "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    print(f"Metadata: {stats['totalAnime']} anime, {stats['totalShards']} shards")
    return stats


def generate_sample_data(conn, output_dir: str):
    top_popular = conn.execute("""
        SELECT id, title_romaji, title_english, cover_large, average_score, popularity, episodes, status, format
        FROM anime WHERE COALESCE(type,'ANIME')='ANIME' AND COALESCE(is_adult,0)=0
        ORDER BY popularity DESC LIMIT 50""").fetchall()
    top_rated = conn.execute("""
        SELECT id, title_romaji, title_english, cover_large, average_score, popularity, episodes, status, format
        FROM anime WHERE COALESCE(type,'ANIME')='ANIME' AND COALESCE(is_adult,0)=0 AND average_score IS NOT NULL
        ORDER BY average_score DESC LIMIT 50""").fetchall()
    sample = {
        "topPopular": [dict(zip(["id", "romaji", "english", "cover", "score", "popularity", "episodes", "status", "format"], r)) for r in top_popular],
        "topRated": [dict(zip(["id", "romaji", "english", "cover", "score", "popularity", "episodes", "status", "format"], r)) for r in top_rated],
    }
    with open(os.path.join(output_dir, "sample.json"), "w", encoding="utf-8") as f:
        json.dump(sample, f, indent=2, ensure_ascii=False)


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
        # migrate old DBs so raw_json + new tables exist before export
        try:
            from db_utils import _migrate
            _migrate(conn)
        except Exception:
            pass
        generate_anime_shards(conn, api_dir)
        generate_search_index(conn, api_dir)
        generate_metadata(conn, api_dir)
        generate_sample_data(conn, api_dir)
        print(f"\nAPI data generated in {api_dir}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
