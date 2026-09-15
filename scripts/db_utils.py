"""
Database utilities for the AniList offline database.
Handles SQLite schema creation, FTS5 indexing, changelog generation, and JSON export.
"""

import sqlite3
import json
import os
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS anime (
    id INTEGER PRIMARY KEY,
    id_mal INTEGER,
    title_romaji TEXT,
    title_english TEXT,
    title_native TEXT,
    description TEXT,
    cover_large TEXT,
    cover_color TEXT,
    banner_image TEXT,
    episodes INTEGER,
    duration INTEGER,
    status TEXT,
    format TEXT,
    season TEXT,
    season_year INTEGER,
    average_score REAL,
    mean_score REAL,
    popularity INTEGER,
    favourites INTEGER,
    trending INTEGER,
    next_airing_episode INTEGER,
    next_airing_at INTEGER,
    start_date TEXT,
    end_date TEXT,
    source TEXT,
    hashtag TEXT,
    country_of_origin TEXT,
    is_adult INTEGER DEFAULT 0,
    created_at TEXT,
    updated_at TEXT,
    fetched_at TEXT
);

CREATE TABLE IF NOT EXISTS anime_titles (
    anime_id INTEGER NOT NULL,
    language TEXT NOT NULL,
    title TEXT NOT NULL,
    PRIMARY KEY (anime_id, language),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS anime_descriptions (
    anime_id INTEGER NOT NULL,
    language TEXT NOT NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (anime_id, language),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS genres (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS anime_genres (
    anime_id INTEGER NOT NULL,
    genre_id INTEGER NOT NULL,
    PRIMARY KEY (anime_id, genre_id),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    category TEXT,
    description TEXT,
    rank INTEGER
);

CREATE TABLE IF NOT EXISTS anime_tags (
    anime_id INTEGER NOT NULL,
    tag_name TEXT NOT NULL,
    tag_rank INTEGER,
    PRIMARY KEY (anime_id, tag_name),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS studios (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    is_animation_studio INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS anime_studios (
    anime_id INTEGER NOT NULL,
    studio_id INTEGER NOT NULL,
    is_main INTEGER DEFAULT 0,
    PRIMARY KEY (anime_id, studio_id),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE,
    FOREIGN KEY (studio_id) REFERENCES studios(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS characters (
    id INTEGER PRIMARY KEY,
    name_full TEXT,
    name_native TEXT,
    name_alternative TEXT,
    image_large TEXT,
    image_medium TEXT,
    description TEXT,
    favourites INTEGER,
    gender TEXT,
    date_of_birth TEXT,
    age TEXT
);

CREATE TABLE IF NOT EXISTS anime_characters (
    anime_id INTEGER NOT NULL,
    character_id INTEGER NOT NULL,
    role TEXT,
    sort_order INTEGER DEFAULT 0,
    PRIMARY KEY (anime_id, character_id),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE,
    FOREIGN KEY (character_id) REFERENCES characters(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS voice_actors (
    id INTEGER PRIMARY KEY,
    name_full TEXT,
    name_native TEXT,
    image_large TEXT,
    image_medium TEXT,
    favourites INTEGER
);

CREATE TABLE IF NOT EXISTS character_voice_actors (
    character_id INTEGER NOT NULL,
    voice_actor_id INTEGER NOT NULL,
    anime_id INTEGER NOT NULL,
    language TEXT NOT NULL,
    PRIMARY KEY (character_id, voice_actor_id, anime_id, language),
    FOREIGN KEY (character_id) REFERENCES characters(id) ON DELETE CASCADE,
    FOREIGN KEY (voice_actor_id) REFERENCES voice_actors(id) ON DELETE CASCADE,
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS relations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER NOT NULL,
    related_anime_id INTEGER NOT NULL,
    relation_type TEXT NOT NULL,
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE,
    FOREIGN KEY (related_anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS recommendations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER NOT NULL,
    recommended_anime_id INTEGER NOT NULL,
    rating INTEGER,
    user_rating TEXT,
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE,
    FOREIGN KEY (recommended_anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS airing_schedule (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER NOT NULL,
    episode INTEGER NOT NULL,
    airing_at INTEGER NOT NULL,
    time_until_airing INTEGER,
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS external_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER NOT NULL,
    site TEXT NOT NULL,
    url TEXT,
    language TEXT,
    color TEXT,
    icon TEXT,
    is_disabled INTEGER DEFAULT 0,
    notes TEXT,
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS streaming_episodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER NOT NULL,
    title TEXT,
    thumbnail TEXT,
    site TEXT,
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS statistics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER NOT NULL UNIQUE,
    score_distribution TEXT,
    rankings TEXT,
    trends TEXT,
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sync_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_anime_status ON anime(status);
CREATE INDEX IF NOT EXISTS idx_anime_format ON anime(format);
CREATE INDEX IF NOT EXISTS idx_anime_season ON anime(season, season_year);
CREATE INDEX IF NOT EXISTS idx_anime_score ON anime(average_score);
CREATE INDEX IF NOT EXISTS idx_anime_popularity ON anime(popularity);
CREATE INDEX IF NOT EXISTS idx_anime_favourites ON anime(favourites);
CREATE INDEX IF NOT EXISTS idx_anime_trending ON anime(trending);
CREATE INDEX IF NOT EXISTS idx_anime_updated_at ON anime(updated_at);
CREATE INDEX IF NOT EXISTS idx_anime_genres_genre ON anime_genres(genre_id);
CREATE INDEX IF NOT EXISTS idx_anime_tags_tag ON anime_tags(tag_name);
CREATE INDEX IF NOT EXISTS idx_anime_studios_studio ON anime_studios(studio_id);
CREATE INDEX IF NOT EXISTS idx_anime_characters_character ON anime_characters(character_id);
CREATE INDEX IF NOT EXISTS idx_relations_anime ON relations(anime_id);
CREATE INDEX IF NOT EXISTS idx_relations_related ON relations(related_anime_id);
CREATE INDEX IF NOT EXISTS idx_recommendations_anime ON recommendations(anime_id);
CREATE INDEX IF NOT EXISTS idx_airing_schedule_anime ON airing_schedule(anime_id);
CREATE INDEX IF NOT EXISTS idx_airing_schedule_airing ON airing_schedule(airing_at);
"""

FTS_SCHEMA = """
CREATE VIRTUAL TABLE IF NOT EXISTS anime_fts USING fts5(
    title_romaji,
    title_english,
    title_native,
    description,
    genres,
    tags,
    studios,
    characters,
    tokenize='porter unicode61'
);

CREATE VIRTUAL TABLE IF NOT EXISTS characters_fts USING fts5(
    name_full,
    name_native,
    name_alternative,
    description,
    tokenize='porter unicode61'
);
"""


def get_db_path(data_dir: str) -> str:
    return os.path.join(data_dir, "anilist.db")


def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA cache_size=-64000")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def init_db(db_path: str) -> sqlite3.Connection:
    conn = connect_db(db_path)
    conn.executescript(DB_SCHEMA)
    return conn


def init_fts(db_path: str) -> sqlite3.Connection:
    conn = connect_db(db_path)
    try:
        conn.executescript(FTS_SCHEMA)
    except sqlite3.OperationalError:
        pass
    return conn


def rebuild_fts(conn: sqlite3.Connection):
    try:
        conn.execute("INSERT INTO anime_fts(anime_fts) VALUES('rebuild')")
        conn.execute("INSERT INTO characters_fts(characters_fts) VALUES('rebuild')")
    except sqlite3.OperationalError:
        pass


def populate_fts(conn: sqlite3.Connection):
    conn.execute("DELETE FROM anime_fts")
    conn.execute("DELETE FROM characters_fts")

    conn.execute("""
        INSERT INTO anime_fts(rowid, title_romaji, title_english, title_native, description, genres, tags, studios, characters)
        SELECT
            a.id,
            a.title_romaji,
            a.title_english,
            a.title_native,
            a.description,
            COALESCE(GROUP_CONCAT(DISTINCT g.name), ''),
            COALESCE(GROUP_CONCAT(DISTINCT at2.tag_name), ''),
            COALESCE(GROUP_CONCAT(DISTINCT s.name), ''),
            COALESCE(GROUP_CONCAT(DISTINCT c.name_full), '')
        FROM anime a
        LEFT JOIN anime_genres ag ON a.id = ag.anime_id
        LEFT JOIN genres g ON ag.genre_id = g.id
        LEFT JOIN anime_tags at2 ON a.id = at2.anime_id
        LEFT JOIN anime_studios ast ON a.id = ast.anime_id
        LEFT JOIN studios s ON ast.studio_id = s.id
        LEFT JOIN anime_characters ac ON a.id = ac.anime_id
        LEFT JOIN characters c ON ac.character_id = c.id
        GROUP BY a.id
    """)

    conn.execute("""
        INSERT INTO characters_fts(rowid, name_full, name_native, name_alternative, description)
        SELECT id, name_full, name_native, name_alternative, description
        FROM characters
    """)


def upsert_anime(conn: sqlite3.Connection, anime: dict):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT INTO anime (
            id, id_mal, title_romaji, title_english, title_native,
            description, cover_large, cover_color, banner_image,
            episodes, duration, status, format, season, season_year,
            average_score, mean_score, popularity, favourites, trending,
            next_airing_episode, next_airing_at, start_date, end_date,
            source, hashtag, country_of_origin, is_adult, created_at, updated_at, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            id_mal=excluded.id_mal,
            title_romaji=excluded.title_romaji,
            title_english=excluded.title_english,
            title_native=excluded.title_native,
            description=excluded.description,
            cover_large=excluded.cover_large,
            cover_color=excluded.cover_color,
            banner_image=excluded.banner_image,
            episodes=excluded.episodes,
            duration=excluded.duration,
            status=excluded.status,
            format=excluded.format,
            season=excluded.season,
            season_year=excluded.season_year,
            average_score=excluded.average_score,
            mean_score=excluded.mean_score,
            popularity=excluded.popularity,
            favourites=excluded.favourites,
            trending=excluded.trending,
            next_airing_episode=excluded.next_airing_episode,
            next_airing_at=excluded.next_airing_at,
            start_date=excluded.start_date,
            end_date=excluded.end_date,
            source=excluded.source,
            hashtag=excluded.hashtag,
            country_of_origin=excluded.country_of_origin,
            is_adult=excluded.is_adult,
            created_at=excluded.created_at,
            updated_at=excluded.updated_at,
            fetched_at=excluded.fetched_at
    """, (
        anime.get("id"),
        anime.get("idMal"),
        anime.get("title", {}).get("romaji"),
        anime.get("title", {}).get("english"),
        anime.get("title", {}).get("native"),
        anime.get("description"),
        anime.get("coverImage", {}).get("large"),
        anime.get("coverImage", {}).get("color"),
        anime.get("bannerImage"),
        anime.get("episodes"),
        anime.get("duration"),
        anime.get("status"),
        anime.get("format"),
        anime.get("season"),
        anime.get("seasonYear"),
        anime.get("averageScore"),
        anime.get("meanScore"),
        anime.get("popularity"),
        anime.get("favourites"),
        anime.get("trending"),
        anime.get("nextAiringEpisode", {}).get("episode") if anime.get("nextAiringEpisode") else None,
        anime.get("nextAiringEpisode", {}).get("airingAt") if anime.get("nextAiringEpisode") else None,
        _format_date(anime.get("startDate")),
        _format_date(anime.get("endDate")),
        anime.get("source"),
        anime.get("hashtag"),
        anime.get("countryOfOrigin"),
        1 if anime.get("isAdult") else 0,
        anime.get("createdAt"),
        anime.get("updatedAt"),
        now
    ))


def upsert_anime_titles(conn: sqlite3.Connection, anime_id: int, title: dict):
    for lang, key in [("romaji", "title_romaji"), ("english", "title_english"), ("native", "title_native")]:
        if title.get(lang):
            conn.execute("""
                INSERT INTO anime_titles (anime_id, language, title)
                VALUES (?, ?, ?)
                ON CONFLICT(anime_id, language) DO UPDATE SET title=excluded.title
            """, (anime_id, lang, title[lang]))

    other_titles = title.get("alternative", []) or []
    for i, alt_title in enumerate(other_titles):
        if alt_title:
            conn.execute("""
                INSERT INTO anime_titles (anime_id, language, title)
                VALUES (?, ?, ?)
                ON CONFLICT(anime_id, language) DO UPDATE SET title=excluded.title
            """, (anime_id, f"alternative_{i}", alt_title))


def upsert_anime_descriptions(conn: sqlite3.Connection, anime_id: int, description: str, synonyms: list):
    if description:
        conn.execute("""
            INSERT INTO anime_descriptions (anime_id, language, description)
            VALUES (?, 'default', ?)
            ON CONFLICT(anime_id, language) DO UPDATE SET description=excluded.description
        """, (anime_id, description))

    if synonyms:
        conn.execute("""
            INSERT INTO anime_titles (anime_id, language, title)
            VALUES (?, 'synonyms', ?)
            ON CONFLICT(anime_id, language) DO UPDATE SET title=excluded.title
        """, (anime_id, "|".join(synonyms)))


def upsert_genres(conn: sqlite3.Connection, anime_id: int, genres: list):
    for genre_name in genres:
        conn.execute("INSERT OR IGNORE INTO genres (name) VALUES (?)", (genre_name,))
        genre_id = conn.execute("SELECT id FROM genres WHERE name=?", (genre_name,)).fetchone()[0]
        conn.execute("""
            INSERT OR IGNORE INTO anime_genres (anime_id, genre_id) VALUES (?, ?)
        """, (anime_id, genre_id))


def upsert_tags(conn: sqlite3.Connection, anime_id: int, tags: list):
    for tag in tags:
        tag_name = tag.get("name", "")
        tag_rank = tag.get("rank", 0)
        tag_category = tag.get("tagName", "")
        tag_desc = tag.get("description", "")
        conn.execute("""
            INSERT INTO tags (name, category, description, rank)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                category=COALESCE(excluded.category, category),
                description=COALESCE(excluded.description, description),
                rank=COALESCE(excluded.rank, rank)
        """, (tag_name, tag_category, tag_desc, tag_rank))
        conn.execute("""
            INSERT INTO anime_tags (anime_id, tag_name, tag_rank) VALUES (?, ?, ?)
            ON CONFLICT(anime_id, tag_name) DO UPDATE SET tag_rank=excluded.tag_rank
        """, (anime_id, tag_name, tag_rank))


def upsert_studios(conn: sqlite3.Connection, anime_id: int, studios_data: dict):
    for edge in studios_data.get("edges", []):
        node = edge.get("node") or {}
        studio_id = node.get("id")
        studio_name = node.get("name")
        is_main = 1 if edge.get("isMain") else 0
        if studio_id and studio_name:
            conn.execute("""
                INSERT INTO studios (id, name) VALUES (?, ?)
                ON CONFLICT(id) DO UPDATE SET name=excluded.name
            """, (studio_id, studio_name))
            conn.execute("""
                INSERT INTO anime_studios (anime_id, studio_id, is_main) VALUES (?, ?, ?)
                ON CONFLICT(anime_id, studio_id) DO UPDATE SET is_main=excluded.is_main
            """, (anime_id, studio_id, is_main))


def upsert_characters(conn: sqlite3.Connection, anime_id: int, characters_data: dict):
    for i, edge in enumerate(characters_data.get("edges", [])):
        node = edge.get("node", {})
        char_id = node.get("id")
        if not char_id:
            continue

        name = node.get("name", {})
        conn.execute("""
            INSERT INTO characters (
                id, name_full, name_native, name_alternative, image_large, image_medium,
                description, favourites, gender, date_of_birth, age
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name_full=excluded.name_full, name_native=excluded.name_native,
                name_alternative=excluded.name_alternative, image_large=excluded.image_large,
                image_medium=excluded.image_medium, description=excluded.description,
                favourites=excluded.favourites, gender=excluded.gender,
                date_of_birth=excluded.date_of_birth, age=excluded.age
        """, (
            char_id,
            name.get("full"),
            name.get("native"),
            "|".join(name.get("alternative", []) or []),
            node.get("image", {}).get("large"),
            node.get("image", {}).get("medium"),
            node.get("description"),
            node.get("favourites"),
            node.get("gender"),
            _format_date(node.get("dateOfBirth")),
            node.get("age")
        ))

        role = edge.get("role")
        conn.execute("""
            INSERT INTO anime_characters (anime_id, character_id, role, sort_order)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(anime_id, character_id) DO UPDATE SET role=excluded.role, sort_order=excluded.sort_order
        """, (anime_id, char_id, role, i))

        for va in edge.get("voiceActors", []):
            va_id = va.get("id")
            if not va_id:
                continue
            va_name = va.get("name", {})
            conn.execute("""
                INSERT INTO voice_actors (id, name_full, name_native, image_large, image_medium, favourites)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name_full=excluded.name_full, name_native=excluded.name_native,
                    image_large=excluded.image_large, image_medium=excluded.image_medium,
                    favourites=excluded.favourites
            """, (
                va_id,
                va_name.get("full"),
                va_name.get("native"),
                va.get("image", {}).get("large"),
                va.get("image", {}).get("medium"),
                va.get("favourites")
            ))
            conn.execute("""
                INSERT OR IGNORE INTO character_voice_actors (character_id, voice_actor_id, anime_id, language)
                VALUES (?, ?, ?, ?)
            """, (char_id, va_id, anime_id, va.get("language", "Japanese")))


def upsert_relations(conn: sqlite3.Connection, anime_id: int, relations_data: dict):
    conn.execute("DELETE FROM relations WHERE anime_id=?", (anime_id,))
    for edge in relations_data.get("edges", []):
        node = edge.get("node") or {}
        related_id = node.get("id")
        rel_type = edge.get("relationType")
        if related_id and rel_type:
            try:
                conn.execute("""
                    INSERT INTO relations (anime_id, related_anime_id, relation_type)
                    VALUES (?, ?, ?)
                """, (anime_id, related_id, rel_type))
            except Exception:
                pass


def upsert_recommendations(conn: sqlite3.Connection, anime_id: int, recommendations_data: dict):
    conn.execute("DELETE FROM recommendations WHERE anime_id=?", (anime_id,))
    for edge in recommendations_data.get("edges", []):
        node = edge.get("node", {})
        if not node:
            continue
        rec = node.get("mediaRecommendation") or {}
        rec_id = rec.get("id")
        rating = node.get("rating")
        user_rating = node.get("userRating")
        if rec_id:
            try:
                conn.execute("""
                    INSERT INTO recommendations (anime_id, recommended_anime_id, rating, user_rating)
                    VALUES (?, ?, ?, ?)
                """, (anime_id, rec_id, rating, user_rating))
            except Exception:
                pass


def upsert_airing_schedule(conn: sqlite3.Connection, anime_id: int, schedule_data: list):
    conn.execute("DELETE FROM airing_schedule WHERE anime_id=?", (anime_id,))
    for ep in schedule_data:
        conn.execute("""
            INSERT INTO airing_schedule (anime_id, episode, airing_at, time_until_airing)
            VALUES (?, ?, ?, ?)
        """, (anime_id, ep.get("episode"), ep.get("airingAt"), ep.get("timeUntilAiring")))


def upsert_external_links(conn: sqlite3.Connection, anime_id: int, links: list):
    conn.execute("DELETE FROM external_links WHERE anime_id=?", (anime_id,))
    for link in links or []:
        conn.execute("""
            INSERT INTO external_links (anime_id, site, url, language, color, icon, is_disabled, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            anime_id, link.get("site"), link.get("url"), link.get("language"),
            link.get("color"), link.get("icon"),
            1 if link.get("isDisabled") else 0, link.get("notes")
        ))


def upsert_streaming_episodes(conn: sqlite3.Connection, anime_id: int, episodes: list):
    conn.execute("DELETE FROM streaming_episodes WHERE anime_id=?", (anime_id,))
    for ep in episodes or []:
        conn.execute("""
            INSERT INTO streaming_episodes (anime_id, title, thumbnail, site)
            VALUES (?, ?, ?, ?)
        """, (anime_id, ep.get("title"), ep.get("thumbnail"), ep.get("site")))


def upsert_statistics(conn: sqlite3.Connection, anime_id: int, stats: dict):
    if stats:
        conn.execute("""
            INSERT INTO statistics (anime_id, score_distribution, rankings, trends)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(anime_id) DO UPDATE SET
                score_distribution=excluded.score_distribution,
                rankings=excluded.rankings,
                trends=excluded.trends
        """, (
            anime_id,
            json.dumps(stats.get("scoreDistribution")),
            json.dumps(stats.get("rankings")),
            json.dumps(stats.get("trends"))
        ))


def set_metadata(conn: sqlite3.Connection, key: str, value: str):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT INTO sync_metadata (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
    """, (key, value, now))


def get_metadata(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM sync_metadata WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def generate_changelog(conn: sqlite3.Connection, old_data: dict, new_data: dict) -> str:
    changes = []
    changes.append(f"# Changelog - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n")

    new_ids = set(new_data.keys())
    old_ids = set(old_data.keys())

    added = new_ids - old_ids
    removed = old_ids - new_ids
    updated = new_ids & old_ids

    if added:
        changes.append(f"\n## Added ({len(added)} anime)\n")
        for aid in sorted(added)[:100]:
            title = new_data[aid].get("title_romaji", "Unknown")
            changes.append(f"- [{aid}] {title}")
        if len(added) > 100:
            changes.append(f"- ... and {len(added) - 100} more")

    if removed:
        changes.append(f"\n## Removed ({len(removed)} anime)\n")
        for aid in sorted(removed)[:100]:
            title = old_data[aid].get("title_romaji", "Unknown")
            changes.append(f"- [{aid}] {title}")
        if len(removed) > 100:
            changes.append(f"- ... and {len(removed) - 100} more")

    field_changes = []
    for aid in sorted(updated):
        old = old_data[aid]
        new = new_data[aid]
        diffs = []
        for field in ["title_romaji", "title_english", "episodes", "status", "average_score", "popularity", "favourites", "trending"]:
            if old.get(field) != new.get(field):
                diffs.append(f"  {field}: {old.get(field)} -> {new.get(field)}")
        if diffs:
            field_changes.append((aid, new.get("title_romaji", "Unknown"), diffs))

    if field_changes:
        changes.append(f"\n## Updated ({len(field_changes)} anime)\n")
        for aid, title, diffs in field_changes[:100]:
            changes.append(f"- [{aid}] {title}")
            for d in diffs:
                changes.append(d)
        if len(field_changes) > 100:
            changes.append(f"- ... and {len(field_changes) - 100} more")

    changes.append(f"\n## Summary\n")
    changes.append(f"- Total anime in database: {len(new_ids)}")
    changes.append(f"- Added: {len(added)}")
    changes.append(f"- Removed: {len(removed)}")
    changes.append(f"- Updated: {len(field_changes)}")

    return "\n".join(changes)


def export_json(conn: sqlite3.Connection, output_path: str):
    rows = conn.execute("""
        SELECT
            a.*,
            GROUP_CONCAT(DISTINCT g.name) as genres_list,
            GROUP_CONCAT(DISTINCT s.name) as studios_list
        FROM anime a
        LEFT JOIN anime_genres ag ON a.id = ag.anime_id
        LEFT JOIN genres g ON ag.genre_id = g.id
        LEFT JOIN anime_studios ast ON a.id = ast.anime_id
        LEFT JOIN studios s ON ast.studio_id = s.id
        GROUP BY a.id
        ORDER BY a.id
    """).fetchall()

    columns = [desc[0] for desc in conn.execute("SELECT * FROM anime LIMIT 0").description]
    anime_list = []
    for row in rows:
        anime_dict = dict(zip(columns, row))
        anime_dict["genres"] = (anime_dict.pop("genres_list") or "").split(",") if anime_dict.get("genres_list") else []
        anime_dict["studios"] = (anime_dict.pop("studios_list") or "").split(",") if anime_dict.get("studios_list") else []

        anime_dict["titles"] = {}
        for t in conn.execute("SELECT language, title FROM anime_titles WHERE anime_id=?", (anime_dict["id"],)):
            anime_dict["titles"][t[0]] = t[1]

        anime_dict["descriptions"] = {}
        for d in conn.execute("SELECT language, description FROM anime_descriptions WHERE anime_id=?", (anime_dict["id"],)):
            anime_dict["descriptions"][d[0]] = d[1]

        anime_dict["characters"] = []
        for c in conn.execute("""
            SELECT c.id, c.name_full, c.name_native, c.image_large, ac.role
            FROM characters c
            JOIN anime_characters ac ON c.id = ac.character_id
            WHERE ac.anime_id=?
            ORDER BY ac.sort_order
        """, (anime_dict["id"],)):
            anime_dict["characters"].append({
                "id": c[0], "name": c[1], "name_native": c[2], "image": c[3], "role": c[4]
            })

        anime_dict["relations"] = []
        for r in conn.execute("""
            SELECT r.relation_type, a.id, a.title_romaji, a.cover_large
            FROM relations r
            LEFT JOIN anime a ON r.related_anime_id = a.id
            WHERE r.anime_id=?
        """, (anime_dict["id"],)):
            anime_dict["relations"].append({
                "relationType": r[0], "id": r[1], "title": r[2], "coverImage": r[3]
            })

        anime_dict["recommendations"] = []
        for rec in conn.execute("""
            SELECT r.recommended_anime_id, a.title_romaji, a.cover_large, r.rating
            FROM recommendations r
            LEFT JOIN anime a ON r.recommended_anime_id = a.id
            WHERE r.anime_id=?
            ORDER BY r.rating DESC
        """, (anime_dict["id"],)):
            anime_dict["recommendations"].append({
                "id": rec[0], "title": rec[1], "coverImage": rec[2], "rating": rec[3]
            })

        anime_list.append(anime_dict)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({
            "totalCount": len(anime_list),
            "exportedAt": datetime.now(timezone.utc).isoformat(),
            "anime": anime_list
        }, f, ensure_ascii=False, indent=2)


def get_database_stats(conn: sqlite3.Connection) -> dict:
    stats = {}
    stats["total_anime"] = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
    stats["total_characters"] = conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]
    stats["total_studios"] = conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0]
    stats["total_genres"] = conn.execute("SELECT COUNT(*) FROM genres").fetchone()[0]
    stats["total_tags"] = conn.execute("SELECT COUNT(*) FROM tags").fetchone()[0]

    stats["by_status"] = {}
    for row in conn.execute("SELECT status, COUNT(*) FROM anime GROUP BY status"):
        stats["by_status"][row[0] or "UNKNOWN"] = row[1]

    stats["by_format"] = {}
    for row in conn.execute("SELECT format, COUNT(*) FROM anime GROUP BY format"):
        stats["by_format"][row[0] or "UNKNOWN"] = row[1]

    stats["by_season"] = {}
    for row in conn.execute("SELECT season, season_year, COUNT(*) FROM anime WHERE season IS NOT NULL GROUP BY season, season_year ORDER BY season_year DESC, season"):
        stats["by_season"][f"{row[0]} {row[1]}"] = row[2]

    return stats


def _format_date(date_obj: dict) -> Optional[str]:
    if not date_obj:
        return None
    year = date_obj.get("year")
    month = date_obj.get("month")
    day = date_obj.get("day")
    if year:
        if month and day:
            return f"{year:04d}-{month:02d}-{day:02d}"
        elif month:
            return f"{year:04d}-{month:02d}"
        else:
            return f"{year:04d}"
    return None
