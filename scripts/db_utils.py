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
    type TEXT DEFAULT 'ANIME',
    title_romaji TEXT,
    title_english TEXT,
    title_native TEXT,
    title_user_preferred TEXT,
    description TEXT,
    cover_extra_large TEXT,
    cover_large TEXT,
    cover_medium TEXT,
    cover_color TEXT,
    banner_image TEXT,
    episodes INTEGER,
    duration INTEGER,
    chapters INTEGER,
    volumes INTEGER,
    status TEXT,
    format TEXT,
    season TEXT,
    season_year INTEGER,
    season_int INTEGER,
    average_score REAL,
    mean_score REAL,
    popularity INTEGER,
    favourites INTEGER,
    trending INTEGER,
    is_locked INTEGER DEFAULT 0,
    is_adult INTEGER DEFAULT 0,
    is_licensed INTEGER,
    source TEXT,
    hashtag TEXT,
    trailer_id TEXT,
    trailer_site TEXT,
    trailer_thumbnail TEXT,
    site_url TEXT,
    country_of_origin TEXT,
    next_airing_episode INTEGER,
    next_airing_at INTEGER,
    start_date TEXT,
    end_date TEXT,
    start_year INTEGER,
    start_month INTEGER,
    start_day INTEGER,
    end_year INTEGER,
    end_month INTEGER,
    end_day INTEGER,
    synonyms TEXT,
    is_favourite_blocked INTEGER DEFAULT 0,
    auto_create_forum_thread INTEGER,
    is_recommendation_blocked INTEGER,
    is_review_blocked INTEGER,
    mod_notes TEXT,
    created_at TEXT,
    updated_at TEXT,
    fetched_at TEXT,
    raw_json TEXT
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
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    category TEXT,
    rank INTEGER,
    is_general_spoiler INTEGER DEFAULT 0,
    is_media_spoiler INTEGER DEFAULT 0,
    is_adult INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS anime_tags (
    anime_id INTEGER NOT NULL,
    tag_name TEXT NOT NULL,
    tag_rank INTEGER,
    PRIMARY KEY (anime_id, tag_name),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS staff (
    id INTEGER PRIMARY KEY,
    name_first TEXT,
    name_middle TEXT,
    name_last TEXT,
    name_full TEXT,
    name_native TEXT,
    name_alternative TEXT,
    name_user_preferred TEXT,
    language TEXT,
    image_large TEXT,
    image_medium TEXT,
    description TEXT,
    primary_occupations TEXT,
    gender TEXT,
    date_of_birth TEXT,
    date_of_death TEXT,
    age INTEGER,
    years_active TEXT,
    home_town TEXT,
    blood_type TEXT,
    is_favourite INTEGER DEFAULT 0,
    is_favourite_blocked INTEGER DEFAULT 0,
    site_url TEXT,
    favourites INTEGER
);

CREATE TABLE IF NOT EXISTS anime_staff (
    anime_id INTEGER NOT NULL,
    staff_id INTEGER NOT NULL,
    edge_id INTEGER,
    role TEXT,
    favourite_order INTEGER,
    sort_order INTEGER DEFAULT 0,
    PRIMARY KEY (anime_id, staff_id, role),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE,
    FOREIGN KEY (staff_id) REFERENCES staff(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS studios (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    is_animation_studio INTEGER DEFAULT 1,
    site_url TEXT,
    favourites INTEGER
);

CREATE TABLE IF NOT EXISTS anime_studios (
    anime_id INTEGER NOT NULL,
    studio_id INTEGER NOT NULL,
    edge_id INTEGER,
    is_main INTEGER DEFAULT 0,
    favourite_order INTEGER,
    PRIMARY KEY (anime_id, studio_id),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE,
    FOREIGN KEY (studio_id) REFERENCES studios(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS characters (
    id INTEGER PRIMARY KEY,
    name_first TEXT,
    name_middle TEXT,
    name_last TEXT,
    name_full TEXT,
    name_native TEXT,
    name_alternative TEXT,
    name_alternative_spoiler TEXT,
    name_user_preferred TEXT,
    image_large TEXT,
    image_medium TEXT,
    description TEXT,
    gender TEXT,
    date_of_birth TEXT,
    age TEXT,
    blood_type TEXT,
    is_favourite INTEGER DEFAULT 0,
    is_favourite_blocked INTEGER DEFAULT 0,
    site_url TEXT,
    favourites INTEGER
);

CREATE TABLE IF NOT EXISTS anime_characters (
    anime_id INTEGER NOT NULL,
    character_id INTEGER NOT NULL,
    edge_id INTEGER,
    role TEXT,
    favourite_order INTEGER,
    sort_order INTEGER DEFAULT 0,
    PRIMARY KEY (anime_id, character_id),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE,
    FOREIGN KEY (character_id) REFERENCES characters(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS voice_actors (
    id INTEGER PRIMARY KEY,
    name_first TEXT,
    name_middle TEXT,
    name_last TEXT,
    name_full TEXT,
    name_native TEXT,
    name_alternative TEXT,
    name_user_preferred TEXT,
    language TEXT,
    image_large TEXT,
    image_medium TEXT,
    description TEXT,
    primary_occupations TEXT,
    gender TEXT,
    date_of_birth TEXT,
    date_of_death TEXT,
    age INTEGER,
    years_active TEXT,
    home_town TEXT,
    blood_type TEXT,
    is_favourite INTEGER DEFAULT 0,
    is_favourite_blocked INTEGER DEFAULT 0,
    site_url TEXT,
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
    id INTEGER PRIMARY KEY,
    anime_id INTEGER NOT NULL,
    episode INTEGER NOT NULL,
    airing_at INTEGER NOT NULL,
    time_until_airing INTEGER,
    media_id INTEGER,
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS external_links (
    id INTEGER PRIMARY KEY,
    anime_id INTEGER NOT NULL,
    site TEXT NOT NULL,
    url TEXT,
    type TEXT,
    language TEXT,
    color TEXT,
    icon TEXT,
    notes TEXT,
    is_disabled INTEGER DEFAULT 0,
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS streaming_episodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER NOT NULL,
    title TEXT,
    thumbnail TEXT,
    url TEXT,
    site TEXT,
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS rankings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER NOT NULL,
    rank_id INTEGER,
    rank INTEGER,
    type TEXT,
    format TEXT,
    year INTEGER,
    season TEXT,
    all_time INTEGER DEFAULT 0,
    context TEXT,
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS trends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER NOT NULL,
    media_id INTEGER,
    date INTEGER,
    trending INTEGER,
    average_score REAL,
    popularity INTEGER,
    episode INTEGER,
    releasing INTEGER,
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS reviews (
    id INTEGER PRIMARY KEY,
    anime_id INTEGER NOT NULL,
    user_id INTEGER,
    user_name TEXT,
    summary TEXT,
    rating INTEGER,
    user_rating TEXT,
    score INTEGER,
    body TEXT,
    created_at INTEGER,
    updated_at INTEGER,
    site_url TEXT,
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
    _migrate(conn)
    return conn


def _migrate(conn: sqlite3.Connection):
    """Add new columns/tables to old DBs. Safe to run every time."""
    anime_cols = [r[1] for r in conn.execute("PRAGMA table_info(anime)").fetchall()]
    def add_col(table, col, ddl):
        if col not in [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")
            except Exception:
                pass
    for col, ddl in [
        ("type", "TEXT DEFAULT 'ANIME'"),
        ("title_user_preferred", "TEXT"),
        ("cover_extra_large", "TEXT"),
        ("cover_medium", "TEXT"),
        ("chapters", "INTEGER"),
        ("volumes", "INTEGER"),
        ("is_licensed", "INTEGER"),
        ("season_int", "INTEGER"),
        ("site_url", "TEXT"),
        ("trailer_id", "TEXT"),
        ("trailer_site", "TEXT"),
        ("trailer_thumbnail", "TEXT"),
        ("is_locked", "INTEGER DEFAULT 0"),
        ("is_favourite_blocked", "INTEGER DEFAULT 0"),
        ("auto_create_forum_thread", "INTEGER"),
        ("is_recommendation_blocked", "INTEGER"),
        ("is_review_blocked", "INTEGER"),
        ("mod_notes", "TEXT"),
        ("start_year", "INTEGER"),
        ("start_month", "INTEGER"),
        ("start_day", "INTEGER"),
        ("end_year", "INTEGER"),
        ("end_month", "INTEGER"),
        ("end_day", "INTEGER"),
        ("synonyms", "TEXT"),
        ("raw_json", "TEXT"),
    ]:
        if col not in anime_cols:
            add_col("anime", col, ddl)
    # new tables for old DBs
    conn.executescript("""
CREATE TABLE IF NOT EXISTS staff (
    id INTEGER PRIMARY KEY, name_first TEXT, name_middle TEXT, name_last TEXT,
    name_full TEXT, name_native TEXT, name_alternative TEXT, name_user_preferred TEXT,
    language TEXT, image_large TEXT, image_medium TEXT, description TEXT,
    primary_occupations TEXT, gender TEXT, date_of_birth TEXT, date_of_death TEXT,
    age INTEGER, years_active TEXT, home_town TEXT, blood_type TEXT,
    is_favourite INTEGER DEFAULT 0, is_favourite_blocked INTEGER DEFAULT 0,
    site_url TEXT, favourites INTEGER
);
CREATE TABLE IF NOT EXISTS anime_staff (
    anime_id INTEGER NOT NULL, staff_id INTEGER NOT NULL, edge_id INTEGER,
    role TEXT, favourite_order INTEGER, sort_order INTEGER DEFAULT 0,
    PRIMARY KEY (anime_id, staff_id, role)
);
CREATE TABLE IF NOT EXISTS rankings (
    id INTEGER PRIMARY KEY AUTOINCREMENT, anime_id INTEGER NOT NULL, rank_id INTEGER,
    rank INTEGER, type TEXT, format TEXT, year INTEGER, season TEXT,
    all_time INTEGER DEFAULT 0, context TEXT
);
CREATE TABLE IF NOT EXISTS trends (
    id INTEGER PRIMARY KEY AUTOINCREMENT, anime_id INTEGER NOT NULL, media_id INTEGER,
    date INTEGER, trending INTEGER, average_score REAL, popularity INTEGER,
    episode INTEGER, releasing INTEGER
);
CREATE TABLE IF NOT EXISTS reviews (
    id INTEGER PRIMARY KEY, anime_id INTEGER NOT NULL, user_id INTEGER, user_name TEXT,
    summary TEXT, rating INTEGER, user_rating TEXT, score INTEGER, body TEXT,
    created_at INTEGER, updated_at INTEGER, site_url TEXT
);
""")
    for t, c, ddl in [
        ("characters", "name_first", "TEXT"), ("characters", "name_middle", "TEXT"),
        ("characters", "name_last", "TEXT"), ("characters", "name_alternative_spoiler", "TEXT"),
        ("characters", "name_user_preferred", "TEXT"), ("characters", "blood_type", "TEXT"),
        ("characters", "is_favourite", "INTEGER DEFAULT 0"),
        ("characters", "is_favourite_blocked", "INTEGER DEFAULT 0"),
        ("characters", "site_url", "TEXT"),
        ("voice_actors", "name_first", "TEXT"), ("voice_actors", "name_middle", "TEXT"),
        ("voice_actors", "name_last", "TEXT"), ("voice_actors", "name_alternative", "TEXT"),
        ("voice_actors", "name_user_preferred", "TEXT"), ("voice_actors", "language", "TEXT"),
        ("voice_actors", "description", "TEXT"), ("voice_actors", "primary_occupations", "TEXT"),
        ("voice_actors", "gender", "TEXT"), ("voice_actors", "date_of_birth", "TEXT"),
        ("voice_actors", "date_of_death", "TEXT"), ("voice_actors", "age", "INTEGER"),
        ("voice_actors", "years_active", "TEXT"), ("voice_actors", "home_town", "TEXT"),
        ("voice_actors", "blood_type", "TEXT"), ("voice_actors", "site_url", "TEXT"),
        ("studios", "site_url", "TEXT"), ("studios", "favourites", "INTEGER"),
        ("anime_characters", "edge_id", "INTEGER"), ("anime_characters", "favourite_order", "INTEGER"),
        ("anime_studios", "edge_id", "INTEGER"), ("anime_studios", "favourite_order", "INTEGER"),
        ("airing_schedule", "media_id", "INTEGER"),
        ("external_links", "id", "INTEGER"), ("external_links", "type", "TEXT"),
        ("streaming_episodes", "url", "TEXT"),
        ("tags", "is_general_spoiler", "INTEGER DEFAULT 0"),
        ("tags", "is_media_spoiler", "INTEGER DEFAULT 0"),
        ("tags", "is_adult", "INTEGER DEFAULT 0"),
    ]:
        add_col(t, c, ddl)
    try:
        conn.commit()
    except Exception:
        pass


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
    import json as _json
    now = datetime.now(timezone.utc).isoformat()
    title = anime.get("title") or {}
    cover = anime.get("coverImage") or {}
    trailer = anime.get("trailer") or {}
    start = anime.get("startDate") or {}
    end = anime.get("endDate") or {}
    nxt = anime.get("nextAiringEpisode") or {}
    synonyms = anime.get("synonyms") or []
    raw = _json.dumps(anime, ensure_ascii=False)
    conn.execute("""
        INSERT INTO anime (
            id, id_mal, type, title_romaji, title_english, title_native, title_user_preferred,
            description, cover_extra_large, cover_large, cover_medium, cover_color, banner_image,
            episodes, duration, chapters, volumes, status, format, season, season_year, season_int,
            average_score, mean_score, popularity, favourites, trending,
            is_locked, is_adult, is_licensed, source, hashtag,
            trailer_id, trailer_site, trailer_thumbnail, site_url, country_of_origin,
            next_airing_episode, next_airing_at, start_date, end_date,
            start_year, start_month, start_day, end_year, end_month, end_day,
            synonyms, is_favourite_blocked, auto_create_forum_thread,
            is_recommendation_blocked, is_review_blocked, mod_notes,
            created_at, updated_at, fetched_at, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            id_mal=excluded.id_mal, type=excluded.type,
            title_romaji=excluded.title_romaji, title_english=excluded.title_english,
            title_native=excluded.title_native, title_user_preferred=excluded.title_user_preferred,
            description=excluded.description,
            cover_extra_large=excluded.cover_extra_large, cover_large=excluded.cover_large,
            cover_medium=excluded.cover_medium, cover_color=excluded.cover_color,
            banner_image=excluded.banner_image,
            episodes=excluded.episodes, duration=excluded.duration,
            chapters=excluded.chapters, volumes=excluded.volumes,
            status=excluded.status, format=excluded.format,
            season=excluded.season, season_year=excluded.season_year, season_int=excluded.season_int,
            average_score=excluded.average_score, mean_score=excluded.mean_score,
            popularity=excluded.popularity, favourites=excluded.favourites, trending=excluded.trending,
            is_locked=excluded.is_locked, is_adult=excluded.is_adult, is_licensed=excluded.is_licensed,
            source=excluded.source, hashtag=excluded.hashtag,
            trailer_id=excluded.trailer_id, trailer_site=excluded.trailer_site,
            trailer_thumbnail=excluded.trailer_thumbnail, site_url=excluded.site_url,
            country_of_origin=excluded.country_of_origin,
            next_airing_episode=excluded.next_airing_episode, next_airing_at=excluded.next_airing_at,
            start_date=excluded.start_date, end_date=excluded.end_date,
            start_year=excluded.start_year, start_month=excluded.start_month, start_day=excluded.start_day,
            end_year=excluded.end_year, end_month=excluded.end_month, end_day=excluded.end_day,
            synonyms=excluded.synonyms, is_favourite_blocked=excluded.is_favourite_blocked,
            auto_create_forum_thread=excluded.auto_create_forum_thread,
            is_recommendation_blocked=excluded.is_recommendation_blocked,
            is_review_blocked=excluded.is_review_blocked, mod_notes=excluded.mod_notes,
            created_at=excluded.created_at, updated_at=excluded.updated_at,
            fetched_at=excluded.fetched_at, raw_json=excluded.raw_json
    """, (
        anime.get("id"),
        anime.get("idMal"),
        anime.get("type") or "ANIME",
        title.get("romaji"), title.get("english"), title.get("native"), title.get("userPreferred"),
        anime.get("description"),
        cover.get("extraLarge"), cover.get("large"), cover.get("medium"), cover.get("color"),
        anime.get("bannerImage"),
        anime.get("episodes"), anime.get("duration"),
        anime.get("chapters"), anime.get("volumes"),
        anime.get("status"), anime.get("format"),
        anime.get("season"), anime.get("seasonYear"), anime.get("seasonInt"),
        anime.get("averageScore"), anime.get("meanScore"),
        anime.get("popularity"), anime.get("favourites"), anime.get("trending"),
        1 if anime.get("isLocked") else 0,
        1 if anime.get("isAdult") else 0,
        1 if anime.get("isLicensed") else (0 if anime.get("isLicensed") is False else None),
        anime.get("source"), anime.get("hashtag"),
        str(trailer.get("id")) if trailer.get("id") is not None else None,
        trailer.get("site"), trailer.get("thumbnail"),
        anime.get("siteUrl"), anime.get("countryOfOrigin"),
        nxt.get("episode"), nxt.get("airingAt"),
        _format_date(start), _format_date(end),
        start.get("year"), start.get("month"), start.get("day"),
        end.get("year"), end.get("month"), end.get("day"),
        _json.dumps(synonyms, ensure_ascii=False),
        1 if anime.get("isFavouriteBlocked") else 0,
        1 if anime.get("autoCreateForumThread") else (0 if anime.get("autoCreateForumThread") is False else None),
        1 if anime.get("isRecommendationBlocked") else (0 if anime.get("isRecommendationBlocked") is False else None),
        1 if anime.get("isReviewBlocked") else (0 if anime.get("isReviewBlocked") is False else None),
        anime.get("modNotes"),
        anime.get("createdAt"), anime.get("updatedAt"), now, raw
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
    # synonyms live on anime.synonyms (JSON) — do NOT pollute anime_titles


def upsert_genres(conn: sqlite3.Connection, anime_id: int, genres: list):
    for genre_name in genres:
        conn.execute("INSERT OR IGNORE INTO genres (name) VALUES (?)", (genre_name,))
        genre_id = conn.execute("SELECT id FROM genres WHERE name=?", (genre_name,)).fetchone()[0]
        conn.execute("""
            INSERT OR IGNORE INTO anime_genres (anime_id, genre_id) VALUES (?, ?)
        """, (anime_id, genre_id))


def upsert_tags(conn: sqlite3.Connection, anime_id: int, tags: list):
    for tag in tags or []:
        tag_name = tag.get("name", "")
        if not tag_name:
            continue
        conn.execute("""
            INSERT INTO tags (id, name, description, category, rank, is_general_spoiler, is_media_spoiler, is_adult)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                id=COALESCE(excluded.id, id),
                description=COALESCE(excluded.description, description),
                category=COALESCE(excluded.category, category),
                rank=COALESCE(excluded.rank, rank),
                is_general_spoiler=excluded.is_general_spoiler,
                is_media_spoiler=excluded.is_media_spoiler,
                is_adult=excluded.is_adult
        """, (
            tag.get("id"), tag_name, tag.get("description"),
            tag.get("category"), tag.get("rank", 0),
            1 if tag.get("isGeneralSpoiler") else 0,
            1 if tag.get("isMediaSpoiler") else 0,
            1 if tag.get("isAdult") else 0,
        ))
        conn.execute("""
            INSERT INTO anime_tags (anime_id, tag_name, tag_rank) VALUES (?, ?, ?)
            ON CONFLICT(anime_id, tag_name) DO UPDATE SET tag_rank=excluded.tag_rank
        """, (anime_id, tag_name, tag.get("rank", 0)))


def upsert_studios(conn: sqlite3.Connection, anime_id: int, studios_data: dict):
    for edge in (studios_data or {}).get("edges", []):
        node = edge.get("node") or {}
        studio_id = node.get("id")
        studio_name = node.get("name")
        if studio_id and studio_name:
            conn.execute("""
                INSERT INTO studios (id, name, is_animation_studio, site_url, favourites)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name,
                    is_animation_studio=COALESCE(excluded.is_animation_studio, is_animation_studio),
                    site_url=COALESCE(excluded.site_url, site_url),
                    favourites=COALESCE(excluded.favourites, favourites)
            """, (
                studio_id, studio_name,
                1 if node.get("isAnimationStudio") else (0 if node.get("isAnimationStudio") is False else 1),
                node.get("siteUrl"), node.get("favourites"),
            ))
            conn.execute("""
                INSERT INTO anime_studios (anime_id, studio_id, edge_id, is_main, favourite_order)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(anime_id, studio_id) DO UPDATE SET
                    is_main=excluded.is_main, edge_id=COALESCE(excluded.edge_id, edge_id)
            """, (anime_id, studio_id, edge.get("id"), 1 if edge.get("isMain") else 0, edge.get("favouriteOrder")))


def upsert_characters(conn: sqlite3.Connection, anime_id: int, characters_data: dict):
    import json as _json
    for i, edge in enumerate((characters_data or {}).get("edges", [])):
        node = edge.get("node") or {}
        char_id = node.get("id")
        if not char_id:
            continue
        name = node.get("name") or {}
        img = node.get("image") or {}
        conn.execute("""
            INSERT INTO characters (
                id, name_first, name_middle, name_last, name_full, name_native,
                name_alternative, name_alternative_spoiler, name_user_preferred,
                image_large, image_medium, description, favourites, gender,
                date_of_birth, age, blood_type, is_favourite, is_favourite_blocked, site_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name_first=excluded.name_first, name_middle=excluded.name_middle,
                name_last=excluded.name_last, name_full=excluded.name_full,
                name_native=excluded.name_native, name_alternative=excluded.name_alternative,
                name_alternative_spoiler=excluded.name_alternative_spoiler,
                name_user_preferred=excluded.name_user_preferred,
                image_large=excluded.image_large, image_medium=excluded.image_medium,
                description=excluded.description, favourites=excluded.favourites,
                gender=excluded.gender, date_of_birth=excluded.date_of_birth,
                age=excluded.age, blood_type=excluded.blood_type, site_url=excluded.site_url
        """, (
            char_id, name.get("first"), name.get("middle"), name.get("last"),
            name.get("full"), name.get("native"),
            _json.dumps(name.get("alternative") or [], ensure_ascii=False),
            _json.dumps(name.get("alternativeSpoiler") or [], ensure_ascii=False),
            name.get("userPreferred"),
            img.get("large"), img.get("medium"),
            node.get("description"), node.get("favourites"), node.get("gender"),
            _format_date(node.get("dateOfBirth")),
            str(node.get("age")) if node.get("age") is not None else None,
            node.get("bloodType"),
            1 if node.get("isFavourite") else 0,
            1 if node.get("isFavouriteBlocked") else 0,
            node.get("siteUrl"),
        ))
        conn.execute("""
            INSERT INTO anime_characters (anime_id, character_id, edge_id, role, favourite_order, sort_order)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(anime_id, character_id) DO UPDATE SET
                role=excluded.role, sort_order=excluded.sort_order,
                edge_id=COALESCE(excluded.edge_id, edge_id)
        """, (anime_id, char_id, edge.get("id"), edge.get("role"), edge.get("favouriteOrder"), i))
        for va in edge.get("voiceActors") or []:
            va_id = va.get("id")
            if not va_id:
                continue
            va_name = va.get("name") or {}
            va_img = va.get("image") or {}
            conn.execute("""
                INSERT INTO voice_actors (
                    id, name_first, name_middle, name_last, name_full, name_native,
                    name_alternative, name_user_preferred, language,
                    image_large, image_medium, favourites, site_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name_first=COALESCE(excluded.name_first, name_first),
                    name_middle=COALESCE(excluded.name_middle, name_middle),
                    name_last=COALESCE(excluded.name_last, name_last),
                    name_full=COALESCE(excluded.name_full, name_full),
                    name_native=COALESCE(excluded.name_native, name_native),
                    image_large=COALESCE(excluded.image_large, image_large),
                    image_medium=COALESCE(excluded.image_medium, image_medium),
                    favourites=COALESCE(excluded.favourites, favourites)
            """, (
                va_id, va_name.get("first"), va_name.get("middle"), va_name.get("last"),
                va_name.get("full"), va_name.get("native"),
                _json.dumps(va_name.get("alternative") or [], ensure_ascii=False),
                va_name.get("userPreferred"), va.get("language"),
                va_img.get("large"), va_img.get("medium"),
                va.get("favourites"), va.get("siteUrl"),
            ))
            conn.execute("""
                INSERT OR IGNORE INTO character_voice_actors (character_id, voice_actor_id, anime_id, language)
                VALUES (?, ?, ?, ?)
            """, (char_id, va_id, anime_id, va.get("language") or "JAPANESE"))


def upsert_staff(conn: sqlite3.Connection, anime_id: int, staff_data: dict):
    import json as _json
    conn.execute("DELETE FROM anime_staff WHERE anime_id=?", (anime_id,))
    for i, edge in enumerate((staff_data or {}).get("edges", [])):
        node = edge.get("node") or {}
        staff_id = node.get("id")
        if not staff_id:
            continue
        name = node.get("name") or {}
        img = node.get("image") or {}
        conn.execute("""
            INSERT INTO staff (
                id, name_first, name_middle, name_last, name_full, name_native,
                name_alternative, name_user_preferred, language,
                image_large, image_medium, description, primary_occupations,
                gender, date_of_birth, date_of_death, age, years_active,
                home_town, blood_type, site_url, favourites
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name_full=COALESCE(excluded.name_full, name_full),
                name_native=COALESCE(excluded.name_native, name_native),
                image_large=COALESCE(excluded.image_large, image_large),
                description=COALESCE(excluded.description, description),
                favourites=COALESCE(excluded.favourites, favourites),
                site_url=COALESCE(excluded.site_url, site_url)
        """, (
            staff_id, name.get("first"), name.get("middle"), name.get("last"),
            name.get("full"), name.get("native"),
            _json.dumps(name.get("alternative") or [], ensure_ascii=False),
            name.get("userPreferred"), node.get("language"),
            img.get("large"), img.get("medium"), node.get("description"),
            _json.dumps(node.get("primaryOccupations") or [], ensure_ascii=False),
            node.get("gender"), _format_date(node.get("dateOfBirth")),
            _format_date(node.get("dateOfDeath")), node.get("age"),
            _json.dumps(node.get("yearsActive") or [], ensure_ascii=False) if node.get("yearsActive") else None,
            node.get("homeTown"), node.get("bloodType"),
            node.get("siteUrl"), node.get("favourites"),
        ))
        try:
            conn.execute("""
                INSERT INTO anime_staff (anime_id, staff_id, edge_id, role, favourite_order, sort_order)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (anime_id, staff_id, edge.get("id"), edge.get("role"), edge.get("favouriteOrder"), i))
        except Exception:
            pass


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
    for ep in schedule_data or []:
        node = ep.get("node") if isinstance(ep, dict) and "node" in ep else ep
        if not isinstance(node, dict):
            continue
        try:
            conn.execute("""
                INSERT INTO airing_schedule (id, anime_id, episode, airing_at, time_until_airing, media_id)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    anime_id=excluded.anime_id, episode=excluded.episode,
                    airing_at=excluded.airing_at, time_until_airing=excluded.time_until_airing
            """, (
                node.get("id"), anime_id, node.get("episode"),
                node.get("airingAt"), node.get("timeUntilAiring"),
                node.get("mediaId") or anime_id,
            ))
        except Exception:
            conn.execute("""
                INSERT INTO airing_schedule (anime_id, episode, airing_at, time_until_airing, media_id)
                VALUES (?, ?, ?, ?, ?)
            """, (
                anime_id, node.get("episode"), node.get("airingAt"),
                node.get("timeUntilAiring"), node.get("mediaId") or anime_id,
            ))


def upsert_external_links(conn: sqlite3.Connection, anime_id: int, links: list):
    conn.execute("DELETE FROM external_links WHERE anime_id=?", (anime_id,))
    for link in links or []:
        conn.execute("""
            INSERT INTO external_links (id, anime_id, site, url, type, language, color, icon, is_disabled, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            link.get("id"), anime_id, link.get("site"), link.get("url"),
            link.get("type"), link.get("language"),
            link.get("color"), link.get("icon"),
            1 if link.get("isDisabled") else 0, link.get("notes")
        ))


def upsert_streaming_episodes(conn: sqlite3.Connection, anime_id: int, episodes: list):
    conn.execute("DELETE FROM streaming_episodes WHERE anime_id=?", (anime_id,))
    for ep in episodes or []:
        conn.execute("""
            INSERT INTO streaming_episodes (anime_id, title, thumbnail, url, site)
            VALUES (?, ?, ?, ?, ?)
        """, (anime_id, ep.get("title"), ep.get("thumbnail"), ep.get("url"), ep.get("site")))


def upsert_rankings(conn: sqlite3.Connection, anime_id: int, rankings: list):
    conn.execute("DELETE FROM rankings WHERE anime_id=?", (anime_id,))
    for r in rankings or []:
        conn.execute("""
            INSERT INTO rankings (anime_id, rank_id, rank, type, format, year, season, all_time, context)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            anime_id, r.get("id"), r.get("rank"), r.get("type"), r.get("format"),
            r.get("year"), r.get("season"),
            1 if r.get("allTime") else 0, r.get("context"),
        ))


def upsert_trends(conn: sqlite3.Connection, anime_id: int, trends_data: dict):
    conn.execute("DELETE FROM trends WHERE anime_id=?", (anime_id,))
    for edge in (trends_data or {}).get("edges", []):
        node = edge.get("node") or {}
        conn.execute("""
            INSERT INTO trends (anime_id, media_id, date, trending, average_score, popularity, episode, releasing)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            anime_id, node.get("mediaId") or anime_id, node.get("date"),
            node.get("trending"), node.get("averageScore"), node.get("popularity"),
            node.get("episode"), 1 if node.get("releasing") else 0,
        ))


def upsert_reviews(conn: sqlite3.Connection, anime_id: int, reviews_data: dict):
    conn.execute("DELETE FROM reviews WHERE anime_id=?", (anime_id,))
    for edge in (reviews_data or {}).get("edges", []):
        node = edge.get("node") or {}
        if not node.get("id"):
            continue
        user = node.get("user") or {}
        conn.execute("""
            INSERT OR REPLACE INTO reviews
                (id, anime_id, user_id, user_name, summary, rating, user_rating, score, body, created_at, updated_at, site_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            node.get("id"), anime_id, node.get("userId") or user.get("id"),
            user.get("name"), node.get("summary"), node.get("rating"),
            node.get("userRating"), node.get("score"),
            node.get("body") or node.get("bodyHtml"),
            node.get("createdAt"), node.get("updatedAt"), node.get("siteUrl"),
        ))


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
            json.dumps(stats.get("scoreDistribution") or stats.get("score_distribution")),
            json.dumps(stats.get("statusDistribution") or stats.get("status_distribution")),
            json.dumps(stats.get("trends")),
        ))


def build_exact_media(conn: sqlite3.Connection, anime_id: int) -> Optional[dict]:
    """Rebuild AniList-exact Media JSON from normalized rows.
    Prefer stored raw_json (byte-identical); else reconstruct."""
    import json as _json
    row = conn.execute("SELECT raw_json FROM anime WHERE id=?", (anime_id,)).fetchone()
    if row and row[0]:
        try:
            raw = _json.loads(row[0])
            # ensure defaults AniList always returns
            raw.setdefault("type", "ANIME")
            raw.setdefault("isFavourite", False)
            raw.setdefault("isFavouriteBlocked", False)
            return raw
        except Exception:
            pass
    # fallback: minimal reconstruction (should rarely happen)
    r = conn.execute("SELECT * FROM anime WHERE id=?", (anime_id,)).fetchone()
    if not r:
        return None
    cols = [d[0] for d in conn.execute("SELECT * FROM anime LIMIT 0").description]
    d = dict(zip(cols, r))
    try:
        synonyms = _json.loads(d.get("synonyms") or "[]")
    except Exception:
        synonyms = []
    return {
        "id": d["id"], "idMal": d.get("id_mal"), "type": d.get("type") or "ANIME",
        "title": {"romaji": d.get("title_romaji"), "english": d.get("title_english"),
                  "native": d.get("title_native"), "userPreferred": d.get("title_user_preferred")},
        "format": d.get("format"), "status": d.get("status"),
        "description": d.get("description"),
        "startDate": {"year": d.get("start_year"), "month": d.get("start_month"), "day": d.get("start_day")},
        "endDate": {"year": d.get("end_year"), "month": d.get("end_month"), "day": d.get("end_day")},
        "season": d.get("season"), "seasonYear": d.get("season_year"), "seasonInt": d.get("season_int"),
        "episodes": d.get("episodes"), "duration": d.get("duration"),
        "chapters": d.get("chapters"), "volumes": d.get("volumes"),
        "countryOfOrigin": d.get("country_of_origin"),
        "isLicensed": bool(d.get("is_licensed")) if d.get("is_licensed") is not None else None,
        "source": d.get("source"), "hashtag": d.get("hashtag"),
        "trailer": {"id": d.get("trailer_id"), "site": d.get("trailer_site"), "thumbnail": d.get("trailer_thumbnail")} if d.get("trailer_site") else None,
        "updatedAt": int(d.get("updated_at")) if d.get("updated_at") else None,
        "coverImage": {"extraLarge": d.get("cover_extra_large"), "large": d.get("cover_large"),
                       "medium": d.get("cover_medium"), "color": d.get("cover_color")},
        "bannerImage": d.get("banner_image"),
        "genres": [x[0] for x in conn.execute(
            "SELECT g.name FROM genres g JOIN anime_genres ag ON g.id=ag.genre_id WHERE ag.anime_id=?", (anime_id,))],
        "synonyms": synonyms,
        "averageScore": d.get("average_score"), "meanScore": d.get("mean_score"),
        "popularity": d.get("popularity"), "isLocked": bool(d.get("is_locked")),
        "trending": d.get("trending"), "favourites": d.get("favourites"),
        "isAdult": bool(d.get("is_adult")),
        "siteUrl": d.get("site_url") or f"https://anilist.co/anime/{anime_id}",
        "isFavourite": False, "isFavouriteBlocked": bool(d.get("is_favourite_blocked")),
        "autoCreateForumThread": bool(d.get("auto_create_forum_thread")) if d.get("auto_create_forum_thread") is not None else None,
        "isRecommendationBlocked": bool(d.get("is_recommendation_blocked")) if d.get("is_recommendation_blocked") is not None else None,
        "isReviewBlocked": bool(d.get("is_review_blocked")) if d.get("is_review_blocked") is not None else None,
        "modNotes": d.get("mod_notes"),
    }


def update_counters(conn: sqlite3.Connection, anime_id: int, fields: dict):
    """Daily lightweight refresh of live counters. Updates indexed columns AND
    patches the same keys inside stored raw_json so served responses stay exact."""
    import json as _json
    conn.execute("""
        UPDATE anime SET popularity=?, trending=?, favourites=?,
            average_score=?, mean_score=?, updated_at=?
        WHERE id=?
    """, (
        fields.get("popularity"), fields.get("trending"), fields.get("favourites"),
        fields.get("averageScore"), fields.get("meanScore"), fields.get("updatedAt"),
        anime_id,
    ))
    row = conn.execute("SELECT raw_json FROM anime WHERE id=?", (anime_id,)).fetchone()
    if row and row[0]:
        try:
            raw = _json.loads(row[0])
            for k in ("popularity", "trending", "favourites",
                      "averageScore", "meanScore", "updatedAt"):
                if fields.get(k) is not None:
                    raw[k] = fields[k]
            conn.execute("UPDATE anime SET raw_json=? WHERE id=?",
                         (_json.dumps(raw, ensure_ascii=False), anime_id))
        except Exception:
            pass


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
    """Stream exact Media rows to JSON (or .json.gz). Constant memory.

    Prefers stored raw_json (byte-identical to AniList); falls back to
    build_exact_media reconstruction. Never materializes the full list."""
    import gzip as _gzip
    total = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE COALESCE(type,'ANIME')='ANIME'").fetchone()[0]
    exported_at = datetime.now(timezone.utc).isoformat()
    opener = (lambda p: _gzip.open(p, "wt", encoding="utf-8")) \
        if output_path.endswith(".gz") else (lambda p: open(p, "w", encoding="utf-8"))
    cur = conn.execute(
        "SELECT id, raw_json FROM anime WHERE COALESCE(type,'ANIME')='ANIME' ORDER BY id")
    with opener(output_path) as f:
        f.write('{"totalCount": %d, "exportedAt": "%s", "anime": [' % (total, exported_at))
        first = True
        batch = cur.fetchmany(200)
        while batch:
            for aid, raw in batch:
                if raw:
                    doc = raw
                else:
                    try:
                        doc = json.dumps(build_exact_media(conn, aid), ensure_ascii=False)
                    except Exception:
                        continue
                if not first:
                    f.write(",")
                first = False
                f.write(doc)
            batch = cur.fetchmany(200)
        f.write("]}")


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
