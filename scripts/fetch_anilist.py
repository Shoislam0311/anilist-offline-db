#!/usr/bin/env python3
"""
AniList Offline Database - Main Fetcher
Scrapes all anime from AniList's public GraphQL API with full data.
Supports incremental updates, crash recovery, and rate limiting.
"""

import os
import sys
import json
import time
import logging
import hashlib
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_utils import (
    init_db, init_fts, populate_fts, set_metadata, get_metadata,
    upsert_anime, upsert_anime_titles, upsert_anime_descriptions,
    upsert_genres, upsert_tags, upsert_studios, upsert_characters,
    upsert_relations, upsert_recommendations, upsert_airing_schedule,
    upsert_external_links, upsert_streaming_episodes, upsert_statistics,
    generate_changelog, export_json, get_database_stats, connect_db
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

ANILIST_API = "https://graphql.anilist.co"
PER_PAGE = 50
RATE_LIMIT_DELAY = 2.1
MAX_RETRIES = 5
BACKOFF_BASE = 2
REQUESTS_PER_MINUTE = 28
BATCH_WINDOW = 60
request_timestamps = []

MEDIA_FIELDS = """
      id
      idMal
      title { romaji english native }
      description(asHtml: false)
      coverImage { large color }
      bannerImage
      episodes
      duration
      status
      format
      season
      seasonYear
      averageScore
      meanScore
      popularity
      favourites
      trending
      genres
      tags { name rank description category }
      studios(isMain: true) { edges { node { id name } isMain } }
      characters(sort: ROLE, perPage: 25) {
        edges {
          node {
            id
            name { full native }
            image { large medium }
            description
            favourites
            gender
            dateOfBirth { year month day }
            age
          }
          role
          voiceActors(language: JAPANESE) {
            id
            name { full native }
            image { large medium }
            favourites
            language
          }
        }
      }
      relations {
        edges {
          node { id title { romaji english } type coverImage { large } }
          relationType
        }
      }
      recommendations(perPage: 10) {
        edges {
          node {
            mediaRecommendation { id title { romaji english } coverImage { large } }
            rating
            userRating
          }
        }
      }
      nextAiringEpisode { episode airingAt timeUntilAiring }
      airingSchedule(notYetAired: true, perPage: 50) {
        edges { node { episode airingAt timeUntilAiring } }
      }
      externalLinks { site url language color icon isDisabled notes }
      streamingEpisodes { title thumbnail site }
      startDate { year month day }
      endDate { year month day }
      source
      hashtag
      countryOfOrigin
      isAdult
      updatedAt
      synonyms
"""

FULL_FETCH_QUERY = """
query ($page: Int, $perPage: Int, $sort: [MediaSort]) {
  Page(page: $page, perPage: $perPage) {
    media(type: ANIME, sort: $sort) {
      %s
    }
    pageInfo { total hasNextPage currentPage lastPage }
  }
}
""" % MEDIA_FIELDS

INCREMENTAL_FETCH_QUERY = """
query ($page: Int, $perPage: Int, $updatedAt_greater: Int) {
  Page(page: $page, perPage: $perPage) {
    media(type: ANIME, sort: UPDATED_AT, updatedAt_greater: $updatedAt_greater) {
      %s
    }
    pageInfo { total hasNextPage currentPage lastPage }
  }
}
""" % MEDIA_FIELDS


class AniListFetcher:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.db_path = os.path.join(data_dir, "anilist.db")
        self.checkpoint_path = os.path.join(data_dir, ".checkpoint.json")
        self.changelog_path = os.path.join(data_dir, "changelog.md")
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "AniList-Offline-DB/1.0"
        })
        self.request_count = 0
        self.last_request_time = 0

    def _rate_limit(self):
        global request_timestamps
        now = time.time()
        request_timestamps = [t for t in request_timestamps if now - t < BATCH_WINDOW]
        if len(request_timestamps) >= REQUESTS_PER_MINUTE:
            wait_until = request_timestamps[0] + BATCH_WINDOW
            sleep_time = wait_until - now + 0.1
            if sleep_time > 0:
                logger.info(f"Rate limit: {REQUESTS_PER_MINUTE} req/{BATCH_WINDOW}s reached. Waiting {sleep_time:.1f}s...")
                time.sleep(sleep_time)
        elapsed = time.time() - self.last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)
        self.last_request_time = time.time()
        request_timestamps.append(time.time())

    def _request(self, query: str, variables: dict, retries: int = 0) -> Optional[dict]:
        self._rate_limit()
        try:
            response = self.session.post(ANILIST_API, json={"query": query, "variables": variables})
            self.request_count += 1

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after)
                return self._request(query, variables, retries)

            if response.status_code == 503:
                wait = BACKOFF_BASE ** retries
                logger.warning(f"Service unavailable. Retry in {wait}s...")
                time.sleep(wait)
                if retries < MAX_RETRIES:
                    return self._request(query, variables, retries + 1)
                return None

            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                for error in data["errors"]:
                    if "rate" in error.get("message", "").lower():
                        wait = BACKOFF_BASE ** retries
                        logger.warning(f"Rate limited in response. Waiting {wait}s...")
                        time.sleep(wait)
                        if retries < MAX_RETRIES:
                            return self._request(query, variables, retries + 1)
                    logger.error(f"API error: {error}")

            return data

        except requests.exceptions.ConnectionError as e:
            wait = BACKOFF_BASE ** retries
            logger.warning(f"Connection error: {e}. Retry in {wait}s...")
            time.sleep(wait)
            if retries < MAX_RETRIES:
                return self._request(query, variables, retries + 1)
            return None
        except requests.exceptions.Timeout:
            wait = BACKOFF_BASE ** retries
            logger.warning(f"Request timeout. Retry in {wait}s...")
            time.sleep(wait)
            if retries < MAX_RETRIES:
                return self._request(query, variables, retries + 1)
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None

    def _save_checkpoint(self, data: dict):
        with open(self.checkpoint_path, "w") as f:
            json.dump(data, f)

    def _load_checkpoint(self) -> Optional[dict]:
        if os.path.exists(self.checkpoint_path):
            with open(self.checkpoint_path, "r") as f:
                return json.load(f)
        return None

    def _clear_checkpoint(self):
        if os.path.exists(self.checkpoint_path):
            os.remove(self.checkpoint_path)

    def _process_anime(self, conn, media: dict):
        upsert_anime(conn, media)
        upsert_anime_titles(conn, media["id"], media.get("title", {}))

        synonyms = media.get("synonyms", []) or []
        upsert_anime_descriptions(conn, media["id"], media.get("description", ""), synonyms)

        upsert_genres(conn, media["id"], media.get("genres", []) or [])
        upsert_tags(conn, media["id"], media.get("tags", []) or [])
        upsert_studios(conn, media["id"], media.get("studios", {}))
        upsert_characters(conn, media["id"], media.get("characters", {}))
        upsert_relations(conn, media["id"], media.get("relations", {}))
        upsert_recommendations(conn, media["id"], media.get("recommendations", {}))

        airing_schedule = []
        if media.get("airingSchedule") and media["airingSchedule"].get("edges"):
            airing_schedule = [e["node"] for e in media["airingSchedule"]["edges"]]
        elif media.get("nextAiringEpisode"):
            airing_schedule = [media["nextAiringEpisode"]]
        upsert_airing_schedule(conn, media["id"], airing_schedule)

        upsert_external_links(conn, media["id"], media.get("externalLinks", []) or [])
        upsert_streaming_episodes(conn, media["id"], media.get("streamingEpisodes", []) or [])

    def full_fetch(self):
        logger.info("Starting full fetch of all anime from AniList...")
        conn = init_db(self.db_path)
        set_metadata(conn, "fetch_type", "full")
        set_metadata(conn, "fetch_started_at", datetime.now(timezone.utc).isoformat())

        old_anime_ids = set()
        for row in conn.execute("SELECT id FROM anime"):
            old_anime_ids.add(row[0])

        page = 1
        total_fetched = 0
        total_pages = None
        checkpoint = self._load_checkpoint()

        if checkpoint and checkpoint.get("fetch_type") == "full":
            page = checkpoint.get("page", 1)
            total_fetched = checkpoint.get("total_fetched", 0)
            logger.info(f"Resuming from checkpoint: page {page}, {total_fetched} anime fetched")

        try:
            while True:
                logger.info(f"Fetching page {page}... ({total_fetched} anime so far)")
                data = self._request(FULL_FETCH_QUERY, {"page": page, "perPage": PER_PAGE, "sort": "ID"})

                if not data or "data" not in data:
                    logger.error(f"Failed to fetch page {page}. Retrying...")
                    time.sleep(5)
                    data = self._request(FULL_FETCH_QUERY, {"page": page, "perPage": PER_PAGE, "sort": "ID"})
                    if not data or "data" not in data:
                        logger.error(f"Failed again on page {page}. Saving checkpoint and exiting.")
                        self._save_checkpoint({
                            "fetch_type": "full",
                            "page": page,
                            "total_fetched": total_fetched,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        })
                        break

                page_data = data["data"]["Page"]
                media_list = page_data.get("media", [])
                page_info = page_data.get("pageInfo", {})

                if total_pages is None:
                    total_pages = page_info.get("lastPage", 0)
                    logger.info(f"Total pages: {total_pages}")

                if not media_list:
                    logger.info("No more media found. Fetch complete.")
                    break

                if total_pages and page >= total_pages:
                    logger.info(f"Reached last page ({total_pages}). Fetch complete.")
                    break

                for media in media_list:
                    self._process_anime(conn, media)
                    total_fetched += 1

                if total_fetched % 500 == 0:
                    conn.commit()
                    self._save_checkpoint({
                        "fetch_type": "full",
                        "page": page,
                        "total_fetched": total_fetched,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })
                    logger.info(f"Saved checkpoint: {total_fetched} anime")

                if not page_info.get("hasNextPage"):
                    logger.info("No more pages. Fetch complete.")
                    break

                page += 1

            conn.commit()
            logger.info(f"Full fetch complete. Total anime: {total_fetched}")

        except KeyboardInterrupt:
            logger.info("Interrupted. Saving checkpoint...")
            conn.commit()
            self._save_checkpoint({
                "fetch_type": "full",
                "page": page,
                "total_fetched": total_fetched,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        finally:
            conn.close()

        return total_fetched

    def incremental_fetch(self):
        logger.info("Starting incremental fetch...")
        conn = init_db(self.db_path)
        set_metadata(conn, "fetch_type", "incremental")
        set_metadata(conn, "fetch_started_at", datetime.now(timezone.utc).isoformat())

        last_updated = get_metadata(conn, "last_updated_at")
        if last_updated:
            updated_after = int(last_updated)
            logger.info(f"Incremental: fetching anime updated after {last_updated}")
        else:
            updated_after = None
            logger.info("No previous update found. Running full fetch...")
            conn.close()
            return self.full_fetch()

        old_anime_data = {}
        for row in conn.execute("SELECT id, title_romaji, episodes, status, average_score, popularity, favourites, trending FROM anime"):
            old_anime_data[row[0]] = {
                "title_romaji": row[1], "episodes": row[2], "status": row[3],
                "average_score": row[4], "popularity": row[5], "favourites": row[6],
                "trending": row[7]
            }

        page = 1
        total_updated = 0
        checkpoint = self._load_checkpoint()

        if checkpoint and checkpoint.get("fetch_type") == "incremental":
            page = checkpoint.get("page", 1)
            total_updated = checkpoint.get("total_updated", 0)
            logger.info(f"Resuming incremental from checkpoint: page {page}")

        try:
            while True:
                logger.info(f"Fetching incremental page {page}...")
                variables = {"page": page, "perPage": PER_PAGE, "updatedAt_greater": updated_after}
                data = self._request(INCREMENTAL_FETCH_QUERY, variables)

                if not data or "data" not in data:
                    logger.error(f"Failed to fetch page {page}. Retrying...")
                    time.sleep(5)
                    data = self._request(INCREMENTAL_FETCH_QUERY, variables)
                    if not data or "data" not in data:
                        logger.error(f"Failed again on page {page}. Saving checkpoint.")
                        self._save_checkpoint({
                            "fetch_type": "incremental",
                            "page": page,
                            "total_updated": total_updated,
                            "updated_after": updated_after,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        })
                        break

                page_data = data["data"]["Page"]
                media_list = page_data.get("media", [])
                page_info = page_data.get("pageInfo", {})

                if not media_list:
                    logger.info("No more updated anime. Incremental fetch complete.")
                    break

                for media in media_list:
                    self._process_anime(conn, media)
                    total_updated += 1

                if total_updated % 200 == 0:
                    conn.commit()
                    self._save_checkpoint({
                        "fetch_type": "incremental",
                        "page": page,
                        "total_updated": total_updated,
                        "updated_after": updated_after,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })

                if not page_info.get("hasNextPage"):
                    break

                page += 1

            new_anime_data = {}
            for row in conn.execute("SELECT id, title_romaji, episodes, status, average_score, popularity, favourites, trending FROM anime"):
                new_anime_data[row[0]] = {
                    "title_romaji": row[1], "episodes": row[2], "status": row[3],
                    "average_score": row[4], "popularity": row[5], "favourites": row[6],
                    "trending": row[7]
                }

            changelog = generate_changelog(conn, old_anime_data, new_anime_data)
            with open(self.changelog_path, "w", encoding="utf-8") as f:
                f.write(changelog)
            logger.info(f"Changelog written to {self.changelog_path}")

            max_updated = 0
            for row in conn.execute("SELECT MAX(updated_at) FROM anime WHERE updated_at IS NOT NULL"):
                if row[0]:
                    max_updated = max(max_updated, int(row[0]))
            if max_updated:
                set_metadata(conn, "last_updated_at", str(max_updated))

            conn.commit()
            logger.info(f"Incremental fetch complete. Updated: {total_updated} anime")

        except KeyboardInterrupt:
            logger.info("Interrupted. Saving checkpoint...")
            conn.commit()
            self._save_checkpoint({
                "fetch_type": "incremental",
                "page": page,
                "total_updated": total_updated,
                "updated_after": updated_after,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        finally:
            conn.close()

        return total_updated

    def post_fetch(self):
        logger.info("Post-fetch: rebuilding FTS index and exporting JSON...")
        from db_utils import init_fts
        init_fts(self.db_path)
        conn = connect_db(self.db_path)

        logger.info("Populating FTS index...")
        populate_fts(conn)
        conn.commit()

        logger.info("Exporting JSON...")
        json_path = os.path.join(self.data_dir, "anilist.json")
        export_json(conn, json_path)

        stats = get_database_stats(conn)
        logger.info(f"Database stats: {json.dumps(stats, indent=2)}")

        set_metadata(conn, "last_fetch_completed_at", datetime.now(timezone.utc).isoformat())
        set_metadata(conn, "total_anime", str(stats["total_anime"]))
        conn.commit()
        conn.close()

        self._clear_checkpoint()
        logger.info("Post-fetch complete.")


def main():
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
    os.makedirs(data_dir, exist_ok=True)

    fetcher = AniListFetcher(data_dir)

    mode = os.environ.get("FETCH_MODE", "incremental")

    logger.info(f"=" * 60)
    logger.info(f"AniList Offline Database - {mode.upper()} Fetch")
    logger.info(f"Rate limit: {REQUESTS_PER_MINUTE} req/min ({RATE_LIMIT_DELAY}s delay)")
    logger.info(f"=" * 60)

    start_time = time.time()

    if mode == "full":
        fetcher.full_fetch()
    else:
        fetcher.incremental_fetch()

    fetcher.post_fetch()

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    logger.info(f"Total time: {minutes}m {seconds}s")
    logger.info(f"Total API requests: {fetcher.request_count}")


if __name__ == "__main__":
    main()
