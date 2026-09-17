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
    upsert_staff, upsert_relations, upsert_recommendations, upsert_airing_schedule,
    upsert_external_links, upsert_streaming_episodes, upsert_statistics,
    upsert_rankings, upsert_trends, upsert_reviews,
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
RATE_LIMIT_DELAY = 2.5
MAX_RETRIES = 5
BACKOFF_BASE = 2
REQUESTS_PER_MINUTE = 25
BATCH_WINDOW = 60
request_timestamps = []

MEDIA_FIELDS = """
      id
      idMal
      title { romaji english native userPreferred }
      type
      format
      status(version: 2)
      description(asHtml: false)
      startDate { year month day }
      endDate { year month day }
      season
      seasonYear
      seasonInt
      episodes
      duration
      chapters
      volumes
      countryOfOrigin
      isLicensed
      source(version: 3)
      hashtag
      trailer { id site thumbnail }
      updatedAt
      coverImage { extraLarge large medium color }
      bannerImage
      genres
      synonyms
      averageScore
      meanScore
      popularity
      isLocked
      trending
      favourites
      isAdult
      tags { id name description category rank isGeneralSpoiler isMediaSpoiler isAdult userId }
      relations {
        edges {
          id
          relationType(version: 2)
          node {
            id idMal title { romaji english native userPreferred }
            type format status(version: 2)
            episodes duration averageScore meanScore popularity favourites trending
            coverImage { extraLarge large medium color }
            bannerImage genres synonyms season seasonYear
            startDate { year month day } endDate { year month day }
            source siteUrl isAdult
          }
        }
      }
      characters(sort: [ROLE], page: 1, perPage: 25) {
        edges {
          id role favouriteOrder
          node {
            id
            name { first middle last full native alternative alternativeSpoiler userPreferred }
            image { large medium }
            description(asHtml: false)
            gender dateOfBirth { year month day } age bloodType
            isFavourite isFavouriteBlocked siteUrl favourites
          }
          voiceActors {
            id
            name { first middle last full native alternative userPreferred }
            language image { large medium }
            description primaryOccupations gender
            dateOfBirth { year month day } dateOfDeath { year month day }
            age yearsActive homeTown bloodType
            isFavourite isFavouriteBlocked siteUrl favourites
          }
          media { id type }
        }
        pageInfo { total perPage currentPage lastPage hasNextPage }
      }
      staff(sort: [RELEVANCE], page: 1, perPage: 25) {
        edges {
          id role favouriteOrder
          node {
            id
            name { first middle last full native alternative userPreferred }
            language image { large medium }
            description primaryOccupations gender
            dateOfBirth { year month day } dateOfDeath { year month day }
            age yearsActive homeTown bloodType
            isFavourite isFavouriteBlocked siteUrl favourites
          }
        }
        pageInfo { total perPage currentPage lastPage hasNextPage }
      }
      studios {
        edges {
          id isMain favouriteOrder
          node { id name isAnimationStudio siteUrl favourites }
        }
      }
      isFavourite
      isFavouriteBlocked
      nextAiringEpisode { id airingAt timeUntilAiring episode mediaId }
      airingSchedule(page: 1, perPage: 50) {
        edges { node { id airingAt timeUntilAiring episode mediaId } }
        pageInfo { total perPage currentPage lastPage hasNextPage }
      }
      trends(page: 1, perPage: 10, releasing: true) {
        edges { node { mediaId date trending averageScore popularity episode releasing } }
      }
      externalLinks { id site url type language color icon }
      streamingEpisodes { title thumbnail url site }
      rankings { id rank type format year season allTime context }
      recommendations(page: 1, perPage: 25, sort: [RATING_DESC]) {
        edges {
          node {
            id rating userRating
            mediaRecommendation {
              id idMal title { romaji english native userPreferred }
              type format status(version: 2)
              episodes duration averageScore meanScore popularity favourites trending
              coverImage { extraLarge large medium color }
              bannerImage genres synonyms season seasonYear source siteUrl isAdult
            }
            user { id name avatar { large medium } siteUrl }
          }
        }
        pageInfo { total perPage currentPage lastPage hasNextPage }
      }
      reviews(page: 1, perPage: 10, sort: [RATING_DESC]) {
        edges {
          node {
            id userId mediaId summary rating userRating score
            body(asHtml: false) createdAt updatedAt siteUrl
            user { id name avatar { large medium } siteUrl }
          }
        }
        pageInfo { total perPage currentPage lastPage hasNextPage }
      }
      stats { scoreDistribution { score amount } statusDistribution { status amount } }
      siteUrl
      autoCreateForumThread
      isRecommendationBlocked
      isReviewBlocked
      modNotes
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

# Lightweight daily counters refresh: 6 numeric fields only (~1KB/anime vs
# ~300KB full payload). Uses explicit id_in chunks (50/request, always page 1)
# because AniList caps offset pagination at 5000 entries deep ("Page depth
# exceeds maximum") — page 101+ with perPage 50 hard-400s. Chunking local IDs
# sidesteps the wall entirely and misses nothing (incl. null-season entries).
COUNTERS_QUERY = """
query ($ids: [Int]) {
  Page(page: 1, perPage: 50) {
    media(type: ANIME, id_in: $ids) {
      id popularity trending favourites averageScore meanScore updatedAt
    }
    pageInfo { total hasNextPage currentPage lastPage }
  }
}
"""

SEASON_FETCH_QUERY = """
query ($page: Int, $perPage: Int, $season: MediaSeason, $seasonYear: Int) {
  Page(page: $page, perPage: $perPage) {
    media(type: ANIME, sort: ID, season: $season, seasonYear: $seasonYear) {
      %s
    }
    pageInfo { total hasNextPage currentPage lastPage }
  }
}
""" % MEDIA_FIELDS

STATUS_FETCH_QUERY = """
query ($page: Int, $perPage: Int, $status: MediaStatus) {
  Page(page: $page, perPage: $perPage) {
    media(type: ANIME, sort: ID, status: $status) {
      %s
    }
    pageInfo { total hasNextPage currentPage lastPage }
  }
}
""" % MEDIA_FIELDS

INCREMENTAL_FETCH_QUERY = """
query ($page: Int, $perPage: Int, $updatedAt_greater: Int) {
  Page(page: $page, perPage: $perPage) {
    media(type: ANIME, sort: UPDATED_AT_DESC, updatedAt_greater: $updatedAt_greater) {
      %s
    }
    pageInfo { total hasNextPage currentPage lastPage }
  }
}
""" % MEDIA_FIELDS

ID_FETCH_QUERY = """
query ($page: Int, $perPage: Int, $id_greater: Int) {
  Page(page: $page, perPage: $perPage) {
    media(type: ANIME, sort: ID, id_greater: $id_greater) {
      %s
    }
    pageInfo { total hasNextPage currentPage lastPage }
  }
}
""" % MEDIA_FIELDS

STATUS_SEASON_FETCH_QUERY = """
query ($page: Int, $perPage: Int, $status: MediaStatus, $season: MediaSeason, $seasonYear: Int) {
  Page(page: $page, perPage: $perPage) {
    media(type: ANIME, sort: ID, status: $status, season: $season, seasonYear: $seasonYear) {
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
        # Delta tracking for Turso sync: only these IDs get pushed upstream,
        # keeping monthly row writes far under the free cap.
        self.touched_full = set()
        self.touched_counters = set()

    def _save_touched(self):
        try:
            with open(os.path.join(self.data_dir, "touched_full.json"), "w") as f:
                json.dump(sorted(self.touched_full), f)
            with open(os.path.join(self.data_dir, "touched_counters.json"), "w") as f:
                json.dump(sorted(self.touched_counters), f)
            logger.info(f"Touched: {len(self.touched_full)} full + {len(self.touched_counters)} counters")
        except Exception as e:
            logger.warning(f"Could not save touched lists: {e}")

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
        # Enforce anime-only exact mirror (skip manga if API ever returns it)
        if media.get("type") and media.get("type") != "ANIME":
            return
        media["type"] = "ANIME"
        self.touched_full.add(media["id"])
        upsert_anime(conn, media)
        upsert_anime_titles(conn, media["id"], media.get("title", {}) or {})

        upsert_anime_descriptions(conn, media["id"], media.get("description", ""), media.get("synonyms", []) or [])

        upsert_genres(conn, media["id"], media.get("genres", []) or [])
        upsert_tags(conn, media["id"], media.get("tags", []) or [])
        upsert_studios(conn, media["id"], media.get("studios", {}) or {})
        upsert_characters(conn, media["id"], media.get("characters", {}) or {})
        # NEW: full staff / rankings / trends / reviews — required for exact parity
        try:
            upsert_staff(conn, media["id"], media.get("staff", {}) or {})
        except Exception:
            pass
        upsert_relations(conn, media["id"], media.get("relations", {}) or {})
        upsert_recommendations(conn, media["id"], media.get("recommendations", {}) or {})
        try:
            upsert_rankings(conn, media["id"], media.get("rankings", []) or [])
        except Exception:
            pass
        try:
            upsert_trends(conn, media["id"], media.get("trends", {}) or {})
        except Exception:
            pass
        try:
            upsert_reviews(conn, media["id"], media.get("reviews", {}) or {})
        except Exception:
            pass
        try:
            upsert_statistics(conn, media["id"], media.get("stats", {}) or {})
        except Exception:
            pass

        # airingSchedule is { edges: [{ node: {...} }] } — pass edges so IDs are kept
        sched = media.get("airingSchedule") or {}
        if isinstance(sched, dict) and sched.get("edges"):
            upsert_airing_schedule(conn, media["id"], sched.get("edges") or [])
        elif media.get("nextAiringEpisode"):
            upsert_airing_schedule(conn, media["id"], [media["nextAiringEpisode"]])

        upsert_external_links(conn, media["id"], media.get("externalLinks", []) or [])
        upsert_streaming_episodes(conn, media["id"], media.get("streamingEpisodes", []) or [])

    def _fetch_season_year(self, conn, season: str, year: int, seen_ids: set) -> int:
        page = 1
        count = 0
        while True:
            data = self._request(SEASON_FETCH_QUERY, {
                "page": page, "perPage": PER_PAGE, "season": season, "seasonYear": year
            })
            if not data or "data" not in data:
                time.sleep(3)
                data = self._request(SEASON_FETCH_QUERY, {
                    "page": page, "perPage": PER_PAGE, "season": season, "seasonYear": year
                })
                if not data or "data" not in data:
                    logger.error(f"Failed {season} {year} page {page} twice, skipping")
                    break

            media_list = data["data"]["Page"].get("media", [])
            page_info = data["data"]["Page"].get("pageInfo", {})

            if not media_list:
                logger.debug(f"{season} {year}: no anime found (page {page})")
                break

            for media in media_list:
                aid = media.get("id")
                if aid and aid not in seen_ids:
                    media_season = (media.get("season") or "").upper()
                    media_year = media.get("seasonYear")
                    if media_season != season or media_year != year:
                        continue
                    seen_ids.add(aid)
                    self._process_anime(conn, media)
                    count += 1

            if not page_info.get("hasNextPage"):
                break
            page += 1

        return count

    def _fetch_missing_by_season_year(self, conn, seen_ids: set) -> int:
        total = 0
        seasons = ["WINTER", "SPRING", "SUMMER", "FALL"]
        current_year = datetime.now(timezone.utc).year

        status_ranges = {
            "FINISHED": range(1940, current_year + 1),
            "RELEASING": range(max(2020, current_year - 6), current_year + 1),
            "NOT_YET_RELEASED": range(current_year, current_year + 3),
            "CANCELLED": range(1990, current_year + 1),
        }

        logger.info("Status sweep: fetching by status+season+year combos...")
        for status, year_range in status_ranges.items():
            status_count = 0
            for year in year_range:
                for season in seasons:
                    page = 1
                    while True:
                        data = self._request(STATUS_SEASON_FETCH_QUERY, {
                            "page": page, "perPage": PER_PAGE, "status": status,
                            "season": season, "seasonYear": year
                        })
                        if not data or "data" not in data:
                            time.sleep(3)
                            data = self._request(STATUS_SEASON_FETCH_QUERY, {
                                "page": page, "perPage": PER_PAGE, "status": status,
                                "season": season, "seasonYear": year
                            })
                            if not data or "data" not in data:
                                break

                        media_list = data["data"]["Page"].get("media", [])
                        page_info = data["data"]["Page"].get("pageInfo", {})

                        if not media_list:
                            break

                        new_in_batch = 0
                        for media in media_list:
                            aid = media.get("id")
                            if aid and aid not in seen_ids:
                                media_season = (media.get("season") or "").upper()
                                media_year = media.get("seasonYear")
                                if media_season != season or media_year != year:
                                    continue
                                seen_ids.add(aid)
                                self._process_anime(conn, media)
                                status_count += 1
                                total += 1
                                new_in_batch += 1

                        if new_in_batch == 0:
                            break

                        if not page_info.get("hasNextPage"):
                            break
                        page += 1

                if year % 5 == 0:
                    conn.commit()

            if status_count > 0:
                logger.info(f"Status sweep ({status}): +{status_count} anime")
            conn.commit()

        return total

    def _fetch_by_id_sweep(self, conn, seen_ids: set) -> int:
        """Walk IDs ascending via id_greater so null-season/year entries are not missed.
        Relations/recommendations point at these IDs — without this pass they stay dangling."""
        total = 0
        id_greater = 0
        page = 1
        while True:
            data = self._request(ID_FETCH_QUERY, {
                "page": 1, "perPage": PER_PAGE, "id_greater": id_greater
            })
            if not data or "data" not in data:
                time.sleep(3)
                data = self._request(ID_FETCH_QUERY, {
                    "page": 1, "perPage": PER_PAGE, "id_greater": id_greater
                })
                if not data or "data" not in data:
                    logger.error(f"ID sweep failed at id_greater={id_greater}, stopping")
                    break
            media_list = data["data"]["Page"].get("media", [])
            page_info = data["data"]["Page"].get("pageInfo", {})
            if not media_list:
                break
            new_in_batch = 0
            for media in media_list:
                aid = media.get("id")
                if aid is None:
                    continue
                id_greater = max(id_greater, aid)
                if aid not in seen_ids:
                    seen_ids.add(aid)
                    self._process_anime(conn, media)
                    total += 1
                    new_in_batch += 1
            if total % 200 == 0:
                conn.commit()
            # id_greater always advances; stop only when API returns empty
            if not page_info.get("hasNextPage") and new_in_batch == 0:
                # still advance to avoid infinite loop on fully-seen windows
                if not media_list:
                    break
            page += 1
            if page > 20000:  # safety: ~1M IDs
                break
        conn.commit()
        return total

    def full_fetch(self):
        logger.info("Starting full fetch of ALL anime from AniList...")
        logger.info("Strategy: season+year combos + status fallback for complete coverage")
        conn = init_db(self.db_path)
        set_metadata(conn, "fetch_type", "full")
        set_metadata(conn, "fetch_started_at", datetime.now(timezone.utc).isoformat())

        seen_ids = set()
        for row in conn.execute("SELECT id FROM anime"):
            seen_ids.add(row[0])

        checkpoint = self._load_checkpoint()
        if checkpoint and checkpoint.get("fetch_type") == "full":
            already_done = checkpoint.get("seasons_done", [])
            skip_combos = set(checkpoint.get("skip_combos", []))
            seen_ids = set(checkpoint.get("seen_ids", []))
            total_fetched = checkpoint.get("total_fetched", 0)
            logger.info(f"Resuming: {len(already_done)} seasons done, {total_fetched} anime, {len(skip_combos)} skipped")
        else:
            already_done = []
            skip_combos = set()
            total_fetched = len(seen_ids)

        seasons = ["WINTER", "SPRING", "SUMMER", "FALL"]
        current_year = datetime.now(timezone.utc).year
        year_range = range(1940, current_year + 2)
        started_empty = len(seen_ids) == 0

        # Persistent skip-list lives in the DB (checkpoint files are wiped by
        # post_fetch, so a file-only list re-walks 350 empty combos every run).
        # Only historical years are persisted — recent years keep gaining entries.
        def _combo_year(combo: str) -> int:
            try:
                return int(combo.rsplit("_", 1)[1])
            except Exception:
                return 0
        persisted = get_metadata(conn, "skip_combos")
        if persisted:
            try:
                skip_combos |= set(json.loads(persisted))
            except Exception:
                pass
        skip_combos = {c for c in skip_combos if _combo_year(c) < current_year - 1}
        if skip_combos:
            logger.info(f"Skipping {len(skip_combos)} historically-empty combos (zero requests)")

        def _persist_skips():
            try:
                set_metadata(conn, "skip_combos", json.dumps(sorted(skip_combos)))
            except Exception:
                pass

        season_new = 0
        try:
            for year in year_range:
                for season in seasons:
                    combo = f"{season}_{year}"
                    if combo in already_done or combo in skip_combos:
                        continue

                    new_count = self._fetch_season_year(conn, season, year, seen_ids)
                    total_fetched += new_count
                    season_new += new_count
                    already_done.append(combo)

                    if new_count > 0:
                        logger.info(f"{season} {year}: +{new_count} anime (total: {total_fetched})")
                    else:
                        if year < current_year - 1:
                            skip_combos.add(combo)
                        logger.debug(f"{season} {year}: 0 new anime")

                    if total_fetched % 200 == 0:
                        conn.commit()
                        _persist_skips()
                        self._save_checkpoint({
                            "fetch_type": "full",
                            "seasons_done": already_done,
                            "skip_combos": list(skip_combos),
                            "seen_ids": list(seen_ids),
                            "total_fetched": total_fetched,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        })

            logger.info(f"Season+year pass complete. Total unique anime: {total_fetched}")
            _persist_skips()
            conn.commit()

            # Sweeps only discover entries missing from season data. On a warm
            # DB where the season pass added nothing, they re-walk ~1700 empty
            # pages for zero gain — skip them.
            if season_new > 0 or started_empty:
                new_count = self._fetch_missing_by_season_year(conn, seen_ids)
                total_fetched += new_count
                if new_count > 0:
                    logger.info(f"Status sweep complete: +{new_count} anime (total: {total_fetched})")

                # ID sweep catches entries with null season/year that both passes miss.
                # This is what guarantees "same as AniList at any cost" for relations/recommendations targets.
                id_new = self._fetch_by_id_sweep(conn, seen_ids)
                total_fetched += id_new
                if id_new > 0:
                    logger.info(f"ID sweep complete: +{id_new} anime (total: {total_fetched})")
            else:
                logger.info("Warm DB, season pass added nothing — skipping status + ID sweeps (zero gain)")

            conn.commit()
            logger.info(f"Full fetch complete. Total unique anime: {total_fetched}")

        except KeyboardInterrupt:
            logger.info("Interrupted. Saving checkpoint...")
            conn.commit()
            self._save_checkpoint({
                "fetch_type": "full",
                "seasons_done": already_done,
                "skip_combos": list(skip_combos),
                "seen_ids": list(seen_ids),
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
            # No full re-walks, ever: the one bootstrap full already happened.
            # Empty DB -> true bootstrap; warm DB -> refresh rails only.
            count = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
            conn.close()
            if count == 0:
                logger.warning("Empty DB with no watermark: running one-time bootstrap full fetch...")
                return self.full_fetch()
            logger.warning(
                f"No watermark but DB holds {count} rows: skipping straight to "
                "upcoming/counters refresh (full walks are retired).")
            return 0

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

        logger.info("Exporting JSON (streamed straight to .gz, no 5GB raw file)...")
        json_gz_path = os.path.join(self.data_dir, "anilist.json.gz")
        export_json(conn, json_gz_path)

        stats = get_database_stats(conn)
        logger.info(f"Database stats: {json.dumps(stats, indent=2)}")

        set_metadata(conn, "last_fetch_completed_at", datetime.now(timezone.utc).isoformat())
        set_metadata(conn, "total_anime", str(stats["total_anime"]))
        # Watermark for incremental mode. Without this, every incremental falls
        # back to a full walk (the loop this run just hit). post_fetch runs in
        # ALL modes, so the watermark advances no matter the fetch type.
        try:
            row = conn.execute(
                "SELECT MAX(updated_at) FROM anime WHERE updated_at IS NOT NULL").fetchone()
            if row and row[0]:
                set_metadata(conn, "last_updated_at", str(row[0]))
        except Exception:
            pass
        conn.commit()
        conn.close()

        self._clear_checkpoint()
        logger.info("Post-fetch complete.")

    def airing_fetch(self):
        logger.info("Daily airing update: refreshing RELEASING anime schedules and scores...")
        conn = init_db(self.db_path)
        set_metadata(conn, "fetch_type", "airing")
        set_metadata(conn, "fetch_started_at", datetime.now(timezone.utc).isoformat())

        total_updated = 0
        page = 1

        try:
            while True:
                logger.info(f"Fetching RELEASING anime page {page}...")
                data = self._request(STATUS_FETCH_QUERY, {
                    "page": page, "perPage": PER_PAGE, "status": "RELEASING"
                })

                if not data or "data" not in data:
                    time.sleep(3)
                    data = self._request(STATUS_FETCH_QUERY, {
                        "page": page, "perPage": PER_PAGE, "status": "RELEASING"
                    })
                    if not data or "data" not in data:
                        logger.error(f"Failed to fetch RELEASING page {page}, stopping")
                        break

                media_list = data["data"]["Page"].get("media", [])
                page_info = data["data"]["Page"].get("pageInfo", {})

                if not media_list:
                    break

                for media in media_list:
                    self._process_anime(conn, media)
                    total_updated += 1

                if total_updated % 200 == 0:
                    conn.commit()

                if not page_info.get("hasNextPage"):
                    break
                page += 1

            conn.commit()
            logger.info(f"Daily airing update complete. Updated: {total_updated} RELEASING anime")

        except KeyboardInterrupt:
            logger.info("Interrupted. Saving progress...")
            conn.commit()
        finally:
            conn.close()

        return total_updated

    def counters_refresh(self):
        """Daily trending/popularity/scores refresh across the WHOLE catalog.

        Chunked by local IDs (50/request, always page 1): offset pagination
        hard-400s past 5000 entries deep, so page-walking can never cover all
        14.6k. Patches both indexed columns and stored raw_json so served
        responses stay same-to-same on live counters."""
        from db_utils import update_counters
        logger.info("Daily counters refresh: trending/popularity/scores for ALL anime...")
        conn = init_db(self.db_path)
        set_metadata(conn, "fetch_type", "counters")
        set_metadata(conn, "fetch_started_at", datetime.now(timezone.utc).isoformat())

        all_ids = [r[0] for r in conn.execute("SELECT id FROM anime ORDER BY id")]
        logger.info(f"Refreshing counters for {len(all_ids)} anime in 50-ID chunks...")

        total_updated = 0
        failures = 0
        try:
            for i in range(0, len(all_ids), 50):
                chunk = all_ids[i:i + 50]
                data = self._request(COUNTERS_QUERY, {"ids": chunk})
                if not data or "data" not in data:
                    time.sleep(3)
                    data = self._request(COUNTERS_QUERY, {"ids": chunk})
                    if not data or "data" not in data:
                        failures += 1
                        logger.error(f"Counters chunk {i // 50 + 1} failed twice, skipping "
                                     f"({len(chunk)} ids go stale until next run)")
                        continue

                media_list = data["data"]["Page"].get("media", []) or []
                for media in media_list:
                    if media.get("id") is None:
                        continue
                    try:
                        update_counters(conn, media["id"], media)
                        self.touched_counters.add(media["id"])
                        total_updated += 1
                    except Exception as e:
                        logger.warning(f"Counters update failed for {media.get('id')}: {e}")

                if (i // 50 + 1) % 20 == 0:
                    conn.commit()
                    logger.info(f"Counters: {total_updated} anime refreshed "
                                f"({i + len(chunk)}/{len(all_ids)})")

            conn.commit()
            set_metadata(conn, "last_counters_refresh_at",
                         datetime.now(timezone.utc).isoformat())
            conn.commit()
            logger.info(f"Counters refresh complete. Updated: {total_updated} anime, "
                        f"failed chunks: {failures}")

        except KeyboardInterrupt:
            logger.info("Interrupted. Saving progress...")
            conn.commit()
        finally:
            conn.close()

        return total_updated

    def upcoming_fetch(self):
        logger.info("Weekly upcoming sweep: fetching NOT_YET_RELEASED anime...")
        conn = init_db(self.db_path)
        set_metadata(conn, "fetch_type", "upcoming")
        set_metadata(conn, "fetch_started_at", datetime.now(timezone.utc).isoformat())

        seen_ids = set()
        for row in conn.execute("SELECT id FROM anime"):
            seen_ids.add(row[0])

        current_year = datetime.now(timezone.utc).year
        seasons = ["WINTER", "SPRING", "SUMMER", "FALL"]
        total_new = 0

        try:
            for status in ["NOT_YET_RELEASED", "RELEASING"]:
                for year in range(current_year - 1, current_year + 3):
                    for season in seasons:
                        page = 1
                        while True:
                            data = self._request(STATUS_SEASON_FETCH_QUERY, {
                                "page": page, "perPage": PER_PAGE, "status": status,
                                "season": season, "seasonYear": year
                            })
                            if not data or "data" not in data:
                                time.sleep(3)
                                data = self._request(STATUS_SEASON_FETCH_QUERY, {
                                    "page": page, "perPage": PER_PAGE, "status": status,
                                    "season": season, "seasonYear": year
                                })
                                if not data or "data" not in data:
                                    break

                            media_list = data["data"]["Page"].get("media", [])
                            page_info = data["data"]["Page"].get("pageInfo", {})

                            if not media_list:
                                break

                            new_in_batch = 0
                            for media in media_list:
                                aid = media.get("id")
                                if aid and aid not in seen_ids:
                                    media_season = (media.get("season") or "").upper()
                                    media_year = media.get("seasonYear")
                                    if media_season != season or media_year != year:
                                        continue
                                    seen_ids.add(aid)
                                    self._process_anime(conn, media)
                                    total_new += 1
                                    new_in_batch += 1

                            if new_in_batch == 0:
                                break

                            if not page_info.get("hasNextPage"):
                                break
                            page += 1

                        conn.commit()

            logger.info(f"Weekly upcoming sweep complete. New anime: {total_new}")

        except KeyboardInterrupt:
            logger.info("Interrupted. Saving progress...")
            conn.commit()
        finally:
            conn.close()

        return total_new


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
        # Bootstrap / repair ONLY (manual dispatch). Never scheduled:
        # on a warm DB the sweeps re-walk history for zero gain.
        fetcher.full_fetch()
    elif mode == "weekly":
        # Sunday refresh: everything the UI rails need, nothing it doesn't.
        # incremental -> changed entries incl. Just Finished flips + new Upcoming (full payloads)
        # upcoming    -> NOT_YET_RELEASED/RELEASING season sweep for announcements
        # counters    -> trending/popularity/scores merged catalog-wide
        fetcher.incremental_fetch()
        fetcher.upcoming_fetch()
        fetcher.counters_refresh()
    elif mode == "daily":
        # Daily schedule: airing titles fully refreshed + live counters
        # (trending/popularity/scores) merged across the whole catalog.
        # Covers Airing rail + 7-day Schedule + Trending rail.
        fetcher.airing_fetch()
        fetcher.counters_refresh()
    elif mode == "airing":
        fetcher.airing_fetch()
    elif mode == "counters":
        fetcher.counters_refresh()
    elif mode == "upcoming":
        fetcher.upcoming_fetch()
    else:
        fetcher.incremental_fetch()

    fetcher._save_touched()
    fetcher.post_fetch()

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    logger.info(f"=" * 60)
    logger.info(f"Completed in {minutes}m {seconds}s")
    logger.info(f"Total API requests: {fetcher.request_count}")
    if fetcher.request_count > 0:
        logger.info(f"Avg: {elapsed/fetcher.request_count:.1f}s per request")
    logger.info(f"=" * 60)


if __name__ == "__main__":
    main()
