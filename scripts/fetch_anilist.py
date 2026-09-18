#!/usr/bin/env python3
"""
AniList Offline Database - Main Fetcher (SMART daily version)

Daily rails logic:
- New title (ID does NOT exist in previous data) → full process + touched_full
- Already known + RELEASING → only update next episode timestamp + touched_airing
- Already known + other rails → skip
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

RAILS_SORTED_QUERY = """
query ($page: Int, $perPage: Int, $sort: [MediaSort], $status: MediaStatus, $format: MediaFormat) {
  Page(page: $page, perPage: $perPage) {
    media(type: ANIME, sort: $sort, status: $status, format: $format) {
      %s
    }
    pageInfo { total hasNextPage currentPage lastPage }
  }
}
""" % MEDIA_FIELDS

RAIL_DEFS = [
    ("trending", ["TRENDING_DESC"], None, None, 2),
    ("top_airing", ["POPULARITY_DESC"], "RELEASING", None, 2),
    ("top_movies", ["SCORE_DESC"], None, "MOVIE", 2),
    ("upcoming", ["ID_DESC"], "NOT_YET_RELEASED", None, 3),
    ("just_finished", ["END_DATE_DESC"], "FINISHED", None, 2),
    ("schedule", ["TRENDING_DESC"], "RELEASING", None, 2),
]
RAIL_PER_PAGE = 50


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

        # SMART tracking
        self.touched_full = set()       # brand-new titles only
        self.touched_airing = set()     # already-known releasing titles (next episode only)
        self.touched_counters = set()

    def _save_touched(self):
        try:
            with open(os.path.join(self.data_dir, "touched_full.json"), "w") as f:
                json.dump(sorted(self.touched_full), f)
            with open(os.path.join(self.data_dir, "touched_airing.json"), "w") as f:
                json.dump(sorted(self.touched_airing), f)
            with open(os.path.join(self.data_dir, "touched_counters.json"), "w") as f:
                json.dump(sorted(self.touched_counters), f)
            logger.info(
                f"Touched: {len(self.touched_full)} NEW (full) + "
                f"{len(self.touched_airing)} airing-only + "
                f"{len(self.touched_counters)} counters"
            )
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
            return response.json()
        except Exception as e:
            logger.error(f"Request error: {e}")
            if retries < MAX_RETRIES:
                time.sleep(BACKOFF_BASE ** retries)
                return self._request(query, variables, retries + 1)
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
        """Full process for brand-new titles only."""
        if media.get("type") and media.get("type") != "ANIME":
            return
        media["type"] = "ANIME"
        aid = media.get("id")
        if not aid:
            return

        self.touched_full.add(aid)

        upsert_anime(conn, media)
        upsert_anime_titles(conn, media["id"], media.get("title", {}) or {})
        upsert_anime_descriptions(conn, media["id"], media.get("description", ""), media.get("synonyms", []) or [])
        upsert_genres(conn, media["id"], media.get("genres", []) or [])
        upsert_tags(conn, media["id"], media.get("tags", []) or [])
        upsert_studios(conn, media["id"], media.get("studios", {}) or {})
        upsert_characters(conn, media["id"], media.get("characters", {}) or {})

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

        sched = media.get("airingSchedule") or {}
        if isinstance(sched, dict) and sched.get("edges"):
            upsert_airing_schedule(conn, media["id"], sched.get("edges") or [])
        elif media.get("nextAiringEpisode"):
            upsert_airing_schedule(conn, media["id"], [media["nextAiringEpisode"]])

        upsert_external_links(conn, media["id"], media.get("externalLinks", []) or [])
        upsert_streaming_episodes(conn, media["id"], media.get("streamingEpisodes", []) or [])

    def _update_next_airing_only(self, conn, media: dict):
        """Ultra-light update for already-known RELEASING titles."""
        aid = media.get("id")
        if not aid:
            return

        next_ep = media.get("nextAiringEpisode") or {}
        airing_at = next_ep.get("airingAt")
        episode = next_ep.get("episode")
        updated_at = media.get("updatedAt")

        try:
            conn.execute("""
                UPDATE anime SET
                    next_airing_at = ?,
                    next_airing_episode = ?,
                    updated_at = ?
                WHERE id = ?
            """, (airing_at, episode, updated_at, aid))
        except Exception:
            # columns may not exist yet on very old DBs
            pass

        try:
            if next_ep:
                upsert_airing_schedule(conn, aid, [next_ep])
        except Exception:
            pass

        self.touched_airing.add(aid)

    def rails_fetch(self):
        """
        SMART daily rails:
        - New title (ID not in previous data) → full process + touched_full
        - Already known + RELEASING → only next episode update + touched_airing
        - Already known + other rails → skip
        """
        logger.info("Daily SMART rails refresh (new titles full + releasing next-episode only)...")
        conn = init_db(self.db_path)
        set_metadata(conn, "fetch_type", "rails")
        set_metadata(conn, "fetch_started_at", datetime.now(timezone.utc).isoformat())

        # Previous data check
        seen_before = {row[0] for row in conn.execute("SELECT id FROM anime")}
        logger.info(f"Local DB already has {len(seen_before)} anime")

        rail_ids = {}
        rail_new = {}
        total_updated = 0

        try:
            for name, sort, status, fmt, max_pages in RAIL_DEFS:
                ids_this_rail = []
                new_this_rail = 0
                is_releasing_rail = name in ("top_airing", "schedule") or status == "RELEASING"

                for page in range(1, max_pages + 1):
                    variables = {"page": page, "perPage": RAIL_PER_PAGE, "sort": sort}
                    if status:
                        variables["status"] = status
                    if fmt:
                        variables["format"] = fmt

                    data = self._request(RAILS_SORTED_QUERY, variables)
                    if not data or "data" not in data:
                        time.sleep(3)
                        data = self._request(RAILS_SORTED_QUERY, variables)
                        if not data or "data" not in data:
                            logger.error(f"Rail {name} page {page} failed twice, skipping")
                            break

                    media_list = data["data"]["Page"].get("media", []) or []
                    page_info = data["data"]["Page"].get("pageInfo", {})

                    if not media_list:
                        break

                    for media in media_list:
                        aid = media.get("id")
                        if not aid:
                            continue

                        ids_this_rail.append(aid)

                        if aid not in seen_before:
                            # Brand new title
                            self._process_anime(conn, media)
                            seen_before.add(aid)
                            new_this_rail += 1
                            total_updated += 1
                        else:
                            # Already exists
                            if is_releasing_rail:
                                self._update_next_airing_only(conn, media)
                                total_updated += 1
                            # else: known + not releasing → do nothing

                    if not page_info.get("hasNextPage"):
                        break

                rail_ids[name] = ids_this_rail
                rail_new[name] = new_this_rail
                logger.info(f"Rail '{name}': {len(ids_this_rail)} titles, {new_this_rail} NEW")

            conn.commit()

            all_rail_ids = set()
            for v in rail_ids.values():
                all_rail_ids.update(v)
            total_new = sum(rail_new.values())
            push_full = sorted(self.touched_full)
            push_airing = sorted(self.touched_airing)

            logger.info(
                f"Rails complete → {len(all_rail_ids)} unique rail IDs | "
                f"{len(push_full)} NEW (full) | {len(push_airing)} airing-only | "
                f"+{total_new} brand-new anime"
            )

            self._save_rail_manifest(rail_ids, rail_new, 0, push_full + push_airing)

        except KeyboardInterrupt:
            logger.info("Interrupted. Saving progress...")
            conn.commit()
        finally:
            conn.close()

        return total_updated

    def _save_rail_manifest(self, rail_ids, rail_new, overlap, push_ids):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        out_path = os.path.join(base_dir, "docs", "api", "rail_manifest.json")
        try:
            manifest = {
                "generatedAt": datetime.now(timezone.utc).isoformat(),
                "fetch_type": "rails",
                "uniqueIds": len(push_ids),
                "overlap": overlap,
                "pushIds": push_ids,
                "rails": {
                    name: {"count": len(ids), "new": rail_new.get(name, 0), "ids": ids}
                    for name, ids in rail_ids.items()
                },
            }
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(manifest, f, ensure_ascii=False)
            logger.info(f"Rail manifest written: {out_path}")
        except Exception as e:
            logger.warning(f"Could not save rail manifest: {e}")

    def post_fetch(self):
        conn = connect_db(self.db_path)
        try:
            stats = get_database_stats(conn)
            logger.info(f"Database stats: {json.dumps(stats, indent=2)}")
            set_metadata(conn, "last_fetch_at", datetime.now(timezone.utc).isoformat())
            conn.commit()
        finally:
            conn.close()

    # Keep other methods (full_fetch, incremental_fetch, etc.) if you still need them.
    # For daily runs we only care about rails_fetch.


def main():
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
    os.makedirs(data_dir, exist_ok=True)

    fetcher = AniListFetcher(data_dir)
    mode = os.environ.get("FETCH_MODE", "daily")

    logger.info("=" * 60)
    logger.info(f"AniList Offline Database - {mode.upper()} Fetch (SMART)")
    logger.info("=" * 60)

    start_time = time.time()

    if mode in ("daily", "rails"):
        fetcher.rails_fetch()
    else:
        # fallback – you can keep old methods if needed
        logger.warning(f"Mode {mode} not fully implemented in this SMART version. Running rails.")
        fetcher.rails_fetch()

    fetcher._save_touched()
    fetcher.post_fetch()

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    logger.info("=" * 60)
    logger.info(f"Completed in {minutes}m {seconds}s")
    logger.info(f"Total API requests: {fetcher.request_count}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
