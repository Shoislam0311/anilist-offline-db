"""Tests for schedule-timeline completion (pastAiring middle pages).

Scenarios:
  A. collect_schedule_nodes merges all 4 sources, deduped by row id
  B. _backfill_middle_pages skips short schedules (no extra requests)
  C. _backfill_middle_pages walks pages 2..N until empty, stores pastAiring
  D. middle pages capped (long-runner with huge total stays bounded)
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))
from db_utils import collect_schedule_nodes
import fetch_anilist as fa

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))


def node(i, ep):
    return {"id": i, "episode": ep, "airingAt": 1700000000 + ep * 1000, "mediaId": 7}


def edge(n):
    return {"node": n}


# A: merge + dedupe across airingSchedule / pastAiring / upcomingAiring / next
media = {
    "airingSchedule": {"edges": [edge(node(1, 1)), edge(node(2, 2))]},
    "pastAiring": {"edges": [edge(node(2, 2)), edge(node(3, 3))]},
    "upcomingAiring": {"edges": [edge(node(4, 4))]},
    "nextAiringEpisode": node(4, 4),
}
merged = collect_schedule_nodes(media)
check("A merge-dedupe", [n["id"] for n in merged] == [1, 2, 3, 4],
      "ids=%s" % [n["id"] for n in merged])
check("A empty-safe", collect_schedule_nodes({}) == []
      and collect_schedule_nodes({"airingSchedule": {"edges": [{"node": {"episode": 1}}]}}) == [],
      "no-id rows dropped")


class FakeFetcher(fa.AniListFetcher):
    def __init__(self):
        self.calls = []

    def _request(self, query, variables, retries=0):
        self.calls.append((variables.get("id"), variables.get("page")))
        total, page = self._total, variables.get("page")
        if page > self._pages_left + 1:
            return {"data": {"Media": {"airingSchedule": {"edges": [], "pageInfo": {}}}}}
        base = 100 + page * 10
        return {"data": {"Media": {"airingSchedule": {
            "edges": [edge(node(base + i, 50 + page * 10 + i)) for i in range(3)],
            "pageInfo": {"total": total, "hasNextPage": page < 4}}}}}


# B: short schedule -> no requests, no pastAiring key
f = FakeFetcher()
m = {"id": 7,
     "airingSchedule": {"edges": [edge(node(1, 1))],
                        "pageInfo": {"total": 26, "hasNextPage": False}},
     "upcomingAiring": {"edges": [edge(node(2, 2))]}}
f._backfill_middle_pages(m)
check("B short-skip", f.calls == [] and "pastAiring" not in m, "calls=%s" % f.calls)

# C: long schedule -> pages 2..4 walked, rows stored
f = FakeFetcher()
f._total, f._pages_left = 100, 3
m = {"id": 7, "airingSchedule": {"edges": [edge(node(1, 1))],
                                 "pageInfo": {"total": 100, "hasNextPage": True}},
     "upcomingAiring": {"edges": []}}
f._backfill_middle_pages(m)
pages = [p for _, p in f.calls]
past = (m.get("pastAiring") or {}).get("edges") or []
check("C walks-pages", pages == [2, 3, 4] and len(past) == 9,
      "pages=%s past=%d" % (pages, len(past)))

# D: huge total capped at MIDDLE_PAGE_CAP
f = FakeFetcher()
f._total, f._pages_left = 99999, 999
m = {"id": 7, "airingSchedule": {"edges": [edge(node(1, 1))],
                                 "pageInfo": {"total": 99999, "hasNextPage": True}},
     "upcomingAiring": {"edges": []}}
f._backfill_middle_pages(m)
pages = [p for _, p in f.calls]
check("D capped", max(pages) <= fa.MIDDLE_PAGE_CAP + 1 and len(pages) <= fa.MIDDLE_PAGE_CAP,
      "max_page=%s n=%d" % (max(pages), len(pages)))

# E: flag defaults off (daily stays light), full turns it on
import tempfile
fx = fa.AniListFetcher(tempfile.mkdtemp(prefix="sched_test_"))
check("E flag-default-off", fx.fetch_middle_pages is False, "")

# F: backfill runs BEFORE the raw_json snapshot, so pastAiring ships in
# shards (regression: calling it after upsert_anime lost the rows)
import json as _json
from db_utils import init_db as _init_db


class RawFetcher(fa.AniListFetcher):
    def _request(self, query, variables, retries=0):
        page = variables.get("page", 2)
        if page > 3:
            return {"data": {"Media": {"airingSchedule": {"edges": [], "pageInfo": {}}}}}
        return {"data": {"Media": {"airingSchedule": {
            "edges": [edge(node(500 + page * 10 + i, 60 + i)) for i in range(2)],
            "pageInfo": {"total": 100, "hasNextPage": True}}}}}


tmp = tempfile.mkdtemp(prefix="sched_raw_")
rf = RawFetcher(tmp)
rf.fetch_middle_pages = True
conn = _init_db(rf.db_path)
m = {"id": 777001, "type": "ANIME", "title": {"romaji": "T"},
     "airingSchedule": {"edges": [edge(node(1, 1))],
                        "pageInfo": {"total": 100, "hasNextPage": True}},
     "upcomingAiring": {"edges": []}}
rf._process_anime(conn, m)
conn.commit()
raw = _json.loads(conn.execute("SELECT raw_json FROM anime WHERE id=777001").fetchone()[0])
past = (raw.get("pastAiring") or {}).get("edges") or []
n_table = conn.execute("SELECT COUNT(*) FROM airing_schedule WHERE anime_id=777001").fetchone()[0]
conn.close()
check("F pastAiring-in-raw", len(past) == 4, "past=%d" % len(past))
check("F table-merged", n_table == 5, "table_rows=%d" % n_table)

failed = [r for r in results if not r[1]]
for name, ok, detail in results:
    print(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")
sys.exit(1 if failed else 0)
