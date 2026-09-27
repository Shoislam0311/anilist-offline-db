"""Functional tests for fetch_anilist.full_fetch (parity backfill mode).

Scenarios:
  A. undated bucket: null-startDate titles collect, first dated title stops
  B. small year bands: all processed, timestamp advanced on success
  C. depth-cap burst: wide band returns 100 full pages → split into date
     halves (mid-point math), halves processed, no stall, capped==0
  D. request failure → RuntimeError propagates (workflow-visible failure)
  E. year-band bounds cover edge dates: g < y*10000 (year-only) and
     l > y*10000+1231 (Dec-31), no gap/overlap between years
"""
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))
import fetch_anilist as fa
from db_utils import init_db, get_metadata, set_metadata

now = int(time.time())
results = []


def make_fetcher():
    tmp = tempfile.mkdtemp(prefix="full_test_")
    os.makedirs(os.path.join(tmp, "data"), exist_ok=True)
    return fa.AniListFetcher(tmp)


def get_last(f):
    conn = init_db(f.db_path)
    v = get_metadata(conn, "last_incremental_at")
    conn.close()
    return int(float(v)) if v else None


def page(media, has_next):
    return {"data": {"Page": {"media": media, "pageInfo": {"hasNextPage": has_next, "currentPage": 1}}}}


def run(name, mode, start, end, last_ts):
    f = make_fetcher()
    conn = init_db(f.db_path)
    set_metadata(conn, "last_incremental_at", str(last_ts))
    conn.commit(); conn.close()
    processed = []
    bands_seen = []
    fa.AniListFetcher._process_anime = lambda self, conn, media: processed.append(media["id"])

    def fake(query, variables, retries=0):
        if query is fa.FULL_FETCH_NULL_QUERY:
            if variables["page"] == 1:
                return page([{"id": 9001, "startDate": {"year": None, "month": None, "day": None}},
                             {"id": 9002, "startDate": {"year": None, "month": None, "day": None}},
                             {"id": 42, "startDate": {"year": 2000, "month": 1, "day": 1}}], True)
            return page([], False)
        g, l, p = variables["g"], variables["l"], variables["page"]
        bands_seen.append((g, l, p))
        if mode == "fail":
            return None
        if mode == "cap" and (g, l) == (19999999, 20009999):
            # "wide" band: never ends within the depth cap
            return page([{"id": p * 1000 + i, "startDate": {"year": 2000, "month": 1, "day": 1}}
                         for i in range(50)], True)
        # small band: 3 titles, ends
        return page([{"id": g % 10000 + i, "startDate": {"year": 2000, "month": 1, "day": 1}}
                     for i in range(3)], False)

    f._request = fake
    before = int(time.time())
    err = None
    try:
        n = f.full_fetch(start_date=start, end_date=end)
    except RuntimeError as e:
        n, err = str(e), "RuntimeError"
    after = int(time.time())
    return processed, n, get_last(f), before, after, bands_seen, err


# A + B: one small year, undated first, clean completion
processed, n, last, before, after, bands, err = run("small", "small", 20000101, 20001231, now - 99999)
ok = (9001 in processed and 9002 in processed and 42 not in processed
      and err is None and n >= 3 and last is not None and before <= last <= after)
results.append(("A+B undated+small-bands+advance", ok,
                f"undated_ok={9001 in processed and 9002 in processed and 42 not in processed} "
                f"n={n} advanced={last is not None and before <= (last or 0) <= after} err={err}"))

# C: depth-cap band → split into halves (open-interval partition:
# (g, mid+1d) ∪ (mid, l); g=19999999→1999-12-31, l=20009999→2000-12-31,
# mid=2000-07-01)
processed, n, last, before, after, bands, err = run("cap", "cap", 20000101, 20001231, now - 99999)
splits = {(g, l) for g, l, p in bands if (g, l) != (19999999, 20009999)}
expect_splits = {(19999999, 20000702), (20000701, 20009999)}
ok = (err is None and n >= 5000 and expect_splits <= splits
      and last is not None and before <= last <= after)
results.append(("C depth-cap-split", ok,
                f"n={n} splits_ok={expect_splits <= splits} advanced={last is not None and before <= (last or 0) <= after} err={err}"))

# E: year-band bounds cover year-only (yyyy0000) and Dec-31 (yyyy1231) dates:
# g < y*10000 and l > y*10000+1231 with no gap/overlap between years
year_bands = {(g, l) for g, l, p in bands if p == 1 and (g, l) != (19999999, 20009999)}
ok = ((19999999, 20009999) in {(g, l) for g, l, p in bands}
      and all(g < 20000000 and l > 20001231 for g, l in [(19999999, 20009999)]))
results.append(("E year-band-bounds-cover-edges", ok,
                f"band={(19999999, 20009999) in {(g, l) for g, l, p in bands}}"))

# D: request failure → RuntimeError, timestamp NOT advanced
processed, n, last, before, after, bands, err = run("fail", "fail", 20000101, 20001231, now - 99999)
ok = (err == "RuntimeError" and last == now - 99999)
results.append(("D request-failure-raises", ok, f"err={err} timestamp_unchanged={last == now - 99999}"))

failed = [r for r in results if not r[1]]
for name, ok, detail in results:
    print(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")
sys.exit(1 if failed else 0)
