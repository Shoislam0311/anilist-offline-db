"""Functional tests for fetch_anilist.incremental_fetch (sliced sweep).

Scenarios:
  A. frontier inside first slice, rest empty → processes window, advances
  B. first slice fails twice             → does NOT advance (retry next run)
  C. all slices empty                    → clean, advances, 0 processed
  D. burst: first slice never reaches frontier (100 pages full) → advances
     anyway (pipeline must never stall) + logs the burst
  E. format-null row in DB               → refetched via id_in; refetch
     failure does not block advance
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


def make_fetcher(with_null_row=False):
    tmp = tempfile.mkdtemp(prefix="sweep_test_")
    os.makedirs(os.path.join(tmp, "data"), exist_ok=True)
    f = fa.AniListFetcher(tmp)
    if with_null_row:
        conn = init_db(f.db_path)
        conn.execute("INSERT OR REPLACE INTO anime (id, format) VALUES (999, NULL)")
        conn.commit()
        conn.close()
    return f


def set_last(f, ts):
    conn = init_db(f.db_path)
    set_metadata(conn, "last_incremental_at", str(ts))
    conn.commit()
    conn.close()


def get_last(f):
    conn = init_db(f.db_path)
    v = get_metadata(conn, "last_incremental_at")
    conn.close()
    return int(float(v)) if v else None


def run_case(name, fake_request, last_ts, with_null_row=False):
    f = make_fetcher(with_null_row)
    set_last(f, last_ts)
    processed = []
    f._request = fake_request
    fa.AniListFetcher._process_anime = lambda self, conn, media: processed.append(media["id"])
    before = int(time.time())
    n = f.incremental_fetch()
    after = int(time.time())
    return name, processed, n, get_last(f), before, after


def page(media, has_next):
    return {"data": {"Page": {"media": media, "pageInfo": {"hasNextPage": has_next, "currentPage": 1}}}}


EMPTY = page([], False)


def empty_slices(fq):
    """Default: every slice/lookup returns empty."""
    def fake(query, variables, retries=0):
        if fq and (query is fa.NULL_FORMAT_REFETCH_QUERY):
            return fq(query, variables)
        return EMPTY
    return fake


# A: first slice (RELEASING/TV) has in-window titles with a frontier mid-page
last_a = now - 3600  # since = now - 3900
def fake_a(query, variables, retries=0):
    if variables.get("status") == "RELEASING" and variables.get("format") == "TV" and variables["page"] == 1:
        return page([{"id": 11, "updatedAt": now - 100},
                     {"id": 12, "updatedAt": now - 200},
                     {"id": 21, "updatedAt": now - 99999}], True)  # last is the frontier
    return EMPTY

name, processed, n, last, before, after = run_case("A frontier-in-first-slice", fake_a, last_a)
ok = processed == [11, 12] and n == 2 and last is not None and before <= last <= after
results.append((name, ok, f"processed={processed} advanced={last is not None and before <= (last or 0) <= after}"))

# B: first slice request fails (None) → no advance
last_b = now - 7200
def fake_b(query, variables, retries=0):
    return None

name, processed, n, last, before, after = run_case("B slice-failure", fake_b, last_b)
ok = processed == [] and n == 0 and last == last_b
results.append((name, ok, f"processed={processed} unchanged={last == last_b}"))

# C: everything empty → clean advance
last_c = now - 5400
name, processed, n, last, before, after = run_case("C all-empty", empty_slices(None), last_c)
ok = processed == [] and n == 0 and last is not None and before <= last <= after
results.append((name, ok, f"processed={processed} advanced={last is not None and before <= (last or 0) <= after}"))

# D: burst — RELEASING/TV returns 100 full in-window pages (depth cap), no frontier
last_d = now - 86400
def fake_d(query, variables, retries=0):
    if variables.get("status") == "RELEASING" and variables.get("format") == "TV":
        base = now - variables["page"] * 100
        return page([{"id": 100000 + variables["page"] * 50 + i, "updatedAt": base} for i in range(50)], True)
    return EMPTY

name, processed, n, last, before, after = run_case("D depth-cap-burst", fake_d, last_d)
ok = n == 5000 and len(processed) == 5000 and last is not None and before <= last <= after
results.append((name, ok, f"n={n} (5000 expected) advanced={last is not None and before <= (last or 0) <= after}"))

# E: format-null row gets refetched via id_in; lookup failure must not block advance
last_e = now - 4500
def fake_e(query, variables, retries=0):
    if query is fa.NULL_FORMAT_REFETCH_QUERY:
        assert variables["ids"] == [999], f"expected null-format id, got {variables['ids']}"
        return page([{"id": 999, "updatedAt": now - 10}], False)
    return EMPTY

name, processed, n, last, before, after = run_case("E null-format-refetch", fake_e, last_e, with_null_row=True)
ok = 999 in processed and last is not None and before <= last <= after
results.append((name, ok, f"null_refetched={999 in processed} advanced={last is not None and before <= (last or 0) <= after}"))

# E2: null-format refetch fails (None) → still advances (slices were clean)
def fake_e2(query, variables, retries=0):
    if query is fa.NULL_FORMAT_REFETCH_QUERY:
        return None
    return EMPTY

name, processed, n, last, before, after = run_case("E2 null-refetch-failure-nonblocking", fake_e2, last_e, with_null_row=True)
ok = processed == [] and last is not None and before <= last <= after
results.append((name, ok, f"advanced_despite_null_fail={last is not None and before <= (last or 0) <= after}"))

failed = [r for r in results if not r[1]]
for name, ok, detail in results:
    print(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")
sys.exit(1 if failed else 0)
