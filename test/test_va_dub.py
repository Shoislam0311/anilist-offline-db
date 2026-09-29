"""Tests for per-role VA dub language + rails raw_json patching.

Scenarios:
  A. va_dub_map partitions VAs by dub alias; leftovers unmapped
  B. stamp_va_dub_languages attaches display form, never overwrites existing
  C. upsert writes enum dub to character_voice_actors (fallback primary)
  D. patch_raw_json merges keys into stored raw_json (rails freshness)
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))
import fetch_anilist as fa
from db_utils import (va_dub_map, stamp_va_dub_languages, patch_raw_json,
                      upsert_characters, init_db)

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))


def chars_payload():
    return {"edges": [
        {"id": 1, "role": "MAIN",
         "node": {"id": 101, "name": {"full": "A"}},
         "voiceActors": [
             {"id": 111, "language": "JAPANESE", "name": {"full": "JP VA"}},
             {"id": 188, "language": "JAPANESE", "name": {"full": "ZH VA"}},
         ],
         "dub_JAPANESE": [{"id": 111}],
         "dub_ENGLISH": [],
         # NOTE: no dub_CHINESE — Chinese has no StaffLanguage enum member,
         # so VA 188 (Chinese dub) stays unmapped -> primary fallback.
         },
    ]}


# A: alias partition (Chinese has no enum member -> unmapped)
m = va_dub_map(chars_payload())
check("A partition", m == {(101, 111): "JAPANESE"}, "map=%s" % m)

# A2: all 10 aliases resolve
edges = {"edges": [{"node": {"id": 5},
                    "voiceActors": [{"id": 50 + i, "language": "JAPANESE"} for i in range(10)],
                    **{"dub_" + lang: [{"id": 50 + i}]
                       for i, lang in enumerate(("JAPANESE", "ENGLISH", "KOREAN", "ITALIAN",
                                                 "SPANISH", "PORTUGUESE", "FRENCH", "GERMAN",
                                                 "HEBREW", "HUNGARIAN"))}}]}
m2 = va_dub_map(edges)
check("A2 ten-langs", len(m2) == 10 and m2[(5, 50)] == "JAPANESE" and m2[(5, 59)] == "HUNGARIAN",
      "n=%d" % len(m2))

# B: stamp display form, keep existing, skip leftovers
media = {"characters": chars_payload()}
stamp_va_dub_languages(media)
vas = media["characters"]["edges"][0]["voiceActors"]
check("B stamp-display", vas[0].get("languageV2") == "Japanese", vas[0].get("languageV2"))
check("B leftover-untouched", "languageV2" not in vas[1], vas[1].get("languageV2"))
vas[0]["languageV2"] = "Custom"
stamp_va_dub_languages(media)
check("B no-overwrite", vas[0]["languageV2"] == "Custom", vas[0]["languageV2"])

# C: table gets enum dub, fallback primary (anime row first for FK)
tmp = tempfile.mkdtemp(prefix="vadub_test_")
conn = init_db(os.path.join(tmp, "t.db"))
conn.execute("INSERT INTO anime (id, raw_json) VALUES (9001, ?)",
             (json.dumps({"id": 9001, "nextAiringEpisode": {"episode": 1}, "title": {"romaji": "T"}}),))
conn.commit()
media2 = {"characters": chars_payload()}
stamp_va_dub_languages(media2)
upsert_characters(conn, 9001, media2["characters"])
conn.commit()
rows = {r[0]: r[1] for r in
        conn.execute("SELECT voice_actor_id, language FROM character_voice_actors WHERE anime_id=9001")}
check("C table-enum", rows.get(111) == "JAPANESE", rows)
check("C table-fallback", rows.get(188) == "JAPANESE", rows)

# D: raw patch merges (row inserted in C)
patch_raw_json(conn, 9001, {"nextAiringEpisode": {"episode": 2}, "updatedAt": 123})
conn.commit()
raw = json.loads(conn.execute("SELECT raw_json FROM anime WHERE id=9001").fetchone()[0])
check("D patch-merge", raw["nextAiringEpisode"] == {"episode": 2} and raw["updatedAt"] == 123
      and raw["title"] == {"romaji": "T"}, raw)
conn.close()

failed = [r for r in results if not r[1]]
for name, ok, detail in results:
    print(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")


# E: _fetch_va_dubs merges alias lists by character id (separate query)
class DubFetcher(fa.AniListFetcher):
    def __init__(self):
        self.calls = []
        self.fetch_middle_pages = True

    def _request(self, query, variables, retries=0):
        self.calls.append(query)
        assert "dub_JAPANESE" in query
        return {"data": {"Media": {"characters": {"edges": [
            {"node": {"id": 101}, "dub_JAPANESE": [{"id": 111}],
             "dub_ENGLISH": [{"id": 188}]},
            {"node": {"id": 999}},
        ]}}}}


_fx = DubFetcher.__new__(DubFetcher)
_fx.calls = []
_fx.fetch_middle_pages = True
_media = {"id": 7, "characters": {"edges": [
    {"node": {"id": 101}, "voiceActors": [{"id": 111}, {"id": 188}]},
]}}
DubFetcher._fetch_va_dubs(_fx, _media)
_edge = _media["characters"]["edges"][0]
e_results = []
e_results.append(("E merge-by-char",
                  _edge.get("dub_JAPANESE") == [{"id": 111}] and _edge.get("dub_ENGLISH") == [{"id": 188}],
                  str({k: v for k, v in _edge.items() if k.startswith("dub_")})))
e_results.append(("E request-made", len(_fx.calls) == 1, str(len(_fx.calls))))

# F: skips titles without voice actors (no wasted request)
_fx2 = DubFetcher.__new__(DubFetcher)
_fx2.calls = []
_fx2.fetch_middle_pages = True
_media2 = {"id": 8, "characters": {"edges": [{"node": {"id": 102}, "voiceActors": []}]}}
DubFetcher._fetch_va_dubs(_fx2, _media2)
e_results.append(("F skip-va-less", _fx2.calls == [] and "dub_JAPANESE" not in _media2["characters"]["edges"][0], ""))

for name, ok, detail in e_results:
    print(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")
    results.append((name, ok, detail))

failed = [r for r in results if not r[1]]
sys.exit(1 if failed else 0)
