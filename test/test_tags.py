"""Regression tests for upsert_tags dual-unique-key reconciliation.

The first full-backfill run on CI died with:
    sqlite3.IntegrityError: UNIQUE constraint failed: tags.id
because tags has TWO unique keys (id PK, name UNIQUE) while the upsert only
handled conflicts on name. AniList renames/re-creates tags over time, so an
incoming (id, name) pair can collide with a stale row on the other key.

Scenarios:
  R1 rename: incoming (id=66, 'New') into existing (id=66, 'Old')       ← the CI crash
  R2 name moved: existing (id=9, 'Action'), incoming (id=42, 'Action')
  R3 crosswise: rows (5,'X'),(7,'Y'); incoming (5,'Y')
  R4 normal path: fresh insert + idempotent re-run + metadata update
  R5 no-id fallback: payload without id stays name-keyed, keeps existing id
  R6 joins: api_generator's tags JOIN anime_tags ON name keeps resolving
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))
from db_utils import init_db, upsert_tags

results = []


def fresh_db():
    tmp = tempfile.mkdtemp(prefix="tag_test_")
    return init_db(os.path.join(tmp, "anilist.db"))


def tags_of(conn):
    return {r[0]: r[1] for r in conn.execute("SELECT id, name FROM tags")}


def tag_row(conn, tid):
    return conn.execute(
        "SELECT name, description, category, rank FROM tags WHERE id=?", (tid,)
    ).fetchone()


# R1: the exact CI crash shape — id owned by a different name
conn = fresh_db()
conn.execute("INSERT INTO tags (id, name, description, category) VALUES (66, 'Old Name', 'old desc', 'cat')")
conn.execute("INSERT INTO anime (id, title_romaji) VALUES (1, 'Test')")
try:
    upsert_tags(conn, 1, [{"id": 66, "name": "New Name", "rank": 3, "description": None, "category": None}])
    ok = tags_of(conn) == {66: "New Name"}
    desc = tag_row(conn, 66)[1]
    ok = ok and desc == "old desc"  # description preserved when payload omits it
    results.append(("R1 rename (CI crash shape)", ok, f"tags={tags_of(conn)} desc={desc}"))
except Exception as e:
    results.append(("R1 rename (CI crash shape)", False, f"raised {type(e).__name__}: {e}"))

# R2: name moved to a new id — stale owner retired, name-keyed link intact
conn = fresh_db()
conn.execute("INSERT INTO tags (id, name, description) VALUES (9, 'Action', 'stale desc')")
conn.execute("INSERT INTO anime (id, title_romaji) VALUES (1, 'Test')")
try:
    upsert_tags(conn, 1, [{"id": 42, "name": "Action", "rank": 10, "description": None}])
    joined = conn.execute(
        "SELECT t.id, t.description FROM tags t JOIN anime_tags at2 ON t.name=at2.tag_name WHERE at2.anime_id=1"
    ).fetchall()
    ok = tags_of(conn) == {42: "Action"} and joined == [(42, "stale desc")]
    results.append(("R2 name-moved-to-new-id", ok, f"tags={tags_of(conn)} join={joined}"))
except Exception as e:
    results.append(("R2 name-moved-to-new-id", False, f"raised {type(e).__name__}: {e}"))

# R3: crosswise collision — incoming id=5 wants name 'Y' owned by id=7
conn = fresh_db()
conn.execute("INSERT INTO tags (id, name) VALUES (5, 'X')")
conn.execute("INSERT INTO tags (id, name) VALUES (7, 'Y')")
conn.execute("INSERT INTO anime (id, title_romaji) VALUES (1, 'Test')")
try:
    upsert_tags(conn, 1, [{"id": 5, "name": "Y", "rank": 1}])
    ok = tags_of(conn) == {5: "Y"}
    results.append(("R3 crosswise id/name swap", ok, f"tags={tags_of(conn)}"))
except Exception as e:
    results.append(("R3 crosswise id/name swap", False, f"raised {type(e).__name__}: {e}"))

# R4: normal insert idempotence + metadata update
conn = fresh_db()
conn.execute("INSERT INTO anime (id, title_romaji) VALUES (1, 'Test')")
upsert_tags(conn, 1, [{"id": 100, "name": "Shounen", "rank": 7, "description": "d1", "category": "Genre"}])
upsert_tags(conn, 1, [{"id": 100, "name": "Shounen", "rank": 7, "description": "d1", "category": "Genre"}])
upsert_tags(conn, 1, [{"id": 100, "name": "Shounen", "rank": 9, "description": "d2", "category": "Genre"}])
row = tag_row(conn, 100)
ok = tags_of(conn) == {100: "Shounen"} and row[2:] == ("Genre", 9) and row[1] == "d2"
results.append(("R4 normal+idempotent+update", ok, f"row={row}"))

# R5: payload without id — name-keyed, existing id kept, no crash
conn = fresh_db()
conn.execute("INSERT INTO tags (id, name, description) VALUES (55, 'Isekai', 'old')")
conn.execute("INSERT INTO anime (id, title_romaji) VALUES (1, 'Test')")
try:
    upsert_tags(conn, 1, [{"name": "Isekai", "rank": 2, "description": None}])
    ok = tags_of(conn) == {55: "Isekai"} and tag_row(conn, 55)[0] == "Isekai"
    results.append(("R5 no-id fallback keeps id", ok, f"tags={tags_of(conn)}"))
except Exception as e:
    results.append(("R5 no-id fallback keeps id", False, f"raised {type(e).__name__}: {e}"))

# R6: api_generator-style join resolves after a rename (link by name, not id)
conn = fresh_db()
conn.execute("INSERT INTO tags (id, name) VALUES (30, 'Psychological')")
conn.execute("INSERT INTO anime (id, title_romaji) VALUES (1, 'Test')")
upsert_tags(conn, 1, [{"id": 30, "name": "Psychological", "rank": 1}])
upsert_tags(conn, 1, [{"id": 31, "name": "Psychological", "rank": 4}])
joined = conn.execute(
    "SELECT t.id, at2.tag_rank FROM tags t JOIN anime_tags at2 ON t.name=at2.tag_name WHERE at2.anime_id=1"
).fetchall()
ok = joined == [(31, 4)]
results.append(("R6 generator join after rename", ok, f"join={joined}"))

failed = [r for r in results if not r[1]]
for name, ok, detail in results:
    print(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")
sys.exit(1 if failed else 0)
