#!/usr/bin/env python3
"""
AniList Offline Database - Turso whole-file upload (one-shot bootstrap).

Turso Cloud exposes a native database-file upload API:
  POST https://{db-host}/v1/upload  (raw SQLite binary + DB token)
Up to 20GB per upload. One ~2-minute transfer replaces ~1.5M row-by-row
statements (which took 6+ hours through per-request latency).

Security model (do NOT weaken this):
  - Browsers NEVER get the full token. Pages calls Vercel, Vercel calls Turso.
  - Turso read-only tokens exist (`turso db tokens create mydb --read-only`)
    for a future optional direct-browser path; not used here.
  - The canonical local DB is never modified; flag stamping happens on a copy.

File requirements (enforced, normalized automatically):
  journal_mode=WAL, page_size=4096, auto_vacuum=NONE(0), encoding=UTF-8.

Env required: TURSO_URL, TURSO_AUTH_TOKEN  (exit 2 if missing)
Usage:
  python3 scripts/turso_upload.py --check    # exit 0 complete / 1 incomplete
  python3 scripts/turso_upload.py            # full upload flow
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

MIN_ANIME_ROWS = 10000


def _env():
    url = (os.environ.get("TURSO_URL", "") or "").strip()
    # Pasted secrets often carry a trailing newline/space, which requests
    # rejects with InvalidHeader. The token itself is unaffected by stripping.
    token = (os.environ.get("TURSO_AUTH_TOKEN", "") or "").strip()
    if not url or not token:
        print("ERROR: TURSO_URL / TURSO_AUTH_TOKEN not set", file=sys.stderr)
        sys.exit(2)
    return url, token


def upload_host(turso_url: str) -> str:
    """Derive the upload hostname from any TURSO_URL shape."""
    u = turso_url.strip()
    for prefix in ("libsql://", "https://", "http://", "wss://", "ws://"):
        if u.startswith(prefix):
            u = u[len(prefix):]
            break
    u = u.split("?")[0].rstrip("/")
    host = u.split("/")[0]
    if not host or "." not in host:
        raise ValueError(f"cannot derive Turso host from {turso_url!r}")
    return host


def check_pragmas(db_path: str) -> dict:
    import sqlite3
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        out = {
            "journal_mode": conn.execute("PRAGMA journal_mode").fetchone()[0],
            "page_size": conn.execute("PRAGMA page_size").fetchone()[0],
            "auto_vacuum": conn.execute("PRAGMA auto_vacuum").fetchone()[0],
            "encoding": conn.execute("PRAGMA encoding").fetchone()[0],
        }
        return out
    finally:
        conn.close()


def conforms(pragmas: dict) -> bool:
    return (
        str(pragmas.get("journal_mode", "")).upper() == "WAL"
        and int(pragmas.get("page_size", 0)) == 4096
        and int(pragmas.get("auto_vacuum", -1)) == 0
        and str(pragmas.get("encoding", "")).upper() == "UTF-8"
    )


def make_upload_copy(src_path: str, dst_path: str):
    """Produce an upload-ready copy: conforming pragmas + completion flag.
    The canonical source DB is never modified."""
    import sqlite3
    import shutil
    shutil.copyfile(src_path, dst_path)
    conn = sqlite3.connect(dst_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA page_size=4096")
        # page_size + vacuum mode apply on rebuild
        conn.execute("VACUUM")
        conn.execute("CREATE TABLE IF NOT EXISTS sync_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        from datetime import datetime, timezone
        total = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
        conn.execute("INSERT OR REPLACE INTO sync_state (key, value) VALUES ('bootstrap_complete', '1')")
        conn.execute("INSERT OR REPLACE INTO sync_state (key, value) VALUES ('total_anime', ?)", (str(total),))
        conn.execute(
            "INSERT OR REPLACE INTO sync_state (key, value) VALUES ('completed_at', ?)",
            (datetime.now(timezone.utc).isoformat(),))
        conn.commit()
        final = {
            "journal_mode": conn.execute("PRAGMA journal_mode").fetchone()[0],
            "page_size": conn.execute("PRAGMA page_size").fetchone()[0],
            "auto_vacuum": conn.execute("PRAGMA auto_vacuum").fetchone()[0],
            "encoding": conn.execute("PRAGMA encoding").fetchone()[0],
            "total_anime": total,
        }
        return final
    finally:
        conn.close()


def remote_is_complete() -> bool:
    """True when the remote already carries the completion flag."""
    from db_utils import connect_db  # noqa: F401  (kept for symmetry; not used remotely)
    import libsql_experimental as libsql
    url, token = _env()
    conn = libsql.connect(database=url, auth_token=token)
    try:
        rows = conn.execute(
            "SELECT value FROM sync_state WHERE key = 'bootstrap_complete'").fetchall()
        return bool(rows) and rows[0][0] == "1"
    except Exception:
        return False


def upload_file(upload_path: str, host: str, token: str, timeout=(30, 1800)):
    import requests
    scheme = os.environ.get("TURSO_UPLOAD_SCHEME", "https")  # http only for local tests
    size = os.path.getsize(upload_path)
    url = f"{scheme}://{host}/v1/upload"
    print(f"Uploading {size / 1024 / 1024:.0f}MB to {host} ...")
    with open(upload_path, "rb") as f:
        resp = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Content-Length": str(size)},
            data=f,
            timeout=timeout,
        )
    if resp.status_code != 200:
        raise RuntimeError(f"upload failed HTTP {resp.status_code}: {resp.text[:300]}")
    print("Upload accepted (HTTP 200).")
    return True


def verify_remote(expected_total: int) -> bool:
    import libsql_experimental as libsql
    url, token = _env()
    conn = libsql.connect(database=url, auth_token=token)
    try:
        total = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
        rows = conn.execute(
            "SELECT value FROM sync_state WHERE key = 'bootstrap_complete'").fetchall()
        flag = bool(rows) and rows[0][0] == "1"
        print(f"Remote verification: anime={total} (expected {expected_total}), flag={flag}")
        return flag and int(total) == int(expected_total)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def check_main() -> int:
    _env()
    ok = remote_is_complete()
    print("complete" if ok else "incomplete")
    return 0 if ok else 1


def main() -> int:
    from db_utils import connect_db, get_db_path
    if len(sys.argv) > 1 and sys.argv[1] == "--check":
        return check_main()
    url, token = _env()
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")
    db_path = get_db_path(data_dir)
    if not os.path.exists(db_path):
        print(f"ERROR: local DB not found at {db_path}", file=sys.stderr)
        return 1
    lconn = connect_db(db_path)
    try:
        total = lconn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
    finally:
        lconn.close()
    if total < MIN_ANIME_ROWS:
        print(f"ERROR: local DB holds only {total} anime (< {MIN_ANIME_ROWS}); refusing to upload partial.",
              file=sys.stderr)
        return 1
    print(f"Local DB: {total} anime, {os.path.getsize(db_path) / 1024 / 1024:.0f}MB")
    print("Source pragmas:", check_pragmas(db_path))

    host = upload_host(url)
    print("Upload host:", host)
    upload_path = os.path.join(data_dir, "upload.db")
    try:
        final = make_upload_copy(db_path, upload_path)
        print("Upload copy ready:", final)
        if not conforms(final):
            print(f"ERROR: copy still non-conforming: {final}", file=sys.stderr)
            return 1
        upload_file(upload_path, host, token)
        if not verify_remote(final["total_anime"]):
            print("ERROR: post-upload verification failed.", file=sys.stderr)
            return 1
        print("SUCCESS: full database live on Turso, flag set, counts match.")
        return 0
    finally:
        try:
            if os.path.exists(upload_path):
                os.remove(upload_path)
        except OSError:
            pass


if __name__ == "__main__":
    sys.exit(main())
