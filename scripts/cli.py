#!/usr/bin/env python3
"""
AniList Offline Database - CLI Tool
Filter, search, and export anime data from the local database.
"""

import os
import sys
import json
import argparse
import csv
import io
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_utils import connect_db, get_db_path, get_database_stats


def get_connection(data_dir: str):
    db_path = get_db_path(data_dir)
    if not os.path.exists(db_path):
        print(f"Error: Database not found at {db_path}")
        print("Run fetch_anilist.py first to create the database.")
        sys.exit(1)
    return connect_db(db_path)


def cmd_search(args, conn):
    query = args.query
    limit = args.limit
    format = args.format

    results = conn.execute("""
        SELECT a.id, a.title_romaji, a.title_english, a.title_native,
               a.episodes, a.status, a.average_score, a.popularity, a.format,
               a.season, a.season_year, a.cover_large
        FROM anime_fts fts
        JOIN anime a ON fts.rowid = a.id
        WHERE anime_fts MATCH ?
        ORDER BY rank
        LIMIT ?
    """, (query, limit)).fetchall()

    if not results:
        print("No results found.")
        return

    if format == "json":
        data = []
        for r in results:
            data.append({
                "id": r[0], "title_romaji": r[1], "title_english": r[2],
                "title_native": r[3], "episodes": r[4], "status": r[5],
                "average_score": r[6], "popularity": r[7], "format": r[8],
                "season": r[9], "season_year": r[10], "cover_image": r[11]
            })
        print(json.dumps(data, indent=2, ensure_ascii=False))
    elif format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID", "Title (Romaji)", "Title (English)", "Title (Native)",
                         "Episodes", "Status", "Score", "Popularity", "Format",
                         "Season", "Year", "Cover"])
        for r in results:
            writer.writerow(r)
        print(output.getvalue())
    else:
        print(f"{'ID':<8} {'Score':<7} {'Popularity':<12} {'Episodes':<10} {'Title'}")
        print("-" * 80)
        for r in results:
            title = r[1] or r[2] or r[3] or "Unknown"
            score = f"{r[6]:.0f}" if r[6] else "N/A"
            pop = str(r[7]) if r[7] else "N/A"
            eps = str(r[4]) if r[4] else "?"
            print(f"{r[0]:<8} {score:<7} {pop:<12} {eps:<10} {title}")


def cmd_filter(args, conn):
    conditions = []
    params = []

    if args.genre:
        conditions.append("g.name = ?")
        params.append(args.genre)
    if args.status:
        conditions.append("a.status = ?")
        params.append(args.status)
    if args.format:
        conditions.append("a.format = ?")
        params.append(args.format)
    if args.season:
        conditions.append("a.season = ?")
        params.append(args.season)
    if args.year:
        conditions.append("a.season_year = ?")
        params.append(args.year)
    if args.min_score:
        conditions.append("a.average_score >= ?")
        params.append(args.min_score)
    if args.max_score:
        conditions.append("a.average_score <= ?")
        params.append(args.max_score)
    if args.min_popularity:
        conditions.append("a.popularity >= ?")
        params.append(args.min_popularity)
    if args.studio:
        conditions.append("s.name LIKE ?")
        params.append(f"%{args.studio}%")
    if args.tag:
        conditions.append("at.tag_name = ?")
        params.append(args.tag)
    if args.airing:
        conditions.append("a.status = 'RELEASING'")
        conditions.append("a.next_airing_at IS NOT NULL")
    if args.adult:
        conditions.append("a.is_adult = 1")
    else:
        conditions.append("a.is_adult = 0")

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    limit = args.limit
    order = args.order

    query = f"""
        SELECT DISTINCT a.id, a.title_romaji, a.title_english, a.title_native,
               a.episodes, a.status, a.average_score, a.popularity, a.format,
               a.season, a.season_year, a.cover_large
        FROM anime a
        LEFT JOIN anime_genres ag ON a.id = ag.anime_id
        LEFT JOIN genres g ON ag.genre_id = g.id
        LEFT JOIN anime_studios ast ON a.id = ast.anime_id
        LEFT JOIN studios s ON ast.studio_id = s.id
        LEFT JOIN anime_tags at ON a.id = at.anime_id
        WHERE {where_clause}
        ORDER BY {order}
        LIMIT ?
    """
    params.append(limit)

    results = conn.execute(query, params).fetchall()

    if not results:
        print("No anime found matching the filters.")
        return

    if args.format_output == "json":
        data = []
        for r in results:
            data.append({
                "id": r[0], "title_romaji": r[1], "title_english": r[2],
                "title_native": r[3], "episodes": r[4], "status": r[5],
                "average_score": r[6], "popularity": r[7], "format": r[8],
                "season": r[9], "season_year": r[10], "cover_image": r[11]
            })
        print(json.dumps(data, indent=2, ensure_ascii=False))
    elif args.format_output == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID", "Title (Romaji)", "Title (English)", "Title (Native)",
                         "Episodes", "Status", "Score", "Popularity", "Format",
                         "Season", "Year", "Cover"])
        for r in results:
            writer.writerow(r)
        print(output.getvalue())
    else:
        print(f"{'ID':<8} {'Score':<7} {'Popularity':<12} {'Episodes':<10} {'Title'}")
        print("-" * 80)
        for r in results:
            title = r[1] or r[2] or r[3] or "Unknown"
            score = f"{r[6]:.0f}" if r[6] else "N/A"
            pop = str(r[7]) if r[7] else "N/A"
            eps = str(r[4]) if r[4] else "?"
            print(f"{r[0]:<8} {score:<7} {pop:<12} {eps:<10} {title}")

    print(f"\nTotal: {len(results)} anime")


def cmd_info(args, conn):
    anime_id = args.id
    anime = conn.execute("SELECT * FROM anime WHERE id=?", (anime_id,)).fetchone()
    if not anime:
        print(f"Anime with ID {anime_id} not found.")
        return

    columns = [desc[0] for desc in conn.execute("SELECT * FROM anime LIMIT 0").description]
    data = dict(zip(columns, anime))

    data["titles"] = {}
    for t in conn.execute("SELECT language, title FROM anime_titles WHERE anime_id=?", (anime_id,)):
        data["titles"][t[0]] = t[1]

    data["descriptions"] = {}
    for d in conn.execute("SELECT language, description FROM anime_descriptions WHERE anime_id=?", (anime_id,)):
        data["descriptions"][d[0]] = d[1]

    data["genres"] = [r[0] for r in conn.execute(
        "SELECT g.name FROM genres g JOIN anime_genres ag ON g.id = ag.genre_id WHERE ag.anime_id=?", (anime_id,)
    )]

    data["tags"] = [{"name": r[0], "rank": r[1]} for r in conn.execute(
        "SELECT tag_name, tag_rank FROM anime_tags WHERE anime_id=?", (anime_id,)
    )]

    data["studios"] = [{"name": r[0], "is_main": bool(r[1])} for r in conn.execute(
        "SELECT s.name, ast.is_main FROM studios s JOIN anime_studios ast ON s.id = ast.studio_id WHERE ast.anime_id=?",
        (anime_id,)
    )]

    data["characters"] = [{"id": r[0], "name": r[1], "role": r[2]} for r in conn.execute(
        """SELECT c.id, c.name_full, ac.role FROM characters c
           JOIN anime_characters ac ON c.id = ac.character_id WHERE ac.anime_id=?
           ORDER BY ac.sort_order""", (anime_id,)
    )]

    data["relations"] = [{"relation": r[0], "id": r[1], "title": r[2]} for r in conn.execute(
        """SELECT r.relation_type, r.related_anime_id, a.title_romaji
           FROM relations r LEFT JOIN anime a ON r.related_anime_id = a.id
           WHERE r.anime_id=?""", (anime_id,)
    )]

    data["recommendations"] = [{"id": r[0], "title": r[1], "rating": r[2]} for r in conn.execute(
        """SELECT r.recommended_anime_id, a.title_romaji, r.rating
           FROM recommendations r LEFT JOIN anime a ON r.recommended_anime_id = a.id
           WHERE r.anime_id=? ORDER BY r.rating DESC""", (anime_id,)
    )]

    data["airing_schedule"] = [{"episode": r[0], "airing_at": r[1]} for r in conn.execute(
        "SELECT episode, airing_at FROM airing_schedule WHERE anime_id=? ORDER BY episode", (anime_id,)
    )]

    if args.format_output == "json":
        print(json.dumps(data, indent=2, ensure_ascii=False, default=str))
    else:
        print(f"\n{'='*60}")
        print(f"  {data.get('title_romaji', 'Unknown')}")
        if data.get('title_english'):
            print(f"  ({data['title_english']})")
        print(f"{'='*60}")
        print(f"  ID: {data.get('id')}")
        print(f"  MAL ID: {data.get('id_mal')}")
        print(f"  Format: {data.get('format')}")
        print(f"  Status: {data.get('status')}")
        print(f"  Episodes: {data.get('episodes') or '?'}")
        print(f"  Duration: {data.get('duration')} min")
        print(f"  Season: {data.get('season')} {data.get('season_year')}")
        print(f"  Score: {data.get('average_score') or 'N/A'}")
        print(f"  Popularity: {data.get('popularity') or 'N/A'}")
        print(f"  Favourites: {data.get('favourites') or 'N/A'}")
        print(f"  Genres: {', '.join(data.get('genres', []))}")
        print(f"  Studios: {', '.join(s['name'] for s in data.get('studios', []))}")
        if data.get('descriptions', {}).get('default'):
            desc = data['descriptions']['default'][:200]
            print(f"  Description: {desc}...")
        print(f"\n  Characters ({len(data.get('characters', []))}):")
        for c in data.get('characters', [])[:10]:
            print(f"    - {c['name']} ({c['role']})")
        if data.get('relations'):
            print(f"\n  Relations:")
            for r in data['relations']:
                print(f"    - {r['relation']}: {r['title']} (ID: {r['id']})")
        print()


def cmd_stats(args, conn):
    stats = get_database_stats(conn)
    if args.format_output == "json":
        print(json.dumps(stats, indent=2, ensure_ascii=False))
    else:
        print(f"\n{'='*40}")
        print(f"  AniList Offline Database Stats")
        print(f"{'='*40}")
        print(f"  Total Anime: {stats['total_anime']}")
        print(f"  Total Characters: {stats['total_characters']}")
        print(f"  Total Studios: {stats['total_studios']}")
        print(f"  Total Genres: {stats['total_genres']}")
        print(f"  Total Tags: {stats['total_tags']}")
        print(f"\n  By Status:")
        for status, count in sorted(stats.get('by_status', {}).items()):
            print(f"    {status}: {count}")
        print(f"\n  By Format:")
        for fmt, count in sorted(stats.get('by_format', {}).items()):
            print(f"    {fmt}: {count}")
        print()


def cmd_genres(args, conn):
    results = conn.execute("""
        SELECT g.name, COUNT(ag.anime_id) as count
        FROM genres g
        LEFT JOIN anime_genres ag ON g.id = ag.genre_id
        GROUP BY g.id
        ORDER BY count DESC
    """).fetchall()

    if args.format_output == "json":
        print(json.dumps([{"name": r[0], "count": r[1]} for r in results], indent=2))
    else:
        print(f"{'Genre':<25} {'Count':<10}")
        print("-" * 35)
        for r in results:
            print(f"{r[0]:<25} {r[1]:<10}")


def cmd_export(args, conn):
    conditions = ["a.is_adult = 0"]
    params = []

    if args.genre:
        conditions.append("g.name = ?")
        params.append(args.genre)
    if args.status:
        conditions.append("a.status = ?")
        params.append(args.status)

    where_clause = " AND ".join(conditions)
    limit_clause = f"LIMIT {args.limit}" if args.limit else ""

    query = f"""
        SELECT DISTINCT a.id, a.title_romaji, a.title_english, a.title_native,
               a.episodes, a.status, a.average_score, a.popularity, a.format,
               a.season, a.season_year, a.cover_large, a.banner_image,
               a.description, a.duration, a.source, a.start_date, a.end_date
        FROM anime a
        LEFT JOIN anime_genres ag ON a.id = ag.anime_id
        LEFT JOIN genres g ON ag.genre_id = g.id
        WHERE {where_clause}
        ORDER BY a.popularity DESC
        {limit_clause}
    """

    results = conn.execute(query, params).fetchall()
    data = []
    for r in results:
        data.append({
            "id": r[0], "title_romaji": r[1], "title_english": r[2],
            "title_native": r[3], "episodes": r[4], "status": r[5],
            "average_score": r[6], "popularity": r[7], "format": r[8],
            "season": r[9], "season_year": r[10], "cover_image": r[11],
            "banner_image": r[12], "description": r[13], "duration": r[14],
            "source": r[15], "start_date": r[16], "end_date": r[17]
        })

    output_path = args.output
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"total": len(data), "anime": data}, f, indent=2, ensure_ascii=False)
    print(f"Exported {len(data)} anime to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="AniList Offline Database CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s search "naruto"
  %(prog)s filter --genre Action --min-score 80 --limit 20
  %(prog)s info 16498
  %(prog)s stats
  %(prog)s genres
  %(prog)s export --output top_anime.json --limit 100
        """
    )
    parser.add_argument("--data-dir", default=None, help="Data directory path")

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    sp_search = subparsers.add_parser("search", help="Full-text search anime")
    sp_search.add_argument("query", help="Search query")
    sp_search.add_argument("--limit", "-n", type=int, default=20, help="Max results")
    sp_search.add_argument("--format", "-f", choices=["text", "json", "csv"], default="text")

    sp_filter = subparsers.add_parser("filter", help="Filter anime by criteria")
    sp_filter.add_argument("--genre", "-g", help="Genre name")
    sp_filter.add_argument("--status", "-s", help="Status (FINISHED, RELEASING, NOT_YET_RELEASED, CANCELLED)")
    sp_filter.add_argument("--format", help="Format (TV, MOVIE, OVA, ONA, SPECIAL, etc.)")
    sp_filter.add_argument("--season", help="Season (WINTER, SPRING, SUMMER, FALL)")
    sp_filter.add_argument("--year", "-y", type=int, help="Season year")
    sp_filter.add_argument("--min-score", type=int, help="Minimum score")
    sp_filter.add_argument("--max-score", type=int, help="Maximum score")
    sp_filter.add_argument("--min-popularity", type=int, help="Minimum popularity")
    sp_filter.add_argument("--studio", help="Studio name (partial match)")
    sp_filter.add_argument("--tag", help="Tag name")
    sp_filter.add_argument("--airing", action="store_true", help="Currently airing only")
    sp_filter.add_argument("--adult", action="store_true", help="Include adult content")
    sp_filter.add_argument("--limit", "-n", type=int, default=50, help="Max results")
    sp_filter.add_argument("--order", "-o", default="a.popularity DESC",
                          help="Order by (e.g., a.average_score DESC, a.popularity DESC)")
    sp_filter.add_argument("--format-output", "-f", choices=["text", "json", "csv"], default="text")

    sp_info = subparsers.add_parser("info", help="Show detailed info for an anime")
    sp_info.add_argument("id", type=int, help="Anime ID")
    sp_info.add_argument("--format-output", "-f", choices=["text", "json"], default="text")

    sp_stats = subparsers.add_parser("stats", help="Show database statistics")
    sp_stats.add_argument("--format-output", "-f", choices=["text", "json"], default="text")

    sp_genres = subparsers.add_parser("genres", help="List all genres with counts")
    sp_genres.add_argument("--format-output", "-f", choices=["text", "json"], default="text")

    sp_export = subparsers.add_parser("export", help="Export filtered data to JSON")
    sp_export.add_argument("--output", "-o", default="export.json", help="Output file")
    sp_export.add_argument("--genre", "-g", help="Genre filter")
    sp_export.add_argument("--status", "-s", help="Status filter")
    sp_export.add_argument("--limit", "-n", type=int, help="Max results")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.data_dir:
        data_dir = args.data_dir
    else:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

    conn = get_connection(data_dir)

    try:
        if args.command == "search":
            cmd_search(args, conn)
        elif args.command == "filter":
            cmd_filter(args, conn)
        elif args.command == "info":
            cmd_info(args, conn)
        elif args.command == "stats":
            cmd_stats(args, conn)
        elif args.command == "genres":
            cmd_genres(args, conn)
        elif args.command == "export":
            cmd_export(args, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
