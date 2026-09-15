# AniList Offline Database

A complete offline database of AniList's anime catalog, built from their public GraphQL API. Includes a **full GraphQL API** on GitHub Pages with **zero rate limits**.

## Features

- **Full GraphQL API** - Mirrors `graphql.anilist.co` with identical schema, zero rate limits
- **SQLite + JSON** - Queryable database + JSON export
- **FTS5 Search** - Full-text search on titles, descriptions, characters, genres, tags
- **Anime Recommendations** - Each anime's recommended similar anime with ratings
- **Anime Relations** - Sequels, prequels, adaptations, side stories, etc.
- **Characters & Voice Actors** - Japanese voice actors, character roles, images
- **Airing Schedule** - Next episode air dates for currently airing anime
- **Studios** - Animation studios with main studio indicator
- **All Languages** - Titles in Romaji, English, Native Japanese + alternative titles
- **Daily Updates** - Automated GitHub Actions workflow runs daily
- **Incremental Updates** - Only fetches anime modified since last run
- **Crash Recovery** - Resumes from checkpoint if interrupted
- **GitHub Releases** - Database files attached to each release

## Quick Start

### Use the GraphQL API

Visit your GitHub Pages URL: `https://<username>.github.io/<repo>/`

```graphql
{
  Page(page: 1, perPage: 10) {
    media(sort: POPULARITY_DESC, type: ANIME) {
      id
      title { romaji english }
      averageScore
      popularity
      genres
      characters { edges { node { name full } role } }
      recommendations { edges { node { mediaRecommendation { title romaji } rating } } }
    }
  }
}
```

### Download the Database

Download from [Releases](https://github.com/<username>/<repo>/releases) or build locally.

```bash
# SQLite
sqlite3 anilist.db "SELECT title_romaji, average_score FROM anime ORDER BY popularity DESC LIMIT 10;"

# JSON
cat anilist.json | jq '.anime[:3] | .[].title_romaji'
```

### CLI Tool

```bash
# Search
python3 scripts/cli.py search "naruto"

# Filter
python3 scripts/cli.py filter --genre Action --min-score 80 --limit 20

# Info
python3 scripts/cli.py info 16498

# Stats
python3 scripts/cli.py stats

# Export
python3 scripts/cli.py export --output top_anime.json --limit 100
```

## Setup

### Prerequisites

- Python 3.10+
- GitHub account

### 1. Create the Repository

```bash
git clone https://github.com/<username>/<repo>.git
cd <repo>
pip install -r requirements.txt
```

### 2. Run Initial Fetch (Full)

```bash
FETCH_MODE=full python3 scripts/fetch_anilist.py
```

This will take **1-3 hours** for the full catalog (~22K anime). The script:
- Fetches 50 anime per request
- Rate limited to 28 req/min (respects AniList's 30 req/min limit)
- Saves checkpoints every 500 anime for crash recovery
- Builds FTS5 search index
- Exports JSON

### 3. Generate API Data

```bash
python3 scripts/api_generator.py
```

### 4. Push to GitHub

```bash
git add .
git commit -m "initial: full fetch of AniList database"
git push
```

### 5. Enable GitHub Pages

1. Go to repo Settings > Pages
2. Source: Deploy from branch
3. Branch: `main` (or `master`), folder: `/docs`
4. Save

The workflow will automatically:
- Run daily at 04:00 UTC
- Fetch only updated anime (incremental)
- Commit changes
- Create GitHub Release with database files
- Deploy updated Pages site

## Database Schema

### Core Tables

| Table | Description |
|-------|-------------|
| `anime` | Main anime data (title, score, episodes, status, etc.) |
| `anime_titles` | All language variants of titles |
| `anime_descriptions` | Descriptions in multiple languages |
| `genres` | Genre list |
| `anime_genres` | Anime-genre associations |
| `tags` | Tags with categories and ranks |
| `anime_tags` | Anime-tag associations |
| `studios` | Animation studios |
| `anime_studios` | Anime-studio associations (main/secondary) |
| `characters` | Character data (name, image, description, favourites) |
| `anime_characters` | Character roles per anime |
| `voice_actors` | Japanese voice actors |
| `character_voice_actors` | VA-character associations |
| `relations` | Anime-anime relations (sequel, prequel, etc.) |
| `recommendations` | Anime recommendations with ratings |
| `airing_schedule` | Upcoming episode air dates |
| `external_links` | External site links (Crunchyroll, MAL, etc.) |
| `streaming_episodes` | Streaming episode info |
| `statistics` | Score distributions, rankings, trends |

### FTS5 Indexes

- `anime_fts` - Search on titles, description, genres, tags, studios, characters
- `characters_fts` - Search on character names and descriptions

## Rate Limiting

The fetcher respects AniList's **30 requests/minute** limit:
- 2.1 second delay between requests
- Sliding window tracking (28 req/60s)
- Exponential backoff on 429/503 errors
- Automatic retry up to 5 times

For the full catalog (~22K anime):
- 440 pages × 50 anime/page
- ~15 minutes at 28 req/min
- With overhead: **20-30 minutes**

## GitHub Pages API

The static site loads JSON data shards and resolves GraphQL queries client-side:

- **Zero rate limits** - All data is local
- **Same schema** - Identical to `graphql.anilist.co`
- **Search** - Full-text search with filters
- **Browse** - Filter by genre, status, format, sort
- **Playground** - Interactive query editor

### API Endpoints (via GraphQL)

```graphql
# Get anime by ID
{ Media(id: 16498) { id title { romaji } } }

# Search
{ Page(search: "one piece") { media { id title { romaji } } } }

# Filter
{ Page(genre: "Action", seasonYear: 2024, sort: SCORE_DESC) { media { id } } }

# Paginate
{ Page(page: 2, perPage: 20, sort: POPULARITY_DESC) { media { id } } }
```

## CLI Reference

```
search <query>           Full-text search anime
  --limit, -n            Max results (default: 20)
  --format, -f           Output format: text, json, csv

filter                   Filter anime by criteria
  --genre, -g            Genre name
  --status, -s           FINISHED, RELEASING, NOT_YET_RELEASED, CANCELLED
  --format               TV, MOVIE, OVA, ONA, SPECIAL
  --season               WINTER, SPRING, SUMMER, FALL
  --year, -y             Season year
  --min-score            Minimum score
  --max-score            Maximum score
  --min-popularity       Minimum popularity
  --studio               Studio name (partial match)
  --tag                  Tag name
  --airing               Currently airing only
  --limit, -n            Max results (default: 50)
  --order, -o            Order by (default: popularity DESC)
  --format-output, -f    Output format: text, json, csv

info <id>                Show detailed anime info
  --format-output, -f    Output format: text, json

stats                    Show database statistics
genres                   List all genres with counts
export                   Export filtered data to JSON
  --output, -o           Output file (default: export.json)
  --genre, -g            Genre filter
  --status, -s           Status filter
  --limit, -n            Max results
```

## Project Structure

```
anilist-offline-db/
├── .github/workflows/update-db.yml    # Daily GitHub Action
├── scripts/
│   ├── fetch_anilist.py                # Main API scraper
│   ├── db_utils.py                     # SQLite + FTS5 utilities
│   ├── cli.py                          # CLI filter/export tool
│   └── api_generator.py                # JSON shard generator
├── docs/
│   ├── index.html                      # GitHub Pages site
│   ├── api.js                          # GraphQL engine (client-side)
│   └── api/                            # Generated JSON data
│       ├── metadata.json
│       ├── search_index.json
│       ├── sample.json
│       └── shards/
│           ├── shard_0000.json
│           ├── shard_0001.json
│           └── ...
├── data/
│   ├── anilist.db                      # SQLite database
│   ├── anilist.json                    # JSON export
│   └── changelog.md                    # Update changelog
├── requirements.txt
├── .gitignore
├── LICENSE                             # Apache-2.0
└── README.md
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

Apache-2.0 - See [LICENSE](LICENSE)

## Acknowledgments

- [AniList](https://anilist.co) for their public GraphQL API
- Data sourced from AniList's public API
