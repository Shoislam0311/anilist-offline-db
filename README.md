# AniList Offline Database

A complete offline database of AniList's anime catalog, built from their public GraphQL API. Includes a **server-side GraphQL API** on Vercel plus a **client-side GraphQL playground** on GitHub Pages — both with **zero rate limits**.

## Live Endpoints

| Endpoint | URL |
|----------|-----|
| **GraphQL API (Vercel)** | `https://graphql.aniraku.tech/` |
| **Web UI + Playground (Pages)** | `https://shoislam0311.github.io/anilist-offline-db/` |
| **Raw JSON data** | `https://shoislam0311.github.io/anilist-offline-db/api/` |
| **Repository** | `https://github.com/Shoislam0311/anilist-offline-db` |

### Use the Vercel GraphQL API

Drop-in replacement for `graphql.anilist.co` — same queries work, just change the URL. CORS is open (`*`), so any app or site can call it directly.

```bash
curl -X POST https://graphql.aniraku.tech/ \
  -H "Content-Type: application/json" \
  -d '{"query": "{ Page(page:1, perPage:5) { media(sort:POPULARITY_DESC) { id title { romaji english } averageScore } } }"}'
```

Query with variables:

```bash
curl -X POST https://graphql.aniraku.tech/ \
  -H "Content-Type: application/json" \
  -d '{"query": "query($search:String){Page(page:1,perPage:5){media(search:$search){id title{romaji}}}}", "variables": {"search": "Naruto"}}'
```

## Features

- **Drop-in AniList replacement** — same GraphQL schema as `graphql.anilist.co`, no auth, no rate limits
- **Full introspection** — `{ __schema { ... } }` / `{ __type(name: "Media") { ... } }` answered from the official AniList SDL
- **Index-only fast path** — card/search/browse queries (filters, sorts, pagination, titles, covers, scores) resolve from a bundled 14.8k-row search index with *zero network I/O*
- **Shard-backed depth** — descriptions, characters, studios, relations, recommendations and airing data hydrate from JSON shards (only the visible page slice is fetched)
- **Edge caching** — GET queries carry `Cache-Control: s-maxage` + `ETag`/`304`; warm queries skip the function entirely
- **Response cache** — identical repeat POSTs answer from a 60s in-memory LRU
- **Anime Recommendations** - Each anime's recommended similar anime with ratings
- **Anime Relations** - Sequels, prequels, adaptations, side stories, etc.
- **Characters & Voice Actors** - Japanese voice actors, character roles, images
- **Airing Schedule** - Next episode air dates for currently airing anime
- **Studios** - Animation studios with main studio indicator
- **All Languages** - Titles in Romaji, English, Native Japanese + alternative titles
- **Daily Updates** - Automated GitHub Actions workflow runs daily
- **Incremental Updates** - Only fetches anime modified since last run
- **Full Backfill** - Manual `fetch_mode=full` dispatch rebuilds the complete AniList catalog (date-banded walks, split automatically around AniList's 5000-entry page-depth cap)
- **Crash Recovery** - Resumes from checkpoint if interrupted
- **GitHub Releases** - Database files attached to each release
- **Tested** — 32-test suite runs the real Vercel handler in-process (`npm test`)

## Quick Start

### Use the GraphQL API (GitHub Pages playground)

Visit the web UI: `https://shoislam0311.github.io/anilist-offline-db/`

```graphql
{
  Page(page: 1, perPage: 10) {
    media(sort: POPULARITY_DESC, type: ANIME) {
      id
      title { romaji english }
      averageScore
      popularity
      genres
      characters { edges { role node { name { full } } } }
      recommendations { edges { node { rating mediaRecommendation { title { romaji } } } } }
    }
  }
}
```

### Download the Database

Download from [Releases](https://github.com/Shoislam0311/anilist-offline-db/releases) or build locally.

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
git clone https://github.com/Shoislam0311/anilist-offline-db.git
cd anilist-offline-db
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
- Run daily at 04:00 UTC (homepage rails + catalog-wide updatedAt sweep)
- Or, when dispatched manually, run a complete catalog backfill (`fetch_mode=full`)
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

## APIs

### GraphQL API (Vercel, primary data path)

`POST https://graphql.aniraku.tech/` (or `https://anilist-offline-db-phi.vercel.app/`) with `{ "query", "variables", "operationName" }` — same contract as `graphql.anilist.co`. Also:

- `GET /?query=...&variables=...` — edge-cacheable (`s-maxage=300` + `ETag`/`304` revalidation)
- Routes: `/`, `/graphql`, `/api`, `/api/graphql`
- Roots: `Page`, `Media`, `Character`, `Staff`, `Studio`, `GenreCollection`, `MediaTagCollection`, `AiringSchedule` (+ `__schema`/`__type` introspection)
- Filters (`search`, `genre(_in/_not_in)`, `tag(_in/_not_in)`, `format`, `status`, `season`, `seasonYear`, `id(_in)`, `isAdult`, …), all `MediaSort` orders, variables, aliases, fragments, `operationName`
- Open CORS (`*`), `X-Content-Type-Options: nosniff`, query depth/size guards

```bash
curl -X POST https://graphql.aniraku.tech/ \
  -H "Content-Type: application/json" \
  -d '{"query": "{ Page(page:1, perPage:5) { media(sort:POPULARITY_DESC) { id title { romaji english } averageScore } } }"}'
```

```bash
# GET variant — served from the CDN edge when warm
curl 'https://graphql.aniraku.tech/?query=%7B%20Page(page%3A1%2C%20perPage%3A5)%7B%20media(sort%3APOPULARITY_DESC)%7B%20id%20title%7B%20romaji%20%7D%20%7D%20%7D%20%7D'
```

### GitHub Pages site

`https://shoislam0311.github.io/anilist-offline-db/` — browse/search/schedule UI plus a playground. It calls the Vercel API above (GET-first, endpoint failover); tiny files (`metadata.json`, `search_index.json`) load directly from the Pages/CDN mirror for offline-capable search.

### API Examples

```graphql
# Get anime by ID
{ Media(id: 16498) { id title { romaji } } }

# Search
{ Page { media(search: "one piece") { id title { romaji } } } }

# Filter
{ Page { media(genre: "Action", seasonYear: 2024, sort: SCORE_DESC) { id } } }

# Paginate
{ Page(page: 2, perPage: 20) { media(sort: POPULARITY_DESC) { id } pageInfo { total lastPage } } }

# Introspection
{ __type(name: "Media") { fields { name } } }
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

## Architecture — why it's fast

1. **Bundled search index first.** `docs/api/search_index.json` (14,780 rows × 29 fields) ships inside the function. Filters, sorts, search relevance, pagination and `pageInfo` totals all run in-process — a card query never touches the network.
2. **Shards only for deep fields.** Selections like `description`, `characters`, `studios`, `relations` hydrate exactly the visible page slice from JSON shards (GitHub release assets, cached in-memory per instance).
3. **Configured data source wins.** With `DATA_BASE_URL` set, that base is tried *before* release assets, with fetch timeouts and a 20s failure backoff; the bundled snapshot degrades gracefully when the source is dead.
4. **Caching at every layer.** Browser `max-age` → CDN `s-maxage` → function response LRU → per-instance shard/index caches.
5. **Introspection via graphql-js** against `api/schema.graphql` (dumped from `graphql.anilist.co`), partitioned from the fast custom engine — mixed documents run both.

## Testing

```bash
npm test          # 32 tests: boots the real Vercel handler in-process
                  # (fixtures + request-logging file server, no network)
npm run probe     # latency probe: root/cards/introspection/detail/GET
npm run dev       # local dev server (reads .env if present, optional)
node scripts/dump_schema.mjs   # re-dump the official SDL -> api/schema.graphql
```

The suite asserts the routing/headers contract, index-only serving (zero shard fetches for cards), filter/sort/pagination correctness, introspection, shard hydration, caching (ETag/304/response cache), and dead-source resilience.

## Project Structure

```
anilist-offline-db/
├── .github/workflows/                    # daily fetch + Pages deploy
├── api/
│   ├── index.js                          # Vercel serverless GraphQL handler
│   └── schema.graphql                    # official AniList SDL (introspection)
├── vercel.json                           # rewrites (/, /graphql, /api) + headers
├── package.json                          # Node deps (graphql) + test/dev/probe
├── scripts/
│   ├── fetch_anilist.py                  # Main API scraper
│   ├── api_generator.py                  # JSON shard + search_index generator
│   ├── cli.py / db_utils.py              # CLI + SQLite/FTS5 utilities
│   ├── test_api.mjs                      # 32-test suite (npm test)
│   ├── probe.mjs                         # latency probe (npm run probe)
│   ├── dev_server.mjs                    # local dev server (npm run dev)
│   ├── dump_schema.mjs                   # SDL dump -> api/schema.graphql
│   └── make_fixtures.mjs                 # builds test/fixtures/
├── test/fixtures/                        # committed fixture shards + metadata
├── docs/
│   ├── index.html                        # GitHub Pages site
│   ├── api.js                            # site data layer (endpoint failover, GET-first)
│   └── api/                              # Generated JSON data
│       ├── metadata.json
│       ├── search_index.json
│       └── shards/                       # generated on update (release assets)
├── requirements.txt
├── LICENSE                               # Apache-2.0
└── README.md
```
## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## [License](./LICENSE)

Apache-2.0 - See [LICENSE](LICENSE)

## Acknowledgments

- [AniList](https://anilist.co) for their public GraphQL API
- Data sourced from AniList's public API
