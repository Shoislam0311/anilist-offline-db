#!/usr/bin/env node
// Accuracy check: compares the mirror against the real AniList API for a few
// canonical anime. Run with the mirror base URL as argv[2] (default localhost).
const BASE = (process.argv[2] || 'http://localhost:8787').replace(/\/+$/, '');

const NESTED = `query ($id: Int) { Media(id: $id) {
  id idMal title{romaji english native} averageScore meanScore popularity favourites
  format status episodes duration season seasonYear source countryOfOrigin isLicensed isAdult hashtag
  genres synonyms
  tags{ name rank category isGeneralSpoiler isMediaSpoiler isAdult }
  startDate{year month day} endDate{year month day}
  trailer{id site thumbnail}
  nextAiringEpisode{ episode airingAt timeUntilAiring }
  relations{ edges{ relationType node{ id } } }
  recommendations(sort:RATING_DESC){ nodes{ id rating mediaRecommendation{ id } } }
  studios{ edges{ isMain node{ id name } } }
  rankings{ rank type context year allTime }
  stats{ scoreDistribution{ score amount } }
  externalLinks{ site url type }
  streamingEpisodes{ title site }
} }`;

async function gql(base, query, variables) {
  const r = await fetch(`${base}/graphql`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, variables }),
    signal: AbortSignal.timeout(60000),
  });
  return r.json();
}

const IDS = [1, 21, 5114]; // Cowboy Bebop, One Piece, Frieren
let issues = 0, compared = 0;

function cmp(label, a, b, tol = 0) {
  compared++;
  const norm = (v) => (Array.isArray(v) ? v.map((x) => (x && typeof x === 'object' ? JSON.stringify(x) : String(x))).sort().join('|') : v === undefined ? '<missing>' : String(v));
  if (norm(a) !== norm(b)) {
    issues++;
    console.log(`  DIFF ${label}: anilist=${norm(a).slice(0, 90)} mirror=${norm(b).slice(0, 90)}`);
  }
}

for (const id of IDS) {
  console.log(`\n== Media(${id}) ==`);
  const [al, mi] = await Promise.all([
    gql('https://graphql.anilist.co', NESTED, { id }),
    gql(BASE, NESTED, { id }),
  ]);
  const A = al?.data?.Media, M = mi?.data?.Media;
  if (!A) { console.log('  (anilist fetch failed — skipping)', JSON.stringify(al?.errors)?.slice(0, 120)); continue; }
  if (!M) { console.log('  MIRROR FAILED', JSON.stringify(mi?.errors)?.slice(0, 200)); issues++; continue; }
  cmp('idMal', A.idMal, M.idMal);
  cmp('averageScore', A.averageScore, M.averageScore);
  cmp('popularity', A.popularity, M.popularity);
  cmp('favourites', A.favourites, M.favourites);
  cmp('format', A.format, M.format);
  cmp('status', A.status, M.status);
  cmp('episodes', A.episodes, M.episodes);
  cmp('season', A.season, M.season);
  cmp('seasonYear', A.seasonYear, M.seasonYear);
  cmp('genres', A.genres, M.genres);
  cmp('synonyms', A.synonyms, M.synonyms);
  cmp('tag names', A.tags?.map((t) => t.name), M.tags?.map((t) => t.name));
  cmp('tag ranks', A.tags?.map((t) => t.rank), M.tags?.map((t) => t.rank));
  cmp('trailer.id', A.trailer?.id ?? null, M.trailer?.id ?? null);
  cmp('relation ids', A.relations?.edges?.map((e) => e.node?.id), M.relations?.edges?.map((e) => e.node?.id));
  cmp('relation types', A.relations?.edges?.map((e) => e.relationType), M.relations?.edges?.map((e) => e.relationType));
  cmp('rec ids', A.recommendations?.nodes?.map((n) => n.mediaRecommendation?.id), M.recommendations?.nodes?.map((n) => n.mediaRecommendation?.id));
  cmp('studio names', A.studios?.edges?.map((e) => e.node?.name), M.studios?.edges?.map((e) => e.node?.name));
  cmp('studio isMain', A.studios?.edges?.map((e) => e.isMain), M.studios?.edges?.map((e) => e.isMain));
  cmp('ranking contexts', A.rankings?.map((r) => `${r.rank}:${r.context}:${r.allTime}`), M.rankings?.map((r) => `${r.rank}:${r.context}:${r.allTime}`));
  cmp('scoreDist scores', A.stats?.scoreDistribution?.map((s) => s.score), M.stats?.scoreDistribution?.map((s) => s.score));
  cmp('extLink sites', A.externalLinks?.map((l) => l.site), M.externalLinks?.map((l) => l.site));
  cmp('streamEp titles', A.streamingEpisodes?.map((s) => s.title), M.streamingEpisodes?.map((s) => s.title));
  // nextAiringEpisode is time-sensitive; compare episode number only
  cmp('nextAiring.episode', A.nextAiringEpisode?.episode ?? null, M.nextAiringEpisode?.episode ?? null);
}

console.log(`\n=== ACCURACY: ${compared} field comparisons, ${issues} diffs (data ages up to 1 day — scores/popularity drift expected) ===`);
console.log(`Note: trending/popularity/score drift vs live AniList is expected (daily snapshot).`);
