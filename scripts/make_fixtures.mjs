#!/usr/bin/env node
// One-off fixture builder: fetches real AniList-exact Media objects for a few
// anime and writes test/fixtures/{shard_0000.json,metadata.json}.
// Fixtures are COMMITTED so the test suite never needs the network.
// Run: node scripts/make_fixtures.mjs
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const outDir = join(here, '..', 'test', 'fixtures');
mkdirSync(outDir, { recursive: true });

const IDS = [1, 20, 1535, 5114, 9253, 16498, 101922, 113415];

// Trimmed but shape-complete AniList Media selection (mirrors api_generator's
// exact shard shape: camelCase, FuzzyDate objects, connection edges).
const Q = `query ($ids: [Int]) {
  Page(page: 1, perPage: ${IDS.length}) {
    media(id_in: $ids, sort: ID) {
      id idMal type format status description episodes duration chapters volumes
      averageScore meanScore popularity favourites trending season seasonYear seasonInt
      countryOfOrigin isLicensed source hashtag bannerImage isAdult updatedAt siteUrl
      title { romaji english native userPreferred }
      coverImage { extraLarge large medium color }
      startDate { year month day }
      endDate { year month day }
      trailer { id site thumbnail }
      synonyms genres
      tags { id name description category rank isGeneralSpoiler isMediaSpoiler isAdult }
      studios { edges { id isMain node { id name isAnimationStudio siteUrl favourites } } }
      characters(page: 1, perPage: 4) { edges { id role node { id name { first middle last full native userPreferred } image { large medium } description gender age favourites siteUrl } } }
      staff(page: 1, perPage: 3) { edges { id role node { id name { full native } image { large medium } language primaryOccupations siteUrl favourites } } }
      relations { edges { id relationType node { id title { romaji english } coverImage { large } } } }
      recommendations(page: 1, perPage: 4, sort: RATING_DESC) { edges { node { id rating userRating mediaRecommendation { id title { romaji english } coverImage { large } } } } }
      externalLinks { id site url type language }
      streamingEpisodes { title thumbnail url site }
      airingSchedule(page: 1, perPage: 4) { edges { node { id episode airingAt mediaId } } }
      nextAiringEpisode { id episode airingAt timeUntilAiring mediaId }
      rankings { id rank type format year season allTime context }
      stats { scoreDistribution { score amount } statusDistribution { status amount } }
    }
  }
}`;

const res = await fetch('https://graphql.anilist.co', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
  body: JSON.stringify({ query: Q, variables: { ids: IDS } }),
});
const body = await res.json().catch(() => null);
if (!res.ok) throw new Error(`AniList HTTP ${res.status}: ${JSON.stringify(body).slice(0, 500)}`);
if (body.errors) throw new Error('AniList errors: ' + JSON.stringify(body.errors));

const media = body.data.Page.media.slice().sort((a, b) => a.id - b.id);
if (media.length !== IDS.length) throw new Error(`expected ${IDS.length} anime, got ${media.length}`);

writeFileSync(join(outDir, 'shard_0000.json'), JSON.stringify(media));
const genres = [...new Set(media.flatMap((m) => m.genres || []))].sort();
writeFileSync(join(outDir, 'metadata.json'), JSON.stringify({
  type: 'ANIME',
  totalAnime: media.length,
  shardStartIds: [media[0].id],
  shardSize: media.length,
  totalShards: 1,
  genres,
  generatedAt: new Date().toISOString(),
  schema: 'anilist-exact-anime-v2',
  releaseTag: null,
}, null, 2));

console.log(`fixtures: ${media.length} anime, ids ${media.map((m) => m.id).join(', ')}`);
