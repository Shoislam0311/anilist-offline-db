#!/usr/bin/env node
/**
 * Test suite for the AniList offline mirror handler (api/index.js).
 *
 * - Boots the REAL Vercel handler in-process (no Turso, no network).
 * - Serves test/fixtures + the real search_index.json from a local HTTP
 *   server that logs every hit, so tests can assert what was (not) fetched.
 * - Asserts TARGET behavior: most tests are red until the fixes land.
 *
 * Run: npm test   (or: node scripts/test_api.mjs)
 */
import http from 'node:http';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, basename } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const FIXTURES = join(ROOT, 'test', 'fixtures');
// Live dataset size: the bundled search_index.json grows with every fetch,
// so totals derive from it instead of a hardcoded snapshot count.
const _indexRaw = JSON.parse(readFileSync(join(ROOT, 'docs', 'api', 'search_index.json'), 'utf8'));
const TOTAL_ANIME = Array.isArray(_indexRaw) ? _indexRaw.length : (_indexRaw.rows || _indexRaw.anime || []).length;

/* ------------------------------ tiny harness ------------------------------ */
let passed = 0, failed = 0;
const failures = [];
const eq = (a, b, msg = '') => {
  if (JSON.stringify(a) !== JSON.stringify(b)) throw new Error(`${msg} expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}`);
};
const ok = (cond, msg = 'assertion failed') => { if (!cond) throw new Error(msg); };
async function test(name, fn) {
  try { await fn(); passed++; console.log(`  \u2713 ${name}`); }
  catch (e) { failed++; failures.push({ name, err: e }); console.log(`  \u2717 ${name}\n      ${e.message}`); }
}

/* --------------------------- fixture file server -------------------------- */
const hits = [];
const fixtureServer = http.createServer((req, res) => {
  hits.push(req.url);
  const url = req.url.split('?')[0];
  let file = null;
  if (url === '/metadata.json') file = join(FIXTURES, 'metadata.json');
  else if (url === '/search_index.json') file = join(ROOT, 'docs', 'api', 'search_index.json');
  else if (url === '/schedule_index.json') file = join(FIXTURES, 'schedule_index.json');
  else if (url.startsWith('/shards/')) {
    const candidate = join(FIXTURES, basename(url));
    if (existsSync(candidate)) file = candidate;
  }
  if (!file || !existsSync(file)) { res.writeHead(404); return res.end('not found'); }
  const body = readFileSync(file);
  res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': body.length });
  res.end(body);
});
await new Promise((r) => fixtureServer.listen(0, '127.0.0.1', r));
const FIXTURE_BASE = `http://127.0.0.1:${fixtureServer.address().port}`;

/* --------------------------- handler test client -------------------------- */
let caseCounter = 0;
async function startApi(env = {}) {
  const saved = {};
  for (const k of Object.keys(env)) { saved[k] = process.env[k]; process.env[k] = env[k]; }
  caseCounter += 1;
  const mod = await import(`../api/index.js?case=${caseCounter}`);
  for (const k of Object.keys(env)) {
    if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
  }
  const srv = http.createServer((req, res) =>
    Promise.resolve(mod.default(req, res)).catch((e) => {
      res.writeHead(e.status || 500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ errors: [{ message: e.message }] }));
    }));
  await new Promise((r) => srv.listen(0, '127.0.0.1', r));
  const port = srv.address().port;
  const post = async (body, path = '/graphql') => {
    const r = await fetch(`http://127.0.0.1:${port}${path}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body), signal: AbortSignal.timeout(30000),
    });
    return { status: r.status, headers: r.headers, json: await r.json().catch(() => null), ms: 0 };
  };
  const get = async (u, headers = {}) => {
    const t = Date.now();
    const r = await fetch(`http://127.0.0.1:${port}${u}`, { headers, signal: AbortSignal.timeout(30000) });
    const text = await r.text();
    let json = null; try { json = JSON.parse(text); } catch { /* non-json */ }
    return { status: r.status, headers: r.headers, json, text, ms: Date.now() - t };
  };
  return { post, get, close: () => new Promise((r) => srv.close(r)), port };
}

const CARD_QUERY = '{ Page(page: 1, perPage: 5) { media(sort: POPULARITY_DESC) { id title { romaji english } averageScore popularity format status seasonYear episodes coverImage { large medium } genres } pageInfo { total perPage lastPage hasNextPage } } }';
const BASE_ENV = { DATA_BASE_URL: FIXTURE_BASE };

/* ================================== tests ================================= */
console.log('\n== routing / vercel.json ==');
await test('vercel.json routes root / to the API (README advertises POST /)', () => {
  const cfg = JSON.parse(readFileSync(join(ROOT, 'vercel.json'), 'utf8'));
  const sources = (cfg.rewrites || []).map((r) => r.source);
  ok(sources.includes('/'), `rewrites must include "/", got ${JSON.stringify(sources)}`);
  ok(sources.includes('/api'), `rewrites must include "/api", got ${JSON.stringify(sources)}`);
  ok(sources.includes('/graphql'), 'rewrites must include "/graphql"');
});
await test('vercel.json sets CORS + caching/security headers globally', () => {
  const cfg = JSON.parse(readFileSync(join(ROOT, 'vercel.json'), 'utf8'));
  const glob = (cfg.headers || []).find((h) => h.source === '/(.*)');
  ok(glob, 'missing catch-all header rule');
  const keys = glob.headers.map((h) => h.key);
  ok(keys.includes('Access-Control-Allow-Origin'), 'missing CORS origin');
  ok(keys.includes('X-Content-Type-Options'), 'missing X-Content-Type-Options');
});

const api = await startApi(BASE_ENV);

console.log('\n== HTTP surface ==');
await test('POST / (root path) answers GraphQL', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:1){ media(sort:POPULARITY_DESC){ id } } }' }, '/');
  eq(r.status, 200, 'status');
  ok(r.json?.data?.Page?.media?.length === 1, 'expected 1 media, got ' + JSON.stringify(r.json).slice(0, 200));
});
await test('OPTIONS preflight returns 204 with CORS', async () => {
  const raw = await new Promise((resolve) => {
    const req = http.request({ host: '127.0.0.1', port: api.port, path: '/graphql', method: 'OPTIONS',
      headers: { Origin: 'https://example.com', 'Access-Control-Request-Method': 'POST' } },
      (res) => { res.resume(); resolve(res); });
    req.end();
  });
  eq(raw.statusCode, 204, 'status');
  eq(raw.headers['access-control-allow-origin'], '*', 'ACAO');
  ok(String(raw.headers['access-control-allow-methods'] || '').includes('POST'), 'ACAM');
});
await test('GET / no query returns INFO json', async () => {
  const r = await api.get('/');
  eq(r.status, 200, 'status');
  ok(r.json?.message, 'INFO message');
});
await test('GET /?health=1 returns health json', async () => {
  const r = await api.get('/?health=1');
  eq(r.status, 200, 'status');
  ok('ok' in r.json, 'has ok field');
});
await test('invalid JSON body -> 400', async () => {
  const raw = await new Promise((resolve) => {
    const req = http.request({ host: '127.0.0.1', port: api.port, path: '/graphql', method: 'POST',
      headers: { 'Content-Type': 'application/json' } }, (res) => { res.resume(); resolve(res); });
    req.end('{not json');
  });
  eq(raw.statusCode, 400, 'status');
});
await test('oversized body -> 413 (guard, currently unbounded)', async () => {
  const raw = await new Promise((resolve) => {
    const req = http.request({ host: '127.0.0.1', port: api.port, path: '/graphql', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': String(10 * 1024 * 1024) } },
      (res) => { res.resume(); resolve(res); });
    req.write('{"query":"'); req.write('x'.repeat(10 * 1024 * 1024)); req.end('"}');
  });
  ok([413, 400].includes(raw.statusCode), `expected 413/400, got ${raw.statusCode}`);
});

console.log('\n== index-only fast path (zero network for card/search/browse queries) ==');
await test('card query served without any shard fetch', async () => {
  hits.length = 0;
  const r = await api.post({ query: CARD_QUERY });
  eq(r.status, 200, 'status');
  const media = r.json?.data?.Page?.media;
  eq(media?.length, 5, 'media count');
  const shardHits = hits.filter((u) => u.startsWith('/shards/'));
  eq(shardHits.length, 0, `shard fetches should be 0, got ${shardHits.length}`);
});
await test('warm card query is fast (< 1200ms end-to-end)', async () => {
  const t = Date.now();
  const r = await api.post({ query: CARD_QUERY });
  const ms = Date.now() - t;
  eq(r.status, 200, 'status');
  ok(ms < 1200, `took ${ms}ms`);
});
await test('POPULARITY_DESC ordering + pageInfo totals come from the index', async () => {
  const r = await api.post({ query: CARD_QUERY });
  const { media, pageInfo } = r.json.data.Page;
  eq(pageInfo.total, TOTAL_ANIME, 'total anime');
  const pops = media.map((m) => m.popularity);
  for (let i = 1; i < pops.length; i++) ok(pops[i - 1] >= pops[i], 'descending popularity');
  ok(media.every((m) => m.coverImage?.large || m.coverImage?.medium), 'covers present');
  ok(media.every((m) => m.title?.romaji), 'titles present');
});
await test('search "naruto" returns full page, all matching, no hydration gaps', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:5) { media(search: "naruto") { id title { romaji } } } }' });
  const media = r.json?.data?.Page?.media;
  eq(media?.length, 5, 'media count');
  ok(media.every((m) => /naruto/i.test(m.title.romaji)), 'every title contains Naruto: ' + media.map((m) => m.title.romaji).join(', '));
});
await test('genre filter + SCORE_DESC sort', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:10) { media(genre: "Romance", sort: SCORE_DESC) { id averageScore genres } } }' });
  const media = r.json?.data?.Page?.media;
  eq(media?.length, 10, 'count');
  ok(media.every((m) => m.genres.includes('Romance')), 'all Romance');
  const scores = media.map((m) => m.averageScore);
  for (let i = 1; i < scores.length; i++) ok(scores[i - 1] >= scores[i], 'descending score');
});
await test('tag_not_in filter excludes the tag (index path)', async () => {
  // "Action" is a genre, not a tag — use a real tag (Isekai) and compare two
  // queries: the excluded set must be exactly total - tagged rows.
  const q1 = await api.post({ query: '{ Page(page:1, perPage:50) { media(tag: "Isekai") { id } pageInfo { total } } }' });
  const q2 = await api.post({ query: '{ Page(page:1, perPage:50) { media(tag_not_in: ["Isekai"]) { id } pageInfo { total } } }' });
  const m1 = q1.json?.data?.Page?.media || [];
  const m2 = q2.json?.data?.Page?.media || [];
  const t1 = q1.json?.data?.Page?.pageInfo?.total;
  const t2 = q2.json?.data?.Page?.pageInfo?.total;
  ok(m1.length > 0, `tag: Isekai returned rows (got ${m1.length})`);
  ok(m2.length > 0, 'tag_not_in returned rows');
  ok(t1 > 0 && t1 < TOTAL_ANIME, `Isekai tag count sane (got ${t1})`);
  eq(t2, TOTAL_ANIME - t1, 'tag_not_in total excludes exactly the tagged rows');
  const ids2 = new Set(m2.map((m) => m.id));
  ok(m1.every((m) => !ids2.has(m.id)), 'no tagged id appears in the excluded set');
});
await test('genre_not_in filter excludes the genre (index path)', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:20) { media(genre_not_in: ["Action"]) { id genres } } }' });
  const media = r.json?.data?.Page?.media;
  ok(media?.length > 0, 'results');
  ok(media.every((m) => !(m.genres || []).includes('Action')), 'no Action genre in results');
});
await test('pagination pageInfo (page 2, perPage 50)', async () => {
  const r = await api.post({ query: '{ Page(page: 2, perPage: 50) { media(sort: ID) { id } pageInfo { total perPage currentPage lastPage hasNextPage hasPreviousPage } } }' });
  const pi = r.json?.data?.Page?.pageInfo;
  eq(pi.currentPage, 2, 'currentPage');
  eq(pi.perPage, 50, 'perPage');
  eq(pi.total, TOTAL_ANIME, 'total');
  eq(pi.lastPage, Math.ceil(TOTAL_ANIME / 50), 'lastPage');
  eq(pi.hasNextPage, true, 'hasNextPage');
  eq(pi.hasPreviousPage, true, 'hasPreviousPage');
});
await test('variables + aliases + fragment + operationName', async () => {
  const q = `query Top($n: Int) {
    first: Page(page: 1, perPage: $n) { media(sort: SCORE_DESC) { ...CardF } }
  }
  fragment CardF on Media { id title { romaji } }`;
  const r = await api.post({ query: q, variables: { n: 3 }, operationName: 'Top' });
  eq(r.status, 200, 'status');
  ok(!r.json.errors, 'no errors: ' + JSON.stringify(r.json.errors));
  eq(r.json.data.first.media.length, 3, 'aliased media count');
  ok(r.json.data.first.media[0].title.romaji, 'fragment field present');
});

console.log('\n== GraphQL compatibility (introspection) ==');
await test('__schema introspection answers', async () => {
  const r = await api.post({ query: '{ __schema { queryType { name } mutationType { name } types { name kind } } }' });
  eq(r.status, 200, 'status');
  const s = r.json?.data?.__schema;
  ok(s, 'data.__schema missing: ' + JSON.stringify(r.json).slice(0, 200));
  eq(s.queryType.name, 'Query', 'queryType name');
  ok(s.types.length > 10, 'types listed');
});
await test('__type(name: "Media") answers with fields', async () => {
  const r = await api.post({ query: '{ __type(name: "Media") { name kind fields { name type { name kind } } } }' });
  const t = r.json?.data?.__type;
  ok(t?.name === 'Media', 'type missing: ' + JSON.stringify(r.json).slice(0, 200));
  ok(t.fields.some((f) => f.name === 'id') && t.fields.some((f) => f.name === 'title'), 'has id/title fields');
});
await test('standard client handshake query (aliases + __typename) works', async () => {
  const r = await api.post({ query: '{ __typename: __schema { queryType { name } } }' });
  eq(r.status, 200, 'status');
});

console.log('\n== detail / shard path ==');
await test('detail resolves quickly against a fast local shard base (< 3000ms)', async () => {
  // NOTE: must run FIRST while the shard cache is cold — a warm cache would
  // hide the release-asset-first URL ordering that adds ~10s per fetch.
  const t = Date.now();
  const r = await api.post({ query: '{ Media(id: 5114) { id title { romaji } description } }' });
  const ms = Date.now() - t;
  eq(r.status, 200, 'status');
  ok(r.json?.data?.Media?.id === 5114, 'resolved: ' + JSON.stringify(r.json).slice(0, 200));
  ok(ms < 3000, `took ${ms}ms (release-asset-first fallback adds ~10s even with a custom base)`);
});
await test('Media(id) full detail resolves from shards (title/desc/characters/studios/relations)', async () => {
  const r = await api.post({ query: '{ Media(id: 16498) { id title { romaji english } description genres studios { edges { node { name } } } characters { edges { role node { name { full } } } } relations { edges { relationType node { id title { romaji } } } } recommendations { edges { node { mediaRecommendation { id title { romaji } } } } } } }' });
  eq(r.status, 200, 'status');
  const m = r.json?.data?.Media;
  ok(m, 'media missing: ' + JSON.stringify(r.json).slice(0, 300));
  eq(m.id, 16498, 'id');
  ok(m.title.romaji === 'Shingeki no Kyojin', 'title');
  ok(m.description && m.description.length > 50, 'description');
  ok(m.genres.includes('Action'), 'genres');
  ok(m.studios.edges.length > 0, 'studios');
  ok(m.characters.edges.length > 0, 'characters');
  ok(m.relations.edges.length > 0, 'relations');
  ok(m.recommendations.edges.length > 0, 'recommendations');
});
await test('Media(idMal) resolves', async () => {
  const r = await api.post({ query: '{ Media(idMal: 16498) { id title { romaji } } }' });
  eq(r.json?.data?.Media?.id, 16498, 'id');
});
await test('Media(id) 404 shape for unknown id', async () => {
  const r = await api.post({ query: '{ Media(id: 999999999) { id } }' });
  ok(r.json?.errors?.length >= 1, 'has errors');
  ok(r.json.errors[0].message.includes('not found'), 'not-found message: ' + r.json.errors[0].message);
});
await test('id_in list query hydrates every row from shards', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:8) { media(id_in: [1, 20, 1535, 5114, 9253, 16498, 101922, 113415], sort: ID) { id title { romaji } description } pageInfo { total } } }' });
  const media = r.json?.data?.Page?.media;
  eq(media?.length, 8, 'all 8 fixture ids resolved');
  ok(media.every((m) => m.description), 'descriptions hydrated');
  eq(r.json.data.Page.pageInfo.total, 8, 'total');
});

console.log('\n== collection roots ==');
await test('GenreCollection returns genres', async () => {
  const r = await api.post({ query: '{ GenreCollection }' });
  ok(Array.isArray(r.json?.data?.GenreCollection) && r.json.data.GenreCollection.length > 3, 'genres: ' + JSON.stringify(r.json).slice(0, 200));
});
await test('MediaTagCollection returns tags', async () => {
  const r = await api.post({ query: '{ MediaTagCollection { name rank } }' });
  const tags = r.json?.data?.MediaTagCollection;
  ok(Array.isArray(tags) && tags.length > 3, 'tags: ' + JSON.stringify(r.json).slice(0, 200));
});

console.log('\n== HTTP caching (edge-first GETs) ==');
await test('GET ?query= serves with s-maxage Cache-Control', async () => {
  const r = await api.get('/?query=' + encodeURIComponent('{ Page(page:1, perPage:2){ media(sort:SCORE_DESC){ id } } }'));
  eq(r.status, 200, 'status');
  ok(r.json?.data?.Page?.media?.length === 2, 'data');
  const cc = r.headers.get('cache-control') || '';
  ok(/s-maxage=\d+/.test(cc), `Cache-Control should carry s-maxage, got "${cc}"`);
});
await test('GET supports ETag / 304 revalidation', async () => {
  const url = '/?query=' + encodeURIComponent('{ Page(page:1, perPage:2){ media(sort:TRENDING_DESC){ id } } }');
  const r1 = await api.get(url);
  const etag = r1.headers.get('etag');
  ok(etag, 'first GET must return an ETag, got none');
  const r2 = await api.get(url, { 'If-None-Match': etag });
  eq(r2.status, 304, 'revalidation status');
});
await test('identical POST is answered from the response cache (fast)', async () => {
  const q = '{ Page(page: 3, perPage: 10) { media(sort: UPDATED_AT_DESC) { id updatedAt } } }';
  await api.post({ query: q }); // warm
  const t = Date.now();
  const r = await api.post({ query: q });
  const ms = Date.now() - t;
  eq(r.status, 200, 'status');
  ok(ms < 400, `cached repeat took ${ms}ms`);
});

console.log('\n== resilience (no Turso, dead data source) ==');
await test('cards still served when DATA_BASE_URL is dead (bundled snapshot fallback)', async () => {
  const dead = await startApi({ DATA_BASE_URL: 'http://127.0.0.1:9' });
  try {
    const r = await dead.post({ query: CARD_QUERY });
    eq(r.status, 200, 'status');
    const media = r.json?.data?.Page?.media;
    ok(media?.length === 5, 'media served from bundled snapshot: ' + JSON.stringify(r.json).slice(0, 250));
    ok(!r.json.errors, 'no errors');
  } finally { await dead.close(); }
});
await test('tursoWhere(tag_not_in) builds a valid, count-matched statement', async () => {
  const mod = await import('../api/index.js?where=1');
  ok(typeof mod.__test?.tursoWhere === 'function', 'handler must export __test.tursoWhere for testing');
  const { where, args } = mod.__test.tursoWhere({ tag_not_in: ['Action', 'Hentai'] });
  const clause = where.join(' ');
  const placeholders = (clause.match(/\?/g) || []).length;
  eq(placeholders, args.length, 'placeholder/arg count must match');
  ok(clause.includes('NOT EXISTS'), 'uses NOT EXISTS');
  eq(args.length, 2, 'two args');
});

console.log('\n== media connection normalization (nodes/pagination/versions) ==');
await test('relations derive nodes + null pageInfo + isMainStudio false', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:1) { media(id: 101922) { relations { edges { id relationType isMainStudio node { id } } nodes { id } pageInfo { total perPage currentPage lastPage hasNextPage } } } } }' });
  const rel = r.json?.data?.Page?.media?.[0]?.relations;
  eq(rel?.edges?.length, 6, 'edges');
  eq(rel?.nodes?.length, 6, 'nodes derived');
  eq(rel.nodes.map((n) => n.id), rel.edges.map((e) => e.node.id), 'nodes match edges');
  ok(rel.edges.every((e) => e.isMainStudio === false), 'isMainStudio false');
  eq(rel.pageInfo, { total: null, perPage: null, currentPage: null, lastPage: null, hasNextPage: false }, 'null pageInfo');
});
await test('relations nodes work without node in edges selection', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:1) { media(id: 101922) { relations { edges { relationType } nodes { id } } } } }' });
  const rel = r.json?.data?.Page?.media?.[0]?.relations;
  eq(rel?.edges?.length, 6, 'edges');
  eq(rel?.nodes?.length, 6, 'nodes');
  ok(rel.nodes.every((n) => n && n.id > 0), 'no null nodes: ' + JSON.stringify(rel.nodes));
});
await test('relationType versions fall back safely without V2/V3 columns', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:1) { media(id: 101922) { relations { edges { relationType relationTypeV2: relationType(version: 2) relationTypeV3: relationType(version: 3) } } } } }' });
  const edges = r.json?.data?.Page?.media?.[0]?.relations?.edges || [];
  eq(edges.length, 6, 'edges');
  // First fixture edge carries stored V2/V3 (mirrors production shards).
  eq(edges[0].relationType, 'ADAPTATION', 'v1 base');
  eq(edges[0].relationTypeV2, 'SOURCE', 'aliased version:2 maps stored V2');
  eq(edges[0].relationTypeV3, 'SOURCE', 'aliased version:3 maps stored V3');
  // Edges without stored versions fall back to the base value.
  ok(edges.slice(1).every((e) => e.relationTypeV2 === e.relationType && e.relationTypeV3 === e.relationType), 'fallback to base');
});
await test('recommendations honor perPage/sort and derive nodes', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:1) { media(id: 101922) { recommendations(page: 1, perPage: 2) { edges { node { rating mediaRecommendation { id } } } nodes { rating } pageInfo { total perPage currentPage hasNextPage } } } } }' });
  const rec = r.json?.data?.Page?.media?.[0]?.recommendations;
  eq(rec?.edges?.length, 2, 'perPage slice');
  eq(rec?.nodes?.length, 2, 'nodes derived');
  eq(rec.pageInfo.perPage, 2, 'pageInfo perPage');
  const ratings = rec.edges.map((e) => e.node.rating);
  ok(ratings[0] >= ratings[1] && ratings[0] === 3346, `RATING_DESC order kept (${ratings})`);
});
await test('characters honor perPage and derive nodes', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:1) { media(id: 101922) { characters(page: 1, perPage: 2) { edges { role node { id } } nodes { id } pageInfo { perPage } } } } }' });
  const ch = r.json?.data?.Page?.media?.[0]?.characters;
  eq(ch?.edges?.length, 2, 'perPage slice');
  eq(ch?.nodes?.length, 2, 'nodes derived');
  eq(ch.pageInfo.perPage, 2, 'pageInfo perPage');
});
await test('characters accept array sort (client shape: [ROLE, RELEVANCE, ID])', async () => {  const r = await api.post({ query: '{ Page(page:1, perPage:1) { media(id: 101922) { characters(perPage: 50, sort: [ROLE, RELEVANCE, ID]) { edges { role node { id } } } } } }' });
  ok(!r.json?.errors, 'no errors, got: ' + JSON.stringify(r.json?.errors));
  const edges = r.json?.data?.Page?.media?.[0]?.characters?.edges || [];
  eq(edges.length, 4, 'all stored rows served');
  ok(edges.every((e) => e.node && e.node.id > 0), 'nodes intact');
});
await test('edge voiceActors project sub-selections (no key leak) + languageV2 mapping', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:1) { media(id: 101922) { characters(perPage: 1) { edges { role voiceActors { id languageV2 name { full } image { large } } } } } } }' });
  ok(!r.json?.errors, 'no errors, got: ' + JSON.stringify(r.json?.errors));
  const vas = r.json?.data?.Page?.media?.[0]?.characters?.edges?.[0]?.voiceActors || [];
  eq(vas.length, 1, 'one VA');
  eq(vas[0], { id: 111635, languageV2: 'Chinese', name: { full: 'Natsuki Hanae' }, image: { large: 'L' } }, 'projected + mapped: ' + JSON.stringify(vas[0]));
});
await test('studios derive nodes and honor isMain filter', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:1) { media(id: 101922) { studios { edges { isMain node { id } } nodes { id } } main: studios(isMain: true) { edges { isMain } } } } }' });
  const st = r.json?.data?.Page?.media?.[0]?.studios;
  eq(st?.nodes?.length, st?.edges?.length, 'nodes derived');
  const main = r.json?.data?.Page?.media?.[0]?.main;
  ok(main?.edges?.length >= 1 && main.edges.every((e) => e.isMain === true), 'isMain filter');
});
await test('empty airingSchedule projects cleanly (no crash)', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:1) { media(id: 101922) { airingSchedule { edges { node { id } } nodes { id } pageInfo { total perPage } } nextAiringEpisode { id } } } }' });
  const m = r.json?.data?.Page?.media?.[0];
  eq(m?.airingSchedule?.edges?.length, 4, 'stored rows served');
  eq(m?.airingSchedule?.nodes?.length, 4, 'nodes derived');
  eq(m?.airingSchedule?.pageInfo?.total, 4, 'total falls back to stored length');
  eq(m?.nextAiringEpisode, null, 'null next episode stays null');
});
await test('Page.airingSchedules resolves to a list (no crash)', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:5) { airingSchedules { id episode airingAt mediaId } pageInfo { total } } }' });
  const rows = r.json?.data?.Page?.airingSchedules;
  ok(Array.isArray(rows), 'list, got: ' + JSON.stringify(r.json).slice(0, 200));
});
await test('pastAiring middle rows merge into airingSchedule timeline', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:1) { media(id: 1) { airingSchedule { edges { node { id episode } } nodes { id episode } pageInfo { total } } } } }' });
  const sch = r.json?.data?.Page?.media?.[0]?.airingSchedule;
  eq(sch?.edges?.length, 1, 'middle row served');
  eq(sch.edges[0].node, { id: 991, episode: 26 }, 'row content');
  eq(sch?.nodes?.length, 1, 'nodes derived');
});
await test('Page.airingSchedules serves window from schedule_index (no shard needed)', async () => {
  const r = await api.post({ query: '{ Page(page:1, perPage:50) { airingSchedules(airingAt_greater: 1499999999, airingAt_lesser: 1500000001, sort: [TIME]) { id episode airingAt mediaId } } }' });
  const rows = r.json?.data?.Page?.airingSchedules || [];
  const ids = rows.map((x) => x.id).sort((a, b) => a - b);
  eq(ids, [991, 555001], 'index rows served, incl. media absent from shards: ' + JSON.stringify(ids));
});
await test('Page.pageInfo follows airingSchedules when media is absent', async () => {  const r = await api.post({ query: '{ Page(page:1, perPage:5) { airingSchedules { id } pageInfo { total perPage currentPage lastPage hasNextPage } } }' });
  const pi = r.json?.data?.Page?.pageInfo;
  eq(pi.total, 5000, 'estimate total');
  eq(pi.perPage, 5, 'perPage');
  eq(pi.lastPage, 1000, 'lastPage');
  eq(pi.hasNextPage, true, 'hasNextPage');
});

/* ================================ summary ================================= */
console.log(`\n${passed} passed, ${failed} failed\n`);
if (failed) {
  for (const f of failures) console.log(`FAIL: ${f.name}\n     ${f.err.message}\n`);
}
await new Promise((r) => fixtureServer.close(r));
process.exit(failed ? 1 : 0);
