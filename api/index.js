import { parse, Kind } from 'graphql';
import { gunzipSync } from 'node:zlib';
import { createClient } from '@libsql/client';
// Deploy-time snapshot of the small files (search_index ~4MB, metadata tiny).
// Bundled into the function -> hot path needs ZERO network for index/metadata,
// and can never serve a stale CDN copy. Refreshed on every data deploy.
// If the bundler ever drops them, the fetch fallback below takes over.
import bundledIndex from '../docs/api/search_index.json' with { type: 'json' };
import bundledMeta from '../docs/api/metadata.json' with { type: 'json' };

/* AniList Offline GraphQL API — EXACT anime-only mirror.
 * Shards already contain AniList-exact Media objects (camelCase, FuzzyDate).
 * This layer only filters/sorts/paginates + field-selects. No snake_case leaks.
 * Data: GitHub Pages static JSON (zero rate limit, downloadable).
 * Better hosting (recommended): Cloudflare R2 + Workers (see README Hosting section).
 * Env override: DATA_BASE_URL
 */
const DATA_BASE = (typeof process !== 'undefined' && process.env?.DATA_BASE_URL)
  || 'https://cdn.jsdelivr.net/gh/Shoislam0311/anilist-offline-db@main/docs/api';
// Credential-free fallback chain (tried in order by the client; Vercel uses DATA_BASE first):
// 1. jsDelivr  2. Statically  3. raw.githack  4. GitHub Pages origin
// Statically: https://cdn.statically.io/gh/Shoislam0311/anilist-offline-db/main/docs/api
// githack:    https://raw.githack.com/Shoislam0311/anilist-offline-db/main/docs/api
// Pages:      https://shoislam0311.github.io/anilist-offline-db/api
const CACHE_TTL_MS = 10 * 60 * 1000;
const MAX_SHARD_CACHE = 12; // decompressed shards are ~15MB each; cap RAM (~180MB)

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Max-Age': '86400',
};

const INFO = {
  message: 'AniList Offline GraphQL API (anime-only, exact mirror)',
  documentation: 'https://docs.anilist.co/',
  usage: 'POST / with { "query": "...", "variables": {...} } — same as graphql.anilist.co',
  data: 'https://shoislam0311.github.io/anilist-offline-db/api/',
  type: 'ANIME',
  rateLimit: 'None — zero rate limits!',
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS },
  });
}

/* ------------------------------- data layer ------------------------------ */
// Shards are exact Media objects — no transform needed except safety defaults.
function asExactMedia(a) {
  if (!a || typeof a !== 'object') return a;
  if (a.__typename) return a;
  return {
    __typename: 'Media',
    type: 'ANIME',
    isFavourite: false,
    ...a,
    __typename: 'Media',
    type: a.type || 'ANIME',
    siteUrl: a.siteUrl || `https://anilist.co/anime/${a.id}`,
  };
}

let metaEntry = null;
let indexEntry = null;
const shardCache = new Map();
let builtShardStartIds = null;

async function fetchJSON(url) {
  // Credential-free multi-CDN: if the primary base fails, retry same path on mirrors.
  const mirrors = [];
  const m = url.match(/^(https:\/\/[^/]+)(\/.*)$/);
  if (m) {
    const path = m[2]; // e.g. /.../docs/api/shards/shard_0000.json or /api/...
    const file = path.slice(path.lastIndexOf('/api/') + 4); // /metadata.json | /shards/...
    mirrors.push(
      `https://cdn.jsdelivr.net/gh/Shoislam0311/anilist-offline-db@main/docs/api${file}`,
      `https://cdn.statically.io/gh/Shoislam0311/anilist-offline-db/main/docs/api${file}`,
      `https://raw.githack.com/Shoislam0311/anilist-offline-db/main/docs/api${file}`,
      `https://shoislam0311.github.io/anilist-offline-db/api${file}`,
    );
  }
  const tried = new Set();
  for (const u of [url, ...mirrors]) {
    if (tried.has(u)) continue;
    tried.add(u);
    try {
      const r = await fetch(u);
      if (r.ok) return r.json();
    } catch { /* try next mirror */ }
  }
  throw new Error(`all mirrors failed for ${url}`);
}
function fresh(entry) { return entry && Date.now() - entry.time < CACHE_TTL_MS; }

function bundledFirst(kind) {
  // Bundled snapshot wins when it looks complete; otherwise fall back to fetch.
  try {
    if (kind === 'meta' && bundledMeta?.totalAnime > 1000) return bundledMeta;
    if (kind === 'index' && Array.isArray(bundledIndex) && bundledIndex.length > 1000) return bundledIndex;
  } catch { /* bundler dropped the files; fetch instead */ }
  return null;
}

async function getMetadata() {
  if (!fresh(metaEntry)) {
    const local = bundledFirst('meta');
    metaEntry = { data: local || await fetchJSON(`${DATA_BASE}/metadata.json`), time: Date.now() };
  }
  return metaEntry.data;
}
async function getSearchIndex() {
  if (!fresh(indexEntry)) {
    const local = bundledFirst('index');
    indexEntry = { data: local || await fetchJSON(`${DATA_BASE}/search_index.json`), time: Date.now() };
  }
  return indexEntry.data;
}
async function getShardStartIds() {
  const meta = await getMetadata().catch(() => null);
  if (meta?.shardStartIds?.length) return meta.shardStartIds;
  if (builtShardStartIds) return builtShardStartIds;
  builtShardStartIds = [];
  const BATCH = 10;
  const phase1 = await Promise.allSettled(
    Array.from({ length: 16 }, (_, k) => getShard(k).then((s) => ({ k, s })))
  );
  let i = 0;
  for (const r of phase1) {
    if (r.status === 'fulfilled' && r.value.s.length) {
      builtShardStartIds[r.value.k] = r.value.s[0].id;
      i = Math.max(i, r.value.k + 1);
    }
  }
  if (!builtShardStartIds.length) return builtShardStartIds;
  for (let start = i; start < 2000; start += BATCH) {
    const batch = await Promise.allSettled(
      Array.from({ length: BATCH }, (_, k) => {
        const idx = start + k;
        return getShard(idx).then((s) => ({ idx, s }));
      })
    );
    let hitGap = false;
    for (const r of batch) {
      if (r.status === 'fulfilled' && r.value.s.length) {
        builtShardStartIds[r.value.idx] = r.value.s[0].id;
      } else hitGap = true;
    }
    if (hitGap) break;
  }
  return builtShardStartIds;
}
const REPO = 'Shoislam0311/anilist-offline-db';
const releaseShardUrls = (padded, tag) => {
  const urls = [];
  if (tag) urls.push(`https://github.com/${REPO}/releases/download/${tag}/shard_${padded}.json.gz`);
  urls.push(`https://github.com/${REPO}/releases/latest/download/shard_${padded}.json.gz`);
  return urls;
};

async function decodeBody(r, url) {
  const buf = Buffer.from(await r.arrayBuffer());
  if (url.endsWith('.gz')) {
    try {
      return JSON.parse(gunzipSync(buf).toString('utf8'));
    } catch {
      return JSON.parse(buf.toString('utf8')); // transparently decoded upstream
    }
  }
  return JSON.parse(buf.toString('utf8'));
}

async function getShard(idx) {
  let entry = shardCache.get(idx);
  if (!fresh(entry)) {
    const padded = String(idx).padStart(4, '0');
    const meta = await getMetadata().catch(() => null);
    // Release assets first (exact, full dataset); git shards as legacy fallback
    // so the API keeps serving while a full re-scrape is in flight.
    const urls = [
      ...releaseShardUrls(padded, meta?.releaseTag),
      `${DATA_BASE}/shards/shard_${padded}.json`,
    ];
    let data = null, lastErr = null;
    for (const u of urls) {
      try {
        const r = await fetch(u);
        if (!r.ok) continue;
        data = await decodeBody(r, u);
        break;
      } catch (e) { lastErr = e; }
    }
    if (!data) throw lastErr || new Error(`shard ${idx} unavailable on all mirrors`);
    entry = { data, time: Date.now() };
    shardCache.set(idx, entry);
    if (shardCache.size > MAX_SHARD_CACHE) {
      const oldest = shardCache.keys().next().value;
      shardCache.delete(oldest);
    }
  }
  return entry.data;
}
function shardIdxForId(shardStartIds, id) {
  let lo = 0, hi = shardStartIds.length - 1, idx = 0;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (shardStartIds[mid] <= id) { idx = mid; lo = mid + 1; } else hi = mid - 1;
  }
  return Math.min(idx, shardStartIds.length - 1);
}
async function getAnimeById(id) {
  const shardStartIds = await getShardStartIds();
  if (!shardStartIds.length) return null;
  const idx = shardIdxForId(shardStartIds, id);
  for (const tryIdx of [idx, idx - 1, idx + 1]) {
    if (tryIdx < 0 || tryIdx >= shardStartIds.length) continue;
    const shard = await getShard(tryIdx).catch(() => []);
    const found = shard.find((a) => a.id === id);
    if (found) return asExactMedia(found);
  }
  return null;
}
async function getAnimeBatch(ids) {
  const shardStartIds = await getShardStartIds();
  // One shard per id (not idx±1): the old fan-out fetched up to 3x shards per
  // id — hundreds of 2.5MB downloads per Page query -> 300s Vercel timeouts.
  const byShard = new Map();
  for (const id of ids) {
    const idx = shardIdxForId(shardStartIds, id);
    if (!byShard.has(idx)) byShard.set(idx, []);
    if (!byShard.get(idx).includes(id)) byShard.get(idx).push(id);
  }
  const found = new Map();
  // Parallel fetch: one batch, not sequential.
  const shards = await Promise.all(
    [...byShard.keys()].map((shardIdx) =>
      getShard(shardIdx).then((s) => ({ shardIdx, s })).catch(() => ({ shardIdx, s: [] }))));
  const byIdx = new Map(shards.map(({ shardIdx, s }) => [shardIdx, s]));
  const missing = [];
  for (const [shardIdx, shardIds] of byShard) {
    const shard = byIdx.get(shardIdx) || [];
    for (const id of shardIds) {
      if (found.has(id)) continue;
      const item = shard.find((a) => a.id === id);
      if (item) found.set(id, asExactMedia(item));
      else missing.push({ id, shardIdx });
    }
  }
  // Narrow fallback only for ids truly absent from their home shard.
  for (const { id, shardIdx } of missing) {
    for (const tryIdx of [shardIdx - 1, shardIdx + 1]) {
      if (found.has(id) || tryIdx < 0 || tryIdx >= shardStartIds.length) continue;
      const shard = await getShard(tryIdx).catch(() => []);
      const item = shard.find((a) => a.id === id);
      if (item) found.set(id, asExactMedia(item));
    }
  }
  // preserve requested order
  return ids.map((id) => found.get(id)).filter(Boolean);
}

/* ----------------------------- field resolver ----------------------------- */
function collectSelections(node, fragments) {
  if (!node?.selectionSet) return [];
  const out = [];
  for (const sel of node.selectionSet.selections) {
    if (sel.kind === Kind.FIELD) out.push(sel);
    else if (sel.kind === Kind.FRAGMENT_SPREAD) {
      const frag = fragments[sel.name.value];
      if (frag) out.push(...collectSelections(frag, fragments));
    } else if (sel.kind === Kind.INLINE_FRAGMENT) {
      out.push(...collectSelections(sel, fragments));
    }
  }
  return out;
}
function pick(obj, sels, fragments) {
  if (obj == null) return obj;
  if (!sels || !sels.length) return obj;
  const out = {};
  for (const s of sels) {
    const key = s.alias?.value || s.name.value;
    if (s.name.value === '__typename') { out[key] = obj.__typename || 'Media'; continue; }
    const sub = s.selectionSet ? collectSelections(s, fragments) : null;
    if (!sub || !sub.length) {
      out[key] = obj[s.name.value] ?? null;
      continue;
    }
    const val = obj[s.name.value];
    if (Array.isArray(val)) {
      out[key] = val.map((item) => (item && typeof item === 'object' ? pick(item, sub, fragments) : item));
    } else if (val && typeof val === 'object') {
      // connection { edges { node } } stays generic — no special-casing needed (exact shape)
      out[key] = pick(val, sub, fragments);
    } else {
      out[key] = val ?? null;
    }
  }
  return out;
}

/* ------------------------- AniList filter + sort -------------------------- */
function str(v) { return typeof v === 'string' ? v.toLowerCase() : v; }

function normStr(s) { return String(s || '').toLowerCase().trim(); }
// Tokenize latin queries into words; CJK strings stay whole (no spaces).
function searchTokens(q) {
  return normStr(q).split(/[\s_.,;:!?()[\]{}'"\/\\|-]+/).map((t) => t.trim()).filter((t) => t.length > 0);
}
// Relevance score for an index row vs query. -1 = no match (AniList-like:
// every word must hit somewhere across romaji/english/native/synonyms).
function searchScore(e, q) {
  const query = normStr(q);
  if (!query) return -1;
  const variants = [e.romaji, e.english, e.native, ...(e.synonyms || [])].filter(Boolean).map(normStr);
  if (!variants.length) return -1;
  if (variants.some((v) => v === query)) return 100;
  const tokens = searchTokens(query).filter((t) => t.length > 1);
  const hay = variants.join('\n');
  if (!tokens.length) return hay.includes(query) ? 20 : -1;
  if (!tokens.every((t) => hay.includes(t))) return -1;
  let score = 10;
  const wordHit = variants.some((v) => v.split(/[^a-z0-9\u00c0-\u024f\u1e00-\u1eff\u3040-\u30ff\u4e00-\u9fff]+/u)
    .some((w) => tokens.some((t) => w.startsWith(t))));
  if (wordHit) score += 30;
  if (variants.some((v) => v.startsWith(query))) score += 20;
  if (variants.some((v) => v.includes(query))) score += 10;
  return score;
}

function matchMedia(e, full, a) {
  // e = search-index row, full = exact Media (may be null when index-only filtering)
  const g = (k) => (full ? full[k] : undefined);
  if (a.type && a.type !== 'ANIME') return false;
  if (a.id !== undefined && e.id !== a.id) return false;
  if (a.id_in && !a.id_in.includes(e.id)) return false;
  if (a.id_not !== undefined && e.id === a.id_not) return false;
  if (a.id_not_in && a.id_not_in.includes(e.id)) return false;
  if (a.idMal !== undefined && e.idMal !== a.idMal) return false;
  if (a.idMal_in && !a.idMal_in.includes(e.idMal)) return false;
  if (a.search && searchScore(e, a.search) < 0) return false;
  if (a.genre && !(e.genres || []).includes(a.genre)) return false;
  if (a.genre_in && !a.genre_in.some((x) => (e.genres || []).includes(x))) return false;
  if (a.genre_not_in && a.genre_not_in.some((x) => (e.genres || []).includes(x))) return false;
  if (a.tag && !(e.tags || []).includes(a.tag)) return false;
  if (a.tag_in && !a.tag_in.some((x) => (e.tags || []).includes(x))) return false;
  if (a.tag_not_in && a.tag_not_in.some((x) => (e.tags || []).includes(x))) return false;
  if (a.format && e.format !== a.format) return false;
  if (a.format_in && !a.format_in.includes(e.format)) return false;
  if (a.format_not && e.format === a.format_not) return false;
  if (a.format_not_in && a.format_not_in.includes(e.format)) return false;
  if (a.status && e.status !== a.status) return false;
  if (a.status_in && !a.status_in.includes(e.status)) return false;
  if (a.status_not && e.status === a.status_not) return false;
  if (a.status_not_in && a.status_not_in.includes(e.status)) return false;
  if (a.season && e.season !== a.season) return false;
  if (a.seasonYear !== undefined && e.year !== a.seasonYear) return false;
  if (a.source_in && !a.source_in.includes(e.source)) return false;
  if (a.countryOfOrigin && e.country !== a.countryOfOrigin) return false;
  if (a.isAdult !== undefined && e.adult !== a.isAdult) return false;
  if (a.episodes_greater !== undefined && !((e.episodes ?? -1) > a.episodes_greater)) return false;
  if (a.episodes_lesser !== undefined && !((e.episodes ?? 1e9) < a.episodes_lesser)) return false;
  if (a.duration_greater !== undefined && !((e.duration ?? -1) > a.duration_greater)) return false;
  if (a.duration_lesser !== undefined && !((e.duration ?? 1e9) < a.duration_lesser)) return false;
  if (a.chapters_greater !== undefined && !((e.chapters ?? -1) > a.chapters_greater)) return false;
  if (a.chapters_lesser !== undefined && !((e.chapters ?? 1e9) < a.chapters_lesser)) return false;
  if (a.volumes_greater !== undefined && !((e.volumes ?? -1) > a.volumes_greater)) return false;
  if (a.volumes_lesser !== undefined && !((e.volumes ?? 1e9) < a.volumes_lesser)) return false;
  if (a.averageScore !== undefined && e.score !== a.averageScore) return false;
  if (a.averageScore_not !== undefined && e.score === a.averageScore_not) return false;
  if (a.averageScore_greater !== undefined && !((e.score ?? -1) > a.averageScore_greater)) return false;
  if (a.averageScore_lesser !== undefined && !((e.score ?? 1e9) < a.averageScore_lesser)) return false;
  if (a.popularity !== undefined && e.popularity !== a.popularity) return false;
  if (a.popularity_not !== undefined && e.popularity === a.popularity_not) return false;
  if (a.popularity_greater !== undefined && !((e.popularity ?? -1) > a.popularity_greater)) return false;
  if (a.popularity_lesser !== undefined && !((e.popularity ?? 1e9) < a.popularity_lesser)) return false;
  if (a.startDate_greater !== undefined && !((e.startDate ?? -1) > a.startDate_greater)) return false;
  if (a.startDate_lesser !== undefined && !((e.startDate ?? 1e9) < a.startDate_lesser)) return false;
  if (a.startDate_like !== undefined && String(e.startDate ?? '') !== String(a.startDate_like)) return false;
  if (a.endDate_greater !== undefined && !((e.endDate ?? -1) > a.endDate_greater)) return false;
  if (a.endDate_lesser !== undefined && !((e.endDate ?? 1e9) < a.endDate_lesser)) return false;
  if (a.endDate_like !== undefined && String(e.endDate ?? '') !== String(a.endDate_like)) return false;
  // tag category / minimumTagRank need full object (post-filter below)
  if (full && (a.tagCategory_in || a.tagCategory_not_in || a.minimumTagRank !== undefined)) {
    const tags = full.tags || [];
    if (a.tagCategory_in && !tags.some((t) => a.tagCategory_in.includes(t.category))) return false;
    if (a.tagCategory_not_in && tags.some((t) => a.tagCategory_not_in.includes(t.category))) return false;
    if (a.minimumTagRank !== undefined && !tags.some((t) => (t.rank ?? 0) >= a.minimumTagRank)) return false;
  }
  void g;
  return true;
}

function mediaSortValue(m, field) {
  switch (field) {
    case 'ID': return m.id ?? 0;
    case 'TITLE_ROMAJI': return (m.title?.romaji || '').toLowerCase();
    case 'TITLE_ENGLISH': return (m.title?.english || m.title?.romaji || '').toLowerCase();
    case 'TITLE_NATIVE': return (m.title?.native || '').toLowerCase();
    case 'TYPE': return m.type || '';
    case 'FORMAT': return m.format || '';
    case 'STATUS': return m.status || '';
    case 'POPULARITY': return m.popularity ?? 0;
    case 'SCORE': return m.averageScore ?? m.meanScore ?? 0;
    case 'TRENDING': return m.trending ?? 0;
    case 'FAVOURITES': return m.favourites ?? 0;
    case 'EPISODES': return m.episodes ?? 0;
    case 'DURATION': return m.duration ?? 0;
    case 'CHAPTERS': return m.chapters ?? 0;
    case 'VOLUMES': return m.volumes ?? 0;
    case 'START_DATE': return (m.startDate?.year || 0) * 10000 + (m.startDate?.month || 0) * 100 + (m.startDate?.day || 0);
    case 'END_DATE': return (m.endDate?.year || 0) * 10000 + (m.endDate?.month || 0) * 100 + (m.endDate?.day || 0);
    case 'UPDATED_AT': return m.updatedAt ?? 0;
    case 'SEARCH_MATCH': return m.popularity ?? 0;
    default: return m.popularity ?? 0;
  }
}
function sortMediaList(list, sort) {
  const sorts = (Array.isArray(sort) ? sort : [sort]).filter(Boolean);
  if (!sorts.length) sorts.push('POPULARITY_DESC');
  // apply last-first for stable multi-sort
  for (let i = sorts.length - 1; i >= 0; i--) {
    const s = sorts[i];
    const desc = s.endsWith('_DESC');
    const field = s.replace(/_DESC$/, '').replace(/_ASC$/, '');
    list.sort((a, b) => {
      const av = mediaSortValue(a, field), bv = mediaSortValue(b, field);
      if (av === bv) return 0;
      const cmp = av > bv ? 1 : -1;
      return desc ? -cmp : cmp;
    });
  }
  return list;
}

function buildPageInfo(total, page, perPage) {
  const lastPage = Math.max(1, Math.ceil(total / perPage));
  return {
    __typename: 'PageInfo',
    total, perPage, currentPage: page, lastPage,
    hasNextPage: page < lastPage, hasPreviousPage: page > 1,
  };
}

/* ----------------------------- query executor ----------------------------- */
function argValue(v, variables) {
  if (!v) return undefined;
  switch (v.kind) {
    case Kind.INT: return parseInt(v.value, 10);
    case Kind.FLOAT: return parseFloat(v.value);
    case Kind.STRING: return v.value;
    case Kind.BOOLEAN: return v.value === true || v.value === 'true';
    case Kind.ENUM: return v.value;
    case Kind.NULL: return null;
    case Kind.LIST: return (v.values || []).map((x) => argValue(x, variables));
    case Kind.OBJECT: {
      const obj = {};
      for (const f of v.fields) obj[f.name.value] = argValue(f.value, variables);
      return obj;
    }
    case Kind.VARIABLE: return variables?.[v.name.value];
    default: return undefined;
  }
}
function collectArgs(fieldNode, variables) {
  const args = {};
  for (const arg of fieldNode.arguments || []) {
    const val = argValue(arg.value, variables);
    if (val !== undefined) args[arg.name.value] = val;
  }
  return args;
}

/* ------------------------- Turso edge-SQL read path -------------------------
 * Indexed SQL (~50-200ms) instead of downloading 2.5MB shards per request.
 * Any failure or missing env falls back to the shard logic below untouched.
 */
let turso;
function tursoClient() {
  if (turso !== undefined) return turso;
  try {
    const url = (typeof process !== 'undefined' && process.env?.TURSO_URL) || '';
    const token = (typeof process !== 'undefined' && process.env?.TURSO_AUTH_TOKEN) || '';
    turso = (url && token) ? createClient({ url, authToken: token }) : null;
  } catch { turso = null; }
  return turso;
}

const HAY = `lower(coalesce(a.title_romaji,'') || ' ' || coalesce(a.title_english,'') || ' ' || coalesce(a.title_native,'') || ' ' || coalesce(a.synonyms,''))`;
function escLike(s) { return String(s).replace(/[\\%_]/g, (c) => '\\' + c).toLowerCase(); }
function searchTokensSql(q) {
  return String(q || '').toLowerCase().trim()
    .split(/[\s_.,;:!?()[\]{}'"\/\\|-]+/).map((t) => t.trim()).filter((t) => t.length > 1);
}
function tursoSearchWhere(search, where, args) {
  const q = String(search || '').toLowerCase().trim();
  if (!q) return;
  const tokens = searchTokensSql(q);
  if (!tokens.length) { where.push(`${HAY} LIKE ? ESCAPE '\\'`); args.push(`%${escLike(q)}%`); return; }
  for (const t of tokens) { where.push(`${HAY} LIKE ? ESCAPE '\\'`); args.push(`%${escLike(t)}%`); }
}
const TURSO_SORTS = {
  ID: ['a.id'], TITLE_ROMAJI: ['a.title_romaji COLLATE NOCASE'],
  TITLE_ENGLISH: ['a.title_english COLLATE NOCASE'], TITLE_NATIVE: ['a.title_native COLLATE NOCASE'],
  TYPE: ['a.type'], FORMAT: ['a.format'], STATUS: ['a.status'],
  POPULARITY: ['a.popularity'], SCORE: ['a.average_score'], TRENDING: ['a.trending'],
  FAVOURITES: ['a.favourites'], EPISODES: ['a.episodes'], DURATION: ['a.duration'],
  CHAPTERS: ['a.chapters'], VOLUMES: ['a.volumes'],
  START_DATE: ['a.start_year', 'a.start_month', 'a.start_day'],
  END_DATE: ['a.end_year', 'a.end_month', 'a.end_day'],
  UPDATED_AT: ['a.updated_at'], SEARCH_MATCH: ['a.popularity'],
};
function tursoOrderClause(sort) {
  const sorts = (Array.isArray(sort) ? sort : [sort]).filter(Boolean);
  if (!sorts.length) sorts.push('POPULARITY_DESC');
  const terms = [];
  for (const s of sorts) {
    const desc = s.endsWith('_DESC');
    const field = s.replace(/_DESC$/, '').replace(/_ASC$/, '');
    const cols = TURSO_SORTS[field] || TURSO_SORTS.POPULARITY;
    for (const c of cols) terms.push(`${c} ${desc ? 'DESC' : 'ASC'} NULLS LAST`);
  }
  return terms.join(', ');
}
function numFilter(col, v, op, where, args) {
  if (v === undefined || v === null) return;
  where.push(`${col} ${op} ?`);
  args.push(v);
}
function tursoWhere(fargs) {
  const where = [`COALESCE(a.type,'ANIME') = 'ANIME'`];
  const args = [];
  const a = fargs;
  if (a.type && a.type !== 'ANIME') return { where: ['1 = 0'], args: [] };
  if (a.id !== undefined) { where.push('a.id = ?'); args.push(a.id); }
  if (a.id_in) { where.push(`a.id IN (${a.id_in.map(() => '?').join(',')})`); args.push(...a.id_in); }
  if (a.id_not !== undefined) { where.push('a.id != ?'); args.push(a.id_not); }
  if (a.id_not_in) { where.push(`a.id NOT IN (${a.id_not_in.map(() => '?').join(',')})`); args.push(...a.id_not_in); }
  if (a.idMal !== undefined) { where.push('a.id_mal = ?'); args.push(a.idMal); }
  if (a.idMal_in) { where.push(`a.id_mal IN (${a.idMal_in.map(() => '?').join(',')})`); args.push(...a.idMal_in); }
  if (a.search) tursoSearchWhere(a.search, where, args);
  if (a.genre) { where.push(`EXISTS (SELECT 1 FROM anime_genres ag JOIN genres g ON g.id = ag.genre_id WHERE ag.anime_id = a.id AND g.name = ?)`); args.push(a.genre); }
  if (a.genre_in) { where.push(`EXISTS (SELECT 1 FROM anime_genres ag JOIN genres g ON g.id = ag.genre_id WHERE ag.anime_id = a.id AND g.name IN (${a.genre_in.map(() => '?').join(',')}))`); args.push(...a.genre_in); }
  if (a.genre_not_in) { where.push(`NOT EXISTS (SELECT 1 FROM anime_genres ag JOIN genres g ON g.id = ag.genre_id WHERE ag.anime_id = a.id AND g.name IN (${a.genre_not_in.map(() => '?').join(',')}))`); args.push(...a.genre_not_in); }
  if (a.tag) { where.push(`EXISTS (SELECT 1 FROM anime_tags at WHERE at.anime_id = a.id AND at.tag_name = ?)`); args.push(a.tag); }
  if (a.tag_in) { where.push(`EXISTS (SELECT 1 FROM anime_tags at WHERE at.anime_id = a.id AND at.tag_name IN (${a.tag_in.map(() => '?').join(',')}))`); args.push(...a.tag_in); }
  if (a.tag_not_in) { where.push(`NOT EXISTS (SELECT 1 FROM anime_tags at WHERE at.anime_id = a.id AND at.tag_name IN (${a.tag_not_in.map(() => '?').join(',')}))`); args.push(...a.tag_not_in); }
  if (a.tagCategory_in) { where.push(`EXISTS (SELECT 1 FROM anime_tags at JOIN tags t ON t.name = at.tag_name WHERE at.anime_id = a.id AND t.category IN (${a.tagCategory_in.map(() => '?').join(',')}))`); args.push(...a.tagCategory_in); }
  if (a.tagCategory_not_in) { where.push(`NOT EXISTS (SELECT 1 FROM anime_tags at JOIN tags t ON t.name = at.tag_name WHERE at.anime_id = a.id AND t.category IN (${a.tagCategory_not_in.map(() => '?').join(',')}))`); args.push(...a.tagCategory_not_in); }
  if (a.minimumTagRank !== undefined) { where.push(`EXISTS (SELECT 1 FROM anime_tags at WHERE at.anime_id = a.id AND at.tag_rank >= ?)`); args.push(a.minimumTagRank); }
  if (a.format) { where.push('a.format = ?'); args.push(a.format); }
  if (a.format_in) { where.push(`a.format IN (${a.format_in.map(() => '?').join(',')})`); args.push(...a.format_in); }
  if (a.format_not) { where.push('a.format != ?'); args.push(a.format_not); }
  if (a.format_not_in) { where.push(`a.format NOT IN (${a.format_not_in.map(() => '?').join(',')})`); args.push(...a.format_not_in); }
  if (a.status) { where.push('a.status = ?'); args.push(a.status); }
  if (a.status_in) { where.push(`a.status IN (${a.status_in.map(() => '?').join(',')})`); args.push(...a.status_in); }
  if (a.status_not) { where.push('a.status != ?'); args.push(a.status_not); }
  if (a.status_not_in) { where.push(`a.status NOT IN (${a.status_not_in.map(() => '?').join(',')})`); args.push(...a.status_not_in); }
  if (a.season) { where.push('a.season = ?'); args.push(a.season); }
  if (a.seasonYear !== undefined) { where.push('a.season_year = ?'); args.push(a.seasonYear); }
  if (a.source_in) { where.push(`a.source IN (${a.source_in.map(() => '?').join(',')})`); args.push(...a.source_in); }
  if (a.countryOfOrigin) { where.push('a.country_of_origin = ?'); args.push(a.countryOfOrigin); }
  if (a.isAdult !== undefined) { where.push('a.is_adult = ?'); args.push(a.isAdult ? 1 : 0); }
  numFilter('a.episodes', a.episodes_greater, '>', where, args);
  numFilter('a.episodes', a.episodes_lesser, '<', where, args);
  numFilter('a.duration', a.duration_greater, '>', where, args);
  numFilter('a.duration', a.duration_lesser, '<', where, args);
  numFilter('a.chapters', a.chapters_greater, '>', where, args);
  numFilter('a.chapters', a.chapters_lesser, '<', where, args);
  numFilter('a.volumes', a.volumes_greater, '>', where, args);
  numFilter('a.volumes', a.volumes_lesser, '<', where, args);
  if (a.averageScore !== undefined) { where.push('a.average_score = ?'); args.push(a.averageScore); }
  if (a.averageScore_not !== undefined) { where.push('a.average_score != ?'); args.push(a.averageScore_not); }
  numFilter('a.average_score', a.averageScore_greater, '>', where, args);
  numFilter('a.average_score', a.averageScore_lesser, '<', where, args);
  if (a.popularity !== undefined) { where.push('a.popularity = ?'); args.push(a.popularity); }
  if (a.popularity_not !== undefined) { where.push('a.popularity != ?'); args.push(a.popularity_not); }
  numFilter('a.popularity', a.popularity_greater, '>', where, args);
  numFilter('a.popularity', a.popularity_lesser, '<', where, args);
  const fuzzy = (prefix, col) => {
    const expr = `(a.${prefix}_year * 10000 + COALESCE(a.${prefix}_month, 0) * 100 + COALESCE(a.${prefix}_day, 0))`;
    if (a[`${col}_greater`] !== undefined) { where.push(`${expr} > ?`); args.push(a[`${col}_greater`]); }
    if (a[`${col}_lesser`] !== undefined) { where.push(`${expr} < ?`); args.push(a[`${col}_lesser`]); }
    if (a[`${col}_like`] !== undefined) { where.push(`${expr} = ?`); args.push(a[`${col}_like`]); }
  };
  fuzzy('start', 'startDate');
  fuzzy('end', 'endDate');
  return { where, args };
}
async function tursoPage(fargs, page, perPage) {
  const c = tursoClient();
  if (!c) return null;
  const { where, args } = tursoWhere(fargs);
  const clause = where.join(' AND ');
  const countRs = await c.execute({ sql: `SELECT COUNT(*) AS n FROM anime a WHERE ${clause}`, args });
  const total = Number(countRs.rows[0]?.n || 0);
  if (total === 0) {
    // Empty result: legit (impossible filter) OR remote not bootstrapped yet.
    // Only fall back to shards when the remote DB itself is empty.
    const allRs = await c.execute({ sql: `SELECT COUNT(*) AS n FROM anime`, args: [] });
    if (Number(allRs.rows[0]?.n || 0) === 0) return null;
  }
  let order = tursoOrderClause(fargs.sort);
  if (fargs.search && !fargs.sort) {
    const q = String(fargs.search).toLowerCase().trim();
    order = `CASE WHEN lower(coalesce(a.title_romaji,'')) = ? OR lower(coalesce(a.title_english,'')) = ? OR lower(coalesce(a.title_native,'')) = ? THEN 0 ELSE 1 END, ${order}`;
    args.push(q, q, q);
  }
  const dataRs = await c.execute({
    sql: `SELECT a.raw_json AS raw_json FROM anime a WHERE ${clause} ORDER BY ${order} LIMIT ? OFFSET ?`,
    args: [...args, perPage, (page - 1) * perPage],
  });
  const items = [];
  for (const row of dataRs.rows) {
    try { if (row.raw_json) items.push(asExactMedia(JSON.parse(row.raw_json))); } catch { /* skip bad row */ }
  }
  return { total, items };
}
async function tursoMediaByArgs(args) {
  const c = tursoClient();
  if (!c) return null;
  let sql, sqlArgs;
  if (args.id) { sql = `SELECT raw_json FROM anime WHERE id = ?`; sqlArgs = [args.id]; }
  else if (args.idMal) { sql = `SELECT raw_json FROM anime WHERE id_mal = ?`; sqlArgs = [args.idMal]; }
  else if (args.search) {
    const where = [`COALESCE(type,'ANIME') = 'ANIME'`];
    const wargs = [];
    tursoSearchWhere(args.search, where, wargs);
    const q = String(args.search).toLowerCase().trim();
    sql = `SELECT raw_json FROM anime a WHERE ${where.join(' AND ')} ORDER BY CASE WHEN lower(coalesce(title_romaji,'')) = ? OR lower(coalesce(title_english,'')) = ? OR lower(coalesce(title_native,'')) = ? THEN 0 ELSE 1 END, popularity DESC NULLS LAST LIMIT 1`;
    sqlArgs = [...wargs, q, q, q];
  } else return null;
  const rs = await c.execute({ sql, args: sqlArgs });
  const raw = rs.rows[0]?.raw_json;
  if (!raw) return null;
  const anime = asExactMedia(JSON.parse(raw));
  if (args.type && args.type !== 'ANIME') return null;
  return anime;
}

function pageOut(fieldNode, fragments, paged, total, page, perPage) {
  const out = {};
  for (const s of fieldNode.selectionSet?.selections || []) {
    if (s.kind !== Kind.FIELD) continue;
    const k = s.alias?.value || s.name.value;
    if (s.name.value === 'media') {
      out[k] = paged.map((item) => pick(item, collectSelections(s, fragments), fragments));
    } else if (s.name.value === 'pageInfo') {
      out[k] = pick(buildPageInfo(total, page, perPage), collectSelections(s, fragments), fragments);
    } else out[k] = null;
  }
  return out;
}

async function resolvePage(fieldNode, fragments, variables, pageArgs) {
  const index = await getSearchIndex();
  const mediaFieldNode = fieldNode.selectionSet?.selections?.find(
    (s) => s.kind === Kind.FIELD && s.name.value === 'media');
  const mediaArgs = mediaFieldNode ? collectArgs(mediaFieldNode, variables) : {};
  const fargs = { ...mediaArgs, ...pageArgs };
  // AniList allows args on Page or on media — merge both
  const page = fargs.page || 1;
  const perPage = Math.min(fargs.perPage || 25, 50);

  // Fast path: indexed edge SQL (milliseconds). Falls back to shards on any error.
  try {
    const t = await tursoPage(fargs, page, perPage);
    if (t) return pageOut(fieldNode, fragments, t.items, t.total, page, perPage);
  } catch { /* shard fallback below */ }

  const needFull = fargs.sort || fargs.tagCategory_in || fargs.tagCategory_not_in
    || fargs.minimumTagRank !== undefined;
  let ids = index.filter((e) => matchMedia(e, null, fargs)).map((e) => e.id);

  let full = [];
  if (needFull || ids.length <= 2000) {
    full = await getAnimeBatch(ids);
    full = full.filter((m) => matchMedia(index.find((e) => e.id === m.id) || {}, m, fargs));
    sortMediaList(full, fargs.sort);
  } else {
    // large un-sorted result: keep index order (popularity DESC) and page by IDs
    full = null;
  }

  let total, paged;
  if (full) {
    total = full.length;
    paged = full.slice((page - 1) * perPage, page * perPage);
  } else {
    total = ids.length;
    const pagedIds = ids.slice((page - 1) * perPage, page * perPage);
    paged = await getAnimeBatch(pagedIds);
  }

  return pageOut(fieldNode, fragments, paged, total, page, perPage);
}

function splitYMD(s) {
  const m = String(s || '').match(/^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?$/);
  if (!m) return { year: null, month: null, day: null };
  return { year: parseInt(m[1], 10), month: m[2] ? parseInt(m[2], 10) : null, day: m[3] ? parseInt(m[3], 10) : null };
}
function jarr(s, fb = []) {
  try { const v = JSON.parse(s || ''); return Array.isArray(v) ? v : fb; } catch { return fb; }
}
// Indexed entity lookups (single-row SQL, ~50ms). Null = fall back to shards.
async function tursoCharacter(args) {
  const c = tursoClient();
  if (!c) return null;
  let rs;
  if (args.id) rs = await c.execute({ sql: `SELECT * FROM characters WHERE id = ?`, args: [args.id] });
  else if (args.search) rs = await c.execute({ sql: `SELECT * FROM characters WHERE lower(name_full) LIKE ? ESCAPE '\\' ORDER BY favourites DESC NULLS LAST LIMIT 1`, args: [`%${escLike(args.search)}%`] });
  else return null;
  const r = rs.rows[0];
  if (!r) return null;
  return {
    __typename: 'Character', id: r.id,
    name: { __typename: 'CharacterName', first: r.name_first, middle: r.name_middle, last: r.name_last, full: r.name_full, native: r.name_native, alternative: jarr(r.name_alternative), alternativeSpoiler: jarr(r.name_alternative_spoiler), userPreferred: r.name_user_preferred || r.name_full },
    image: { __typename: 'CharacterImage', large: r.image_large, medium: r.image_medium },
    description: r.description, gender: r.gender, dateOfBirth: { __typename: 'FuzzyDate', ...splitYMD(r.date_of_birth) },
    age: r.age, bloodType: r.blood_type, favourites: r.favourites, siteUrl: r.site_url,
    isFavourite: false, isFavouriteBlocked: !!r.is_favourite_blocked,
  };
}
async function tursoStaff(args) {
  const c = tursoClient();
  if (!c) return null;
  let rs;
  if (args.id) rs = await c.execute({ sql: `SELECT * FROM staff WHERE id = ?`, args: [args.id] });
  else if (args.search) rs = await c.execute({ sql: `SELECT * FROM staff WHERE lower(name_full) LIKE ? ESCAPE '\\' ORDER BY favourites DESC NULLS LAST LIMIT 1`, args: [`%${escLike(args.search)}%`] });
  else return null;
  const r = rs.rows[0];
  if (!r) return null;
  return {
    __typename: 'Staff', id: r.id, language: r.language,
    name: { __typename: 'StaffName', first: r.name_first, middle: r.name_middle, last: r.name_last, full: r.name_full, native: r.name_native, alternative: jarr(r.name_alternative), userPreferred: r.name_user_preferred || r.name_full },
    image: { __typename: 'StaffImage', large: r.image_large, medium: r.image_medium },
    description: r.description, primaryOccupations: jarr(r.primary_occupations), gender: r.gender,
    dateOfBirth: { __typename: 'FuzzyDate', ...splitYMD(r.date_of_birth) },
    dateOfDeath: { __typename: 'FuzzyDate', ...splitYMD(r.date_of_death) },
    age: r.age, yearsActive: jarr(r.years_active), homeTown: r.home_town, bloodType: r.blood_type,
    favourites: r.favourites, siteUrl: r.site_url,
    isFavourite: false, isFavouriteBlocked: !!r.is_favourite_blocked,
  };
}
async function tursoStudio(args) {
  const c = tursoClient();
  if (!c) return null;
  let rs;
  if (args.id) rs = await c.execute({ sql: `SELECT * FROM studios WHERE id = ?`, args: [args.id] });
  else if (args.search) rs = await c.execute({ sql: `SELECT * FROM studios WHERE lower(name) LIKE ? ESCAPE '\\' ORDER BY favourites DESC NULLS LAST LIMIT 1`, args: [`%${escLike(args.search)}%`] });
  else return null;
  const r = rs.rows[0];
  if (!r) return null;
  return { __typename: 'Studio', id: r.id, name: r.name, isAnimationStudio: !!r.is_animation_studio, siteUrl: r.site_url, favourites: r.favourites, isFavourite: false };
}
async function tursoHasRows(table) {
  try {
    const c = tursoClient();
    if (!c) return false;
    const rs = await c.execute({ sql: `SELECT COUNT(*) AS n FROM ${table}`, args: [] });
    return Number(rs.rows[0]?.n || 0) > 0;
  } catch { return false; }
}
async function tursoAiring(args) {
  const c = tursoClient();
  if (!c) return null;
  let rs;
  if (args.id) rs = await c.execute({ sql: `SELECT * FROM airing_schedule WHERE id = ?`, args: [args.id] });
  else if (args.mediaId) rs = await c.execute({ sql: `SELECT * FROM airing_schedule WHERE anime_id = ? ORDER BY episode LIMIT 1`, args: [args.mediaId] });
  else return null;
  const r = rs.rows[0];
  if (!r) return null;
  const now = Math.floor(Date.now() / 1000);
  return { __typename: 'AiringSchedule', id: r.id, episode: r.episode, airingAt: r.airing_at, timeUntilAiring: Math.max(0, (r.airing_at || 0) - now), mediaId: r.media_id || r.anime_id };
}
async function tursoTags() {
  const c = tursoClient();
  if (!c) return null;
  const rs = await c.execute({ sql: `SELECT id, name, description, category, rank, is_general_spoiler, is_media_spoiler, is_adult FROM tags ORDER BY name`, args: [] });
  return rs.rows.map((r) => ({
    __typename: 'MediaTag', id: r.id, name: r.name, description: r.description, category: r.category, rank: r.rank,
    isGeneralSpoiler: !!r.is_general_spoiler, isMediaSpoiler: !!r.is_media_spoiler, isAdult: !!r.is_adult,
  }));
}
async function scanNested(kind, args) {
  // Shard-scan fallback for entity roots (used only when Turso is unreachable).
  // Character/Staff/Studio/AiringSchedule standalone lookup by scanning cached shards.
  // Anime-only offline: sufficient and exact (same objects as Media nested).
  const shardStartIds = await getShardStartIds();
  const match = [];
  const maxScan = Math.min(shardStartIds.length, 8);
  for (let i = 0; i < maxScan && match.length < 50; i++) {
    const shard = await getShard(i).catch(() => []);
    for (const a of shard) {
      if (kind === 'characters') {
        for (const e of a.characters?.edges || []) {
          const n = e.node;
          if (!n) continue;
          if (args.id && n.id !== args.id) continue;
          if (args.search && !(n.name?.full || '').toLowerCase().includes(String(args.search).toLowerCase())) continue;
          match.push({ __typename: 'Character', ...n });
        }
      } else if (kind === 'staff') {
        for (const e of a.staff?.edges || []) {
          const n = e.node;
          if (!n) continue;
          if (args.id && n.id !== args.id) continue;
          if (args.search && !(n.name?.full || '').toLowerCase().includes(String(args.search).toLowerCase())) continue;
          match.push({ __typename: 'Staff', ...n });
        }
      } else if (kind === 'studios') {
        for (const e of a.studios?.edges || []) {
          const n = e.node;
          if (!n) continue;
          if (args.id && n.id !== args.id) continue;
          if (args.search && !(n.name || '').toLowerCase().includes(String(args.search).toLowerCase())) continue;
          match.push({ __typename: 'Studio', ...n });
        }
      }
    }
  }
  const seen = new Map();
  for (const m of match) if (!seen.has(m.id)) seen.set(m.id, m);
  return [...seen.values()];
}

async function resolveNode(typeName, fieldNode, fragments, variables) {
  const sels = collectSelections(fieldNode, fragments);
  const args = collectArgs(fieldNode, variables);

  if (typeName === 'RootQuery' || typeName === 'Query') {
    switch (fieldNode.name.value) {
      case 'Page':
        return resolvePage(fieldNode, fragments, variables, args);
      case 'Media': {
        try {
          const t = await tursoMediaByArgs(args);
          if (t) return pick(t, sels, fragments);
        } catch { /* shard fallback below */ }
        if (args.id) {
          const anime = await getAnimeById(args.id);
          if (!anime || (args.type && args.type !== 'ANIME')) {
            const e = new Error(`Media not found: ${args.id}`);
            e.status = 404;
            throw e;
          }
          return pick(anime, sels, fragments);
        }
        if (args.idMal) {
          const index = await getSearchIndex();
          const hit = index.find((e) => e.idMal === args.idMal);
          if (!hit) throw Object.assign(new Error(`Media not found: idMal ${args.idMal}`), { status: 404 });
          const anime = await getAnimeById(hit.id);
          return pick(anime, sels, fragments);
        }
        if (args.search) {
          const index = await getSearchIndex();
          const ranked = index
            .map((e) => ({ e, s: searchScore(e, args.search) }))
            .filter((x) => x.s >= 0)
            .sort((a, b) => (b.s - a.s) || ((b.e.popularity || 0) - (a.e.popularity || 0)));
          if (!ranked.length) throw Object.assign(new Error('Media not found'), { status: 404 });
          const anime = await getAnimeById(ranked[0].e.id);
          return pick(anime, sels, fragments);
        }
        throw Object.assign(new Error('Media query requires id, idMal or search'), { status: 400 });
      }
      case 'Character':
      case 'Staff':
      case 'Studio': {
        const fname = fieldNode.name.value;
        const table = fname === 'Character' ? 'characters' : fname === 'Staff' ? 'staff' : 'studios';
        try {
          const t = table === 'characters' ? await tursoCharacter(args)
            : table === 'staff' ? await tursoStaff(args) : await tursoStudio(args);
          if (t) return pick(t, sels, fragments);
          // Turso miss: 404 only when the remote table actually holds data;
          // otherwise (not bootstrapped yet) fall through to shard scan.
          if ((args.id || args.search) && await tursoHasRows(table)) {
            throw Object.assign(new Error(`${fname} not found`), { status: 404 });
          }
        } catch (e) {
          if (e?.status === 404) throw e;
          // fall through to shard scan below
        }
        const kind = table === 'characters' ? 'characters'
          : table === 'staff' ? 'staff' : 'studios';
        const list = await scanNested(kind, args);
        if (args.id) {
          const one = list.find((x) => x.id === args.id);
          if (!one) throw Object.assign(new Error(`${fieldNode.name.value} not found: ${args.id}`), { status: 404 });
          return pick(one, sels, fragments);
        }
        return pick(list[0] || null, sels, fragments);
      }
      case 'AiringSchedule': {
        try {
          const t = await tursoAiring(args);
          if (t) return pick(t, sels, fragments);
        } catch { /* shard fallback below */ }
        if (args.id || args.mediaId) {
          const shardStartIds = await getShardStartIds();
          for (let i = 0; i < Math.min(shardStartIds.length, 8); i++) {
            const shard = await getShard(i).catch(() => []);
            for (const a of shard) {
              if (args.mediaId && a.id !== args.mediaId) continue;
              const edges = a.airingSchedule?.edges || [];
              const hit = args.id
                ? edges.find((e) => e.node?.id === args.id)?.node
                : edges[0]?.node;
              if (hit) return pick({ __typename: 'AiringSchedule', ...hit }, sels, fragments);
              if (args.mediaId && a.nextAiringEpisode) {
                return pick({ __typename: 'AiringSchedule', ...a.nextAiringEpisode }, sels, fragments);
              }
            }
          }
        }
        throw Object.assign(new Error('AiringSchedule not found'), { status: 404 });
      }
      case 'GenreCollection': {
        // Zero-read path: bundled at deploy time. Falls back to fetched metadata.
        try {
          if (bundledMeta?.genres?.length) return bundledMeta.genres;
        } catch { /* fetched fallback below */ }
        return (await getMetadata().catch(() => null))?.genres || [];
      }
      case 'MediaTagCollection': {
        try {
          const t = await tursoTags();
          if (t?.length) {
            if (!sels.length) return t;
            return t.map((x) => pick(x, sels, fragments));
          }
        } catch { /* shard fallback below */ }
        const shard = await getShard(0).catch(() => []);
        const tags = new Map();
        for (const a of shard.slice(0, 50)) {
          for (const t of a.tags || []) if (!tags.has(t.name)) tags.set(t.name, t);
        }
        const list = [...tags.values()].map((t) => ({ __typename: 'MediaTag', ...t }));
        if (!sels.length) return list;
        return list.map((t) => pick(t, sels, fragments));
      }
      default:
        throw Object.assign(new Error(`Unsupported root field in offline anime mirror: ${fieldNode.name.value}`), { status: 400 });
    }
  }
  return null;
}

async function execute(query, variables = {}, operationName = null) {
  let doc;
  try { doc = parse(query); }
  catch (e) { return { errors: [{ message: `Syntax Error: ${e.message}`, status: 400 }] }; }
  const fragments = {};
  for (const def of doc.definitions) {
    if (def.kind === Kind.FRAGMENT_DEFINITION) fragments[def.name.value] = def;
  }
  let op = operationName
    ? doc.definitions.find((d) => d.kind === Kind.OPERATION_DEFINITION && d.name?.value === operationName)
    : doc.definitions.find((d) => d.kind === Kind.OPERATION_DEFINITION);
  if (!op) return { errors: [{ message: 'No operation found', status: 400 }] };

  const data = {};
  const errors = [];
  for (const field of op.selectionSet.selections) {
    if (field.kind !== Kind.FIELD) continue;
    try {
      const result = await resolveNode('RootQuery', field, fragments, variables);
      data[field.alias?.value || field.name.value] = result;
    } catch (err) {
      errors.push({ message: err.message, status: err.status || 500, path: [field.name.value] });
    }
  }
  return errors.length ? { data, errors } : { data };
}

/* -------------------------------- handler --------------------------------- */
function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString())); }
      catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}
function nodeJson(res, data, status = 200, cacheSeconds = 0) {
  const headers = { 'Content-Type': 'application/json', ...CORS };
  // Edge-cache GET responses (identical rail/search queries served from edge,
  // zero compute and zero Turso reads). POSTs stay uncached (same as AniList).
  if (cacheSeconds > 0) {
    headers['Cache-Control'] = `public, s-maxage=${cacheSeconds}, stale-while-revalidate=86400`;
  }
  res.writeHead(status, headers);
  res.end(JSON.stringify(data));
}
export default async function handler(req, res) {
  if (req.method === 'OPTIONS') {
    res.writeHead(204, CORS);
    return res.end();
  }
  if (req.method === 'GET') {
    const parsed = new URL(req.url, `https://${req.headers.host || 'localhost'}`);
    const query = parsed.searchParams.get('query');
    if (!query) return nodeJson(res, INFO, 200);
    let variables = {};
    try { variables = JSON.parse(parsed.searchParams.get('variables') || '{}'); }
    catch { return nodeJson(res, { errors: [{ message: 'Invalid variables JSON', status: 400 }] }, 400); }
    return nodeJson(res, await execute(query, variables, parsed.searchParams.get('operationName')), 200, 300);
  }
  if (req.method === 'POST') {
    let body;
    try { body = await readBody(req); }
    catch { return nodeJson(res, { errors: [{ message: 'Invalid JSON body', status: 400 }] }, 400); }
    if (!body || !body.query) return nodeJson(res, { errors: [{ message: 'No query provided', status: 400 }] }, 400);
    try {
      return nodeJson(res, await execute(body.query, body.variables || {}, body.operationName || null), 200);
    } catch (e) {
      return nodeJson(res, { errors: [{ message: e.message || 'Internal error', status: 500 }] }, 500);
    }
  }
  return nodeJson(res, { errors: [{ message: 'Method not allowed', status: 405 }] }, 405);
}
