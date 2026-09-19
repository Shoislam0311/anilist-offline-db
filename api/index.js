import { parse, Kind } from 'graphql';
import { gunzipSync } from 'node:zlib';
import { createClient } from '@libsql/client';
// Deploy-time snapshot of the small files (search_index ~12MB, metadata tiny).
// LAZY-loaded: the Turso hot path never touches them, so cold starts must NOT
// pay a 12MB JSON parse. Only the shard fallback (Turso down / not required)
// triggers the first load, then caches in memory.
let _bundledIndex = null, _bundledIndexTried = false;
let _bundledMeta = null, _bundledMetaTried = false;
async function loadBundled(kind) {
  try {
    if (kind === 'index') {
      if (!_bundledIndexTried) {
        _bundledIndexTried = true;
        const m = await import('../docs/api/search_index.json', { with: { type: 'json' } });
        _bundledIndex = m?.default ?? null;
      }
      return _bundledIndex;
    }
    if (!_bundledMetaTried) {
      _bundledMetaTried = true;
      const m = await import('../docs/api/metadata.json', { with: { type: 'json' } });
      _bundledMeta = m?.default ?? null;
    }
    return _bundledMeta;
  } catch { return null; }
}

/* AniList Offline GraphQL API — EXACT anime-only mirror.
 * Primary read path: Turso (libSQL) — FTS5 search + indexed column projection +
 * batched single-round-trip page assembly. Shard/CDN layer is the fallback.
 * Env: TURSO_URL, TURSO_AUTH_TOKEN, TURSO_REQUIRED=1 (no shard fallback), DATA_BASE_URL.
 */
const DATA_BASE = (typeof process !== 'undefined' && process.env?.DATA_BASE_URL)
  || 'https://cdn.jsdelivr.net/gh/Shoislam0311/anilist-offline-db@main/docs/api';
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

async function bundledFirst(kind) {
  // Bundled snapshot wins when it looks complete; otherwise fall back to fetch.
  try {
    const local = await loadBundled(kind);
    if (kind === 'meta' && local?.totalAnime > 1000) return local;
    if (kind === 'index' && Array.isArray(local) && local.length > 1000) return local;
  } catch { /* bundler dropped the files; fetch instead */ }
  return null;
}

async function getMetadata() {
  if (!fresh(metaEntry)) {
    const local = await bundledFirst('meta');
    metaEntry = { data: local || await fetchJSON(`${DATA_BASE}/metadata.json`), time: Date.now() };
  }
  return metaEntry.data;
}
async function getSearchIndex() {
  if (!fresh(indexEntry)) {
    const local = await bundledFirst('index');
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
  if (a.idMal_not !== undefined && e.idMal === a.idMal_not) return false;
  if (a.idMal_not_in && a.idMal_not_in.includes(e.idMal)) return false;
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
  if (a.countryOfOrigin_in && !a.countryOfOrigin_in.includes(e.country)) return false;
  if (a.countryOfOrigin_not_in && !a.countryOfOrigin_not_in.includes(e.country)) return false;
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
  // tag category / minimumTagRank / licensed / exact-date need full object (post-filter below)
  if (full && (a.tagCategory || a.tagCategory_in || a.tagCategory_not_in || a.minimumTagRank !== undefined)) {
    const tags = full.tags || [];
    if (a.tagCategory && !tags.some((t) => t.category === a.tagCategory)) return false;
    if (a.tagCategory_in && !tags.some((t) => a.tagCategory_in.includes(t.category))) return false;
    if (a.tagCategory_not_in && tags.some((t) => a.tagCategory_not_in.includes(t.category))) return false;
    if (a.minimumTagRank !== undefined && !tags.some((t) => (t.rank ?? 0) >= a.minimumTagRank)) return false;
  }
  if (full && a.isLicensed !== undefined && a.isLicensed !== null) {
    const lic = !!full.isLicensed;
    if (lic !== !!a.isLicensed) return false;
  }
  const dateObjMatch = (fuzzyInt, obj) => {
    if (fuzzyInt == null) return false;
    const y = Math.floor(fuzzyInt / 10000), m = Math.floor((fuzzyInt % 10000) / 100), d = fuzzyInt % 100;
    if (obj.year !== undefined && obj.year !== null && y !== obj.year) return false;
    if (obj.month !== undefined && obj.month !== null && m !== obj.month) return false;
    if (obj.day !== undefined && obj.day !== null && d !== obj.day) return false;
    return true;
  };
  if (a.startDate && !dateObjMatch(e.startDate, a.startDate)) return false;
  if (a.endDate && !dateObjMatch(e.endDate, a.endDate)) return false;
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

/* ------------------------------ Turso layer -------------------------------
 * Design (free-budget, 1M+ req/week):
 *  - FTS5 MATCH for latin search (rows-read O(matches), NOT O(table)) — the
 *    LIKE scans read all 14.6k/91k rows per token and would burn Turso's free
 *    read budget at scale. LIKE stays only for CJK queries (unicode61 cannot
 *    segment CJK).
 *  - Column projection: card fields resolve from indexed columns (~200B/row),
 *    never the 350KB raw_json blob.
 *  - Nested connections (genres/tags/relations/characters/recommendations/…)
 *    assemble from child tables via client.batch() — ONE round trip.
 *  - TTL caches: page 5min, count 30min, entities 2min, raw_json LRU 30min.
 */

let turso;
function tursoClient() {
  if (turso !== undefined) return turso;
  try {
    const url = ((typeof process !== 'undefined' && process.env?.TURSO_URL) || '').trim();
    const token = ((typeof process !== 'undefined' && process.env?.TURSO_AUTH_TOKEN) || '').trim();
    turso = (url && token) ? createClient({ url, authToken: token }) : null;
  } catch { turso = null; }
  return turso;
}

/* Turso is the primary DB. When TURSO_REQUIRED=1, GraphQL never falls back
 * to downloading shards — Turso errors surface as 503 instead of silently
 * serving slow shard scans.
 */
function tursoRequired() {
  try {
    return ((typeof process !== 'undefined' && process.env?.TURSO_REQUIRED) || '').trim() === '1';
  } catch { return false; }
}
function tursoUnavailable(msg = 'Turso unavailable') {
  return Object.assign(new Error(msg), { status: 503 });
}
const DEBUG_API = (typeof process !== 'undefined' && process.env?.DEBUG_API) === '1';
function dbg(where, e) {
  if (DEBUG_API) console.error(`[api:${where}]`, e?.message || e);
}

/* ------------------------------- caches ----------------------------------- */
const COUNT_TTL_MS = 30 * 60 * 1000;  // totals move only on the daily sync
const PAGE_TTL_MS = 5 * 60 * 1000;    // repeat rail/home queries = zero reads
const ENTITY_TTL_MS = 2 * 60 * 1000;  // Character/Staff/Studio/Airing lookups
const RAW_TTL_MS = 30 * 60 * 1000;    // parsed raw_json LRU (heavy 350KB blobs)
const MAX_CACHE_ENTRIES = 120;        // per cache; bounded RAM on the lambda
const countCache = new Map();
const pageCache = new Map();
const entityCache = new Map();
const rawCache = new Map(); // id -> { raw, time }
function cacheGet(map, key, ttl) {
  const e = map.get(key);
  if (!e) return undefined;
  if (Date.now() - e.time > ttl) { map.delete(key); return undefined; }
  return e.data;
}
function cacheSet(map, key, data) {
  map.set(key, { data, time: Date.now() });
  if (map.size > MAX_CACHE_ENTRIES) {
    const oldest = map.keys().next().value;
    map.delete(oldest);
  }
}
function countCacheKey(clause, args) {
  return `c:${clause}|${JSON.stringify(args)}`;
}
function pageCacheKey(clause, args, order, page, perPage, projKey, childrenKey) {
  return `p:${clause}|${JSON.stringify(args)}|${order}|${page}|${perPage}|${projKey}|${childrenKey}`;
}
function entityKey(kind, args) { return `${kind}:${JSON.stringify(args)}`; }
function entityGet(kind, args) { return cacheGet(entityCache, entityKey(kind, args), ENTITY_TTL_MS); }
function entitySet(kind, args, data) { cacheSet(entityCache, entityKey(kind, args), data); }

/* ------------------------------ FTS search -------------------------------- */
const CJK_RE = /[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]/;
function ftsExpr(q) {
  const tokens = String(q || '').toLowerCase().trim()
    .split(/[\s_.,;:!?()[\]{}'"\/\\|-]+/).map((t) => t.trim())
    .filter((t) => t.length > 1).slice(0, 6);
  if (!tokens.length) return null;
  // phrase-prefix per token, ANDed: "cow"* AND "beb"*
  return tokens.map((t) => `"${t.replace(/"/g, '""')}"*`).join(' AND ');
}
function hasCJK(q) { return CJK_RE.test(String(q || '')); }

/* --------------------------- WHERE construction --------------------------- */
const HAY = `lower(coalesce(a.title_romaji,'') || ' ' || coalesce(a.title_english,'') || ' ' || coalesce(a.title_native,'') || ' ' || coalesce(a.synonyms,''))`;
function escLike(s) { return String(s).replace(/[\\%_]/g, (c) => '\\' + c).toLowerCase(); }
function searchTokensSql(q) {
  return String(q || '').toLowerCase().trim()
    .split(/[\s_.,;:!?()[\]{}'"\/\\|-]+/).map((t) => t.trim()).filter((t) => t.length > 1);
}
function searchWhere(search, where, args) {
  const q = String(search || '').toLowerCase().trim();
  if (!q) return;
  if (q.length < 2) { where.push('1 = 0'); return; } // 1-char: full scan, no signal
  if (!hasCJK(q)) {
    const expr = ftsExpr(q);
    if (expr) {
      // FTS5: rows-read proportional to matches. Fallback to LIKE happens at
      // execution time if the fts table is missing (older DBs).
      where.push(`a.id IN (SELECT rowid FROM anime_fts WHERE anime_fts MATCH ?)`);
      args.push(expr);
      return;
    }
  }
  const tokens = searchTokensSql(q).slice(0, 5);
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
  if (a.idMal_not !== undefined) { where.push('(a.id_mal IS NULL OR a.id_mal != ?)'); args.push(a.idMal_not); }
  if (a.idMal_not_in) { where.push(`(a.id_mal IS NULL OR a.id_mal NOT IN (${a.idMal_not_in.map(() => '?').join(',')}))`); args.push(...a.idMal_not_in); }
  if (a.search) searchWhere(a.search, where, args);
  if (a.genre) { where.push(`EXISTS (SELECT 1 FROM anime_genres ag JOIN genres g ON g.id = ag.genre_id WHERE ag.anime_id = a.id AND g.name = ?)`); args.push(a.genre); }
  if (a.genre_in) { where.push(`EXISTS (SELECT 1 FROM anime_genres ag JOIN genres g ON g.id = ag.genre_id WHERE ag.anime_id = a.id AND g.name IN (${a.genre_in.map(() => '?').join(',')}))`); args.push(...a.genre_in); }
  if (a.genre_not_in) { where.push(`NOT EXISTS (SELECT 1 FROM anime_genres ag JOIN genres g ON g.id = ag.genre_id WHERE ag.anime_id = a.id AND g.name IN (${a.genre_not_in.map(() => '?').join(',')}))`); args.push(...a.genre_not_in); }
  if (a.tag) { where.push(`EXISTS (SELECT 1 FROM anime_tags at WHERE at.anime_id = a.id AND at.tag_name = ?)`); args.push(a.tag); }
  if (a.tag_in) { where.push(`EXISTS (SELECT 1 FROM anime_tags at WHERE at.anime_id = a.id AND at.tag_name IN (${a.tag_in.map(() => '?').join(',')}))`); args.push(...a.tag_in); }
  if (a.tag_not_in) { where.push(`NOT EXISTS (SELECT 1 FROM anime_tags at WHERE at.anime_id = a.id AND at.tag_name IN (${a.tag_in.map(() => '?').join(',')}))`); args.push(...a.tag_not_in); }
  if (a.tagCategory_in) { where.push(`EXISTS (SELECT 1 FROM anime_tags at JOIN tags t ON t.name = at.tag_name WHERE at.anime_id = a.id AND t.category IN (${a.tagCategory_in.map(() => '?').join(',')}))`); args.push(...a.tagCategory_in); }
  if (a.tagCategory) { where.push(`EXISTS (SELECT 1 FROM anime_tags at JOIN tags t ON t.name = at.tag_name WHERE at.anime_id = a.id AND t.category = ?)`); args.push(a.tagCategory); }
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
  if (a.countryOfOrigin_in) { where.push(`a.country_of_origin IN (${a.countryOfOrigin_in.map(() => '?').join(',')})`); args.push(...a.countryOfOrigin_in); }
  if (a.countryOfOrigin_not_in) { where.push(`(a.country_of_origin IS NULL OR a.country_of_origin NOT IN (${a.countryOfOrigin_not_in.map(() => '?').join(',')}))`); args.push(...a.countryOfOrigin_not_in); }
  if (a.isLicensed !== undefined && a.isLicensed !== null) { where.push('a.is_licensed = ?'); args.push(a.isLicensed ? 1 : 0); }
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
  const dateObj = (prefix, obj) => {
    if (!obj || typeof obj !== 'object') return;
    if (obj.year !== undefined && obj.year !== null) { where.push(`a.${prefix}_year = ?`); args.push(obj.year); }
    if (obj.month !== undefined && obj.month !== null) { where.push(`a.${prefix}_month = ?`); args.push(obj.month); }
    if (obj.day !== undefined && obj.day !== null) { where.push(`a.${prefix}_day = ?`); args.push(obj.day); }
  };
  dateObj('start', a.startDate);
  dateObj('end', a.endDate);
  return { where, args };
}

/* --------------------- column projection (no raw_json) -------------------- */
const COL_FIELDS = {
  'id': 'a.id AS id',
  'idMal': 'a.id_mal AS idMal',
  'type': "COALESCE(a.type,'ANIME') AS type",
  'format': 'a.format AS format',
  'status': 'a.status AS status',
  'description': 'a.description AS description',
  'episodes': 'a.episodes AS episodes',
  'duration': 'a.duration AS duration',
  'chapters': 'a.chapters AS chapters',
  'volumes': 'a.volumes AS volumes',
  'averageScore': 'a.average_score AS averageScore',
  'meanScore': 'a.mean_score AS meanScore',
  'popularity': 'a.popularity AS popularity',
  'favourites': 'a.favourites AS favourites',
  'trending': 'a.trending AS trending',
  'season': 'a.season AS season',
  'seasonYear': 'a.season_year AS seasonYear',
  'seasonInt': 'a.season_int AS seasonInt',
  'countryOfOrigin': 'a.country_of_origin AS countryOfOrigin',
  'isLicensed': 'a.is_licensed AS isLicensed',
  'source': 'a.source AS source',
  'hashtag': 'a.hashtag AS hashtag',
  'bannerImage': 'a.banner_image AS bannerImage',
  'isAdult': 'a.is_adult AS isAdult',
  'isLocked': 'a.is_locked AS isLocked',
  'modNotes': 'a.mod_notes AS modNotes',
  'autoCreateForumThread': 'a.auto_create_forum_thread AS autoCreateForumThread',
  'isRecommendationBlocked': 'a.is_recommendation_blocked AS isRecommendationBlocked',
  'isReviewBlocked': 'a.is_review_blocked AS isReviewBlocked',
  'updatedAt': 'a.updated_at AS updatedAt',
  'siteUrl': 'a.site_url AS siteUrl',
  'title.romaji': 'a.title_romaji AS title_romaji',
  'title.english': 'a.title_english AS title_english',
  'title.native': 'a.title_native AS title_native',
  'title.userPreferred': 'a.title_user_preferred AS title_user_preferred',
  'coverImage.extraLarge': 'a.cover_extra_large AS cover_extra_large',
  'coverImage.large': 'a.cover_large AS cover_large',
  'coverImage.medium': 'a.cover_medium AS cover_medium',
  'coverImage.color': 'a.cover_color AS cover_color',
  'startDate.year': 'a.start_year AS start_year',
  'startDate.month': 'a.start_month AS start_month',
  'startDate.day': 'a.start_day AS start_day',
  'endDate.year': 'a.end_year AS end_year',
  'endDate.month': 'a.end_month AS end_month',
  'endDate.day': 'a.end_day AS end_day',
  'trailer.id': 'a.trailer_id AS trailer_id',
  'trailer.site': 'a.trailer_site AS trailer_site',
  'trailer.thumbnail': 'a.trailer_thumbnail AS trailer_thumbnail',
  'synonyms': 'a.synonyms AS synonyms',
};
const PROJ_OBJECTS = new Set(['title', 'coverImage', 'startDate', 'endDate', 'trailer']);
/* Nested connections assembled from child tables (one batched round trip),
 * so card queries with genres/tags/description/etc never touch raw_json. */
const ASSEMBLER_FIELDS = new Set([
  'genres', 'tags', 'studios', 'relations', 'characters', 'characterPreview',
  'staff', 'staffPreview', 'recommendations', 'rankings', 'externalLinks',
  'streamingEpisodes', 'stats', 'airingSchedule', 'reviews', 'trends',
  'nextAiringEpisode',
]);
function planProjection(mediaNode, fragments) {
  const sels = mediaNode ? collectSelections(mediaNode, fragments) : [];
  if (!sels.length) return { cols: ['a.id AS id'], children: [] };
  const cols = new Set();
  const children = new Set();
  let needNextAiringCols = false;
  const walk = (nodes, prefix) => {
    for (const s of nodes) {
      if (s.name.value === '__typename') continue;
      const path = prefix ? prefix + '.' + s.name.value : s.name.value;
      const sub = s.selectionSet ? collectSelections(s, fragments) : null;
      if (sub && sub.length) {
        if (PROJ_OBJECTS.has(s.name.value)) {
          if (walk(sub, path) === null) return null;
        } else if (s.name.value === 'nextAiringEpisode') {
          children.add('nextAiringEpisode'); needNextAiringCols = true;
        } else if (ASSEMBLER_FIELDS.has(s.name.value)) {
          children.add(s.name.value);
        } else return null;
      } else {
        // scalar-list fields (genres, tags) come from the assembler, not columns
        if (!prefix && ASSEMBLER_FIELDS.has(s.name.value)) { children.add(s.name.value); continue; }
        if (!(path in COL_FIELDS)) return null;
        cols.add(COL_FIELDS[path]);
      }
    }
    return true;
  };
  if (walk(sels, '') === null) return null;
  if (needNextAiringCols) {
    cols.add('a.next_airing_episode AS next_airing_episode');
    cols.add('a.next_airing_at AS next_airing_at');
  }
  // queries that select ONLY nested connections still need a base row
  if (!cols.size) cols.add('a.id AS id');
  return { cols: [...cols], children: [...children] };
}
function rowToMedia(row) {
  const m = { __typename: 'Media', type: 'ANIME', isFavourite: false };
  const simple = ['id', 'idMal', 'format', 'status', 'description', 'episodes', 'duration', 'chapters',
    'volumes', 'averageScore', 'meanScore', 'popularity', 'favourites', 'trending', 'season',
    'seasonYear', 'seasonInt', 'countryOfOrigin', 'isLicensed', 'source', 'hashtag', 'bannerImage',
    'isAdult', 'isLocked', 'modNotes', 'autoCreateForumThread', 'isRecommendationBlocked',
    'isReviewBlocked', 'updatedAt'];
  for (const k of simple) if (k in row) m[k] = row[k];
  if ('type' in row && row.type) m.type = row.type;
  if ('isAdult' in row && row.isAdult !== null && row.isAdult !== undefined) m.isAdult = !!row.isAdult;
  if ('isLicensed' in row && row.isLicensed !== null && row.isLicensed !== undefined) m.isLicensed = !!row.isLicensed;
  if ('isLocked' in row && row.isLocked !== null && row.isLocked !== undefined) m.isLocked = !!row.isLocked;
  if ('title_romaji' in row || 'title_english' in row || 'title_native' in row || 'title_user_preferred' in row) {
    m.title = {
      __typename: 'MediaTitle',
      romaji: row.title_romaji ?? null, english: row.title_english ?? null,
      native: row.title_native ?? null, userPreferred: row.title_user_preferred ?? row.title_romaji ?? row.title_english ?? null,
    };
  }
  if ('cover_large' in row || 'cover_medium' in row || 'cover_extra_large' in row || 'cover_color' in row) {
    m.coverImage = {
      __typename: 'CoverImage',
      extraLarge: row.cover_extra_large ?? null, large: row.cover_large ?? null,
      medium: row.cover_medium ?? null, color: row.cover_color ?? null,
    };
  }
  const fuzzy = (p) => {
    const y = `${p}_year`, mo = `${p}_month`, d = `${p}_day`;
    if (!(y in row) && !(mo in row) && !(d in row)) return undefined;
    return { __typename: 'FuzzyDate', year: row[y] ?? null, month: row[mo] ?? null, day: row[d] ?? null };
  };
  const sd = fuzzy('start'), ed = fuzzy('end');
  if (sd) m.startDate = sd;
  if (ed) m.endDate = ed;
  if ('trailer_id' in row || 'trailer_site' in row || 'trailer_thumbnail' in row) {
    if (row.trailer_site || row.trailer_id) {
      m.trailer = { __typename: 'MediaTrailer', id: row.trailer_id ?? null, site: row.trailer_site ?? null, thumbnail: row.trailer_thumbnail ?? null };
    } else m.trailer = null;
  }
  if ('synonyms' in row) {
    try { m.synonyms = JSON.parse(row.synonyms || '[]'); }
    catch { m.synonyms = []; }
    if (!Array.isArray(m.synonyms)) m.synonyms = [];
  }
  if ('next_airing_episode' in row && (row.next_airing_episode != null || row.next_airing_at != null)) {
    const airingAt = row.next_airing_at ?? null;
    m.nextAiringEpisode = {
      __typename: 'MediaAiringEpisode',
      id: null,
      episode: row.next_airing_episode ?? null,
      airingAt,
      timeUntilAiring: airingAt ? Math.max(0, airingAt - Math.floor(Date.now() / 1000)) : 0,
      mediaId: row.id ?? null,
    };
  }
  if (!m.siteUrl && m.id) m.siteUrl = `https://anilist.co/anime/${m.id}`;
  return m;
}

/* ------------------------- child-table assembler --------------------------
 * Builds nested AniList connection objects from indexed child tables.
 * Statements run inside the SAME client.batch() as the page query = one
 * round trip per level. perPage/sort args on nested fields are honored.
 */
function splitYMD(s) {
  const m = String(s || '').match(/^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?$/);
  if (!m) return { year: null, month: null, day: null };
  return { year: parseInt(m[1], 10), month: m[2] ? parseInt(m[2], 10) : null, day: m[3] ? parseInt(m[3], 10) : null };
}
function jarr(s, fb = []) {
  try { const v = JSON.parse(s || ''); return Array.isArray(v) ? v : fb; } catch { return fb; }
}
function charFromRow(r) {
  return {
    __typename: 'Character', id: r.id,
    name: { __typename: 'CharacterName', first: r.name_first, middle: r.name_middle, last: r.name_last, full: r.name_full, native: r.name_native, alternative: jarr(r.name_alternative), alternativeSpoiler: jarr(r.name_alternative_spoiler), userPreferred: r.name_user_preferred || r.name_full },
    image: { __typename: 'CharacterImage', large: r.image_large, medium: r.image_medium },
    description: r.description, gender: r.gender, dateOfBirth: { __typename: 'FuzzyDate', ...splitYMD(r.date_of_birth) },
    age: r.age, bloodType: r.blood_type, favourites: r.favourites, siteUrl: r.site_url,
    isFavourite: false, isFavouriteBlocked: !!r.is_favourite_blocked,
  };
}
function staffFromRow(r) {
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
function charSortFn(sort) {
  const s = Array.isArray(sort) ? sort[0] : sort;
  switch (s) {
    case 'ID': return (a, b) => a.id - b.id;
    case 'ID_DESC': return (a, b) => b.id - a.id;
    case 'FAVOURITES': return (a, b) => (a.favourites ?? 0) - (b.favourites ?? 0);
    case 'FAVOURITES_DESC': return (a, b) => (b.favourites ?? 0) - (a.favourites ?? 0);
    case 'ROLE_REVERSED': return (a, b) => (a.role === 'MAIN' ? 1 : 0) - (b.role === 'MAIN' ? 1 : 0);
    default: return (a, b) => (b.role === 'MAIN' ? 1 : 0) - (a.role === 'MAIN' ? 1 : 0); // ROLE
  }
}
function chunkIds(ids, n = 90) {
  const out = [];
  for (let i = 0; i < ids.length; i += n) out.push(ids.slice(i, i + n));
  return out;
}
function inPlaceholders(n) { return Array.from({ length: n }, () => '?').join(','); }

async function batchExecute(stmts) {
  // One round trip for N statements; falls back to parallel executes.
  const c = tursoClient();
  if (!c) throw new Error('no turso client');
  if (stmts.length === 1) return [await c.execute(stmts[0])];
  try {
    return await c.batch(stmts, 'read');
  } catch {
    return Promise.all(stmts.map((s) => c.execute(s).catch(() => ({ rows: [], columns: [] }))));
  }
}

// Fetch nested Media nodes (relations.node, mediaRecommendation) honoring the
// client's sub-selection: projected columns when possible, raw_json otherwise.
async function fetchNestedMedia(ids, mediaSelNode, fragments) {
  const uniq = [...new Set(ids)].filter((x) => x != null).sort((a, b) => a - b);
  if (!uniq.length) return new Map();
  const out = new Map();
  const proj = planProjection(mediaSelNode, fragments);
  const hasChildren = proj && proj.children.length > 0;
  if (proj && !hasChildren) {
    for (const chunk of chunkIds(uniq)) {
      const rs = await batchExecute([{ sql: `SELECT ${proj.cols.join(', ')} FROM anime a WHERE a.id IN (${inPlaceholders(chunk.length)})`, args: chunk }]);
      for (const row of rs[0].rows) out.set(row.id, rowToMedia(row));
    }
    return out;
  }
  // raw path (with LRU) — raw_json carries its own fetch-time nested children
  for (const chunk of chunkIds(uniq)) {
    const rs = await batchExecute([{ sql: `SELECT id, raw_json FROM anime WHERE id IN (${inPlaceholders(chunk.length)})`, args: chunk }]);
    for (const r of rs[0].rows) {
      const cached = rawCache.get(r.id);
      if (cached && Date.now() - cached.time < RAW_TTL_MS) { out.set(r.id, cached.data); continue; }
      try {
        const media = asExactMedia(JSON.parse(r.raw_json));
        rawCache.set(r.id, { data: media, time: Date.now() });
        if (rawCache.size > MAX_CACHE_ENTRIES) rawCache.delete(rawCache.keys().next().value);
        out.set(r.id, media);
      } catch { /* skip bad row */ }
    }
  }
  return out;
}

/* Assemble requested children onto page rows. Runs child statements in ONE
 * batch; stitches per-anime groups; honors per-field page/perPage/sort args. */
async function assembleChildren(items, children, mediaFieldNode, fragments, variables) {
  if (!items.length || !children.length) return;
  const ids = items.map((m) => m.id);
  const idSet = new Set(ids);
  const byAnime = new Map(ids.map((id) => [id, []]));
  const put = (animeId, v) => { const l = byAnime.get(animeId); if (l) l.push(v); };
  const mediaSels = collectSelections(mediaFieldNode, fragments);
  const childArgs = {};
  for (const name of children) {
    const node = mediaSels.find((s) => s.name.value === name);
    childArgs[name] = node ? collectArgs(node, variables) : {};
  }
  const stmts = [];
  const stmtKeys = [];
  const add = (key, sql, args) => { stmts.push({ sql, args }); stmtKeys.push(key); };

  const q = inPlaceholders(ids.length);
  if (children.includes('genres')) {
    add('genres', `SELECT ag.anime_id, g.name FROM anime_genres ag JOIN genres g ON g.id = ag.genre_id WHERE ag.anime_id IN (${q}) ORDER BY ag.rowid`, ids);
  }
  if (children.includes('tags')) {
    add('tags', `SELECT at.anime_id, at.tag_name, at.tag_rank, t.id AS t_id, t.description AS t_desc, t.category AS t_cat, t.is_general_spoiler AS t_gs, t.is_media_spoiler AS t_ms, t.is_adult AS t_ad FROM anime_tags at LEFT JOIN tags t ON t.name = at.tag_name WHERE at.anime_id IN (${q}) ORDER BY at.tag_rank DESC, at.rowid`, ids);
  }
  if (children.includes('studios')) {
    add('studios', `SELECT ast.anime_id, ast.edge_id, ast.is_main, s.id, s.name, s.is_animation_studio, s.site_url, s.favourites FROM anime_studios ast JOIN studios s ON s.id = ast.studio_id WHERE ast.anime_id IN (${q}) ORDER BY ast.favourite_order, ast.rowid`, ids);
  }
  if (children.includes('relations')) {
    add('relations', `SELECT anime_id, related_anime_id, relation_type FROM relations WHERE anime_id IN (${q}) ORDER BY rowid`, ids);
  }
  if (children.includes('characters') || children.includes('characterPreview')) {
    add('characters', `SELECT ac.anime_id, ac.character_id, ac.edge_id, ac.role, ac.sort_order, ac.favourite_order, c.* FROM anime_characters ac JOIN characters c ON c.id = ac.character_id WHERE ac.anime_id IN (${q}) ORDER BY ac.anime_id, CASE ac.role WHEN 'MAIN' THEN 0 ELSE 1 END, ac.sort_order, ac.rowid`, ids);
  }
  if (children.includes('staff') || children.includes('staffPreview')) {
    add('staff', `SELECT ast.anime_id, ast.staff_id, ast.edge_id, ast.role, ast.sort_order, s.* FROM anime_staff ast JOIN staff s ON s.id = ast.staff_id WHERE ast.anime_id IN (${q}) ORDER BY ast.anime_id, ast.sort_order, ast.rowid`, ids);
  }
  if (children.includes('recommendations')) {
    add('recommendations', `SELECT id, anime_id, recommended_anime_id, rating, user_rating FROM recommendations WHERE anime_id IN (${q}) ORDER BY rating DESC, rowid`, ids);
  }
  if (children.includes('rankings')) {
    add('rankings', `SELECT anime_id, rank_id, rank, type, format, year, season, all_time, context FROM rankings WHERE anime_id IN (${q}) ORDER BY rank, rowid`, ids);
  }
  if (children.includes('externalLinks')) {
    add('externalLinks', `SELECT anime_id, id, site, url, type, language, color, icon FROM external_links WHERE anime_id IN (${q}) AND COALESCE(is_disabled,0)=0 ORDER BY rowid`, ids);
  }
  if (children.includes('streamingEpisodes')) {
    add('streamingEpisodes', `SELECT anime_id, title, thumbnail, url, site FROM streaming_episodes WHERE anime_id IN (${q}) ORDER BY id`, ids);
  }
  if (children.includes('stats')) {
    add('stats', `SELECT anime_id, score_distribution, rankings FROM statistics WHERE anime_id IN (${q})`, ids);
  }
  if (children.includes('airingSchedule')) {
    add('airingSchedule', `SELECT anime_id, id, episode, airing_at FROM airing_schedule WHERE anime_id IN (${q}) ORDER BY anime_id, episode`, ids);
  }
  if (children.includes('reviews')) {
    add('reviews', `SELECT id, anime_id, user_id, user_name, summary, rating, user_rating, score, body, created_at, site_url FROM reviews WHERE anime_id IN (${q}) ORDER BY rating DESC, rowid`, ids);
  }
  if (children.includes('trends')) {
    add('trends', `SELECT anime_id, date, trending, average_score, popularity, episode, releasing FROM trends WHERE anime_id IN (${q}) ORDER BY date`, ids);
  }

  const results = new Map();
  if (stmts.length) {
    const rss = await batchExecute(stmts);
    for (let i = 0; i < stmtKeys.length; i++) results.set(stmtKeys[i], rss[i]?.rows || []);
  }

  // voice actors join runs as its own batch level (depends on character ids)
  let vaByChar = null;
  if (children.includes('characters') || children.includes('characterPreview')) {
    const rows = results.get('characters') || [];
    const charIds = [...new Set(rows.map((r) => r.character_id))];
    vaByChar = new Map();
    if (charIds.length) {
      const vaRows = [];
      for (const chunk of chunkIds(charIds)) {
        const rs = await batchExecute([{
          sql: `SELECT cva.character_id, cva.anime_id, cva.language, va.id, va.name_first, va.name_middle, va.name_last, va.name_full, va.name_native, va.image_large, va.image_medium, va.language AS va_language FROM character_voice_actors cva JOIN voice_actors va ON va.id = cva.voice_actor_id WHERE cva.character_id IN (${inPlaceholders(chunk.length)})`,
          args: chunk,
        }]);
        vaRows.push(...rs[0].rows);
      }
      for (const r of vaRows) {
        const key = `${r.anime_id}:${r.character_id}`;
        if (!vaByChar.has(key)) vaByChar.set(key, []);
        vaByChar.get(key).push({
          __typename: 'Staff', id: r.id, language: r.va_language || r.language,
          name: { __typename: 'StaffName', first: r.name_first, middle: r.name_middle, last: r.name_last, full: r.name_full, native: r.name_native, userPreferred: r.name_full },
          image: { __typename: 'StaffImage', large: r.image_large, medium: r.image_medium },
          isFavourite: false,
        });
      }
    }
  }

  const now = Math.floor(Date.now() / 1000);
  const sliceConn = (list, args, defPage = 1, defPer = 25) => {
    const page = Math.max(1, args.page || defPage);
    const perPage = Math.min(Math.max(1, args.perPage || defPer), 50);
    const total = list.length;
    const sliced = list.slice((page - 1) * perPage, page * perPage);
    return { list: sliced, pageInfo: buildPageInfo(total, page, perPage) };
  };

  for (const name of children) {
    const args = childArgs[name] || {};
    if (name === 'genres') {
      for (const r of results.get('genres') || []) put(r.anime_id, r.name);
      for (const m of items) m.genres = byAnime.get(m.id) || [];
    } else if (name === 'tags') {
      const tmp = new Map(ids.map((id) => [id, []]));
      for (const r of results.get('tags') || []) {
        if (!tmp.has(r.anime_id)) continue;
        tmp.get(r.anime_id).push({
          __typename: 'MediaTag', id: r.t_id ?? null, name: r.tag_name, rank: r.tag_rank,
          description: r.t_desc ?? null, category: r.t_cat ?? null,
          isGeneralSpoiler: !!r.t_gs, isMediaSpoiler: !!r.t_ms, isAdult: !!r.t_ad,
        });
      }
      for (const m of items) m.tags = tmp.get(m.id) || [];
    } else if (name === 'studios') {
      const tmp = new Map(ids.map((id) => [id, []]));
      for (const r of results.get('studios') || []) {
        if (!tmp.has(r.anime_id)) continue;
        tmp.get(r.anime_id).push({
          __typename: 'StudioEdge', id: r.edge_id ?? null, isMain: !!r.is_main,
          node: { __typename: 'Studio', id: r.id, name: r.name, isAnimationStudio: !!r.is_animation_studio, siteUrl: r.site_url, favourites: r.favourites, isFavourite: false },
        });
      }
      for (const m of items) {
        const edges = tmp.get(m.id) || [];
        m.studios = { __typename: 'StudioConnection', edges, nodes: edges.map((e) => e.node) };
      }
    } else if (name === 'relations') {
      const relRows = results.get('relations') || [];
      const relIds = relRows.map((r) => r.related_anime_id);
      // node sub-selection from edges{node{...}} or nodes{...}
      const relSel = mediaSels.find((s) => s.name.value === 'relations');
      let relNodeSel = null;
      if (relSel?.selectionSet) {
        const rs = collectSelections(relSel, fragments);
        const edgeField = rs.find((s) => s.name.value === 'edges');
        const nodeField = edgeField ? collectSelections(edgeField, fragments).find((s) => s.name.value === 'node') : null;
        const direct = rs.find((s) => s.name.value === 'nodes');
        const src = nodeField || direct;
        if (src) relNodeSel = { selectionSet: src.selectionSet };
      }
      const mediaMap = relIds.length
        ? await fetchNestedMedia(relIds, relNodeSel, fragments)
        : new Map();
      const tmp = new Map(ids.map((id) => [id, []]));
      for (const r of relRows) {
        if (!tmp.has(r.anime_id)) continue;
        tmp.get(r.anime_id).push({
          __typename: 'MediaEdge', relationType: r.relation_type, isMainStudio: false,
          node: mediaMap.get(r.related_anime_id) || null,
        });
      }
      for (const m of items) m.relations = { __typename: 'MediaConnection', edges: tmp.get(m.id) || [] };
    } else if (name === 'characters' || name === 'characterPreview') {
      const rows = results.get('characters') || [];
      const perChar = new Map();
      for (const r of rows) {
        const key = `${r.anime_id}:${r.character_id}`;
        if (!perChar.has(key)) {
          perChar.set(key, { anime_id: r.anime_id, role: r.role, sort_order: r.sort_order ?? 0, fav: r.favourites ?? 0, edge_id: r.edge_id, node: charFromRow(r) });
        }
      }
      if (vaByChar) {
        // honor voiceActors(language:) args from the client's edge selection
        const charSel = mediaSels.find((s) => s.name.value === name);
        let vaArgs = {};
        if (charSel?.selectionSet) {
          const cs = collectSelections(charSel, fragments);
          const edgeField = cs.find((s) => s.name.value === 'edges');
          const vaField = edgeField ? collectSelections(edgeField, fragments).find((s) => s.name.value === 'voiceActors') : null;
          if (vaField) vaArgs = collectArgs(vaField, variables);
        }
        for (const [, e] of perChar) {
          let vas = vaByChar.get(`${e.anime_id}:${e.node.id}`) || [];
          if (vaArgs.language) vas = vas.filter((v) => v.language === vaArgs.language);
          e.vas = vas;
          e.node.media = { __typename: 'Media', id: e.anime_id };
        }
      }
      const grouped = new Map(ids.map((id) => [id, []]));
      for (const e of perChar.values()) if (grouped.has(e.anime_id)) grouped.get(e.anime_id).push(e);
      for (const m of items) {
        let list = grouped.get(m.id) || [];
        const cmp = charSortFn(args.sort || 'ROLE');
        list = [...list].sort((a, b) => cmp(a.node, b.node));
        const connArgs = name === 'characterPreview' ? { ...args, perPage: 8, page: 1 } : args;
        const { list: sliced, pageInfo } = sliceConn(list, connArgs);
        m[name] = {
          __typename: 'CharacterConnection',
          edges: sliced.map((e) => ({ __typename: 'CharacterEdge', id: e.edge_id, role: e.role, node: e.node, voiceActors: e.vas || [], voiceActorRoles: [] })),
          nodes: sliced.map((e) => e.node),
          pageInfo,
        };
      }
    } else if (name === 'staff' || name === 'staffPreview') {
      const grouped = new Map(ids.map((id) => [id, []]));
      for (const r of results.get('staff') || []) {
        if (!grouped.has(r.anime_id)) continue;
        grouped.get(r.anime_id).push({ edge_id: r.edge_id, role: r.role, sort_order: r.sort_order ?? 0, node: staffFromRow(r) });
      }
      for (const m of items) {
        let list = (grouped.get(m.id) || []).sort((a, b) => a.sort_order - b.sort_order);
        if ((args.sort || '').startsWith('FAVOURITES')) list = [...list].sort((a, b) => (b.node.favourites ?? 0) - (a.node.favourites ?? 0));
        const connArgs = name === 'staffPreview' ? { ...args, perPage: 8, page: 1 } : args;
        const { list: sliced, pageInfo } = sliceConn(list, connArgs);
        m[name] = {
          __typename: 'StaffConnection',
          edges: sliced.map((e) => ({ __typename: 'StaffEdge', id: e.edge_id, role: e.role, favouriteOrder: null, node: e.node })),
          nodes: sliced.map((e) => e.node),
          pageInfo,
        };
      }
    } else if (name === 'recommendations') {
      const recRows = results.get('recommendations') || [];
      const sort = args.sort || 'RATING_DESC';
      for (const m of items) {
        let rows = recRows.filter((r) => r.anime_id === m.id);
        if (sort === 'RATING') rows = [...rows].reverse();
        else if (sort === 'ID') rows = [...rows].sort((a, b) => a.id - b.id);
        else if (sort === 'ID_DESC') rows = [...rows].sort((a, b) => b.id - a.id);
        const { list: sliced, pageInfo } = sliceConn(rows, { perPage: 25, ...args });
        m.recommendations = {
          __typename: 'RecommendationConnection',
          edges: sliced.map((r) => ({
            __typename: 'RecommendationEdge', id: r.id, rating: r.rating, userRating: r.user_rating || null,
            node: {
              __typename: 'Recommendation', id: r.id, rating: r.rating, userRating: r.user_rating || null,
              media: { __typename: 'Media', id: r.anime_id },
              mediaRecommendation: null, // patched after nested fetch
            },
          })),
          nodes: [],
          pageInfo,
        };
        m._recIds = sliced.map((r) => ({ recId: r.id, target: r.recommended_anime_id }));
      }
      // fetch recommended media with the client's mediaRecommendation selection
      const recSel = mediaSels.find((s) => s.name.value === 'recommendations');
      let recNodeSel = null;
      if (recSel?.selectionSet) {
        const nodeEdge = collectSelections(recSel, fragments).find((s) => s.name.value === 'edges');
        const nodeField = nodeEdge ? collectSelections(nodeEdge, fragments).find((s) => s.name.value === 'node') : null;
        const direct = collectSelections(recSel, fragments).find((s) => s.name.value === 'nodes');
        const src = nodeField || direct;
        if (src) {
          const mRec = collectSelections(src, fragments).find((s) => s.name.value === 'mediaRecommendation');
          recNodeSel = mRec || src; // fall back to whole-node projection
        }
      }
      const targets = items.flatMap((m) => (m._recIds || []).map((x) => x.target));
      const mediaMap = targets.length ? await fetchNestedMedia(targets, recNodeSel, fragments) : new Map();
      for (const m of items) {
        if (!m.recommendations) continue;
        const byId = new Map((m._recIds || []).map((x) => [x.recId, x.target]));
        for (const edge of m.recommendations.edges) {
          edge.node.mediaRecommendation = mediaMap.get(byId.get(edge.id)) || null;
        }
        m.recommendations.nodes = m.recommendations.edges.map((e) => e.node);
        delete m._recIds;
      }
    } else if (name === 'rankings') {
      const tmp = new Map(ids.map((id) => [id, []]));
      for (const r of results.get('rankings') || []) {
        if (!tmp.has(r.anime_id)) continue;
        tmp.get(r.anime_id).push({
          __typename: 'MediaRank', id: r.rank_id, rank: r.rank, type: r.type, format: r.format,
          year: r.year, season: r.season, allTime: !!r.all_time, context: r.context,
        });
      }
      for (const m of items) m.rankings = tmp.get(m.id) || [];
    } else if (name === 'externalLinks') {
      const tmp = new Map(ids.map((id) => [id, []]));
      for (const r of results.get('externalLinks') || []) {
        if (!tmp.has(r.anime_id)) continue;
        tmp.get(r.anime_id).push({
          __typename: 'MediaExternalLink', id: r.id, url: r.url, site: r.site, type: r.type,
          language: r.language, color: r.color, icon: r.icon, notes: null, isDisabled: false,
        });
      }
      for (const m of items) m.externalLinks = tmp.get(m.id) || [];
    } else if (name === 'streamingEpisodes') {
      const tmp = new Map(ids.map((id) => [id, []]));
      for (const r of results.get('streamingEpisodes') || []) {
        if (!tmp.has(r.anime_id)) continue;
        tmp.get(r.anime_id).push({ __typename: 'MediaStreamingEpisode', title: r.title, thumbnail: r.thumbnail, url: r.url, site: r.site });
      }
      for (const m of items) m.streamingEpisodes = tmp.get(m.id) || [];
    } else if (name === 'stats') {
      const tmp = new Map(ids.map((id) => [id, null]));
      for (const r of results.get('stats') || []) {
        if (!tmp.has(r.anime_id)) continue;
        let scoreDistribution = [], statusDistribution = [];
        try { scoreDistribution = JSON.parse(r.score_distribution || '[]') || []; } catch { /* keep [] */ }
        try { statusDistribution = JSON.parse(r.rankings || '[]') || []; } catch { /* keep [] */ }
        tmp.set(r.anime_id, {
          __typename: 'MediaStats',
          scoreDistribution: scoreDistribution.map((s) => ({ __typename: 'ScoreDistribution', score: s.score, amount: s.amount })),
          statusDistribution: statusDistribution.map((s) => ({ __typename: 'StatusDistribution', status: s.status, amount: s.amount })),
        });
      }
      for (const m of items) m.stats = tmp.get(m.id) || null;
    } else if (name === 'airingSchedule') {
      const grouped = new Map(ids.map((id) => [id, []]));
      for (const r of results.get('airingSchedule') || []) {
        if (!grouped.has(r.anime_id)) continue;
        grouped.get(r.anime_id).push({
          __typename: 'AiringSchedule', id: r.id, episode: r.episode, airingAt: r.airing_at,
          timeUntilAiring: Math.max(0, (r.airing_at || 0) - now), mediaId: r.anime_id,
        });
      }
      for (const m of items) {
        let list = grouped.get(m.id) || [];
        if ((args.sort || '').includes('TIME_UNTIL')) list = [...list].sort((a, b) => a.airingAt - b.airingAt);
        const { list: sliced, pageInfo } = sliceConn(list, { perPage: 50, ...args });
        m.airingSchedule = {
          __typename: 'AiringScheduleConnection',
          edges: sliced.map((n) => ({ __typename: 'AiringScheduleEdge', node: n, media: { __typename: 'Media', id: m.id } })),
          nodes: sliced,
          pageInfo,
        };
      }
    } else if (name === 'reviews') {
      const grouped = new Map(ids.map((id) => [id, []]));
      for (const r of results.get('reviews') || []) {
        if (!grouped.has(r.anime_id)) continue;
        grouped.get(r.anime_id).push({
          __typename: 'Review', id: r.id, userId: r.user_id, mediaId: r.anime_id,
          summary: r.summary, body: r.body, rating: r.rating, ratingAmount: r.rating,
          userRating: r.user_rating || null, score: r.score, private: false, siteUrl: r.site_url,
          createdAt: r.created_at ? Math.floor(new Date(r.created_at).getTime() / 1000) : null,
          user: { __typename: 'User', id: r.user_id, name: r.user_name },
        });
      }
      for (const m of items) {
        const list = grouped.get(m.id) || [];
        const { list: sliced, pageInfo } = sliceConn(list, { perPage: 10, ...args });
        m.reviews = { __typename: 'ReviewConnection', edges: sliced.map((n) => ({ __typename: 'ReviewEdge', node: n })), nodes: sliced, pageInfo };
      }
    } else if (name === 'trends') {
      const grouped = new Map(ids.map((id) => [id, []]));
      for (const r of results.get('trends') || []) {
        if (!grouped.has(r.anime_id)) continue;
        grouped.get(r.anime_id).push({
          __typename: 'MediaTrend', date: r.date, trending: r.trending, averageScore: r.average_score,
          popularity: r.popularity, episode: r.episode, releasing: !!r.releasing, mediaId: r.anime_id,
        });
      }
      for (const m of items) {
        const list = grouped.get(m.id) || [];
        const { list: sliced, pageInfo } = sliceConn(list, { perPage: 10, ...args });
        m.trends = { __typename: 'MediaTrendConnection', edges: sliced.map((n) => ({ __typename: 'MediaTrendEdge', node: n })), nodes: sliced, pageInfo };
      }
    }
  }
}

/* ----------------------------- page execution ----------------------------- */
async function runPageQuery(fargs, page, perPage, proj, mediaFieldNode, fragments, variables) {
  const c = tursoClient();
  if (!c) return null;
  const { where, args } = tursoWhere(fargs);
  const clause = where.join(' AND ');
  const filterArgs = [...args]; // WHERE-only args for the COUNT probe
  let order = tursoOrderClause(fargs.sort);
  if (fargs.search && !fargs.sort) {
    const q = String(fargs.search).toLowerCase().trim();
    order = `CASE WHEN lower(coalesce(a.title_romaji,'')) = ? OR lower(coalesce(a.title_english,'')) = ? OR lower(coalesce(a.title_native,'')) = ? THEN 0 ELSE 1 END, ${order}`;
    args.push(q, q, q); // ORDER-only args: must NOT leak into the COUNT query
  }
  const projKey = proj ? `cols:${proj.cols.length}` : 'raw';
  const childrenKey = proj ? [...proj.children].sort().join(',') : '';
  const pKey = pageCacheKey(clause, args, order, page, perPage, projKey, childrenKey);
  const hit = cacheGet(pageCache, pKey, PAGE_TTL_MS);
  if (hit) return hit;
  const cKey = countCacheKey(clause, filterArgs);
  const cachedTotal = cacheGet(countCache, cKey, COUNT_TTL_MS);
  const dataSql = proj
    ? `SELECT ${proj.cols.join(', ')} FROM anime a WHERE ${clause} ORDER BY ${order} LIMIT ? OFFSET ?`
    : `SELECT a.raw_json AS raw_json FROM anime a WHERE ${clause} ORDER BY ${order} LIMIT ? OFFSET ?`;
  const dataArgs = [...args, perPage, (page - 1) * perPage];
  // Round trip 1: COUNT + page rows together.
  const stmts = [];
  const stmtKeys = [];
  if (cachedTotal === undefined) { stmts.push({ sql: `SELECT COUNT(*) AS n FROM anime a WHERE ${clause}`, args: filterArgs }); stmtKeys.push('count'); }
  stmts.push({ sql: dataSql, args: dataArgs }); stmtKeys.push('data');
  let rss;
  try {
    rss = await batchExecute(stmts);
  } catch (e) {
    // FTS table missing on old DBs → retry once with LIKE instead of MATCH
    if (fargs.search && !hasCJK(fargs.search) && clause.includes('anime_fts')) {
      const fb = tursoWhere({ ...fargs, search: undefined });
      searchWhereLikeOnly(fargs.search, fb.where, fb.args);
      return runPageQueryLike({ ...fargs }, page, perPage, proj, mediaFieldNode, fragments, variables, fb);
    }
    throw e;
  }
  let total = cachedTotal;
  const rowsets = {};
  for (let i = 0; i < stmtKeys.length; i++) {
    if (stmtKeys[i] === 'count') total = Number(rss[i].rows[0]?.n || 0);
    else rowsets.data = rss[i].rows;
  }
  if (total === 0) {
    const empty = { total: 0, items: [] };
    cacheSet(pageCache, pKey, empty);
    return empty;
  }
  let items;
  if (proj) items = rowsets.data.map(rowToMedia);
  else {
    items = [];
    for (const row of rowsets.data) {
      try { if (row.raw_json) items.push(asExactMedia(JSON.parse(row.raw_json))); } catch { /* skip bad row */ }
    }
  }
  // Round trip 2: nested children in one batch (and up to one more for nested media).
  const children = proj ? proj.children : ALL_CHILDREN;
  if (children.length && mediaFieldNode) {
    try {
      await assembleChildren(items, children, mediaFieldNode, fragments, variables);
    } catch (e) { dbg('assembleChildren', e); /* children stay as raw_json provided them (raw path) or absent */ }
  }
  const out = { total, items };
  cacheSet(pageCache, pKey, out);
  return out;
}
const ALL_CHILDREN = ['genres', 'tags', 'studios', 'relations', 'characters', 'staff', 'recommendations', 'rankings', 'externalLinks', 'streamingEpisodes', 'stats', 'airingSchedule', 'reviews', 'trends', 'nextAiringEpisode'];
function searchWhereLikeOnly(search, where, args) {
  const q = String(search || '').toLowerCase().trim();
  if (!q || q.length < 2) { where.push('1 = 0'); return; }
  const tokens = searchTokensSql(q).slice(0, 5);
  if (!tokens.length) { where.push(`${HAY} LIKE ? ESCAPE '\\'`); args.push(`%${escLike(q)}%`); return; }
  for (const t of tokens) { where.push(`${HAY} LIKE ? ESCAPE '\\'`); args.push(`%${escLike(t)}%`); }
}
async function runPageQueryLike(fargs, page, perPage, proj, mediaFieldNode, fragments, variables, fb) {
  // LIKE-forced variant (FTS unavailable). Same shape as runPageQuery.
  const clause = fb.where.join(' AND ');
  const filterArgs = [...fb.args];
  let order = tursoOrderClause(fargs.sort);
  if (fargs.search && !fargs.sort) {
    const q = String(fargs.search).toLowerCase().trim();
    order = `CASE WHEN lower(coalesce(a.title_romaji,'')) = ? OR lower(coalesce(a.title_english,'')) = ? OR lower(coalesce(a.title_native,'')) = ? THEN 0 ELSE 1 END, ${order}`;
    fb.args.push(q, q, q);
  }
  const projKey = proj ? `cols:${proj.cols.length}` : 'raw';
  const childrenKey = proj ? [...proj.children].sort().join(',') : '';
  const pKey = pageCacheKey(clause, fb.args, order, page, perPage, `${projKey}:like`, childrenKey);
  const hit = cacheGet(pageCache, pKey, PAGE_TTL_MS);
  if (hit) return hit;
  const dataSql = proj
    ? `SELECT ${proj.cols.join(', ')} FROM anime a WHERE ${clause} ORDER BY ${order} LIMIT ? OFFSET ?`
    : `SELECT a.raw_json AS raw_json FROM anime a WHERE ${clause} ORDER BY ${order} LIMIT ? OFFSET ?`;
  const stmts = [
    { sql: `SELECT COUNT(*) AS n FROM anime a WHERE ${clause}`, args: filterArgs },
    { sql: dataSql, args: [...fb.args, perPage, (page - 1) * perPage] },
  ];
  const rss = await batchExecute(stmts);
  const total = Number(rss[0].rows[0]?.n || 0);
  if (total === 0) { const empty = { total: 0, items: [] }; cacheSet(pageCache, pKey, empty); return empty; }
  let items;
  if (proj) items = rss[1].rows.map(rowToMedia);
  else {
    items = [];
    for (const row of rss[1].rows) {
      try { if (row.raw_json) items.push(asExactMedia(JSON.parse(row.raw_json))); } catch { /* skip */ }
    }
  }
  const children = proj ? proj.children : [];
  if (children.length && mediaFieldNode) {
    try { await assembleChildren(items, children, mediaFieldNode, fragments, variables); } catch { /* best effort */ }
  }
  const out = { total, items };
  cacheSet(pageCache, pKey, out);
  return out;
}

/* --------------------------- single Media fetch --------------------------- */
async function tursoMediaByArgs(args, mediaFieldNode, fragments, variables) {
  const c = tursoClient();
  if (!c) return null;
  const proj = planProjection(mediaFieldNode, fragments);
  const select = proj ? proj.cols.join(', ') : 'a.id AS id, a.raw_json AS raw_json';

  const finishBase = async (row) => {
    if (!row) return null;
    if (proj) return rowToMedia(row);
    // raw path with LRU (raw_json blobs are ~350KB — cache parses, not fetches)
    const key = `r${row.id}`;
    let m;
    const cch = rawCache.get(key);
    if (cch && Date.now() - cch.time < RAW_TTL_MS) m = cch.data;
    else {
      m = asExactMedia(JSON.parse(row.raw_json));
      rawCache.set(key, { data: m, time: Date.now() });
      if (rawCache.size > MAX_CACHE_ENTRIES) rawCache.delete(rawCache.keys().next().value);
    }
    return m;
  };

  let row = null;
  if (args.id != null) {
    const rs = await c.execute({ sql: `SELECT ${select} FROM anime a WHERE a.id = ?`, args: [args.id] });
    row = rs.rows[0] || null;
  } else if (args.idMal != null) {
    const rs = await c.execute({ sql: `SELECT id FROM anime WHERE id_mal = ?`, args: [args.idMal] });
    const id = rs.rows[0]?.id;
    if (id == null) return null;
    const rs2 = await c.execute({ sql: `SELECT ${select} FROM anime a WHERE a.id = ?`, args: [id] });
    row = rs2.rows[0] || null;
  } else if (args.search) {
    const where = [`COALESCE(a.type,'ANIME') = 'ANIME'`];
    const wargs = [];
    searchWhere(args.search, where, wargs);
    const q = String(args.search).toLowerCase().trim();
    const order = `CASE WHEN lower(coalesce(a.title_romaji,'')) = ? OR lower(coalesce(a.title_english,'')) = ? OR lower(coalesce(a.title_native,'')) = ? THEN 0 ELSE 1 END, popularity DESC NULLS LAST`;
    const sql = `SELECT ${select} FROM anime a WHERE ${where.join(' AND ')} ORDER BY ${order} LIMIT 1`;
    try {
      const rs = await c.execute({ sql, args: [...wargs, q, q, q] });
      row = rs.rows[0] || null;
    } catch (e) {
      // FTS unavailable on this DB → LIKE retry
      if (hasCJK(args.search)) throw e;
      const fb = [`COALESCE(a.type,'ANIME') = 'ANIME'`];
      const fbArgs = [];
      searchWhereLikeOnly(args.search, fb, fbArgs);
      const rs = await c.execute({ sql: `SELECT ${select} FROM anime a WHERE ${fb.join(' AND ')} ORDER BY ${order} LIMIT 1`, args: [...fbArgs, q, q, q] });
      row = rs.rows[0] || null;
    }
  } else return null;

  let media = await finishBase(row);
  if (media && mediaFieldNode) {
    // assembler overrides raw fetch-time children and honors perPage/sort
    const children = proj ? proj.children : assemblerChildrenFor(mediaFieldNode, fragments);
    if (children.length) {
      try { await assembleChildren([media], children, mediaFieldNode, fragments, variables); } catch (e) { dbg('mediaAssemble', e); /* raw content stands */ }
    }
  }
  if (media && args.type && args.type !== 'ANIME') return null;
  return media;
}
function finalizeMedia(m, args) {
  if (args.type && args.type !== 'ANIME') return null;
  return m;
}
function assemblerChildrenFor(mediaFieldNode, fragments) {
  const sels = collectSelections(mediaFieldNode, fragments);
  const out = new Set();
  for (const s of sels) {
    if (s.name.value === 'nextAiringEpisode' || ASSEMBLER_FIELDS.has(s.name.value)) {
      if (s.selectionSet || s.name.value === 'genres') out.add(s.name.value);
    }
  }
  return [...out];
}

async function resolvePage(fieldNode, fragments, variables, pageArgs) {
  const mediaFieldNode = fieldNode.selectionSet?.selections?.find(
    (s) => s.kind === Kind.FIELD && s.name.value === 'media');
  const mediaArgs = mediaFieldNode ? collectArgs(mediaFieldNode, variables) : {};
  const fargs = { ...mediaArgs, ...pageArgs };
  // AniList allows args on Page or on media — merge both
  const page = fargs.page || 1;
  const perPage = Math.min(fargs.perPage || 25, 50);

  // Fast path: Turso is the primary DB (indexed SQL). Only when the remote
  // is fully bootstrapped (tursoReady) do we serve from it; partial data
  // never serves. With TURSO_REQUIRED=1 there is no shard fallback.
  const required = tursoRequired();
  if (await tursoReady().catch(() => false)) {
    try {
      let proj;
      try { proj = planProjection(mediaFieldNode, fragments); } catch { proj = null; }
      const t = await runPageQuery(fargs, page, perPage, proj, mediaFieldNode, fragments, variables);
      if (t) return pageOut(fieldNode, fragments, t.items, t.total, page, perPage);
    } catch (e) {
      dbg('pageQuery', e);
      if (required) throw tursoUnavailable(`Turso Page query failed: ${e.message || e}`);
      /* shard fallback below */
    }
  } else if (required) {
    throw tursoUnavailable('Turso not ready (bootstrap flag missing) and TURSO_REQUIRED=1');
  }

  // Shard fallback (pre-bootstrap or Turso errors). The 12MB search index
  // loads here for the first time — never on the Turso hot path.
  const index = await getSearchIndex();
  const needFull = fargs.sort || fargs.tagCategory || fargs.tagCategory_in || fargs.tagCategory_not_in
    || fargs.minimumTagRank !== undefined || fargs.isLicensed !== undefined
    || fargs.startDate || fargs.endDate;
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

function pageOut(fieldNode, fragments, paged, total, page, perPage) {
  const out = {};
  for (const s of fieldNode.selectionSet?.selections || []) {
    if (s.kind !== Kind.FIELD) continue;
    const k = s.alias?.value || s.name.value;
    if (s.name.value === 'media') {
      out[k] = paged.map((item) => pick(item, collectSelections(s, fragments), fragments));
    } else if (s.name.value === 'pageInfo') {
      out[k] = pick(buildPageInfo(total, page, perPage), collectSelections(s, fragments), fragments);
    } else if (s.name.value === '__typename') {
      out[k] = 'Page';
    } else out[k] = null;
  }
  return out;
}

/* ------------- indexed entity lookups (Character/Staff/Studio) ------------ */
async function tursoCharacter(args) {
  const c = tursoClient();
  if (!c) return null;
  let rs;
  if (args.id) rs = await c.execute({ sql: `SELECT * FROM characters WHERE id = ?`, args: [args.id] });
  else if (args.search) {
    const expr = ftsExpr(args.search);
    if (expr && !hasCJK(args.search)) {
      try {
        rs = await c.execute({ sql: `SELECT c.* FROM characters c JOIN characters_fts f ON f.rowid = c.id WHERE characters_fts MATCH ? ORDER BY c.favourites DESC NULLS LAST LIMIT 1`, args: [expr] });
      } catch { rs = null; }
    }
    if (!rs || !rs.rows.length) {
      rs = await c.execute({ sql: `SELECT * FROM characters WHERE lower(name_full) LIKE ? ESCAPE '\\' ORDER BY favourites DESC NULLS LAST LIMIT 1`, args: [`%${escLike(args.search)}%`] });
    }
  } else return null;
  const r = rs.rows[0];
  if (!r) return null;
  return charFromRow(r);
}
async function tursoStaff(args) {
  const c = tursoClient();
  if (!c) return null;
  let rs;
  if (args.id) rs = await c.execute({ sql: `SELECT * FROM staff WHERE id = ?`, args: [args.id] });
  else if (args.search) {
    const expr = ftsExpr(args.search);
    if (expr && !hasCJK(args.search)) {
      try {
        rs = await c.execute({ sql: `SELECT s.* FROM staff s JOIN staff_fts f ON f.rowid = s.id WHERE staff_fts MATCH ? ORDER BY s.favourites DESC NULLS LAST LIMIT 1`, args: [expr] });
      } catch { rs = null; }
    }
    if (!rs || !rs.rows.length) {
      rs = await c.execute({ sql: `SELECT * FROM staff WHERE lower(name_full) LIKE ? ESCAPE '\\' ORDER BY favourites DESC NULLS LAST LIMIT 1`, args: [`%${escLike(args.search)}%`] });
    }
  } else return null;
  const r = rs.rows[0];
  if (!r) return null;
  return staffFromRow(r);
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
async function tursoHealth() {
  // Public health signal: which DB host is configured (never the token),
  // whether it answers, whether the completion flag is set, and whether the
  // child tables actually hold data (drift detection).
  const out = { configured: false, host: null, reachable: false, flagged: false, remoteAnime: null };
  try {
    const raw = (typeof process !== 'undefined' && process.env?.TURSO_URL) || '';
    if (!raw) return out;
    out.configured = true;
    let host = String(raw).trim();
    for (const p of ['libsql://', 'https://', 'http://', 'wss://', 'ws://']) {
      if (host.startsWith(p)) { host = host.slice(p.length); break; }
    }
    out.host = host.split('?')[0].split('/')[0] || null;
    const c = tursoClient();
    if (!c) return out;
    const withTimeout = (p, ms) => Promise.race([
      p, new Promise((_, rej) => setTimeout(() => rej(new Error('health timeout')), ms))]);
    const rs = await withTimeout(c.batch([
      { sql: `SELECT (SELECT COUNT(*) FROM anime) AS n`, args: [] },
      { sql: `SELECT (SELECT COUNT(*) FROM anime_genres) AS g, (SELECT COUNT(*) FROM relations) AS r, (SELECT COUNT(*) FROM recommendations) AS rec, (SELECT value FROM sync_state WHERE key='bootstrap_complete') AS f`, args: [] },
    ], 'read'), 8000);
    out.reachable = true;
    out.remoteAnime = Number(rs[0].rows[0]?.n ?? -1);
    out.childCounts = {
      animeGenres: Number(rs[1].rows[0]?.g ?? -1),
      relations: Number(rs[1].rows[0]?.r ?? -1),
      recommendations: Number(rs[1].rows[0]?.rec ?? -1),
    };
    out.flagged = rs[1].rows[0]?.f === '1';
    out.ok = out.flagged && out.remoteAnime > 1000 && out.childCounts.animeGenres > 0 && out.childCounts.relations > 0;
  } catch { /* stays unreachable */ }
  return out;
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
let tursoReadyCache = null;
async function tursoReady() {
  // Partial remote must NEVER serve as complete: the sync writes
  // sync_state.bootstrap_complete only at the very end.
  if (tursoReadyCache && Date.now() - tursoReadyCache.time < CACHE_TTL_MS) return tursoReadyCache.ok;
  let ok = false;
  try {
    const c = tursoClient();
    if (c) {
      const rs = await c.execute({ sql: `SELECT value FROM sync_state WHERE key = 'bootstrap_complete'`, args: [] });
      ok = rs.rows[0]?.value === '1';
    }
  } catch { ok = false; }
  tursoReadyCache = { ok, time: Date.now() };
  return ok;
}
async function tursoTry(fn) {
  try {
    if (!(await tursoReady())) return null;
    return await fn();
  } catch { return null; }
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
        if (await tursoReady().catch(() => false)) {
          try {
            const t = await tursoMediaByArgs(args, fieldNode, fragments, variables);
            if (t) return pick(t, sels, fragments);
            if ((args.id != null || args.idMal != null) && await tursoHasRows('anime')) {
              throw Object.assign(new Error(`Media not found: ${args.id ?? args.idMal}`), { status: 404 });
            }
          } catch (e) {
            dbg('mediaRoot', e);
            if (e?.status === 404) throw e;
            if (tursoRequired()) throw tursoUnavailable(`Turso Media query failed: ${e.message || e}`);
            /* shard fallback below */
          }
        } else if (tursoRequired()) {
          throw tursoUnavailable('Turso not ready (bootstrap flag missing) and TURSO_REQUIRED=1');
        }
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
        const cached = entityGet(fname, args);
        if (cached !== undefined) return pick(cached, sels, fragments);
        if (await tursoReady().catch(() => false)) {
          try {
            const t = table === 'characters' ? await tursoCharacter(args)
              : table === 'staff' ? await tursoStaff(args) : await tursoStudio(args);
            if (t) { entitySet(fname, args, t); return pick(t, sels, fragments); }
            // Authoritative miss only when the remote table holds data.
            if ((args.id || args.search) && await tursoHasRows(table)) {
              throw Object.assign(new Error(`${fname} not found`), { status: 404 });
            }
          } catch (e) {
            if (e?.status === 404) throw e;
            if (tursoRequired()) throw tursoUnavailable(`Turso ${fname} query failed: ${e.message || e}`);
            // fall through to shard scan below
          }
        } else if (tursoRequired()) {
          throw tursoUnavailable('Turso not ready (bootstrap flag missing) and TURSO_REQUIRED=1');
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
        const cached = entityGet('airing', args);
        if (cached !== undefined) return pick(cached, sels, fragments);
        if (await tursoReady().catch(() => false)) {
          try {
            const t = await tursoAiring(args);
            if (t) { entitySet('airing', args, t); return pick(t, sels, fragments); }
          } catch (e) {
            if (tursoRequired()) throw tursoUnavailable(`Turso AiringSchedule query failed: ${e.message || e}`);
            /* shard fallback below */
          }
        } else if (tursoRequired()) {
          throw tursoUnavailable('Turso not ready (bootstrap flag missing) and TURSO_REQUIRED=1');
        }
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
              if (hit) { const o = { __typename: 'AiringSchedule', ...hit }; entitySet('airing', args, o); return pick(o, sels, fragments); }
              if (args.mediaId && a.nextAiringEpisode) {
                const o = { __typename: 'AiringSchedule', ...a.nextAiringEpisode };
                entitySet('airing', args, o);
                return pick(o, sels, fragments);
              }
            }
          }
        }
        throw Object.assign(new Error('AiringSchedule not found'), { status: 404 });
      }
      case 'GenreCollection': {
        // Zero-read path: bundled at deploy time (lazy: first fallback use
        // only — never parsed on the Turso hot path). Falls back to fetched.
        try {
          const bundledGenres = (await loadBundled('meta'))?.genres;
          if (bundledGenres?.length) return bundledGenres;
        } catch { /* fetched fallback below */ }
        return (await getMetadata().catch(() => null))?.genres || [];
      }
      case 'MediaTagCollection': {
        if (await tursoReady().catch(() => false)) {
          try {
            const t = await tursoTags();
            if (t?.length) {
              if (!sels.length) return t;
              return t.map((x) => pick(x, sels, fragments));
            }
          } catch (e) {
            if (tursoRequired()) throw tursoUnavailable(`Turso tags query failed: ${e.message || e}`);
            /* shard fallback below */
          }
        } else if (tursoRequired()) {
          throw tursoUnavailable('Turso not ready (bootstrap flag missing) and TURSO_REQUIRED=1');
        }
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
  const fields = op.selectionSet.selections.filter((f) => f.kind === Kind.FIELD);
  // resolve root fields concurrently (independent data sources)
  const settled = await Promise.all(fields.map(async (field) => {
    try {
      return [field, await resolveNode('RootQuery', field, fragments, variables), null];
    } catch (err) {
      return [field, null, err];
    }
  }));
  for (const [field, result, err] of settled) {
    data[field.alias?.value || field.name.value] = result;
    if (err) errors.push({ message: err.message, status: err.status || 500, path: [field.name.value] });
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
    // Ops probe: ?health=1 reports Turso reachability + row counts (one
    // cheap batched read). Never exposes the auth token.
    if (parsed.searchParams.get('health') === '1') {
      const h = await tursoHealth().catch(() => ({
        configured: false, host: null, reachable: false, flagged: false, remoteAnime: null,
      }));
      return nodeJson(res, {
        ok: !!(h.configured && h.reachable && h.flagged && (h.ok ?? true)),
        tursoRequired: tursoRequired(),
        ...h,
      }, 200);
    }
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
