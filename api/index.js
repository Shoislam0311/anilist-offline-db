import { parse, Kind } from 'graphql';
import { gunzipSync } from 'node:zlib';

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
const MAX_SHARD_CACHE = 25;

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

async function getMetadata() {
  if (!fresh(metaEntry)) {
    metaEntry = { data: await fetchJSON(`${DATA_BASE}/metadata.json`), time: Date.now() };
  }
  return metaEntry.data;
}
async function getSearchIndex() {
  if (!fresh(indexEntry)) {
    indexEntry = { data: await fetchJSON(`${DATA_BASE}/search_index.json`), time: Date.now() };
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
  const byShard = new Map();
  for (const id of ids) {
    const idx = shardIdxForId(shardStartIds, id);
    for (const tryIdx of [idx, idx - 1, idx + 1]) {
      if (tryIdx < 0 || tryIdx >= shardStartIds.length) continue;
      if (!byShard.has(tryIdx)) byShard.set(tryIdx, []);
      if (!byShard.get(tryIdx).includes(id)) byShard.get(tryIdx).push(id);
    }
  }
  const found = new Map();
  const results = [];
  for (const [shardIdx, shardIds] of byShard) {
    const shard = await getShard(shardIdx).catch(() => []);
    for (const id of shardIds) {
      if (found.has(id)) continue;
      const item = shard.find((a) => a.id === id);
      if (item) { found.set(id, true); results.push(asExactMedia(item)); }
    }
  }
  // preserve requested order
  const byId = new Map(results.map((r) => [r.id, r]));
  return ids.map((id) => byId.get(id)).filter(Boolean);
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

async function resolvePage(fieldNode, fragments, variables, pageArgs) {
  const index = await getSearchIndex();
  const mediaFieldNode = fieldNode.selectionSet?.selections?.find(
    (s) => s.kind === Kind.FIELD && s.name.value === 'media');
  const mediaArgs = mediaFieldNode ? collectArgs(mediaFieldNode, variables) : {};
  const fargs = { ...mediaArgs, ...pageArgs };
  // AniList allows args on Page or on media — merge both
  const page = fargs.page || 1;
  const perPage = Math.min(fargs.perPage || 25, 50);

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

async function scanNested(kind, args) {
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
        const kind = fieldNode.name.value === 'Character' ? 'characters'
          : fieldNode.name.value === 'Staff' ? 'staff' : 'studios';
        const list = await scanNested(kind, args);
        if (args.id) {
          const one = list.find((x) => x.id === args.id);
          if (!one) throw Object.assign(new Error(`${fieldNode.name.value} not found: ${args.id}`), { status: 404 });
          return pick(one, sels, fragments);
        }
        return pick(list[0] || null, sels, fragments);
      }
      case 'AiringSchedule': {
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
      case 'GenreCollection':
        return (await getMetadata().catch(() => null))?.genres || [];
      case 'MediaTagCollection': {
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
function nodeJson(res, data, status = 200) {
  res.writeHead(status, { 'Content-Type': 'application/json', ...CORS });
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
    return nodeJson(res, await execute(query, variables, parsed.searchParams.get('operationName')), 200);
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
