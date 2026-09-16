/**
 * AniList Offline GraphQL API — drop-in mirror of https://graphql.anilist.co
 *
 * POST /api/graphql (also POST / and POST /graphql via rewrites)
 * Body: { "query": "...", "variables": {...}, "operationName": "..." }
 * GET  /api/graphql?query=...&variables=... (same as AniList)
 *
 * Data is read from the static shards published on GitHub Pages, so the
 * database updates automatically without redeploying this function.
 */
import { parse, Kind } from 'graphql';

const DATA_BASE = 'https://shoislam0311.github.io/anilist-offline-db/api';
const CACHE_TTL_MS = 10 * 60 * 1000;
const MAX_SHARD_CACHE = 15;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Max-Age': '86400',
};

function json(data, status = 200) {
  return Response.json(data, { status, headers: CORS });
}

/* ------------------------------- data layer ------------------------------ */

let metaEntry = null; // { data, time }
let indexEntry = null; // { data, time }
const shardCache = new Map(); // idx -> { data, time }

async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`upstream ${r.status} for ${url}`);
  return r.json();
}

function fresh(entry) {
  return entry && Date.now() - entry.time < CACHE_TTL_MS;
}

async function getMetadata() {
  if (!fresh(metaEntry)) {
    metaEntry = { data: await fetchJSON(`${DATA_BASE}/metadata.json`), time: Date.now() };
  }
  return metaEntry.data;
}

async function getIndex() {
  if (!fresh(indexEntry)) {
    indexEntry = { data: await fetchJSON(`${DATA_BASE}/search_index.json`), time: Date.now() };
  }
  return indexEntry.data;
}

async function getShard(idx) {
  const hit = shardCache.get(idx);
  if (hit && Date.now() - hit.time < CACHE_TTL_MS) {
    // refresh LRU position
    shardCache.delete(idx);
    shardCache.set(idx, hit);
    return hit.data;
  }
  const key = `shard_${String(idx).padStart(4, '0')}`;
  const data = await fetchJSON(`${DATA_BASE}/shards/${key}.json`);
  shardCache.set(idx, { data, time: Date.now() });
  while (shardCache.size > MAX_SHARD_CACHE) {
    shardCache.delete(shardCache.keys().next().value);
  }
  return data;
}

/** Binary-search shardStartIds to find which shard holds an anime id. */
function shardForId(id, meta) {
  const starts = meta.shardStartIds;
  if (!Array.isArray(starts) || !starts.length) {
    return Math.floor(id / (meta.shardSize || 200));
  }
  let lo = 0;
  let hi = starts.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (starts[mid] <= id) lo = mid;
    else hi = mid - 1;
  }
  return lo;
}

/** Fetch full anime objects for a list of ids (grouped by shard, in parallel). */
async function getFullByIds(ids) {
  const meta = await getMetadata();
  const byShard = new Map();
  for (const id of ids) {
    const s = shardForId(id, meta);
    if (!byShard.has(s)) byShard.set(s, []);
    byShard.get(s).push(id);
  }
  const out = new Map();
  await Promise.all(
    [...byShard.entries()].map(async ([shardIdx, wanted]) => {
      const shard = await getShard(shardIdx);
      const want = new Set(wanted);
      for (const a of shard) {
        if (want.has(a.id)) out.set(a.id, a);
      }
    }),
  );
  return out;
}

async function getFullById(id) {
  const m = await getFullByIds([id]);
  return m.get(id) || null;
}

/* ------------------------------ AST helpers ------------------------------ */

function valueFromAST(node, vars) {
  switch (node.kind) {
    case Kind.VARIABLE:
      return vars ? vars[node.name.value] : undefined;
    case Kind.INT:
      return parseInt(node.value, 10);
    case Kind.FLOAT:
      return parseFloat(node.value);
    case Kind.STRING:
    case Kind.BOOLEAN:
      return node.value;
    case Kind.ENUM:
      return node.value;
    case Kind.NULL:
      return null;
    case Kind.LIST:
      return node.values.map((v) => valueFromAST(v, vars));
    case Kind.OBJECT: {
      const o = {};
      for (const f of node.fields) o[f.name.value] = valueFromAST(f.value, vars);
      return o;
    }
    default:
      return undefined;
  }
}

function argsOf(fieldNode, vars) {
  const a = {};
  for (const arg of fieldNode.arguments || []) {
    a[arg.name.value] = valueFromAST(arg.value, vars);
  }
  return a;
}

/** Flatten a selection set, resolving fragment spreads + inline fragments. */
function collectSelections(node, fragments) {
  const out = [];
  const walk = (selSet) => {
    for (const sel of selSet?.selections || []) {
      if (sel.kind === Kind.FIELD) out.push(sel);
      else if (sel.kind === Kind.INLINE_FRAGMENT) walk(sel.selectionSet);
      else if (sel.kind === Kind.FRAGMENT_SPREAD) {
        const f = fragments[sel.name.value];
        if (f) walk(f.selectionSet);
      }
    }
  };
  walk(node.selectionSet);
  return out;
}

/* --------------------------- filtering + sorting -------------------------- */

function asArray(v) {
  if (v == null) return null;
  return Array.isArray(v) ? v : [v];
}

function applyFilters(rows, a) {
  let r = rows;
  if (a.search != null && a.search !== '') {
    const q = String(a.search).toLowerCase();
    r = r.filter(
      (x) =>
        (x.romaji || '').toLowerCase().includes(q) ||
        (x.english || '').toLowerCase().includes(q) ||
        (x.native || '').includes(q),
    );
  }
  if (a.id != null) r = r.filter((x) => x.id === a.id);
  const idIn = asArray(a.id_in);
  if (idIn) {
    const s = new Set(idIn);
    r = r.filter((x) => s.has(x.id));
  }
  if (a.genre != null) r = r.filter((x) => x.genres && x.genres.includes(a.genre));
  const genreIn = asArray(a.genre_in);
  if (genreIn) r = r.filter((x) => x.genres && x.genres.some((g) => genreIn.includes(g)));
  const fmt = asArray(a.format);
  if (fmt) r = r.filter((x) => fmt.includes(x.format));
  const fmtIn = asArray(a.format_in);
  if (fmtIn) r = r.filter((x) => fmtIn.includes(x.format));
  const st = asArray(a.status);
  if (st) r = r.filter((x) => st.includes(x.status));
  const stIn = asArray(a.status_in);
  if (stIn) r = r.filter((x) => stIn.includes(x.status));
  if (a.season != null) r = r.filter((x) => x.season === a.season);
  if (a.seasonYear != null) r = r.filter((x) => x.year === a.seasonYear);
  if (a.type != null && a.type !== 'ANIME') return [];
  return r;
}

function sortRows(rows, sort) {
  const list = (Array.isArray(sort) ? sort : [sort]).filter(Boolean);
  const key = list[0] || 'POPULARITY_DESC';
  const by = {
    POPULARITY_DESC: (a, b) => (b.popularity || 0) - (a.popularity || 0),
    POPULARITY: (a, b) => (a.popularity || 0) - (b.popularity || 0),
    SCORE_DESC: (a, b) => (b.score || 0) - (a.score || 0),
    SCORE: (a, b) => (a.score || 0) - (b.score || 0),
    ID_DESC: (a, b) => b.id - a.id,
    ID: (a, b) => a.id - b.id,
  }[key];
  // Unknown sorts (e.g. TRENDING_DESC, UPDATED_AT_DESC) need fields the
  // lightweight index doesn't carry — fall back to popularity ranking.
  return [...rows].sort(by || ((a, b) => (b.popularity || 0) - (a.popularity || 0)));
}

/* ------------------------------ field mapping ----------------------------- */

const FIELD_MAP = {
  titleRomaji: 'title_romaji',
  titleEnglish: 'title_english',
  titleNative: 'title_native',
  coverLarge: 'cover_large',
  bannerImage: 'banner_image',
  averageScore: 'average_score',
  meanScore: 'mean_score',
  seasonYear: 'season_year',
  nextAiringEpisode: 'next_airing_episode',
  nextAiringAt: 'next_airing_at',
  startDate: 'start_date',
  endDate: 'end_date',
  countryOfOrigin: 'country_of_origin',
  isAdult: 'is_adult',
  idMal: 'id_mal',
};

function camelToSnake(s) {
  return s.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);
}

function get(obj, key) {
  if (obj == null) return undefined;
  if (key in obj) return obj[key];
  if (key in FIELD_MAP && FIELD_MAP[key] in obj) return obj[FIELD_MAP[key]];
  const sn = camelToSnake(key);
  if (sn in obj) return obj[sn];
  return undefined;
}

function parseFuzzyDate(s) {
  if (!s || typeof s !== 'string' || s.length < 10) return { year: null, month: null, day: null };
  return {
    year: parseInt(s.substring(0, 4), 10),
    month: parseInt(s.substring(5, 7), 10),
    day: parseInt(s.substring(8, 10), 10),
  };
}

function pick(obj, subSels) {
  if (!subSels || !subSels.length) return obj;
  const out = {};
  for (const s of subSels) {
    const k = s.alias?.value || s.name.value;
    out[k] = obj != null && typeof obj === 'object' ? obj[s.name.value] : undefined;
  }
  return out;
}

function resolveCharacterNode(c, subSels) {
  const node = {
    id: c.id,
    name: { first: null, last: null, full: c.name || null, native: c.nameNative || null },
    image: { large: c.image || null, medium: c.image || null },
  };
  if (!subSels || !subSels.length) return node;
  const out = {};
  for (const s of subSels) {
    const k = s.alias?.value || s.name.value;
    if (s.name.value === 'name' && s.selectionSet) {
      out[k] = pick(node.name, collectSelections(s, {}));
    } else if (s.name.value === 'image' && s.selectionSet) {
      out[k] = pick(node.image, collectSelections(s, {}));
    } else {
      out[k] = node[s.name.value];
    }
  }
  return out;
}

function resolveMediaField(obj, fieldNode, fragments) {
  const name = fieldNode.name.value;
  const sub = fieldNode.selectionSet ? collectSelections(fieldNode, fragments) : null;

  switch (name) {
    case 'id':
      return obj.id;
    case 'title': {
      const t = {
        romaji: obj.title_romaji ?? obj.romaji ?? null,
        english: obj.title_english ?? obj.english ?? null,
        native: obj.title_native ?? obj.native ?? null,
      };
      t.userPreferred = t.romaji;
      if (!sub || !sub.length) return t;
      const out = {};
      for (const s of sub) out[s.alias?.value || s.name.value] = t[s.name.value] ?? null;
      return out;
    }
    case 'coverImage': {
      const c = {
        large: obj.cover_large ?? obj.cover ?? null,
        medium: obj.cover_large ?? obj.cover ?? null,
        extraLarge: obj.cover_large ?? obj.cover ?? null,
        color: obj.cover_color ?? null,
      };
      if (!sub || !sub.length) return c;
      const out = {};
      for (const s of sub) out[s.alias?.value || s.name.value] = c[s.name.value] ?? null;
      return out;
    }
    case 'startDate':
      return pick(parseFuzzyDate(obj.start_date), sub);
    case 'endDate':
      return pick(parseFuzzyDate(obj.end_date), sub);
    case 'nextAiringEpisode': {
      if (obj.next_airing_episode == null) return null;
      const n = { episode: obj.next_airing_episode, airingAt: obj.next_airing_at ?? null };
      return sub && sub.length ? pick(n, sub) : n;
    }
    case 'studios': {
      const buildEdges = () =>
        (obj.studios || []).map((s) => {
          const edgeSels = sub?.find((x) => x.name.value === 'edges');
          const edgeSub = edgeSels?.selectionSet ? collectSelections(edgeSels, fragments) : [];
          const nodeSel = edgeSub.find((x) => x.name.value === 'node');
          const nodeSub = nodeSel?.selectionSet ? collectSelections(nodeSel, fragments) : null;
          const node = nodeSub && nodeSub.length ? pick({ name: s.name }, nodeSub) : { name: s.name };
          if (!edgeSub.length) return { node, isMain: s.isMain ?? null };
          const edge = {};
          for (const x of edgeSub) {
            const k = x.alias?.value || x.name.value;
            if (x.name.value === 'node') edge[k] = node;
            else if (x.name.value === 'isMain') edge[k] = s.isMain ?? null;
            else edge[k] = null;
          }
          return edge;
        });
      if (!sub || !sub.length) return { edges: buildEdges() };
      const res = {};
      for (const s of sub) {
        const k = s.alias?.value || s.name.value;
        if (s.name.value === 'edges') res[k] = buildEdges();
        else if (s.name.value === 'nodes') {
          res[k] = (obj.studios || []).map((x) => ({ name: x.name }));
        } else res[k] = null;
      }
      return res;
    }
    case 'characters': {
      const edges = (obj.characters || []).map((c) => {
        const edgeSels = sub?.find((x) => x.name.value === 'edges');
        const edgeSub = edgeSels?.selectionSet ? collectSelections(edgeSels, fragments) : [];
        const nodeSel = edgeSub.find((x) => x.name.value === 'node');
        const nodeSub = nodeSel?.selectionSet ? collectSelections(nodeSel, fragments) : null;
        const node = resolveCharacterNode(c, nodeSub);
        if (!edgeSub.length) return { node, role: c.role ?? null };
        const edge = {};
        for (const x of edgeSub) {
          const k = x.alias?.value || x.name.value;
          if (x.name.value === 'node') edge[k] = node;
          else if (x.name.value === 'role') edge[k] = c.role ?? null;
          else edge[k] = null;
        }
        return edge;
      });
      if (!sub || !sub.length) return { edges };
      const res = {};
      for (const s of sub) {
        const k = s.alias?.value || s.name.value;
        if (s.name.value === 'edges') res[k] = edges;
        else res[k] = null;
      }
      return res;
    }
    case 'relations': {
      const edges = (obj.relations || []).map((r) => {
        const partial = { id: r.id, title: { romaji: r.title ?? null }, format: r.format ?? null, type: r.type ?? null };
        const edgeSels = sub?.find((x) => x.name.value === 'edges');
        const edgeSub = edgeSels?.selectionSet ? collectSelections(edgeSels, fragments) : [];
        const nodeSel = edgeSub.find((x) => x.name.value === 'node');
        const nodeSub = nodeSel?.selectionSet ? collectSelections(nodeSel, fragments) : null;
        const node = nodeSub && nodeSub.length ? resolveGeneric(partial, nodeSub, fragments) : partial;
        if (!edgeSub.length) return { node, relationType: r.relationType ?? null };
        const edge = {};
        for (const x of edgeSub) {
          const k = x.alias?.value || x.name.value;
          if (x.name.value === 'node') edge[k] = node;
          else if (x.name.value === 'relationType') edge[k] = r.relationType ?? null;
          else edge[k] = null;
        }
        return edge;
      });
      if (!sub || !sub.length) return { edges };
      const res = {};
      for (const s of sub) {
        const k = s.alias?.value || s.name.value;
        if (s.name.value === 'edges') res[k] = edges;
        else res[k] = null;
      }
      return res;
    }
    case 'recommendations': {
      const edges = (obj.recommendations || []).map((r) => ({
        node: {
          mediaRecommendation: { id: r.id, title: { romaji: r.title ?? null } },
          rating: r.rating ?? null,
        },
      }));
      if (!sub || !sub.length) return { edges };
      const res = {};
      for (const s of sub) {
        const k = s.alias?.value || s.name.value;
        if (s.name.value === 'edges') res[k] = edges;
        else res[k] = null;
      }
      return res;
    }
    case 'tags': {
      const tags = obj.tags || [];
      if (!sub || !sub.length) return tags;
      return tags.map((t) => {
        const o = {};
        for (const s of sub) o[s.alias?.value || s.name.value] = t[s.name.value] ?? null;
        return o;
      });
    }
    case 'airingSchedule': {
      const edges = (obj.airingSchedule || obj.airing_schedule || []).map((a) => ({
        node: { episode: a.episode ?? null, airingAt: a.airingAt ?? a.airing_at ?? null },
      }));
      if (!sub || !sub.length) return { edges };
      const res = {};
      for (const s of sub) {
        const k = s.alias?.value || s.name.value;
        if (s.name.value === 'edges') res[k] = edges;
        else res[k] = null;
      }
      return res;
    }
    case 'genres':
      return obj.genres ?? null;
    case 'externalLinks':
    case 'streamingEpisodes':
    case 'stats':
    case 'siteUrl':
      return null;
    default: {
      const v = get(obj, name);
      if (v == null) return null;
      if (Array.isArray(v)) {
        if (!sub || !sub.length) return v;
        return v.map((item) =>
          item != null && typeof item === 'object' ? resolveGeneric(item, sub, fragments) : item,
        );
      }
      if (typeof v === 'object') {
        if (!sub || !sub.length) return v;
        return resolveGeneric(v, sub, fragments);
      }
      return v;
    }
  }
}

function resolveGeneric(obj, subSels, fragments) {
  const out = {};
  for (const s of subSels) {
    const k = s.alias?.value || s.name.value;
    if (s.selectionSet) {
      const sub = collectSelections(s, fragments);
      const v = get(obj, s.name.value);
      if (v == null) out[k] = null;
      else if (Array.isArray(v)) {
        out[k] = v.map((item) =>
          item != null && typeof item === 'object' ? resolveGeneric(item, sub, fragments) : item,
        );
      } else if (typeof v === 'object') out[k] = resolveGeneric(v, sub, fragments);
      else out[k] = v;
    } else {
      out[k] = get(obj, s.name.value) ?? null;
    }
  }
  return out;
}

function resolveMedia(obj, fieldNodes, fragments) {
  const out = {};
  for (const f of fieldNodes) {
    const k = f.alias?.value || f.name.value;
    out[k] = resolveMediaField(obj, f, fragments);
  }
  return out;
}

/* ------------------------------- operations ------------------------------ */

async function handlePage(pageNode, vars, fragments) {
  const pageArgs = argsOf(pageNode, vars);
  const sels = collectSelections(pageNode, fragments);
  const mediaNode = sels.find((s) => s.name.value === 'media');
  const pageInfoNode = sels.find((s) => s.name.value === 'pageInfo');

  const mediaArgs = mediaNode ? argsOf(mediaNode, vars) : {};
  const merged = { ...pageArgs, ...mediaArgs };

  const page = Math.max(1, pageArgs.page || mediaArgs.page || 1);
  const perPage = Math.min(mediaArgs.perPage || pageArgs.perPage || 10, 50);

  let rows = applyFilters(await getIndex(), merged);
  rows = sortRows(rows, merged.sort);
  const total = rows.length;
  const slice = rows.slice((page - 1) * perPage, page * perPage);

  const full = await getFullByIds(slice.map((r) => r.id));
  const mediaSels = mediaNode ? collectSelections(mediaNode, fragments) : [];
  const media = slice.map((r) => {
    const obj = full.get(r.id);
    if (!obj) {
      // Fallback to the lightweight index row if the shard missed.
      return resolveMedia(
        {
          id: r.id,
          title_romaji: r.romaji,
          title_english: r.english,
          title_native: r.native,
          average_score: r.score,
          popularity: r.popularity,
          episodes: r.episodes,
          status: r.status,
          format: r.format,
          season: r.season,
          season_year: r.year,
          genres: r.genres,
          cover_large: r.cover,
        },
        mediaSels,
        fragments,
      );
    }
    return resolveMedia(obj, mediaSels, fragments);
  });

  const out = {};
  if (mediaNode) out[mediaNode.alias?.value || 'media'] = media;
  if (pageInfoNode) {
    const pi = {
      total,
      perPage,
      currentPage: page,
      lastPage: Math.max(1, Math.ceil(total / perPage)),
      hasNextPage: page * perPage < total,
    };
    const piSels = collectSelections(pageInfoNode, fragments);
    if (!piSels.length) out[pageInfoNode.alias?.value || 'pageInfo'] = pi;
    else {
      const o = {};
      for (const s of piSels) o[s.alias?.value || s.name.value] = pi[s.name.value] ?? null;
      out[pageInfoNode.alias?.value || 'pageInfo'] = o;
    }
  }
  return out;
}

async function handleMedia(mediaNode, vars, fragments) {
  const a = argsOf(mediaNode, vars);
  let full = null;
  if (a.id != null) {
    full = await getFullById(a.id);
  } else if (a.search) {
    const rows = sortRows(applyFilters(await getIndex(), { search: a.search }), 'POPULARITY_DESC');
    if (rows.length) full = await getFullById(rows[0].id);
  }
  if (!full) return null;
  return resolveMedia(full, collectSelections(mediaNode, fragments), fragments);
}

async function execute(query, variables = {}, operationName = null) {
  let doc;
  try {
    doc = parse(query);
  } catch (e) {
    return { errors: [{ message: `GraphQL syntax error: ${e.message}` }] };
  }
  const fragments = {};
  const ops = [];
  for (const d of doc.definitions) {
    if (d.kind === Kind.FRAGMENT_DEFINITION) fragments[d.name.value] = d;
    else if (d.kind === Kind.OPERATION_DEFINITION) ops.push(d);
  }
  const op = (operationName && ops.find((o) => o.name?.value === operationName)) || ops[0];
  if (!op) return { errors: [{ message: 'No operation found' }] };
  if (op.operation !== 'query') return { errors: [{ message: 'Only query operations are supported' }] };

  const data = {};
  for (const sel of op.selectionSet.selections) {
    if (sel.kind !== Kind.FIELD || sel.name.value.startsWith('__')) continue;
    const fname = sel.name.value;
    const key = sel.alias?.value || fname;
    try {
      if (fname === 'Page') data[key] = await handlePage(sel, variables, fragments);
      else if (fname === 'Media') data[key] = await handleMedia(sel, variables, fragments);
      else data[key] = null;
    } catch (e) {
      return { errors: [{ message: String(e?.message || e) }] };
    }
  }
  return { data };
}

/* -------------------------------- handlers -------------------------------- */

const INFO = {
  name: 'anilist-offline-graphql',
  description: 'Drop-in offline mirror of https://graphql.anilist.co — same queries, zero rate limits.',
  usage: {
    method: 'POST',
    endpoint: '/api/graphql',
    body: { query: '{ Page(page: 1, perPage: 5) { media { id title { romaji } } } }', variables: {} },
    note: 'GET with ?query=...&variables=... is also supported, like AniList.',
  },
};

export async function OPTIONS() {
  return new Response(null, { status: 204, headers: CORS });
}

export async function GET(request) {
  const url = new URL(request.url);
  const query = url.searchParams.get('query');
  if (!query) return json(INFO, 200);
  let variables = {};
  try {
    variables = JSON.parse(url.searchParams.get('variables') || '{}');
  } catch {
    return json({ errors: [{ message: 'Invalid variables JSON' }] }, 400);
  }
  return json(await execute(query, variables, url.searchParams.get('operationName')), 200);
}

export async function POST(request) {
  let body;
  try {
    body = await request.json();
  } catch {
    return json({ errors: [{ message: 'Invalid JSON body' }] }, 400);
  }
  if (!body || !body.query) return json({ errors: [{ message: 'No query provided' }] }, 400);
  return json(await execute(body.query, body.variables || {}, body.operationName || null), 200);
}
