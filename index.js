/**
 * AniList Offline GraphQL API — Vercel entrypoint
 * Drop-in mirror of https://graphql.anilist.co
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

const INFO = {
  message: 'AniList Offline GraphQL API',
  documentation: 'https://anilist.gitbook.io/anilist-apiv2-docs/overview/graphql/getting-started',
  usage: 'POST / with { "query": "...", "variables": {...} }',
  data: 'https://shoislam0311.github.io/anilist-offline-db/api/',
  totalAnime: 14546,
  rateLimit: 'None — zero rate limits!',
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...CORS,
    },
  });
}

/* ------------------------------- data layer ------------------------------ */

// Transform flat shard data to nested GraphQL format
function transformAnime(anime) {
  return {
    ...anime,
    __typename: 'Media',
    title: {
      romaji: anime.title_romaji,
      english: anime.title_english,
      native: anime.title_native,
      __typename: 'MediaTitle',
    },
    coverImage: {
      large: anime.cover_large,
      color: anime.cover_color,
      medium: anime.cover_large?.replace('/large/', '/medium/'),
      __typename: 'CoverImage',
    },
    averageScore: anime.average_score,
    seasonYear: anime.season_year,
    startDate: anime.start_date || null,
    endDate: anime.end_date || null,
    genres: anime.genres || [],
    studios: anime.studios || [],
    characters: anime.characters || [],
    relations: anime.relations || [],
    recommendations: anime.recommendations || [],
    tags: anime.tags || [],
    airingSchedule: anime.airing_schedule || null,
    nextAiringEpisode: anime.next_airing_episode || null,
    streamingEpisodes: anime.streaming_episodes || [],
    status: anime.status || null,
    format: anime.format || null,
    season: anime.season || null,
    episodes: anime.episodes || null,
    duration: anime.duration || null,
    description: anime.description || null,
    bannerImage: anime.banner_image || null,
    popularity: anime.popularity || 0,
    trending: anime.trending || 0,
    favourites: anime.favourites || 0,
    isAdult: anime.is_adult || false,
    source: anime.source || null,
    countryOfOrigin: anime.country_of_origin || null,
    hashtag: anime.hashtag || null,
    synonyms: anime.synonyms || [],
    trailer: anime.trailer || null,
    externalLinks: anime.external_links || [],
    rankings: anime.rankings || [],
    stats: anime.stats || null,
  };
}

let metaEntry = null;
let indexEntry = null;
const shardCache = new Map();

async function fetchJSON(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10000); // 10 second timeout
  try {
    const r = await fetch(url, { signal: controller.signal });
    if (!r.ok) throw new Error(`upstream ${r.status} for ${url}`);
    return await r.json();
  } finally {
    clearTimeout(timeout);
  }
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

async function getSearchIndex() {
  if (!fresh(indexEntry)) {
    indexEntry = { data: await fetchJSON(`${DATA_BASE}/search_index.json`), time: Date.now() };
  }
  return indexEntry.data;
}

async function getShard(idx) {
  let entry = shardCache.get(idx);
  if (!fresh(entry)) {
    const padded = String(idx).padStart(4, '0');
    const data = await fetchJSON(`${DATA_BASE}/shards/shard_${padded}.json`);
    entry = { data, time: Date.now() };
    shardCache.set(idx, entry);
    if (shardCache.size > MAX_SHARD_CACHE) {
      const oldest = shardCache.keys().next().value;
      shardCache.delete(oldest);
    }
  }
  return entry.data;
}

async function getAnimeById(id) {
  const meta = await getMetadata();
  const shardSize = meta.shardSize || 200;
  const shardStartIds = meta.shardStartIds || [];
  let lo = 0, hi = shardStartIds.length - 1, idx = 0;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (shardStartIds[mid] <= id) { idx = mid; lo = mid + 1; } else hi = mid - 1;
  }
  const shard = await getShard(idx);
  const found = shard.find((a) => a.id === id);
  return found ? transformAnime(found) : null;
}

async function getAnimeBatch(ids) {
  const meta = await getMetadata();
  const shardSize = meta.shardSize || 200;
  const shardStartIds = meta.shardStartIds || [];
  const byShard = new Map();
  for (const id of ids) {
    let lo = 0, hi = shardStartIds.length - 1, idx = 0;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (shardStartIds[mid] <= id) { idx = mid; lo = mid + 1; } else hi = mid - 1;
    }
    if (!byShard.has(idx)) byShard.set(idx, []);
    byShard.get(idx).push(id);
  }
  const results = [];
  for (const [shardIdx, shardIds] of byShard) {
    const shard = await getShard(shardIdx);
    for (const id of shardIds) {
      const found = shard.find((a) => a.id === id);
      if (found) results.push(transformAnime(found));
    }
  }
  return results;
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
  if (!sels || !sels.length) return obj;
  const out = {};
  for (const s of sels) {
    const key = s.alias?.value || s.name.value;
    if (s.name.value === '__typename') { out[key] = obj.__typename; continue; }
    const sub = s.selectionSet ? collectSelections(s, fragments) : null;
    if (!sub || !sub.length) {
      out[key] = obj[key] ?? null;
      continue;
    }
    const val = obj[key];
    if (Array.isArray(val)) {
      out[key] = val.map((item) => {
        if (item && typeof item === 'object' && item.__typename) return pick(item, sub, fragments);
        if (item && typeof item === 'object') return pick(item, sub, fragments);
        return item;
      });
    } else if (val && typeof val === 'object') {
      out[key] = pick(val, sub, fragments);
    } else {
      out[key] = val ?? null;
    }
  }
  return out;
}

function filterMedia(animeList, args, fieldNode, fragments) {
  let list = [...animeList];
  if (args.search) {
    const q = args.search.toLowerCase();
    list = list.filter((a) => {
      const t = a.title || {};
      return (t.romaji || '').toLowerCase().includes(q) ||
        (t.english || '').toLowerCase().includes(q) ||
        (t.native || '').toLowerCase().includes(q);
    });
  }
  if (args.id) list = list.filter((a) => a.id === args.id);
  if (args.id_in) list = list.filter((a) => args.id_in.includes(a.id));
  if (args.genre) list = list.filter((a) => (a.genres || []).includes(args.genre));
  if (args.genre_in) list = list.filter((a) => args.genre_in.some((g) => (a.genres || []).includes(g)));
  if (args.format) list = list.filter((a) => a.format === args.format);
  if (args.format_in) list = list.filter((a) => args.format_in.includes(a.format));
  if (args.status) list = list.filter((a) => a.status === args.status);
  if (args.status_in) list = list.filter((a) => args.status_in.includes(a.status));
  if (args.season) list = list.filter((a) => a.season === args.season);
  if (args.seasonYear) list = list.filter((a) => a.seasonYear === args.seasonYear);
  if (args.startDate_greater) list = list.filter((a) => a.startDate?.year > args.startDate_greater);
  if (args.startDate_lesser) list = list.filter((a) => a.startDate?.year < args.startDate_lesser);
  if (args.popularity_greater) list = list.filter((a) => (a.popularity || 0) > args.popularity_greater);
  if (args.averageScore_greater) list = list.filter((a) => (a.averageScore || 0) > args.averageScore_greater);
  if (args.averageScore_lesser) list = list.filter((a) => (a.averageScore || 0) < args.averageScore_lesser);

  if (args.sort) {
    const sorts = Array.isArray(args.sort) ? args.sort : [args.sort];
    for (const s of sorts) {
      const desc = s.endsWith('_DESC');
      const field = s.replace(/_DESC$/, '').replace(/_ASC$/, '').toLowerCase();
      list.sort((a, b) => {
        const av = a[field] ?? 0, bv = b[field] ?? 0;
        return desc ? (bv > av ? 1 : bv < av ? -1 : 0) : (av > bv ? 1 : av < bv ? -1 : 0);
      });
    }
  }

  const sels = fieldNode ? collectSelections(fieldNode, fragments) : null;
  return list.map((item) => pick(item, sels, fragments));
}

function buildPageInfo(total, page, perPage) {
  const lastPage = Math.ceil(total / perPage) || 1;
  return {
    total,
    perPage,
    currentPage: page,
    lastPage,
    hasNextPage: page < lastPage,
    hasNextPageAnimated: undefined,
  };
}

/* ----------------------------- query executor ----------------------------- */

async function resolveNode(typeName, fieldNode, fragments, root) {
  const sels = collectSelections(fieldNode, fragments);
  const args = {};
  if (fieldNode.arguments) {
    for (const arg of fieldNode.arguments) {
      if (arg.value.kind === Kind.INT) args[arg.name.value] = parseInt(arg.value.value, 10);
      else if (arg.value.kind === Kind.FLOAT) args[arg.name.value] = parseFloat(arg.value.value);
      else if (arg.value.kind === Kind.BOOLEAN) args[arg.name.value] = arg.value.value === 'true' || arg.value.value === true;
      else if (arg.value.kind === Kind.STRING) args[arg.name.value] = arg.value.value;
      else if (arg.value.kind === Kind.ENUM) args[arg.name.value] = arg.value.value;
      else if (arg.value.kind === Kind.LIST) args[arg.name.value] = arg.value.value.map((v) => v.value ?? v);
      else if (arg.value.kind === Kind.OBJECT) {
        const obj = {};
        for (const f of arg.value.fields) {
          if (f.value.kind === Kind.INT) obj[f.name.value] = parseInt(f.value.value, 10);
          else if (f.value.kind === Kind.STRING) obj[f.name.value] = f.value.value;
          else obj[f.name.value] = f.value.value;
        }
        args[arg.name.value] = obj;
      }
      else if (arg.value.kind === Kind.VARIABLE) args[arg.name.value] = undefined;
    }
  }

  if (typeName === 'RootQuery' || typeName === 'Query') {
    if (fieldNode.name.value === 'Page') {
      const meta = await getMetadata();
      const index = await getSearchIndex();
      const page = args.page || 1;
      const perPage = Math.min(args.perPage || 50, 50);

      let candidateIds = null;
      const hasFilter = args.search || args.genre || args.genre_in || args.format || args.format_in ||
        args.status || args.status_in || args.season || args.seasonYear ||
        args.id || args.id_in || args.startDate_greater || args.startDate_lesser ||
        args.popularity_greater || args.averageScore_greater || args.averageScore_lesser;

      if (hasFilter) {
        candidateIds = index
          .filter((e) => {
            if (args.search) {
              const q = args.search.toLowerCase();
              if (!(e.romaji || '').toLowerCase().includes(q) &&
                !(e.english || '').toLowerCase().includes(q) &&
                !(e.native || '').toLowerCase().includes(q)) return false;
            }
            if (args.genre && !e.genres?.includes(args.genre)) return false;
            if (args.genre_in && !args.genre_in.some((g) => e.genres?.includes(g))) return false;
            if (args.format && e.format !== args.format) return false;
            if (args.format_in && !args.format_in.includes(e.format)) return false;
            if (args.status && e.status !== args.status) return false;
            if (args.status_in && !args.status_in.includes(e.status)) return false;
            if (args.season && e.season !== args.season) return false;
            if (args.seasonYear && e.year !== args.seasonYear) return false;
            if (args.id && e.id !== args.id) return false;
            if (args.id_in && !args.id_in.includes(e.id)) return false;
            return true;
          })
          .map((e) => e.id);
      }

      if (candidateIds && (args.sort || args.popularity_greater || args.averageScore_greater || args.averageScore_lesser || args.startDate_greater || args.startDate_lesser)) {
        const full = await getAnimeBatch(candidateIds);
        let filtered = full;
        if (args.popularity_greater) filtered = filtered.filter((a) => (a.popularity || 0) > args.popularity_greater);
        if (args.averageScore_greater) filtered = filtered.filter((a) => (a.averageScore || 0) > args.averageScore_greater);
        if (args.averageScore_lesser) filtered = filtered.filter((a) => (a.averageScore || 0) < args.averageScore_lesser);
        if (args.startDate_greater) filtered = filtered.filter((a) => (a.startDate?.year || 0) > args.startDate_greater);
        if (args.startDate_lesser) filtered = filtered.filter((a) => (a.startDate?.year || 9999) < args.startDate_lesser);
        const sorts = args.sort ? (Array.isArray(args.sort) ? args.sort : [args.sort]) : ['POPULARITY_DESC'];
        for (const s of sorts) {
          const desc = s.endsWith('_DESC');
          const field = s.replace(/_DESC$/, '').replace(/_ASC$/, '').toLowerCase();
          filtered.sort((a, b) => {
            const av = a[field] ?? 0, bv = b[field] ?? 0;
            return desc ? (bv > av ? 1 : bv < av ? -1 : 0) : (av > bv ? 1 : av < bv ? -1 : 0);
          });
        }
        const total = filtered.length;
        const start = (page - 1) * perPage;
        const paged = filtered.slice(start, start + perPage);
        const subSels = fieldNode.selectionSet?.selections?.find((s) => s.name.value === 'media')?.selectionSet?.selections;
        const mediaNode = fieldNode.selectionSet?.selections?.find((s) => s.name.value === 'media');
        const media = paged.map((item) => pick(item, collectSelections(mediaNode, fragments), fragments));
        const pageInfoNode = fieldNode.selectionSet?.selections?.find((s) => s.name.value === 'pageInfo');
        const pageInfo = pageInfoNode ? pick(buildPageInfo(total, page, perPage), collectSelections(pageInfoNode, fragments), fragments) : buildPageInfo(total, page, perPage);
        const out = {};
        for (const s of (fieldNode.selectionSet?.selections || [])) {
          const k = s.alias?.value || s.name.value;
          if (s.name.value === 'media') out[k] = media;
          else if (s.name.value === 'pageInfo') out[k] = pageInfo;
          else out[k] = null;
        }
        return out;
      }

      if (candidateIds && !args.sort && !args.popularity_greater && !args.averageScore_greater && !args.averageScore_lesser && !args.startDate_greater && !args.startDate_lesser) {
        const total = candidateIds.length;
        const start = (page - 1) * perPage;
        const pagedIds = candidateIds.slice(start, start + perPage);
        const media = await getAnimeBatch(pagedIds);
        const mediaNode = fieldNode.selectionSet?.selections?.find((s) => s.name.value === 'media');
        const mediaResolved = media.map((item) => pick(item, collectSelections(mediaNode, fragments), fragments));
        const pageInfoNode = fieldNode.selectionSet?.selections?.find((s) => s.name.value === 'pageInfo');
        const pageInfo = pageInfoNode ? pick(buildPageInfo(total, page, perPage), collectSelections(pageInfoNode, fragments), fragments) : buildPageInfo(total, page, perPage);
        const out = {};
        for (const s of (fieldNode.selectionSet?.selections || [])) {
          const k = s.alias?.value || s.name.value;
          if (s.name.value === 'media') out[k] = mediaResolved;
          else if (s.name.value === 'pageInfo') out[k] = pageInfo;
          else out[k] = null;
        }
        return out;
      }

      const total = meta.totalAnime || index.length;
      const start = (page - 1) * perPage;
      const paged = index.slice(start, start + perPage);
      const animeIds = paged.map((e) => e.id);
      const animeList = await getAnimeBatch(animeIds);
      const mediaNode = fieldNode.selectionSet?.selections?.find((s) => s.name.value === 'media');
      const sorts = args.sort ? (Array.isArray(args.sort) ? args.sort : [args.sort]) : null;
      let mediaResolved;
      if (sorts) {
        mediaResolved = filterMedia(animeList, args, mediaNode, fragments);
      } else {
        mediaResolved = animeList.map((item) => pick(item, collectSelections(mediaNode, fragments), fragments));
      }
      const pageInfoNode = fieldNode.selectionSet?.selections?.find((s) => s.name.value === 'pageInfo');
      const pageInfo = pageInfoNode ? pick(buildPageInfo(total, page, perPage), collectSelections(pageInfoNode, fragments), fragments) : buildPageInfo(total, page, perPage);
      const out = {};
      for (const s of (fieldNode.selectionSet?.selections || [])) {
        const k = s.alias?.value || s.name.value;
        if (s.name.value === 'media') out[k] = mediaResolved;
        else if (s.name.value === 'pageInfo') out[k] = pageInfo;
        else out[k] = null;
      }
      return out;
    }

    if (fieldNode.name.value === 'Media') {
      const id = args.id;
      if (!id) return json({ errors: [{ message: 'id argument required' }] }, 400);
      const anime = await getAnimeById(id);
      if (!anime) return json({ errors: [{ message: `Media not found: ${id}` }] }, 404);
      return pick(anime, sels, fragments);
    }

    if (fieldNode.name.value === 'Page') {
      const meta = await getMetadata();
      const page = args.page || 1;
      const perPage = Math.min(args.perPage || 50, 50);
      const total = meta.totalAnime;
      const pageInfoNode = fieldNode.selectionSet?.selections?.find((s) => s.name.value === 'pageInfo');
      const pageInfo = pageInfoNode ? pick(buildPageInfo(total, page, perPage), collectSelections(pageInfoNode, fragments), fragments) : buildPageInfo(total, page, perPage);
      return { pageInfo };
    }
  }

  if (fieldNode.selectionSet) {
    const out = {};
    for (const s of fieldNode.selectionSet.selections) {
      const k = s.alias?.value || s.name.value;
      if (s.name.value === '__typename') out[k] = typeName;
      else out[k] = await resolveField(typeName, s, fragments, root);
    }
    return out;
  }
  return root;
}

async function resolveField(typeName, fieldNode, fragments, root) {
  if (!root || typeof root !== 'object') return null;

  const args = {};
  if (fieldNode.arguments) {
    for (const arg of fieldNode.arguments) {
      if (arg.value.kind === Kind.INT) args[arg.name.value] = parseInt(arg.value.value, 10);
      else if (arg.value.kind === Kind.FLOAT) args[arg.name.value] = parseFloat(arg.value.value);
      else if (arg.value.kind === Kind.BOOLEAN) args[arg.name.value] = arg.value.value === 'true' || arg.value.value === true;
      else if (arg.value.kind === Kind.STRING) args[arg.name.value] = arg.value.value;
      else if (arg.value.kind === Kind.ENUM) args[arg.name.value] = arg.value.value;
      else if (arg.value.kind === Kind.LIST) args[arg.name.value] = arg.value.value.map((v) => v.value ?? v);
      else if (arg.value.kind === Kind.VARIABLE) args[arg.name.value] = undefined;
    }
  }

  const val = root[fieldNode.name.value];
  if (val === undefined) return null;

  if (!fieldNode.selectionSet) return val;

  if (Array.isArray(val)) {
    return val.map((item) => pick(item, collectSelections(fieldNode, fragments), fragments));
  }

  if (val && typeof val === 'object') {
    if (fieldNode.name.value === 'characters' || fieldNode.name.value === 'relations' ||
      fieldNode.name.value === 'recommendations' || fieldNode.name.value === 'studios' ||
      fieldNode.name.value === 'airingSchedule' || fieldNode.name.value === 'streamingEpisodes') {
      const subSels = collectSelections(fieldNode, fragments);
      return pick(val, subSels, fragments);
    }
    return pick(val, collectSelections(fieldNode, fragments), fragments);
  }

  return val;
}

async function execute(query, variables = {}, operationName = null) {
  const doc = parse(query);
  const fragments = {};
  for (const def of doc.definitions) {
    if (def.kind === Kind.FRAGMENT_DEFINITION) fragments[def.name.value] = def;
  }
  let op;
  if (operationName) {
    op = doc.definitions.find((d) => d.kind === Kind.OPERATION_DEFINITION && d.name?.value === operationName);
  }
  if (!op) op = doc.definitions.find((d) => d.kind === Kind.OPERATION_DEFINITION);
  if (!op) return { errors: [{ message: 'No operation found' }] };

  const variableValues = {};
  if (op.variableDefinitions) {
    for (const vd of op.variableDefinitions) {
      const name = vd.variable.name.value;
      variableValues[name] = variables[name] ?? vd.defaultValue?.value ?? null;
    }
  }

  const data = {};
  const errors = [];
  for (const field of op.selectionSet.selections) {
    if (field.kind !== Kind.FIELD) continue;
    try {
      const result = await resolveNode('RootQuery', field, fragments, {});
      const key = field.alias?.value || field.name.value;
      data[key] = result;
    } catch (err) {
      errors.push({ message: err.message, locations: [{ line: field.loc?.startLine, column: field.loc?.startColumn }], path: [field.name.value] });
    }
  }
  return Object.keys(errors).length ? { data, errors } : { data };
}

/* -------------------------------- handler --------------------------------- */

export default async function handler(request) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 55000); // 55 second timeout (less than maxDuration)
  
  try {
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS });
    }
    if (request.method === 'GET') {
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
    if (request.method === 'POST') {
      let body;
      try {
        body = await request.json();
      } catch {
        return json({ errors: [{ message: 'Invalid JSON body' }] }, 400);
      }
      if (!body || !body.query) return json({ errors: [{ message: 'No query provided' }] }, 400);
      return json(await execute(body.query, body.variables || {}, body.operationName || null), 200);
    }
    return json({ errors: [{ message: 'Method not allowed' }] }, 405);
  } catch (err) {
    if (err.name === 'AbortError') {
      return json({ errors: [{ message: 'Request timeout' }] }, 504);
    }
    return json({ errors: [{ message: err.message || 'Internal server error' }] }, 500);
  } finally {
    clearTimeout(timeout);
  }
}
