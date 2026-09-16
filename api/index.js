const GITHUB_PAGES = 'https://shoislam0311.github.io/anilist-offline-db/api';
const CACHE_TTL = 5 * 60 * 1000;
const cache = new Map();

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Max-Age': '86400',
};

async function cached(url) {
  const c = cache.get(url);
  if (c && Date.now() - c.t < CACHE_TTL) return c.d;
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${r.status}`);
  const d = await r.json();
  cache.set(url, { d, t: Date.now() });
  return d;
}

let meta = null;
let allAnime = null;

async function getMeta() {
  if (!meta) meta = await cached(`${GITHUB_PAGES}/metadata.json`);
  return meta;
}

async function getAllAnime() {
  if (allAnime) return allAnime;
  const m = await getMeta();
  const n = Math.ceil(m.totalAnime / m.shardSize);
  const shards = await Promise.all(
    Array.from({ length: n }, (_, i) =>
      cached(`${GITHUB_PAGES}/shards/shard_${String(i).padStart(4, '0')}.json`)
    )
  );
  allAnime = shards.flat();
  return allAnime;
}

function camelToSnake(s) {
  return s.replace(/[A-Z]/g, c => '_' + c.toLowerCase());
}

const FIELD_MAP = {
  titleRomaji: 'title_romaji', titleEnglish: 'title_english', titleNative: 'title_native',
  coverLarge: 'cover_large', bannerImage: 'banner_image', averageScore: 'average_score',
  meanScore: 'mean_score', seasonYear: 'season_year', nextAiringEpisode: 'next_airing_episode',
  nextAiringAt: 'next_airing_at', startDate: 'start_date', endDate: 'end_date',
  countryOfOrigin: 'country_of_origin', isAdult: 'is_adult', idMal: 'id_mal',
  trending: 'trending', favourites: 'favourites',
};

function get(obj, key) {
  if (obj == null) return undefined;
  if (key in obj) return obj[key];
  if (key in FIELD_MAP && FIELD_MAP[key] in obj) return obj[FIELD_MAP[key]];
  const sn = camelToSnake(key);
  if (sn in obj) return obj[sn];
  return undefined;
}

function parseVars(s) {
  const v = {};
  if (!s || !s.trim()) return v;
  const re = /(\w+)\s*:\s*(?:"([^"]*)"|(-?\d+(?:\.\d+)?)|(\w+))/g;
  let m;
  while ((m = re.exec(s))) {
    if (m[2] !== undefined) v[m[1]] = m[2];
    else if (m[3] !== undefined) v[m[1]] = Number(m[3]);
    else if (m[4] !== undefined) {
      if (m[4] === 'true') v[m[1]] = true;
      else if (m[4] === 'false') v[m[1]] = false;
      else v[m[1]] = m[4];
    }
  }
  return v;
}

function parseSelections(sel) {
  const out = [];
  const lines = sel.split('\n').map(l => l.trim()).filter(Boolean);
  const stack = [{ ch: out, ind: -1 }];
  for (const line of lines) {
    const ind = line.search(/\S/);
    const clean = line.replace(/\s*\{.*$/, '');
    const hasCh = line.includes('{');
    while (stack.length > 1 && stack[stack.length - 1].ind >= ind) stack.pop();
    const field = { name: clean.split('(')[0].split(':')[0].trim(), args: {}, ch: null };
    const am = clean.match(/\(([^)]+)\)/);
    if (am) field.args = parseVars(am[1]);
    const al = clean.match(/^(\w+)\s*:\s*(\w+)/);
    if (al) { field.alias = al[1]; field.name = al[2]; }
    if (hasCh) {
      field.ch = [];
      stack[stack.length - 1].ch.push(field);
      stack.push({ ch: field.ch, ind: ind + 2 });
    } else {
      stack[stack.length - 1].ch.push(field);
    }
  }
  return out;
}

function parseQuery(q) {
  const t = q.trim();
  const m1 = t.match(/^\{?\s*query\s*(?:\w*)\s*(?:\(([^)]*)\))?\s*\{([\s\S]*)\}\s*\}?$/);
  if (m1) return { vars: parseVars(m1[1]), sels: parseSelections(m1[2]) };
  const m2 = t.match(/^\{?\s*(\w+)\s*(?:\(([^)]*)\))?\s*\{([\s\S]*)\}\s*\}?$/);
  if (m2) return { op: m2[1], vars: parseVars(m2[2] || ''), sels: parseSelections(m2[3]) };
  return { sels: parseSelections(t.replace(/^\{|\}$/g, '')) };
}

function resolve(obj, sels) {
  if (!sels || !sels.length || obj == null) return obj;
  const r = {};
  for (const s of sels) {
    const key = s.alias || s.name;
    if (s.ch) {
      if (s.name === 'title') {
        r[key] = {
          romaji: get(obj, 'title_romaji') || get(obj, 'romaji'),
          english: get(obj, 'title_english') || get(obj, 'english'),
          native: get(obj, 'title_native') || get(obj, 'native'),
        };
        if (s.ch.length) {
          const sub = {};
          for (const c of s.ch) sub[c.name] = r[key][c.name];
          r[key] = sub;
        }
      } else if (s.name === 'coverImage') {
        r[key] = { large: get(obj, 'cover_large'), medium: get(obj, 'cover_large'), color: get(obj, 'cover_color') };
        if (s.ch.length) {
          const sub = {};
          for (const c of s.ch) sub[c.name] = r[key][c.name];
          r[key] = sub;
        }
      } else if (s.name === 'startDate' && obj.start_date) {
        const d = obj.start_date;
        r[key] = { year: parseInt(d?.substring(0,4)), month: parseInt(d?.substring(5,7)), day: parseInt(d?.substring(8,10)) };
      } else if (s.name === 'endDate' && obj.end_date) {
        const d = obj.end_date;
        r[key] = { year: parseInt(d?.substring(0,4)), month: parseInt(d?.substring(5,7)), day: parseInt(d?.substring(8,10)) };
      } else if (s.name === 'relations' && obj.relations) {
        r[key] = { edges: obj.relations.map(r2 => ({ node: resolve(r2, s.ch), relationType: r2.relationType })) };
      } else if (s.name === 'recommendations' && obj.recommendations) {
        r[key] = { edges: obj.recommendations.map(r2 => ({ node: resolve({ mediaRecommendation: r2, rating: r2.rating }, s.ch), rating: r2.rating })) };
      } else if (s.name === 'characters' && obj.characters) {
        r[key] = { edges: obj.characters.map(c => ({ node: resolve(c, s.ch), role: c.role })) };
      } else if (s.name === 'studios' && obj.studios) {
        r[key] = { edges: obj.studios.map(s2 => ({ node: resolve(s2, s.ch), isMain: s2.isMain })) };
      } else if (s.name === 'tags' && obj.tags) {
        r[key] = obj.tags.map(t => resolve(t, s.ch));
      } else if (s.name === 'airingSchedule' && obj.airingSchedule) {
        r[key] = { edges: obj.airingSchedule.map(a => ({ node: resolve(a, s.ch) })) };
      } else if (s.name === 'nextAiringEpisode') {
        r[key] = obj.next_airing_episode || obj.nextAiringEpisode || null;
        if (r[key] && s.ch) r[key] = resolve(r[key], s.ch);
      } else {
        const child = get(obj, s.name);
        if (Array.isArray(child)) {
          r[key] = child.map(item => resolve(item, s.ch));
        } else if (child != null) {
          r[key] = resolve(child, s.ch);
        } else {
          r[key] = null;
        }
      }
    } else {
      r[key] = get(obj, s.name);
    }
  }
  return r;
}

function sortMedia(media, sort) {
  const m = {
    POPULARITY_DESC: (a, b) => (b.popularity || 0) - (a.popularity || 0),
    POPULARITY: (a, b) => (a.popularity || 0) - (b.popularity || 0),
    SCORE_DESC: (a, b) => (b.average_score || 0) - (a.average_score || 0),
    SCORE: (a, b) => (a.average_score || 0) - (b.average_score || 0),
    UPDATED_AT_DESC: (a, b) => (b.updated_at || 0) - (a.updated_at || 0),
    UPDATED_AT: (a, b) => (a.updated_at || 0) - (b.updated_at || 0),
    START_DATE_DESC: (a, b) => (b.start_date || '').localeCompare(a.start_date || ''),
    START_DATE: (a, b) => (a.start_date || '').localeCompare(b.start_date || ''),
    FAVOURITES_DESC: (a, b) => (b.favourites || 0) - (a.favourites || 0),
    TRENDING_DESC: (a, b) => (b.trending || 0) - (a.trending || 0),
    ID_DESC: (a, b) => b.id - a.id,
    ID: (a, b) => a.id - b.id,
    TITLE_ENGLISH_DESC: (a, b) => (b.title_english || b.title_romaji || '').localeCompare(a.title_english || a.title_romaji || ''),
    TITLE_ROMAJI_DESC: (a, b) => (b.title_romaji || '').localeCompare(a.title_romaji || ''),
  };
  return media.sort(m[sort] || m.POPULARITY_DESC);
}

function filterMedia(media, v) {
  let f = [...media];
  if (v.search) {
    const q = v.search.toLowerCase();
    f = f.filter(a =>
      (a.title_romaji || '').toLowerCase().includes(q) ||
      (a.title_english || '').toLowerCase().includes(q) ||
      (a.title_native || '').includes(q)
    );
  }
  if (v.genre) f = f.filter(a => a.genres && a.genres.includes(v.genre));
  if (v.format) f = f.filter(a => a.format === v.format);
  if (v.status) f = f.filter(a => a.status === v.status);
  if (v.season) f = f.filter(a => a.season === v.season);
  if (v.seasonYear) f = f.filter(a => a.season_year === v.seasonYear);
  if (v.id) f = f.filter(a => a.id === v.id);
  if (v.type && v.type !== 'ANIME') return [];
  return f;
}

async function executeGraphQL(query, variables = {}) {
  const parsed = parseQuery(query);
  const vars = { ...parsed.vars, ...variables };
  let media = await getAllAnime();
  media = filterMedia(media, vars);
  const sort = vars.sort || 'POPULARITY_DESC';
  media = sortMedia(media, sort);
  const page = vars.page || 1;
  const perPage = Math.min(vars.perPage || 10, 50);
  const start = (page - 1) * perPage;
  const paged = media.slice(start, start + perPage);
  const mediaSels = parsed.sels?.[0]?.ch?.find(s => s.name === 'media');
  const resolved = paged.map(item => resolve(item, mediaSels?.ch));
  return {
    data: {
      Page: {
        media: resolved,
        pageInfo: {
          total: media.length,
          perPage,
          currentPage: page,
          lastPage: Math.ceil(media.length / perPage),
          hasNextPage: start + perPage < media.length,
          hasPreviousPage: page > 1,
        }
      }
    }
  };
}

module.exports = async function handler(req, res) {
  Object.entries(CORS).forEach(([k, v]) => res.setHeader(k, v));
  if (req.method === 'OPTIONS') return res.status(200).end();

  if (req.method === 'GET' && req.url === '/api/metadata') {
    try {
      const m = await getMeta();
      return res.status(200).json(m);
    } catch (e) {
      return res.status(500).json({ error: 'Failed to load metadata' });
    }
  }

  if (req.method === 'GET' && req.url.startsWith('/api/anime/')) {
    const id = parseInt(req.url.split('/').pop());
    if (isNaN(id)) return res.status(400).json({ error: 'Invalid ID' });
    try {
      const media = await getAllAnime();
      const anime = media.find(a => a.id === id);
      if (!anime) return res.status(404).json({ error: 'Not found' });
      return res.status(200).json({ data: { Media: anime } });
    } catch (e) {
      return res.status(500).json({ error: 'Failed' });
    }
  }

  if (req.method === 'POST') {
    try {
      const body = req.body || {};
      const query = body.query || '';
      const variables = body.variables || {};
      if (!query) return res.status(400).json({ errors: [{ message: 'No query provided' }] });
      const result = await executeGraphQL(query, variables);
      return res.status(200).json(result);
    } catch (e) {
      return res.status(500).json({ errors: [{ message: e.message }] });
    }
  }

  return res.status(405).json({ error: 'Method not allowed' });
};
