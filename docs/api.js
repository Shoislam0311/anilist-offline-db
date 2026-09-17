/**
 * AniList Offline API - Client-side GraphQL Engine (EXACT anime-only mirror)
 * BREAKING: shards are now AniList-exact Media objects (camelCase, FuzzyDate).
 * No snake_case. Same response shape as graphql.anilist.co.
 * Free + fastest delivery: same-origin `api/` -> jsDelivr CDN -> Vercel.
 */

const API_BASES = (() => {
  // 100% credential-free: all of these mirror the public GitHub repo, no keys/signup.
  // Order = fastest/most reliable first. fetchFirst() tries each in turn.
  const USER = 'Shoislam0311', REPO = 'anilist-offline-db', BRANCH = 'main';
  return [
    'api', // same-origin (Pages) — fastest when user is already on the site
    `https://cdn.jsdelivr.net/gh/${USER}/${REPO}@${BRANCH}/docs/api`, // global edge, brotli, immutable tags
    `https://cdn.statically.io/gh/${USER}/${REPO}/${BRANCH}/docs/api`, // second independent CDN, no key
    `https://raw.githack.com/${USER}/${REPO}/${BRANCH}/docs/api`, // raw proxy w/ caching, no key
    `https://${USER.toLowerCase()}.github.io/${REPO}/api`, // Pages origin fallback
  ];
})();
let API_BASE = API_BASES[0];
let metadata = null;
let searchIndex = null;

async function fetchFirst(urls) {
  let lastErr = null;
  for (const u of urls) {
    try {
      const r = await fetch(u);
      if (r.ok) {
        API_BASE = u.slice(0, u.lastIndexOf('/api') > 0 ? u.lastIndexOf('/api') : u.length);
        if (API_BASE.endsWith('/')) API_BASE = API_BASE.slice(0, -1);
        // normalize: keep full base for subsequent calls
        const m = u.match(/^(.*)\/api\//);
        if (m) API_BASE = m[1] + '/api';
        return await r.json();
      }
    } catch (e) { lastErr = e; }
  }
  throw lastErr || new Error('all API bases failed');
}
const apiUrl = (p) => `${API_BASE}/${p}`;

/**
 * AniList Offline — site data layer (EXACT anime-only mirror)
 *
 * Browsers CANNOT fetch release assets directly: github.com sends no
 * CORS headers (every release fetch dies with ERR_FAILED), release URLs
 * 404/302 unpredictably, and git no longer holds shards. So ALL anime
 * payloads go through our own Vercel GraphQL API (open CORS, exact same
 * schema as graphql.anilist.co), which reads the release shards server-side.
 * Only tiny git-tracked files (metadata, search_index) load directly.
 */

const VERCEL_API = 'https://anilist-offline-db-phi.vercel.app/';

async function apiQuery(query, variables = {}) {
  const r = await fetch(VERCEL_API, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, variables }),
  });
  if (!r.ok) throw new Error('API offline (HTTP ' + r.status + ')');
  const d = await r.json();
  if (d.errors && !d.data) throw new Error(d.errors[0]?.message || 'API error');
  return d.data;
}
// Force https on every rendered image (kills Mixed Content blocks).
const https = (u) => String(u || '').replace(/^http:\/\//i, 'https://');

async function init() {
    try {
        const resp = await fetch(`${API_BASE}/metadata.json`).catch(() => null);
        if (resp && resp.ok) {
          metadata = await resp.json();
        } else {
          metadata = await fetchFirst(API_BASES.map((b) => `${b}/metadata.json`));
        }
        const base = API_BASE.replace(/\/api$/, '');
        document.getElementById('apiEndpoint').textContent = window.location.origin + '/api/graphql (Vercel) + this Pages mirror';
        document.getElementById('docBaseUrl').textContent = base + '/api/';
        document.getElementById('statTotal').textContent = (metadata.totalAnime || 0).toLocaleString();
        document.getElementById('statCharacters').textContent = (metadata.totalCharacters || 0).toLocaleString();
        document.getElementById('statStudios').textContent = (metadata.totalStudios || 0).toLocaleString();
        document.getElementById('statGenres').textContent = metadata.genres?.length || 0;

        const genreSelect = document.getElementById('browseGenre');
        const searchGenre = document.getElementById('searchGenre');
        if (metadata.genres) {
            metadata.genres.forEach(g => {
                const opt = document.createElement('option');
                opt.value = g;
                opt.textContent = g;
                genreSelect.appendChild(opt);
                const opt2 = document.createElement('option');
                opt2.value = g;
                opt2.textContent = g;
                searchGenre.appendChild(opt2);
            });
        }
        renderRecents();
        renderDiscover();
        browseAnime();
        wireSearchBox();
    } catch (e) {
        console.error('Failed to init:', e);
    }
}

function wireSearchBox() {
  const input = document.getElementById('searchInput');
  if (!input || input.dataset.wired) return;
  input.dataset.wired = '1';
  input.addEventListener('input', onSearchInput);
  input.addEventListener('keydown', dropKey);
  input.addEventListener('blur', () => setTimeout(hideDrop, 150));
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeDetail(); });
  document.getElementById('modalBack')?.addEventListener('click', (e) => {
    if (e.target.id === 'modalBack') closeDetail();
  });
  for (const id of ['searchGenre', 'searchStatus', 'searchFormat', 'searchSort', 'searchAdult']) {
    document.getElementById(id)?.addEventListener('change', () => {
      if (document.getElementById('searchInput').value.trim()) searchAnime();
    });
  }
}

async function loadSearchIndex() {
    if (searchIndex) return searchIndex;
    try {
        const resp = await fetch(`${API_BASE}/search_index.json`);
        if (resp.ok) { searchIndex = await resp.json(); return searchIndex; }
        searchIndex = await fetchFirst(API_BASES.map((b) => `${b}/search_index.json`));
        return searchIndex;
    } catch (e) {
        console.error('Failed to load search index:', e);
        return [];
    }
}

const CARD_FIELDS = `id title { romaji english } coverImage { large } averageScore popularity favourites trending format status season seasonYear episodes`;
const DETAIL_FIELDS = `id idMal title { romaji english native userPreferred } description
  coverImage { extraLarge large medium color } bannerImage episodes duration status format
  season seasonYear averageScore meanScore popularity trending favourites
  genres synonyms siteUrl hashtag source trailer { id site }
  studios { edges { isMain node { name } } }
  tags { name rank description }
  characters(page: 1, perPage: 15) { edges { role node { id name { full } image { large } } voiceActors(language: JAPANESE) { name { full } language } } }
  relations { edges { relationType node { id title { romaji english } coverImage { large } } } }
  recommendations(page: 1, perPage: 15, sort: RATING_DESC) { edges { node { rating mediaRecommendation { id title { romaji english } coverImage { large } } } } }
  airingSchedule { edges { node { episode airingAt } } } nextAiringEpisode { episode airingAt }
  streamingEpisodes { title url site } externalLinks { site url }`;

async function loadAnimeById(id) {
  try {
    const d = await apiQuery(`{ Media(id: ${parseInt(id, 10)}) { ${DETAIL_FIELDS} } }`);
    return d?.Media || null;
  } catch (e) { return null; }
}
async function fetchRail({ sort, status, limit = 18 }) {
  const args = [`type: ANIME`, `sort: ${sort}`, `isAdult: false`];
  if (status) args.push(`status: ${status}`);
  const d = await apiQuery(`{ Page(page: 1, perPage: ${limit}) { media(${args.join(', ')}) { ${CARD_FIELDS} } } }`);
  return d?.Page?.media || [];
}

/* ---------- minimal GraphQL parser: variables, aliases, args, fragments ---- */
function tokenizeArgs(argStr, variables) {
  // parses: key: value, key2: [A, B], key3: $var, key4: "str", key5: 123
  const args = {};
  if (!argStr || !argStr.trim()) return args;
  // split top-level commas (respect brackets/quotes)
  const parts = [];
  let cur = '', depth = 0, inStr = false;
  for (let i = 0; i < argStr.length; i++) {
    const c = argStr[i];
    if (c === '"' && argStr[i - 1] !== '\\') inStr = !inStr;
    if (!inStr) {
      if (c === '[' || c === '{') depth++;
      if (c === ']' || c === '}') depth--;
      if (c === ',' && depth === 0) { parts.push(cur); cur = ''; continue; }
    }
    cur += c;
  }
  if (cur.trim()) parts.push(cur);
  for (const p of parts) {
    const m = p.match(/^\s*(\w+)\s*:\s*([\s\S]+)\s*$/);
    if (!m) continue;
    args[m[1]] = parseValue(m[2].trim(), variables);
  }
  return args;
}
function parseValue(s, variables) {
  if (!s) return undefined;
  if (s.startsWith('$')) return variables?.[s.slice(1)];
  if (s.startsWith('"') && s.endsWith('"')) return JSON.parse(s);
  if (s === 'true') return true;
  if (s === 'false') return false;
  if (s === 'null') return null;
  if (/^-?\d+$/.test(s)) return parseInt(s, 10);
  if (/^-?\d*\.\d+$/.test(s)) return parseFloat(s);
  if (s.startsWith('[') && s.endsWith(']')) {
    const inner = s.slice(1, -1).trim();
    if (!inner) return [];
    return splitTop(inner).map((x) => parseValue(x.trim(), variables));
  }
  return s; // enum
}
function splitTop(s) {
  const out = []; let cur = '', depth = 0, inStr = false;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (c === '"' && s[i - 1] !== '\\') inStr = !inStr;
    if (!inStr) {
      if (c === '[' || c === '{') depth++;
      if (c === ']' || c === '}') depth--;
      if (c === ',' && depth === 0) { out.push(cur); cur = ''; continue; }
    }
    cur += c;
  }
  if (cur.trim()) out.push(cur);
  return out;
}

// Recursive descent over { field(arg){ sub } alias: field } with fragments inlined.
function parseBlock(s, i, variables, fragments) {
  const selections = [];
  let n = s.length;
  const skip = () => { while (i < n && /[\s,}]/.test(s[i]) && s[i] !== '}') i++; while (i < n && /\s/.test(s[i])) i++; };
  while (i < n) {
    while (i < n && /\s|,/.test(s[i])) i++;
    if (i >= n || s[i] === '}') { i++; break; }
    if (s.startsWith('...', i)) {
      const m = s.slice(i).match(/^\.\.\.(\w+)/);
      if (m) {
        const frag = fragments[m[1]];
        if (frag) selections.push(...frag);
        i += m[0].length;
        continue;
      }
      // inline fragment ... on Type { }
      const b = s.indexOf('{', i);
      if (b >= 0) { const [sub, ni] = parseBlock(s, b + 1, variables, fragments); selections.push(...sub); i = ni; continue; }
      i += 3; continue;
    }
    const m = s.slice(i).match(/^([\w]+)(\s*:\s*([\w]+))?(\s*\(([^()]*|\([^()]*\))*\))?(\s*\{)?/);
    if (!m) { i++; continue; }
    let name = m[1], alias = null;
    if (m[3]) { alias = m[1]; name = m[3]; }
    const argStr = m[4] ? m[4].slice(1, -1) : '';
    const hasBlock = !!m[6];
    i += m[0].length;
    let children = null;
    if (hasBlock) {
      const [sub, ni] = parseBlock(s, i, variables, fragments);
      children = sub; i = ni;
    }
    selections.push({ name, alias, args: tokenizeArgs(argStr, variables), children });
  }
  return [selections, i];
}
function parseQuery(query, variables = {}) {
  const fragments = {};
  const fragRe = /fragment\s+(\w+)\s+on\s+\w+\s*\{/g;
  let m;
  // extract fragments with brace matching
  while ((m = fragRe.exec(query))) {
    let depth = 1, j = fragRe.lastIndex;
    while (j < query.length && depth > 0) {
      if (query[j] === '{') depth++;
      if (query[j] === '}') depth--;
      j++;
    }
    const body = query.slice(fragRe.lastIndex, j - 1);
    const [sel] = parseBlock(body, 0, variables, {});
    fragments[m[1]] = sel;
  }
  const bodyOnly = query.replace(/fragment\s+\w+\s+on\s+\w+\s*\{[\s\S]*?\n\}/g, '');
  const b = bodyOnly.indexOf('{');
  const e = bodyOnly.lastIndexOf('}');
  const inner = b >= 0 ? bodyOnly.slice(b + 1, e) : bodyOnly;
  // strip query(...) / mutation header: find first top-level {
  const [selections] = parseBlock(inner, 0, variables, fragments);
  // selections[0] may be `query` wrapper if user wrote `query { Page... }` without our strip
  return { selections, fragments };
}

function pickExact(obj, selections) {
  if (obj == null || !selections || !selections.length) return obj;
  const out = {};
  for (const s of selections) {
    const key = s.alias || s.name;
    if (s.name === '__typename') { out[key] = obj.__typename || 'Media'; continue; }
    const val = obj[s.name];
    if (val === undefined) { out[key] = null; continue; }
    if (!s.children) { out[key] = val; continue; }
    if (Array.isArray(val)) out[key] = val.map((v) => (v && typeof v === 'object' ? pickExact(v, s.children) : v));
    else if (val && typeof val === 'object') out[key] = pickExact(val, s.children);
    else out[key] = val;
  }
  return out;
}

/* ---------------- exact filter + sort (mirrors Vercel) -------------------- */
function normStr(s) { return String(s || '').toLowerCase().trim(); }
function searchTokens(q) {
  return normStr(q).split(/[\s_.,;:!?()[\]{}'"\/\\|-]+/).map((t) => t.trim()).filter((t) => t.length > 0);
}
// Same semantics as the Vercel engine: every word must hit somewhere;
// score ranks exact > word-start > substring, ties by popularity.
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
function rankSearch(index, q, limit) {
  return index
    .map((e) => ({ e, s: searchScore(e, q) }))
    .filter((x) => x.s >= 0)
    .sort((a, b) => (b.s - a.s) || ((b.e.popularity || 0) - (a.e.popularity || 0)))
    .slice(0, limit || 20)
    .map((x) => x.e);
}

function matchExact(e, full, a) {
  if (a.type && a.type !== 'ANIME') return false;
  if (a.id !== undefined && e.id !== a.id) return false;
  if (a.id_in && !a.id_in.includes(e.id)) return false;
  if (a.id_not !== undefined && e.id === a.id_not) return false;
  if (a.id_not_in && a.id_not_in.includes(e.id)) return false;
  if (a.idMal !== undefined && e.idMal !== a.idMal) return false;
  if (a.search && searchScore(e, a.search) < 0) return false;
  if (a.genre && !(e.genres || []).includes(a.genre)) return false;
  if (a.genre_in && !a.genre_in.some((x) => (e.genres || []).includes(x))) return false;
  if (a.genre_not_in && a.genre_not_in.some((x) => (e.genres || []).includes(x))) return false;
  if (a.tag && !(e.tags || []).includes(a.tag)) return false;
  if (a.tag_in && !a.tag_in.some((x) => (e.tags || []).includes(x))) return false;
  if (a.tag_not_in && a.tag_not_in.some((x) => (e.tags || []).includes(x))) return false;
  if (a.format && e.format !== a.format) return false;
  if (a.format_in && !a.format_in.includes(e.format)) return false;
  if (a.status && e.status !== a.status) return false;
  if (a.status_in && !a.status_in.includes(e.status)) return false;
  if (a.season && e.season !== a.season) return false;
  if (a.seasonYear !== undefined && e.year !== a.seasonYear) return false;
  if (a.source_in && !a.source_in.includes(e.source)) return false;
  if (a.countryOfOrigin && e.country !== a.countryOfOrigin) return false;
  if (a.isAdult !== undefined && e.adult !== a.isAdult) return false;
  if (a.averageScore_greater !== undefined && !((e.score ?? -1) > a.averageScore_greater)) return false;
  if (a.averageScore_lesser !== undefined && !((e.score ?? 1e9) < a.averageScore_lesser)) return false;
  if (a.popularity_greater !== undefined && !((e.popularity ?? -1) > a.popularity_greater)) return false;
  if (a.popularity_lesser !== undefined && !((e.popularity ?? 1e9) < a.popularity_lesser)) return false;
  if (full && (a.tagCategory_in || a.minimumTagRank !== undefined)) {
    const tags = full.tags || [];
    if (a.tagCategory_in && !tags.some((t) => a.tagCategory_in.includes(t.category))) return false;
    if (a.minimumTagRank !== undefined && !tags.some((t) => (t.rank ?? 0) >= a.minimumTagRank)) return false;
  }
  return true;
}
function sortVal(m, f) {
  switch (f) {
    case 'ID': return m.id ?? 0;
    case 'TITLE_ROMAJI': return (m.title?.romaji || '').toLowerCase();
    case 'TITLE_ENGLISH': return (m.title?.english || m.title?.romaji || '').toLowerCase();
    case 'SCORE': return m.averageScore ?? 0;
    case 'POPULARITY': return m.popularity ?? 0;
    case 'TRENDING': return m.trending ?? 0;
    case 'FAVOURITES': return m.favourites ?? 0;
    case 'EPISODES': return m.episodes ?? 0;
    case 'START_DATE': return (m.startDate?.year || 0) * 10000 + (m.startDate?.month || 0) * 100 + (m.startDate?.day || 0);
    case 'UPDATED_AT': return m.updatedAt ?? 0;
    default: return m.popularity ?? 0;
  }
}
function sortMedia(media, sort) {
  const sorts = (Array.isArray(sort) ? sort : [sort]).filter(Boolean);
  if (!sorts.length) sorts.push('POPULARITY_DESC');
  for (let i = sorts.length - 1; i >= 0; i--) {
    const s = sorts[i];
    const desc = s.endsWith('_DESC');
    const field = s.replace(/_DESC$/, '').replace(/_ASC$/, '');
    media.sort((a, b) => {
      const av = sortVal(a, field), bv = sortVal(b, field);
      if (av === bv) return 0;
      return desc ? (av > bv ? -1 : 1) : (av > bv ? 1 : -1);
    });
  }
  return media;
}

async function executeGraphQL(query, variables = {}) {
    const { selections } = parseQuery(query, variables);
    const root = selections[0];
    const isPage = root?.name === 'Page' || (!root?.name?.match(/^(Media|Character|Staff|Studio)$/) && selections.some((s) => s.name === 'Page'));
    const pageSel = root?.name === 'Page' ? root : selections.find((s) => s.name === 'Page');

    if (root?.name === 'Media' || root?.name === 'media') {
      const args = { ...root.args, ...variables };
      let anime = null;
      if (args.id) anime = await loadAnimeById(args.id);
      else if (args.search) {
        const index = await loadSearchIndex();
        const hits = rankSearch(index, args.search, 1);
        if (hits.length) anime = await loadAnimeById(hits[0].id);
      }
      if (!anime) return { data: null, errors: [{ message: 'Media not found', status: 404 }] };
      return { data: { Media: pickExact(anime, root.children) } };
    }

    if (!pageSel) return { errors: [{ message: 'Only Page and Media roots are supported in Pages mirror (use Vercel for Character/Staff/Studio)', status: 400 }] };
    const allVars = { ...(pageSel.args || {}), ...variables };
    const mediaSel = (pageSel.children || []).find((s) => s.name === 'media');
    Object.assign(allVars, mediaSel?.args || {});

    const index = await loadSearchIndex();
    const hits = index.filter((e) => matchExact(e, null, allVars)).map((e) => e.id);
    // hydrate page slice only (fastest free path), then sort slice when sort present
    const page = allVars.page || 1;
    const perPage = Math.min(allVars.perPage || 10, 50);
    let paged;
    if (allVars.sort || allVars.tagCategory_in || allVars.minimumTagRank !== undefined) {
      const all = [];
      for (const id of hits) { const a = await loadAnimeById(id); if (a && matchExact(index.find((e) => e.id === id) || {}, a, allVars)) all.push(a); }
      sortMedia(all, allVars.sort);
      paged = all.slice((page - 1) * perPage, page * perPage);
      const total = all.length;
      return {
        data: { Page: {
          media: paged.map((a) => pickExact(a, mediaSel?.children)),
          pageInfo: { total, perPage, currentPage: page, lastPage: Math.max(1, Math.ceil(total / perPage)), hasNextPage: page * perPage < total, hasPreviousPage: page > 1 },
        } },
      };
    }
    const total = hits.length;
    const ids = hits.slice((page - 1) * perPage, page * perPage);
    paged = [];
    for (const id of ids) { const a = await loadAnimeById(id); if (a) paged.push(a); }
    return {
      data: { Page: {
        media: paged.map((a) => pickExact(a, mediaSel?.children)),
        pageInfo: { total, perPage, currentPage: page, lastPage: Math.max(1, Math.ceil(total / perPage)), hasNextPage: page * perPage < total, hasPreviousPage: page > 1 },
      } },
    };
}

function esc(s) {
  return String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}
function stripHtml(s) { return String(s || '').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim(); }
function fmtCountdown(airingAt) {
  const diff = airingAt * 1000 - Date.now();
  if (diff <= 0) return 'Aired';
  const m = Math.floor(diff / 60000);
  const d = Math.floor(m / 1440), h = Math.floor((m % 1440) / 60), mm = m % 60;
  if (d > 0) return `in ${d}d ${h}h`;
  if (h > 0) return `in ${h}h ${mm}m`;
  return `in ${mm}m`;
}
function weekdayOf(airingAt) {
  return new Date(airingAt * 1000).toLocaleDateString('en-US', { weekday: 'long' });
}
function timeOf(airingAt) {
  return new Date(airingAt * 1000).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
}
const PLACEHOLDER_IMG = 'data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 140%22><rect fill=%22%23253746%22 width=%22100%22 height=%22140%22/></svg>';
const imgErr = `onerror="this.src='${PLACEHOLDER_IMG}'"`;

function indexCard(e) {
  const title = e.romaji || e.english || 'Unknown';
  return `<div class="card" onclick="openDetail(${e.id})">
    <div class="imgwrap">${e.score ? `<span class="score">${e.score}%</span>` : ''}
    <img loading="lazy" src="${esc(https(e.cover || ''))}" alt="${esc(title)}" ${imgErr}></div>
    <div class="body"><h4 title="${esc(title)}">${esc(title)}</h4>
    <p>${esc(e.format || '')}${e.year ? ' · ' + e.year : ''} · ${(e.popularity || 0).toLocaleString()} users</p></div>
  </div>`;
}

function searchFilters() {
  return {
    genre: document.getElementById('searchGenre')?.value || '',
    status: document.getElementById('searchStatus')?.value || '',
    format: document.getElementById('searchFormat')?.value || '',
    sort: document.getElementById('searchSort')?.value || 'RELEVANCE',
    adult: document.getElementById('searchAdult')?.checked || false,
  };
}
function applySearchFilters(list) {
  const f = searchFilters();
  let out = list.filter((e) => {
    if (!f.adult && e.adult) return false;
    if (f.genre && !(e.genres || []).includes(f.genre)) return false;
    if (f.status && e.status !== f.status) return false;
    if (f.format && e.format !== f.format) return false;
    return true;
  });
  if (f.sort === 'POPULARITY_DESC') out.sort((a, b) => (b.popularity || 0) - (a.popularity || 0));
  else if (f.sort === 'SCORE_DESC') out.sort((a, b) => (b.score || 0) - (a.score || 0));
  else if (f.sort === 'TRENDING_DESC') out.sort((a, b) => (b.trending || 0) - (a.trending || 0));
  else if (f.sort === 'START_DATE_DESC') out.sort((a, b) => (b.startDate || 0) - (a.startDate || 0));
  return out;
}
function highlightMatch(text, query) {
  const tokens = searchTokens(query).filter((t) => t.length > 1);
  let out = esc(text);
  for (const t of tokens) {
    const re = new RegExp('(' + t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')', 'ig');
    out = out.replace(re, '<mark>$1</mark>');
  }
  return out;
}

let searchDebounce = null;
let dropSel = -1;
let dropItems = [];
async function onSearchInput() {
  clearTimeout(searchDebounce);
  searchDebounce = setTimeout(renderSearchDropdown, 200);
}
function hideDrop() {
  document.getElementById('searchDrop')?.classList.remove('open');
  dropSel = -1; dropItems = [];
}
async function renderSearchDropdown() {
  const q = document.getElementById('searchInput').value.trim();
  const drop = document.getElementById('searchDrop');
  if (q.length < 2) { hideDrop(); return; }
  const index = await loadSearchIndex();
  dropItems = applySearchFilters(rankSearch(index, q, 8));
  if (!dropItems.length) { hideDrop(); return; }
  dropSel = -1;
  drop.innerHTML = dropItems.map((e, i) => {
    const title = e.romaji || e.english || 'Unknown';
    return `<div class="drop-item" data-i="${i}" onmousedown="openDetail(${e.id})">
      <img loading="lazy" src="${esc(https(e.cover || ''))}" ${imgErr}>
      <div><div class="t">${highlightMatch(title, q)}</div>
      <div class="s">${e.score ? e.score + '% · ' : ''}${esc(e.format || '')} ${e.year || ''} · ${(e.popularity || 0).toLocaleString()} users</div></div>
    </div>`;
  }).join('');
  drop.classList.add('open');
}
function dropKey(e) {
  const drop = document.getElementById('searchDrop');
  if (!drop?.classList.contains('open')) {
    if (e.key === 'Enter') searchAnime();
    return;
  }
  if (e.key === 'ArrowDown') { e.preventDefault(); dropSel = Math.min(dropSel + 1, dropItems.length - 1); paintDropSel(); }
  else if (e.key === 'ArrowUp') { e.preventDefault(); dropSel = Math.max(dropSel - 1, 0); paintDropSel(); }
  else if (e.key === 'Enter' && dropSel >= 0) { openDetail(dropItems[dropSel].id); hideDrop(); }
  else if (e.key === 'Enter') { hideDrop(); searchAnime(); }
  else if (e.key === 'Escape') hideDrop();
}
function paintDropSel() {
  document.querySelectorAll('.drop-item').forEach((el) => {
    el.classList.toggle('sel', parseInt(el.dataset.i, 10) === dropSel);
  });
}
function saveRecent(q) {
  try {
    const k = 'al_recent_searches';
    let arr = JSON.parse(localStorage.getItem(k) || '[]').filter((x) => x !== q);
    arr.unshift(q); arr = arr.slice(0, 8);
    localStorage.setItem(k, JSON.stringify(arr));
  } catch (e) {}
}
function renderRecents() {
  const box = document.getElementById('searchRecent');
  if (!box) return;
  let arr = [];
  try { arr = JSON.parse(localStorage.getItem('al_recent_searches') || '[]'); } catch (e) {}
  box.innerHTML = arr.length
    ? `<p style="color:#8ba0b0;font-size:0.8rem;margin-bottom:0.4rem;">Recent:</p><div class="chips">` +
      arr.map((q) => `<span class="chip" style="cursor:pointer" onclick="recentSearch('${esc(q)}')">${esc(q)}</span>`).join('') + `</div>`
    : '';
}
function recentSearch(q) {
  document.getElementById('searchInput').value = q;
  searchAnime();
}

async function searchAnime() {
    const query = document.getElementById('searchInput').value.trim();
    if (!query) return;
    hideDrop();
    saveRecent(query);
    renderRecents();

    const resultsDiv = document.getElementById('searchResults');
    resultsDiv.innerHTML = '<div class="loading">Searching 14,000+ anime...</div>';

    const index = await loadSearchIndex();
    const ranked = rankSearch(index, query, 400);
    const matches = applySearchFilters(ranked).slice(0, 24);

    if (!matches.length) {
        resultsDiv.innerHTML = '<p style="color: #8ba0b0;">No results found. Try fewer words, a synonym, or the Japanese title.</p>';
        return;
    }

    resultsDiv.innerHTML = `<p style="color:#8ba0b0;font-size:0.85rem;margin-bottom:0.5rem;">${matches.length} results for "${esc(query)}"</p>` +
      matches.map(a => `
        <div class="result-item" onclick="openDetail(${a.id})">
            <img loading="lazy" src="${esc(https(a.cover || ''))}" alt="${esc(a.romaji)}" ${imgErr}>
            <div class="result-info">
                <h3>${highlightMatch(a.romaji || 'Unknown', query)}</h3>
                ${a.english && a.english !== a.romaji ? `<p>${esc(a.english)}</p>` : ''}
                <div class="result-meta">
                    <span>Score: ${a.score || 'N/A'}</span>
                    <span>Popularity: ${(a.popularity || 0).toLocaleString()}</span>
                    <span>Eps: ${a.episodes || '?'}</span>
                    <span>${esc(a.format || '')}</span>
                    <span>${esc(a.status || '')}</span>
                </div>
            </div>
        </div>
    `).join('');
}

function mediaCard(a) {
  const t = a.title || {};
  const title = t.romaji || t.english || 'Unknown';
  return `<div class="card" onclick="openDetail(${a.id})">
    <div class="imgwrap">${a.averageScore ? `<span class="score">${a.averageScore}%</span>` : ''}
    <img loading="lazy" src="${https(a.coverImage?.large || '')}" alt="${esc(title)}" ${imgErr}></div>
    <div class="body"><h4 title="${esc(title)}">${esc(title)}</h4>
    <p>${esc(a.format || '')}${a.seasonYear ? ' · ' + a.seasonYear : ''} · ${(a.popularity || 0).toLocaleString()} users</p></div>
  </div>`;
}

async function browseAnime() {
    const genre = document.getElementById('browseGenre').value;
    const status = document.getElementById('browseStatus').value;
    const format = document.getElementById('browseFormat').value;
    const sort = document.getElementById('browseSort').value;

    const resultsDiv = document.getElementById('browseResults');
    resultsDiv.innerHTML = '<div class="loading">Loading...</div>';

    try {
      const args = ['type: ANIME', 'isAdult: false'];
      if (genre) args.push(`genre: "${genre}"`);
      if (status) args.push(`status: ${status}`);
      if (format) args.push(`format: ${format}`);
      if (sort) args.push(`sort: ${sort}`);
      const d = await apiQuery(
        `{ Page(page: 1, perPage: 24) { media(${args.join(', ')}) { ${CARD_FIELDS} } } }`);
      const media = d?.Page?.media || [];
      if (!media.length) { resultsDiv.innerHTML = '<p style="color:#8ba0b0;">No anime match.</p>'; return; }
      resultsDiv.innerHTML = media.map(a => `
        <div class="result-item" onclick="openDetail(${a.id})">
            <img loading="lazy" src="${https(a.coverImage?.large || '')}" alt="${esc(a.title?.romaji)}" ${imgErr}>
            <div class="result-info">
                <h3>${esc(a.title?.romaji || 'Unknown')}</h3>
                ${a.title?.english && a.title.english !== a.title.romaji ? `<p>${esc(a.title.english)}</p>` : ''}
                <div class="result-meta">
                    <span>Score: ${a.averageScore || 'N/A'}</span>
                    <span>Popularity: ${(a.popularity || 0).toLocaleString()}</span>
                    <span>Eps: ${a.episodes || '?'}</span>
                    <span>${esc(a.format || '')}</span>
                    <span>${esc(a.season || '')} ${a.seasonYear || ''}</span>
                </div>
            </div>
        </div>
    `).join('');
    } catch (e) {
      resultsDiv.innerHTML = `<p class="error">Browse failed: ${esc(e.message)} — try again in a few seconds (cold start).</p>`;
    }
}

const noAdult = (e) => !e.adult;
function railHtml(title, items) {
  if (!items.length) return '';
  return `<h2 class="rail-title">${esc(title)}</h2><div class="rail">${items.map(mediaCard).join('')}</div>`;
}
async function renderDiscover() {
  const box = document.getElementById('discoverRails');
  if (!box || box.dataset.done) return;
  try {
    const [trending, airing, finished, upcoming, topRated] = await Promise.all([
      fetchRail({ sort: 'TRENDING_DESC', limit: 18 }),
      fetchRail({ sort: 'POPULARITY_DESC', status: 'RELEASING', limit: 18 }),
      fetchRail({ sort: 'END_DATE_DESC', status: 'FINISHED', limit: 18 }),
      fetchRail({ sort: 'POPULARITY_DESC', status: 'NOT_YET_RELEASED', limit: 18 }),
      fetchRail({ sort: 'SCORE_DESC', limit: 18 }),
    ]);
    // Just Finished by end date needs full objects; API returns END_DATE_DESC order already.
    box.innerHTML =
      railHtml('Trending Now', trending) +
      railHtml('Top Airing', airing) +
      railHtml('Just Finished', finished) +
      railHtml('Upcoming', upcoming) +
      railHtml('Top Rated All Time', topRated);
    box.dataset.done = '1';
  } catch (e) {
    box.innerHTML = `<p class="error">Discover failed to load: ${esc(e.message)} — retry in a few seconds (cold start).</p>`;
  }
}

let scheduleLoaded = false;
async function renderSchedule() {
  const box = document.getElementById('scheduleBody');
  if (!box || scheduleLoaded) return;
  scheduleLoaded = true;
  box.innerHTML = '<div class="loading">Loading this week\'s episodes...</div>';
  try {
    const F = `id title { romaji english } coverImage { large } airingSchedule { edges { node { episode airingAt } } } nextAiringEpisode { episode airingAt }`;
    const pages = await Promise.all([1, 2, 3, 4].map((p) =>
      apiQuery(`{ Page(page: ${p}, perPage: 50) { media(type: ANIME, status: RELEASING, sort: POPULARITY_DESC, isAdult: false) { ${F} } } }`)
        .then((d) => d?.Page?.media || []).catch(() => [])));
    const now = Date.now() / 1000, week = now + 7 * 86400;
    const eps = [];
    for (const a of pages.flat()) {
      for (const ed of a.airingSchedule?.edges || []) {
        const n = ed.node;
        if (n && n.airingAt >= now - 86400 && n.airingAt <= week) {
          eps.push({ at: n.airingAt, ep: n.episode, id: a.id, title: a.title?.romaji || a.title?.english, cover: a.coverImage?.large });
        }
      }
      const nx = a.nextAiringEpisode;
      if (nx && nx.airingAt <= week && nx.airingAt >= now - 3600
          && !eps.some((x) => x.id === a.id && x.ep === nx.episode)) {
        eps.push({ at: nx.airingAt, ep: nx.episode, id: a.id, title: a.title?.romaji || a.title?.english, cover: a.coverImage?.large });
      }
    }
    eps.sort((a, b) => a.at - b.at);
  if (!eps.length) { box.innerHTML = '<p style="color:#8ba0b0;">No episodes in the next 7 days.</p>'; return; }
  const days = {};
  for (const e of eps) {
    const d = weekdayOf(e.at) + ' · ' + new Date(e.at * 1000).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    (days[d] = days[d] || []).push(e);
  }
  box.innerHTML = Object.entries(days).map(([d, list]) => `
    <div class="sched-day"><h3>${esc(d)} (${list.length})</h3>` +
    list.map((e) => `<div class="sched-row" onclick="openDetail(${e.id})">
      <span class="time">${timeOf(e.at)} · ${fmtCountdown(e.at)}</span>
      <img loading="lazy" src="${esc(https(e.cover || ''))}" ${imgErr}>
      <span>${esc(e.title || 'Unknown')}</span><span class="ep">EP ${e.ep ?? '?'}</span>
    </div>`).join('') + `</div>`).join('');
  } catch (e) {
    box.innerHTML = `<p class="error">Schedule failed to load: ${esc(e.message)} — retry in a few seconds (cold start).</p>`;
  }
}

function relCard(r) {
  const n = r.node || {};
  const t = n.title || {};
  const title = t.romaji || t.english || 'Unknown';
  return `<div class="mini" onclick="openDetail(${n.id})" title="${esc(r.relationType || '')}">
    <img loading="lazy" src="${esc(https(n.coverImage?.large || ''))}" ${imgErr}>
    <p>${esc(title)}</p><span>${esc((r.relationType || '').replace(/_/g, ' '))}</span>
  </div>`;
}
function recCard(r) {
  const node = r.node || {};
  const m = node.mediaRecommendation || node;
  const t = m.title || {};
  const title = t.romaji || t.english || 'Unknown';
  return `<div class="mini" onclick="openDetail(${m.id})">
    <img loading="lazy" src="${esc(https(m.coverImage?.large || ''))}" ${imgErr}>
    <p>${esc(title)}</p><span>${node.rating ? '★ ' + node.rating : ''}</span>
  </div>`;
}
function charStrip(a) {
  const edges = a.characters?.edges || [];
  if (!edges.length) return '';
  return `<h3 style="color:#e5c07b;margin:1rem 0 0.3rem;">Characters</h3><div class="strip">` +
    edges.slice(0, 15).map((e) => {
      const n = e.node || {};
      const va = (e.voiceActors || []).find((v) => v.language === 'JAPANESE') || (e.voiceActors || [])[0];
      return `<div class="mini char"><img loading="lazy" src="${esc(https(n.image?.large || ''))}" ${imgErr}>
        <p>${esc(n.name?.full || '?')}</p><span>${esc(e.role || '')}${va ? ' · ' + esc(va.name?.full || '') : ''}</span></div>`;
    }).join('') + `</div>`;
}

async function openDetail(id) {
  const back = document.getElementById('modalBack');
  const box = document.getElementById('modalBox');
  back.classList.add('open');
  document.body.style.overflow = 'hidden';
  box.innerHTML = '<div class="mbody"><div class="loading">Loading...</div></div>';
  const a = await loadAnimeById(id);
  if (!a) { box.innerHTML = '<div class="mbody"><p class="error">Anime not found.</p></div>'; return; }
  const t = a.title || {};
  const title = t.romaji || t.english || 'Unknown';
  const studios = (a.studios?.edges || []).map((e) => e.node?.name).filter(Boolean);
  const rels = (a.relations?.edges || []).filter((e) => e.node?.id);
  const recs = (a.recommendations?.edges || []).filter((e) => (e.node?.mediaRecommendation || e.node)?.id);
  const tags = (a.tags || []).slice().sort((x, y) => (y.rank || 0) - (x.rank || 0)).slice(0, 14);
  const streams = a.streamingEpisodes || [];
  const links = a.externalLinks || [];
  box.innerHTML = `
    <div class="banner" style="background-image:url('${esc(https(a.bannerImage || a.coverImage?.extraLarge || ''))}')"></div>
    <div class="mbody">
      <button class="mclose" onclick="closeDetail()">✕</button>
      <div class="mhead">
        <img class="cover" src="${esc(https(a.coverImage?.large || ''))}" ${imgErr}>
        <div><h2>${esc(title)}</h2>
          ${t.english && t.english !== title ? `<p class="alt">${esc(t.english)}</p>` : ''}
          ${t.native ? `<p class="alt">${esc(t.native)}</p>` : ''}
          <div class="chips">
            ${a.averageScore ? `<span class="chip score">${a.averageScore}%</span>` : ''}
            <span class="chip">${esc(a.format || '?')}</span>
            <span class="chip">${esc(a.status || '?')}</span>
            <span class="chip">${esc(a.season || '')} ${a.seasonYear || ''}</span>
            <span class="chip">${(a.popularity || 0).toLocaleString()} users</span>
          </div>
        </div>
      </div>
      ${a.description ? `<p class="desc">${esc(stripHtml(a.description)).slice(0, 1200)}</p>` : ''}
      <div class="mgrid">
        <div><b>Episodes</b>${a.episodes ?? '?'}</div>
        <div><b>Duration</b>${a.duration ? a.duration + ' min' : '?'}</div>
        <div><b>Source</b>${esc(a.source || '?')}</div>
        <div><b>Studio</b>${esc(studios.slice(0, 2).join(', ') || '?')}</div>
        <div><b>Hashtag</b>${esc(a.hashtag || '-')}</div>
        <div><b>AniList</b><a href="${esc(a.siteUrl || '')}" target="_blank" style="color:#3db4f2">open ↗</a></div>
      </div>
      ${(a.genres || []).length ? `<div class="chips">${a.genres.map((g) => `<span class="chip">${esc(g)}</span>`).join('')}</div>` : ''}
      ${tags.length ? `<div class="chips">${tags.map((g) => `<span class="chip" title="${esc(g.description || '')}">${esc(g.name)}${g.rank ? ' ' + g.rank + '%' : ''}</span>`).join('')}</div>` : ''}
      ${a.trailer?.site === 'youtube' ? `<p><a href="https://www.youtube.com/watch?v=${esc(a.trailer.id)}" target="_blank" style="color:#e74c3c">▶ Trailer</a></p>` : ''}
      ${charStrip(a)}
      ${rels.length ? `<h3 style="color:#e5c07b;margin:1rem 0 0.3rem;">Relations</h3><div class="strip">${rels.slice(0, 15).map(relCard).join('')}</div>` : ''}
      ${recs.length ? `<h3 style="color:#e5c07b;margin:1rem 0 0.3rem;">Similar Anime — Recommendations</h3><div class="strip">${recs.slice(0, 15).map(recCard).join('')}</div>` : ''}
      ${streams.length ? `<h3 style="color:#e5c07b;margin:1rem 0 0.3rem;">Watch</h3><div class="chips">${streams.slice(0, 8).map((s) => `<a class="chip" href="${esc(s.url || '')}" target="_blank">${esc(s.site || 'Stream')}</a>`).join('')}</div>` : ''}
      ${links.length ? `<h3 style="color:#e5c07b;margin:1rem 0 0.3rem;">Links</h3><div class="chips">${links.slice(0, 10).map((s) => `<a class="chip" href="${esc(s.url || '')}" target="_blank">${esc(s.site || 'Link')}</a>`).join('')}</div>` : ''}
    </div>`;
  back.scrollTop = 0;
}
function closeDetail() {
  document.getElementById('modalBack')?.classList.remove('open');
  document.body.style.overflow = '';
}
// Backwards-compat: old cards call showAnimeDetail
async function showAnimeDetail(id) { return openDetail(id); }

async function executeQuery() {
    const query = document.getElementById('queryInput').value;
    const output = document.getElementById('queryOutput');
    output.innerHTML = '<span style="color: #8ba0b0;">Executing...</span>';

    try {
        // Single hop straight to the edge API (exact schema, Turso-fast).
        const r = await fetch(VERCEL_API, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query }),
        });
        const result = await r.json();
        output.textContent = JSON.stringify(result, null, 2);
    } catch (e) {
        output.innerHTML = `<span class="error">${esc(e.message)}</span>`;
    }
}

function loadSampleQuery(type) {
    const queries = {
        popular: `{
  Page(page: 1, perPage: 10) {
    media(sort: POPULARITY_DESC, type: ANIME) {
      id
      title {
        romaji
        english
      }
      averageScore
      popularity
      episodes
      status
      format
      coverImage {
        large
      }
    }
    pageInfo {
      total
      hasNextPage
    }
  }
}`,
        search: `{
  Page(page: 1, perPage: 5) {
    media(search: "one piece", type: ANIME) {
      id
      title {
        romaji
        english
      }
      episodes
      averageScore
      genres
    }
  }
}`,
        filter: `{
  Page(page: 1, perPage: 10) {
    media(genre: "Action", seasonYear: 2024, sort: SCORE_DESC, type: ANIME) {
      id
      title {
        romaji
        english
      }
      averageScore
      format
      status
      coverImage {
        large
      }
    }
  }
}`,
        details: `{
  Media(id: 21) {
    id
    idMal
    title { romaji english native userPreferred }
    type format status
    description
    startDate { year month day }
    endDate { year month day }
    season seasonYear seasonInt
    episodes duration chapters volumes
    countryOfOrigin isLicensed source hashtag
    trailer { id site thumbnail }
    updatedAt
    coverImage { extraLarge large medium color }
    bannerImage
    genres synonyms
    averageScore meanScore popularity trending favourites
    isAdult siteUrl
    tags { id name description category rank isGeneralSpoiler isMediaSpoiler isAdult }
    studios { edges { isMain node { id name isAnimationStudio siteUrl } } }
    characters(page: 1, perPage: 5) { edges { role node { id name { full } } voiceActors { id name { full } language } } }
    staff(page: 1, perPage: 5) { edges { role node { id name { full } } } }
    relations { edges { relationType node { id title { romaji } } } }
    recommendations(page: 1, perPage: 5) { edges { node { rating mediaRecommendation { id title { romaji } } } } }
    nextAiringEpisode { episode airingAt timeUntilAiring mediaId }
    externalLinks { site url type }
    streamingEpisodes { title url site }
    rankings { rank type year season allTime }
    stats { scoreDistribution { score amount } statusDistribution { status amount } }
  }
}`
    };
    document.getElementById('queryInput').value = queries[type] || '';
}

function switchTab(tabName) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    document.querySelector(`[data-tab="${tabName}"]`)?.classList.add('active');
    document.getElementById(`tab-${tabName}`)?.classList.add('active');
    if (tabName === 'schedule') renderSchedule();
    if (tabName === 'discover') renderDiscover();
}

document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => switchTab(tab.dataset.tab));
});

init();
