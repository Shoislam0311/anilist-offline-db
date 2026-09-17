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
let shardCache = {};
let loadingShards = new Set();

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
        if (metadata.genres) {
            metadata.genres.forEach(g => {
                const opt = document.createElement('option');
                opt.value = g;
                opt.textContent = g;
                genreSelect.appendChild(opt);
            });
        }
        browseAnime();
    } catch (e) {
        console.error('Failed to init:', e);
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

async function loadShard(shardIdx) {
    const key = `shard_${String(shardIdx).padStart(4, '0')}`;
    if (shardCache[key]) return shardCache[key];
    if (loadingShards.has(key)) {
        while (loadingShards.has(key)) await new Promise(r => setTimeout(r, 50));
        return shardCache[key];
    }
    loadingShards.add(key);
    try {
        let resp = await fetch(`${API_BASE}/shards/${key}.json`);
        if (!resp.ok) {
          const data = await fetchFirst(API_BASES.map((b) => `${b}/shards/${key}.json`));
          shardCache[key] = data;
          return data;
        }
        shardCache[key] = await resp.json();
        return shardCache[key];
    } catch (e) {
        return [];
    } finally {
        loadingShards.delete(key);
    }
}

async function loadAllAnime() {
    if (!metadata) return [];
    const all = [];
    const totalShards = Math.ceil(metadata.totalAnime / metadata.shardSize);
    const promises = [];
    for (let i = 0; i < totalShards; i++) {
        promises.push(loadShard(i));
    }
    const shards = await Promise.all(promises);
    shards.forEach(shard => all.push(...shard));
    return all;
}

async function loadAnimeById(id) {
    if (!metadata) return null;
    let shardIdx = 0;
    if (metadata.shardStartIds) {
        const starts = metadata.shardStartIds;
        let lo = 0, hi = starts.length - 1;
        while (lo < hi) {
            const mid = (lo + hi + 1) >> 1;
            if (starts[mid] <= id) lo = mid; else hi = mid - 1;
        }
        shardIdx = lo;
    } else {
        shardIdx = Math.floor(id / metadata.shardSize);
    }
    const shard = await loadShard(shardIdx);
    return shard.find(a => a.id === id) || null;
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
function matchExact(e, full, a) {
  if (a.type && a.type !== 'ANIME') return false;
  if (a.id !== undefined && e.id !== a.id) return false;
  if (a.id_in && !a.id_in.includes(e.id)) return false;
  if (a.id_not !== undefined && e.id === a.id_not) return false;
  if (a.id_not_in && a.id_not_in.includes(e.id)) return false;
  if (a.idMal !== undefined && e.idMal !== a.idMal) return false;
  if (a.search) {
    const q = String(a.search).toLowerCase();
    const hay = [e.romaji, e.english, e.native, ...(e.synonyms || [])].filter(Boolean).map((x) => String(x).toLowerCase());
    if (!hay.some((h) => h.includes(q))) return false;
  }
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
        const q = String(args.search).toLowerCase();
        const hit = index.find((e) => [e.romaji, e.english, e.native].filter(Boolean).some((t) => String(t).toLowerCase().includes(q)));
        if (hit) anime = await loadAnimeById(hit.id);
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

async function searchAnime() {
    const query = document.getElementById('searchInput').value.trim();
    if (!query) return;

    const resultsDiv = document.getElementById('searchResults');
    resultsDiv.innerHTML = '<div class="loading">Searching...</div>';

    const index = await loadSearchIndex();
    const q = query.toLowerCase();
    const matches = index.filter(a =>
        (a.romaji || '').toLowerCase().includes(q) ||
        (a.english || '').toLowerCase().includes(q) ||
        (a.native || '').includes(q) ||
        (a.synonyms || []).some((s) => String(s).toLowerCase().includes(q))
    ).slice(0, 20);

    if (!matches.length) {
        resultsDiv.innerHTML = '<p style="color: #8ba0b0;">No results found.</p>';
        return;
    }

    resultsDiv.innerHTML = matches.map(a => `
        <div class="result-item" onclick="showAnimeDetail(${a.id})">
            <img src="${a.cover || ''}" alt="${a.romaji}" onerror="this.src='data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 140%22><rect fill=%22%23253746%22 width=%22100%22 height=%22140%22/></svg>'">
            <div class="result-info">
                <h3>${a.romaji || 'Unknown'}</h3>
                ${a.english && a.english !== a.romaji ? `<p>${a.english}</p>` : ''}
                <div class="result-meta">
                    <span>Score: ${a.score || 'N/A'}</span>
                    <span>Popularity: ${(a.popularity || 0).toLocaleString()}</span>
                    <span>Eps: ${a.episodes || '?'}</span>
                    <span>${a.format || ''}</span>
                    <span>${a.status || ''}</span>
                </div>
            </div>
        </div>
    `).join('');
}

async function browseAnime() {
    const genre = document.getElementById('browseGenre').value;
    const status = document.getElementById('browseStatus').value;
    const format = document.getElementById('browseFormat').value;
    const sort = document.getElementById('browseSort').value;

    const resultsDiv = document.getElementById('browseResults');
    resultsDiv.innerHTML = '<div class="loading">Loading...</div>';

    const index = await loadSearchIndex();
    let hits = index.filter((e) => {
      if (genre && !(e.genres || []).includes(genre)) return false;
      if (status && e.status !== status) return false;
      if (format && e.format !== format) return false;
      return true;
    });
    // sort via exact field using hydrated page (top 50 only for speed)
    const ids = hits.slice(0, 200).map((e) => e.id);
    let media = [];
    for (const id of ids.slice(0, 50)) { const a = await loadAnimeById(id); if (a) media.push(a); }
    sortMedia(media, sort);

    resultsDiv.innerHTML = media.map(a => `
        <div class="result-item" onclick="showAnimeDetail(${a.id})">
            <img src="${a.coverImage?.large || ''}" alt="${a.title?.romaji}" onerror="this.src='data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 140%22><rect fill=%22%23253746%22 width=%22100%22 height=%22140%22/></svg>'">
            <div class="result-info">
                <h3>${a.title?.romaji || 'Unknown'}</h3>
                ${a.title?.english && a.title.english !== a.title.romaji ? `<p>${a.title.english}</p>` : ''}
                <div class="result-meta">
                    <span>Score: ${a.averageScore || 'N/A'}</span>
                    <span>Popularity: ${(a.popularity || 0).toLocaleString()}</span>
                    <span>Eps: ${a.episodes || '?'}</span>
                    <span>${a.format || ''}</span>
                    <span>${a.season || ''} ${a.seasonYear || ''}</span>
                </div>
            </div>
        </div>
    `).join('');
}

async function showAnimeDetail(id) {
    const query = `{
        Media(id: ${id}) {
            id
            title { romaji english native userPreferred }
            description
            coverImage { extraLarge large medium color }
            bannerImage
            episodes
            duration
            status
            format
            season
            seasonYear
            averageScore
            meanScore
            popularity
            favourites
            genres
            synonyms
            siteUrl
        }
    }`;
    const result = await executeGraphQL(query);
    const media = result.data?.Media;
    if (media) {
        document.getElementById('queryOutput').textContent = JSON.stringify({ data: { Media: media } }, null, 2);
        switchTab('playground');
    }
}

async function executeQuery() {
    const query = document.getElementById('queryInput').value;
    const output = document.getElementById('queryOutput');
    output.innerHTML = '<span style="color: #8ba0b0;">Executing...</span>';

    try {
        const result = await executeGraphQL(query);
        output.textContent = JSON.stringify(result, null, 2);
    } catch (e) {
        output.innerHTML = `<span class="error">${e.message}</span>`;
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
}

document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => switchTab(tab.dataset.tab));
});

init();
