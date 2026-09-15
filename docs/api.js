/**
 * AniList Offline API - Client-side GraphQL Engine
 * Resolves GraphQL queries against pre-built JSON data shards.
 * Zero rate limits. Same schema as graphql.anilist.co.
 */

const API_BASE = 'api';
let metadata = null;
let searchIndex = null;
let shardCache = {};
let loadingShards = new Set();

async function init() {
    try {
        const resp = await fetch(`${API_BASE}/metadata.json`);
        metadata = await resp.json();
        document.getElementById('apiEndpoint').textContent = window.location.origin + '/' + API_BASE + '/graphql';
        document.getElementById('docBaseUrl').textContent = window.location.origin + '/' + API_BASE + '/graphql';
        document.getElementById('statTotal').textContent = metadata.totalAnime.toLocaleString();
        document.getElementById('statCharacters').textContent = metadata.totalCharacters.toLocaleString();
        document.getElementById('statStudios').textContent = metadata.totalStudios.toLocaleString();
        document.getElementById('statGenres').textContent = metadata.totalGenres || metadata.genres?.length || 0;

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
        searchIndex = await resp.json();
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
        const resp = await fetch(`${API_BASE}/shards/${key}.json`);
        if (!resp.ok) return [];
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

function parseQuery(query) {
    const trimmed = query.trim();
    const match = trimmed.match(/^\{?\s*query\s*\(([^)]*)\)\s*\{([\s\S]*)\}\s*\}?$/);
    if (match) {
        return { variables: parseVariables(match[1]), selections: parseSelections(match[2]) };
    }
    const match2 = trimmed.match(/^\{?\s*(\w+)\s*(?:\(([^)]*)\))?\s*\{([\s\S]*)\}\s*\}?$/);
    if (match2) {
        return { operation: match2[1], variables: parseVariables(match2[2] || ''), selections: parseSelections(match2[3]) };
    }
    return { selections: parseSelections(trimmed.replace(/^\{|\}$/g, '')) };
}

function parseVariables(varStr) {
    const vars = {};
    if (!varStr.trim()) return vars;
    const regex = /(\w+)\s*:\s*(?:"([^"]*)"|(\d+)|(\w+))/g;
    let m;
    while ((m = regex.exec(varStr))) {
        if (m[2] !== undefined) vars[m[1]] = m[2];
        else if (m[3] !== undefined) vars[m[1]] = parseInt(m[3]);
        else if (m[4] !== undefined) {
            if (m[4] === 'true') vars[m[1]] = true;
            else if (m[4] === 'false') vars[m[1]] = false;
            else vars[m[1]] = m[4];
        }
    }
    return vars;
}

function parseSelections(selStr) {
    const selections = [];
    const lines = selStr.split('\n').map(l => l.trim()).filter(l => l);
    const stack = [{ children: selections, indent: -1 }];

    for (const line of lines) {
        const indent = line.search(/\S/);
        const clean = line.replace(/\s*{.*$/, '');
        const hasChildren = line.includes('{');

        while (stack.length > 1 && stack[stack.length - 1].indent >= indent) {
            stack.pop();
        }

        const field = { name: clean.split('(')[0].split(':')[0].trim(), args: {}, children: null };
        const argsMatch = clean.match(/\(([^)]+)\)/);
        if (argsMatch) {
            field.args = parseVariables(argsMatch[1]);
        }
        const aliasMatch = clean.match(/^(\w+)\s*:\s*(\w+)/);
        if (aliasMatch) {
            field.alias = aliasMatch[1];
            field.name = aliasMatch[2];
        }
        if (hasChildren) {
            field.children = [];
            stack[stack.length - 1].children.push(field);
            stack.push({ children: field.children, indent: indent + 2 });
        } else {
            stack[stack.length - 1].children.push(field);
        }
    }
    return selections;
}

function resolveField(obj, fieldName) {
    if (obj === null || obj === undefined) return null;
    const aliases = { titleRomaji: 'title_romaji', titleEnglish: 'title_english', titleNative: 'title_native',
        coverLarge: 'cover_large', bannerImage: 'banner_image', averageScore: 'average_score',
        meanScore: 'mean_score', seasonYear: 'season_year', nextAiringEpisode: 'next_airing_episode',
        nextAiringAt: 'next_airing_at', startDate: 'start_date', endDate: 'end_date',
        countryOfOrigin: 'country_of_origin', isAdult: 'is_adult', createdAt: 'created_at',
        updatedAt: 'updated_at', idMal: 'id_mal' };

    if (fieldName in obj) return obj[fieldName];
    if (fieldName in aliases && aliases[fieldName] in obj) return obj[aliases[fieldName]];
    if (camelToSnake(fieldName) in obj) return obj[camelToSnake(fieldName)];
    return null;
}

function camelToSnake(str) {
    return str.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);
}

function resolveSelections(data, selections) {
    if (!selections || !selections.length) return data;
    const result = {};
    for (const sel of selections) {
        const key = sel.alias || sel.name;
        if (sel.name === 'id' || sel.name === 'romaji' || sel.name === 'english' || sel.name === 'native') {
            if (sel.children) {
                if (typeof data === 'object' && !Array.isArray(data)) {
                    result[key] = resolveSelections(data, sel.children);
                }
            } else {
                result[key] = resolveField(data, sel.name);
            }
        } else if (sel.children) {
            const childData = resolveField(data, sel.name);
            if (sel.name === 'title') {
                result[key] = {
                    romaji: resolveField(data, 'title_romaji') || resolveField(data, 'romaji'),
                    english: resolveField(data, 'title_english') || resolveField(data, 'english'),
                    native: resolveField(data, 'title_native') || resolveField(data, 'native')
                };
            } else if (sel.name === 'coverImage') {
                result[key] = {
                    large: resolveField(data, 'cover_large'),
                    color: resolveField(data, 'cover_color'),
                    medium: resolveField(data, 'cover_large')
                };
            } else if (sel.name === 'relations' && data.relations) {
                result[key] = { edges: data.relations.map(r => ({ node: r, relationType: r.relationType })) };
            } else if (sel.name === 'recommendations' && data.recommendations) {
                result[key] = { edges: data.recommendations.map(r => ({ node: { mediaRecommendation: r, rating: r.rating } })) };
            } else if (sel.name === 'characters' && data.characters) {
                result[key] = { edges: data.characters.map(c => ({ node: c, role: c.role })) };
            } else if (sel.name === 'studios' && data.studios) {
                result[key] = { edges: data.studios.map(s => ({ node: s, isMain: s.isMain })) };
            } else if (sel.name === 'tags' && data.tags) {
                result[key] = data.tags;
            } else if (sel.name === 'airingSchedule' && data.airingSchedule) {
                result[key] = { edges: data.airingSchedule.map(a => ({ node: a })) };
            } else if (sel.name === 'nextAiringEpisode') {
                result[key] = data.nextAiringEpisode || null;
            } else if (sel.name === 'startDate') {
                result[key] = data.startDate ? { year: parseInt(data.startDate?.substring(0,4)), month: parseInt(data.startDate?.substring(5,7)), day: parseInt(data.startDate?.substring(8,10)) } : null;
            } else if (sel.name === 'endDate') {
                result[key] = data.endDate ? { year: parseInt(data.endDate?.substring(0,4)), month: parseInt(data.endDate?.substring(5,7)), day: parseInt(data.endDate?.substring(8,10)) } : null;
            } else {
                const childData = resolveField(data, sel.name);
                if (Array.isArray(childData)) {
                    result[key] = childData.map(item => resolveSelections(item, sel.children));
                } else if (childData) {
                    result[key] = resolveSelections(childData, sel.children);
                } else {
                    result[key] = null;
                }
            }
        } else {
            result[key] = resolveField(data, sel.name);
        }
    }
    return result;
}

function sortMedia(media, sortBy) {
    const sortMap = {
        'POPULARITY_DESC': (a, b) => (b.popularity || 0) - (a.popularity || 0),
        'POPULARITY': (a, b) => (a.popularity || 0) - (b.popularity || 0),
        'SCORE_DESC': (a, b) => (b.average_score || b.averageScore || 0) - (a.average_score || a.averageScore || 0),
        'SCORE': (a, b) => (a.average_score || a.averageScore || 0) - (b.average_score || b.averageScore || 0),
        'UPDATED_AT_DESC': (a, b) => (b.updated_at || b.updatedAt || 0) - (a.updated_at || a.updatedAt || 0),
        'UPDATED_AT': (a, b) => (a.updated_at || a.updatedAt || 0) - (b.updated_at || b.updatedAt || 0),
        'START_DATE_DESC': (a, b) => (b.start_date || '').localeCompare(a.start_date || ''),
        'START_DATE': (a, b) => (a.start_date || '').localeCompare(b.start_date || ''),
        'FAVOURITES_DESC': (a, b) => (b.favourites || 0) - (a.favourites || 0),
        'TRENDING_DESC': (a, b) => (b.trending || 0) - (a.trending || 0),
        'ID_DESC': (a, b) => b.id - a.id,
        'ID': (a, b) => a.id - b.id,
        'TITLE_ENGLISH_DESC': (a, b) => (b.title_english || b.title_romaji || '').localeCompare(a.title_english || a.title_romaji || ''),
        'TITLE_ROMAJI_DESC': (a, b) => (b.title_romaji || '').localeCompare(a.title_romaji || ''),
    };
    return media.sort(sortMap[sortBy] || sortMap['POPULARITY_DESC']);
}

function filterMedia(media, vars) {
    let filtered = [...media];
    if (vars.search) {
        const q = vars.search.toLowerCase();
        filtered = filtered.filter(a =>
            (a.title_romaji || '').toLowerCase().includes(q) ||
            (a.title_english || '').toLowerCase().includes(q) ||
            (a.title_native || '').includes(q)
        );
    }
    if (vars.genre) {
        filtered = filtered.filter(a => a.genres && a.genres.includes(vars.genre));
    }
    if (vars.format) {
        filtered = filtered.filter(a => a.format === vars.format);
    }
    if (vars.status) {
        filtered = filtered.filter(a => a.status === vars.status);
    }
    if (vars.season) {
        filtered = filtered.filter(a => a.season === vars.season);
    }
    if (vars.seasonYear) {
        filtered = filtered.filter(a => a.season_year === vars.seasonYear);
    }
    if (vars.id) {
        filtered = filtered.filter(a => a.id === vars.id);
    }
    if (vars.id_in) {
        const ids = Array.isArray(vars.id_in) ? vars.id_in : [vars.id_in];
        filtered = filtered.filter(a => ids.includes(a.id));
    }
    if (vars.type && vars.type !== 'ANIME') {
        return [];
    }
    return filtered;
}

async function executeGraphQL(query, variables = {}) {
    const parsed = parseQuery(query);
    const allVars = { ...parsed.variables, ...variables };

    if (parsed.operation === 'query' || parsed.selections?.[0]?.name === 'Page' || !parsed.operation) {
        let media = await loadAllAnime();
        media = filterMedia(media, allVars);

        const sort = allVars.sort || 'POPULARITY_DESC';
        media = sortMedia(media, sort);

        const page = allVars.page || 1;
        const perPage = Math.min(allVars.perPage || 10, 50);
        const start = (page - 1) * perPage;
        const paged = media.slice(start, start + perPage);

        const mediaSelections = parsed.selections?.[0]?.children?.find(s => s.name === 'media');
        const resolvedMedia = paged.map(item => resolveSelections(item, mediaSelections?.children));

        return {
            data: {
                Page: {
                    media: resolvedMedia,
                    pageInfo: {
                        total: media.length,
                        perPage: perPage,
                        currentPage: page,
                        lastPage: Math.ceil(media.length / perPage),
                        hasNextPage: start + perPage < media.length,
                        hasPreviousPage: page > 1
                    }
                }
            }
        };
    }

    return { errors: [{ message: 'Unknown operation' }] };
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
        (a.native || '').includes(q)
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

    let media = await loadAllAnime();
    if (genre) media = media.filter(a => a.genres && a.genres.includes(genre));
    if (status) media = media.filter(a => a.status === status);
    if (format) media = media.filter(a => a.format === format);
    media = sortMedia(media, sort);
    media = media.slice(0, 50);

    resultsDiv.innerHTML = media.map(a => `
        <div class="result-item" onclick="showAnimeDetail(${a.id})">
            <img src="${a.cover_large || ''}" alt="${a.title_romaji}" onerror="this.src='data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 140%22><rect fill=%22%23253746%22 width=%22100%22 height=%22140%22/></svg>'">
            <div class="result-info">
                <h3>${a.title_romaji || 'Unknown'}</h3>
                ${a.title_english && a.title_english !== a.title_romaji ? `<p>${a.title_english}</p>` : ''}
                <div class="result-meta">
                    <span>Score: ${a.average_score || 'N/A'}</span>
                    <span>Popularity: ${(a.popularity || 0).toLocaleString()}</span>
                    <span>Eps: ${a.episodes || '?'}</span>
                    <span>${a.format || ''}</span>
                    <span>${a.season || ''} ${a.season_year || ''}</span>
                </div>
            </div>
        </div>
    `).join('');
}

async function showAnimeDetail(id) {
    const anime = await loadAnimeById(id);
    if (!anime) return;
    const query = `{
        Media(id: ${id}) {
            id
            title { romaji english native }
            description
            coverImage { large color }
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
            tags { name rank }
            studios { edges { node { name } isMain } }
            characters { edges { node { name full } role } }
            relations { edges { relationType node { id title { romaji } } } }
            nextAiringEpisode { episode airingAt }
            startDate { year month day }
            endDate { year month day }
        }
    }`;
    const result = await executeGraphQL(query);
    const media = result.data?.Page?.media?.[0] || result.data?.Media;
    if (media) {
        const output = JSON.stringify({ data: { Media: media } }, null, 2);
        document.getElementById('queryOutput').textContent = output;
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
  Page(page: 1, perPage: 5, search: "one piece") {
    media(type: ANIME) {
      id
      title {
        romaji
        english
      }
      episodes
      averageScore
      genres
      studios {
        edges {
          node {
            name
          }
          isMain
        }
      }
    }
  }
}`,
        filter: `{
  Page(page: 1, perPage: 10, genre: "Psychological", seasonYear: 2024, sort: SCORE_DESC) {
    media(type: ANIME) {
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
  Page(page: 1, perPage: 1, id: 16498) {
    media {
      id
      title {
        romaji
        english
        native
      }
      description
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
      trending
      genres
      tags {
        name
        rank
      }
      studios {
        edges {
          node {
            name
          }
          isMain
        }
      }
      characters {
        edges {
          node {
            name {
              full
            }
          }
          role
        }
      }
      relations {
        edges {
          relationType
          node {
            id
            title {
              romaji
            }
          }
        }
      }
      recommendations {
        edges {
          node {
            mediaRecommendation {
              id
              title {
                romaji
              }
            }
            rating
          }
        }
      }
      nextAiringEpisode {
        episode
        airingAt
      }
      startDate {
        year
        month
        day
      }
      endDate {
        year
        month
        day
      }
      coverImage {
        large
        color
      }
      bannerImage
    }
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
