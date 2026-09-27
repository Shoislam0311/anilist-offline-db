import http from 'node:http';
import { readFileSync, existsSync } from 'node:fs';
import { join, dirname, basename } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const FIXTURES = join(ROOT, 'test', 'fixtures');
const hits = [];
const fx = http.createServer((req, res) => {
  hits.push(req.url);
  const url = req.url.split('?')[0];
  let file = null;
  if (url === '/metadata.json') file = join(FIXTURES, 'metadata.json');
  else if (url === '/search_index.json') file = join(ROOT, 'docs', 'api', 'search_index.json');
  else if (url.startsWith('/shards/')) { const c = join(FIXTURES, basename(url)); if (existsSync(c)) file = c; }
  if (!file || !existsSync(file)) { res.writeHead(404); return res.end('nf'); }
  const body = readFileSync(file);
  res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': body.length });
  res.end(body);
});
await new Promise((r) => fx.listen(0, '127.0.0.1', r));
const BASE = `http://127.0.0.1:${fx.address().port}`;
process.env.DATA_BASE_URL = BASE;
console.log('DATA_BASE_URL =', process.env.DATA_BASE_URL);

const { default: handler } = await import('../api/index.js?debug=1');
const srv = http.createServer((req, res) => Promise.resolve(handler(req, res)));
await new Promise((r) => srv.listen(0, '127.0.0.1', r));
const port = srv.address().port;

const post = async (body) => {
  const r = await fetch(`http://127.0.0.1:${port}/graphql`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body), signal: AbortSignal.timeout(30000),
  });
  return r.json();
};

const q = '{ Page(page: 1, perPage: 5) { media(sort: POPULARITY_DESC) { id title { romaji } popularity genres } pageInfo { total perPage lastPage } } }';
const out = await post({ query: q });
console.log('hits before:', hits);
console.log('pageInfo:', JSON.stringify(out?.data?.Page?.pageInfo));
console.log('media ids:', JSON.stringify(out?.data?.Page?.media?.map((m) => [m.id, m.title?.romaji, m.popularity])));
console.log('errors:', JSON.stringify(out?.errors));
console.log('hits after:', hits);

srv.close(); fx.close();
