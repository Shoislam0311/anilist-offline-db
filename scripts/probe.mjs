// Ad-hoc probe: boots the real handler in-process and reports status/latency
// for the queries that matter. Run: node scripts/probe.mjs [port]
import http from 'node:http';

const port = Number(process.argv[2] || 8791);
const { default: handler } = await import(new URL('../api/index.js', import.meta.url));
const srv = http.createServer((req, res) =>
  Promise.resolve(handler(req, res)).catch((e) => {
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ errors: [{ message: e.message }] }));
  }));
await new Promise((r) => srv.listen(port, r));

const TIMEOUT_MS = Number(process.env.PROBE_TIMEOUT_MS || 25000);
const post = async (body, path = '/graphql', label = '') => {
  console.log(`... ${label || path}`);
  const t = Date.now();
  try {
    const r = await fetch(`http://localhost:${port}${path}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    const json = await r.json().catch(() => null);
    return { ms: Date.now() - t, status: r.status, json };
  } catch (e) { return { ms: Date.now() - t, status: 'ERR', json: { errors: [{ message: e.message }] } }; }
};
const get = async (u, label = '') => {
  console.log(`... ${label || u.slice(0, 40)}`);
  const t = Date.now();
  try {
    const r = await fetch(`http://localhost:${port}${u}`, { signal: AbortSignal.timeout(TIMEOUT_MS) });
    const json = await r.json().catch(() => null);
    return { ms: Date.now() - t, status: r.status, ct: r.headers.get('content-type'), json };
  } catch (e) { return { ms: Date.now() - t, status: 'ERR', json: { errors: [{ message: e.message }] } }; }
};

const show = (label, r, len = 220) =>
  console.log(`${label.padEnd(34)} -> ${r.status} ${String(r.ms + 'ms').padStart(7)}  ${JSON.stringify(r.json).slice(0, len)}`);

show('POST / (README root URL)', await post({ query: '{Page(page:1,perPage:1){media(sort:POPULARITY_DESC){id}}}' }, '/'));
show('POST /graphql cards', await post({ query: '{ Page(page:1, perPage:3){ media(sort:POPULARITY_DESC){ id title{romaji} averageScore } } }' }));
show('POST /graphql tag_not_in', await post({ query: '{ Page(page:1, perPage:3){ media(tag_not_in:["Action"]){ id } } }' }));
show('POST introspection __schema', await post({ query: '{ __schema { queryType { name } } }' }));
show('POST /graphql warm cards', await post({ query: '{ Page(page:1, perPage:3){ media(sort:POPULARITY_DESC){ id title{romaji} averageScore } } }' }));
show('GET /?query= cards', await get('/?query=' + encodeURIComponent('{ Page(page:1, perPage:2){ media(sort:SCORE_DESC){ id } } }')));
show('GET /?health=1', await get('/?health=1'));
show('POST Media(id) detail', await post({ query: '{ Media(id: 16498){ id title{romaji} description characters{edges{node{name{full}}}} } }' }));
show('GET / (no query = INFO)', await get('/'));

srv.close();
