#!/usr/bin/env node
// Local test server: runs api/index.js (the Vercel handler) as-is on :8787.
// Credentials come from .env when present — but the server also runs
// credential-free (shard/CDN path) so a fresh clone works out of the box.
//   node scripts/dev_server.mjs [port]
import http from 'node:http';
import { readFileSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const envPath = join(here, '..', '.env');
if (existsSync(envPath)) {
  for (const line of readFileSync(envPath, 'utf8').split('\n')) {
    const m = line.match(/^\s*([A-Z_]+)\s*=\s*(.+)\s*$/);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2].trim();
  }
  console.log('.env loaded');
} else {
  console.log('no .env found — running credential-free (shard/CDN path)');
}
process.env.TURSO_REQUIRED = process.env.TURSO_REQUIRED || '';

const { default: handler } = await import(join(here, '..', 'api', 'index.js'));

const port = Number(process.argv[2] || 8787);
const server = http.createServer(async (req, res) => {
  try {
    await handler(req, res);
  } catch (e) {
    res.writeHead(e.status || 500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ errors: [{ message: e.message }] }));
  }
});
server.listen(port, () => console.log(`local API on http://localhost:${port}/ (also /graphql, /api)`));
