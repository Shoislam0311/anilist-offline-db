#!/usr/bin/env node
// Local test server: runs api/index.js (the Vercel handler) as-is on :8787
// with Turso creds from .env. Used to test changes before deploy.
import http from 'node:http';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const env = readFileSync(join(here, '..', '.env'), 'utf8');
for (const line of env.split('\n')) {
  const m = line.match(/^\s*([A-Z_]+)\s*=\s*(.+)\s*$/);
  if (m && !process.env[m[1]]) process.env[m[1]] = m[2].trim();
}
process.env.TURSO_REQUIRED = process.env.TURSO_REQUIRED || '';

const { default: handler } = await import(join(here, '..', 'api', 'index.js'));

const server = http.createServer(async (req, res) => {
  try {
    await handler(req, res);
  } catch (e) {
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ errors: [{ message: e.message }] }));
  }
});
server.listen(8787, () => console.log('local API on http://localhost:8787/graphql'));
