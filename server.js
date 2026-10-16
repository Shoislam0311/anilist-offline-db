#!/usr/bin/env node
/* Standalone HTTP server for hosts without serverless functions
 * (Render/Railway/VPS). Delegates to the Vercel handler export, which uses
 * the standard (req, res) shape. `PORT` env is honored (Render injects it).
 */
import http from 'node:http';
import handler from './api/index.js';

const PORT = Number(process.env.PORT) || 3000;
const server = http.createServer((req, res) => {
  Promise.resolve(handler(req, res)).catch((e) => {
    try {
      res.writeHead(e?.status || 500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ errors: [{ message: e?.message || 'Internal error' }] }));
    } catch { /* socket already gone */ }
  });
});
server.listen(PORT, () => {
  console.log(`anilist mirror listening on :${PORT} (shard cache: ${process.env.SHARD_CACHE_SIZE || 12})`);
});
