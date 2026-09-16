const DATA_BASE = 'https://shoislam0311.github.io/anilist-offline-db/api';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Max-Age': '86400',
};

function json(data, status = 200) {
  return Response.json(data, { status, headers: CORS });
}

export async function OPTIONS() {
  return new Response(null, { status: 204, headers: CORS });
}

export async function GET() {
  try {
    const r = await fetch(`${DATA_BASE}/metadata.json`);
    if (!r.ok) return json({ errors: [{ message: 'Metadata unavailable' }] }, 502);
    return json(await r.json(), 200);
  } catch (e) {
    return json({ errors: [{ message: String(e?.message || e) }] }, 502);
  }
}
