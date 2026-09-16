// Simple test handler
export default async function handler(request) {
  return new Response(JSON.stringify({ 
    message: 'Hello from Vercel!',
    method: request.method,
    url: request.url,
    timestamp: new Date().toISOString()
  }), {
    status: 200,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
    },
  });
}
