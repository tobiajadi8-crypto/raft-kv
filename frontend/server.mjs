import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';

const port = 8780;
const nodes = [8001, 8002, 8003].map(port => 'http://127.0.0.1:' + port);
const pages = new Map([
  ['/', ['index.html', 'text/html']], ['/raft', ['index.html', 'text/html']],
  ['/style.css', ['style.css', 'text/css']], ['/forms.js', ['forms.js', 'text/javascript']]
]);

async function leader() {
  const states = await Promise.all(nodes.map(async url => {
    try {
      const response = await fetch(url + '/status', {signal: AbortSignal.timeout(1500)});
      if (response.ok && (await response.json()).role === 'Leader') return url;
    } catch { /* A stopped voter is expected during failover. */ }
  }));
  return states.find(Boolean);
}

async function handle(req, res) {
  function send(status, text, type = 'text/plain') {
    res.writeHead(status, {'Content-Type': type + '; charset=utf-8', 'Cache-Control': 'no-store'});
    res.end(text);
  }
  if (req.headers.host !== '127.0.0.1:' + port && req.headers.host !== 'localhost:' + port)
    return send(403, 'Local access only.');
  if (req.headers.origin && req.headers.origin !== 'http://' + req.headers.host)
    return send(403, 'Cross-origin requests are disabled.');
  const url = new URL(req.url, 'http://localhost');
  if (req.method === 'GET' && pages.has(url.pathname)) {
    const [file, type] = pages.get(url.pathname);
    return send(200, await readFile(new URL(file, import.meta.url)), type);
  }
  let target;
  if (url.pathname === '/api/kv' && ['PUT', 'GET'].includes(req.method)) {
    const key = url.searchParams.get('key') || '';
    if (!key.trim() || ['.', '..'].includes(key) || Buffer.byteLength(key) > 256)
      return send(400, 'Enter a key of 1–256 bytes, other than a single or double period.');
    const node = await leader();
    if (!node) return send(503, 'No leader available. Check the Raft nodes and try again.');
    target = node + '/kv/' + encodeURIComponent(key);
  } else return send(404, 'Page unavailable.');
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > 65536) return send(413, 'Input is too long.');
    chunks.push(chunk);
  }
  try {
    const upstream = await fetch(target, {
      method: req.method, body: req.method === 'GET' ? undefined : Buffer.concat(chunks),
      headers: {'Content-Type': 'text/plain'},
      signal: AbortSignal.timeout(12000)
    });
    res.writeHead(upstream.status, {'Content-Type': upstream.headers.get('content-type') || 'text/plain', 'Cache-Control':'no-store'});
    res.end(Buffer.from(await upstream.arrayBuffer()));
  } catch {
    send(503, 'Service unavailable. Check the project terminal and try again.');
  }
}
createServer((req, res) => handle(req, res).catch(() => {
  if (!res.headersSent) res.writeHead(500);
  res.end('Request failed.');
})).listen(port, '127.0.0.1', () => console.log('Frontend running at http://127.0.0.1:' + port));
