// 운빨 망겜 체스 서버: 방 정보를 메모리에 저장하고, 접속한 사람들에게 실시간으로 전달해요.
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const srv = http.createServer((q, r) => {
  if (q.url == '/health') { r.end('ok'); return; }
  fs.readFile(path.join(__dirname, 'index.html'), (e, d) => {
    r.writeHead(e ? 500 : 200, { 'Content-Type': 'text/html; charset=utf-8' });
    r.end(e ? 'index.html 파일이 없어요' : d);
  });
});

const wss = new WebSocketServer({ server: srv, maxPayload: 65536 });
const docs = new Map(), subs = new Map();
const send = (w, o) => { if (w.readyState == 1) w.send(JSON.stringify(o)); };
const val = p => docs.has(p) ? docs.get(p).d : null;

wss.on('connection', ws => {
  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    const p = m && m.path;
    if (typeof p != 'string' || p.length > 80) return;
    if (m.op == 'sub') {
      if (!subs.has(p)) subs.set(p, new Set());
      subs.get(p).add(ws);
      send(ws, { op: 'snap', path: p, data: val(p) });
    } else if (m.op == 'get') {
      send(ws, { rid: m.rid, data: val(p) });
    } else if ((m.op == 'set' || m.op == 'upd') && m.data && typeof m.data == 'object') {
      if (docs.size >= 500 && !docs.has(p)) return send(ws, { rid: m.rid, err: 'full' });
      docs.set(p, { d: m.op == 'set' ? m.data : Object.assign({}, val(p), m.data), t: Date.now() });
      send(ws, { rid: m.rid });
      (subs.get(p) || []).forEach(w => send(w, { op: 'snap', path: p, data: val(p) }));
    }
  });
  ws.on('close', () => subs.forEach(s => s.delete(ws)));
});

// 끊긴 연결 정리 + 6시간 지난 방 삭제
setInterval(() => {
  wss.clients.forEach(w => { if (!w.alive) return w.terminate(); w.alive = false; w.ping(); });
  const old = Date.now() - 6 * 36e5;
  docs.forEach((v, k) => { if (v.t < old) docs.delete(k); });
}, 30000);

srv.listen(process.env.PORT || 3000, () => console.log('서버 켜짐'));
