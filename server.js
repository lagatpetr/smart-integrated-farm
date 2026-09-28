// Smart Farm cloud server: serves index.html, tracks devices, relays commands, status and camera frames.
// Env vars: DEVICE_KEY (secret used by both ESP32 boards), DASH_PIN (PIN needed to send commands or watch the camera)
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const KEY  = process.env.DEVICE_KEY || 'change-this-device-key';
const PIN  = process.env.DASH_PIN || '1234';
const NAMES = { main: 'Main controller (ESP32)', cam: 'Rabbit camera (ESP32-CAM)' };
const dev = { main: { ws: null, seen: 0, info: {} }, cam: { ws: null, seen: 0, info: {} } };
let last = null;

const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.end('ok'); return; }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(fs.readFileSync(path.join(__dirname, 'index.html')));
});

const wss = new WebSocketServer({ noServer: true });
const send = (ws, o) => ws && ws.readyState === 1 && ws.send(typeof o === 'string' ? o : JSON.stringify(o));
const webs = () => [...wss.clients].filter(c => c.role === 'web');
const list = () => Object.keys(dev).map(id => ({
  id, name: NAMES[id], online: !!dev[id].ws, info: dev[id].info,
  ago: dev[id].seen ? Math.round((Date.now() - dev[id].seen) / 1000) : null
}));
const pushList = () => webs().forEach(c => send(c, { type: 'devices', list: list() }));
const syncStream = () => send(dev.cam.ws, 'stream:' + (webs().some(c => c.watch) ? 1 : 0));

server.on('upgrade', (req, sock, head) => {
  const u = new URL(req.url, 'http://x');
  const isDev = u.pathname === '/device', id = u.searchParams.get('id');
  if ((!isDev && u.pathname !== '/ws') || (isDev && (u.searchParams.get('key') !== KEY || !dev[id]))) { sock.destroy(); return; }
  wss.handleUpgrade(req, sock, head, ws => { ws.role = isDev ? 'device' : 'web'; ws.id = id; wss.emit('connection', ws); });
});

wss.on('connection', ws => {
  ws.alive = true;
  ws.on('pong', () => (ws.alive = true));

  if (ws.role === 'device') {
    const d = dev[ws.id];
    if (d.ws) d.ws.terminate();
    d.ws = ws; d.seen = Date.now(); d.info = {};
    pushList();
    if (ws.id === 'cam') syncStream();
    ws.on('message', (data, isBin) => {
      d.seen = Date.now();
      if (ws.id === 'cam' && isBin) { webs().forEach(c => c.watch && c.bufferedAmount < 300000 && c.send(data, { binary: true })); return; }
      try {
        const m = JSON.parse(String(data));
        if (ws.id === 'main') { last = m; d.info = { rssi: m.rssi, up: m.up }; webs().forEach(c => send(c, { type: 'status', s: m })); }
        else d.info = m;
      } catch (e) {}
    });
    ws.on('close', () => { if (d.ws === ws) { d.ws = null; pushList(); } });
  } else {
    send(ws, { type: 'devices', list: list() });
    if (last) send(ws, { type: 'status', s: last });
    ws.on('message', d => {
      try {
        const m = JSON.parse(String(d));
        if (m.pin !== PIN) return send(ws, { error: 'Wrong PIN' });
        const cmd = String(m.cmd).slice(0, 20);
        if (cmd.startsWith('watch:')) { ws.watch = cmd.endsWith('1'); return syncStream(); }
        const target = cmd.startsWith('flash:') ? dev.cam.ws : dev.main.ws;
        if (!target) return send(ws, { error: 'Device is offline' });
        send(target, cmd);
      } catch (e) {}
    });
    ws.on('close', () => setTimeout(syncStream, 0));
  }
});

setInterval(() => wss.clients.forEach(c => { if (!c.alive) return c.terminate(); c.alive = false; c.ping(); }), 25000);
setInterval(pushList, 5000);
server.listen(PORT, () => console.log('Smart Farm server on port ' + PORT));
