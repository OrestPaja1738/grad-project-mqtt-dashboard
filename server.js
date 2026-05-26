// ─────────────────────────────────────────────────────────────
//  Environmental Station Dashboard — server.js
//  One process, four jobs:
//    1. MQTT subscriber   (receives records from the ESP32)
//    2. Dedup + SQLite    (stores unique records)
//    3. HTTP server        (serves the dashboard page + REST API)
//    4. WebSocket push     (live updates to every open browser)
// ─────────────────────────────────────────────────────────────

const http  = require('http');
const fs    = require('fs');
const path  = require('path');
const mqtt  = require('mqtt');
const Database = require('better-sqlite3');
const { WebSocketServer } = require('ws');

// ── Configuration ──────────────────────────────────────────
const MQTT_BROKER   = 'mqtt://192.168.0.4:1883';
const MQTT_TOPIC    = 'sensors/xiao01/readings';
const HTTP_PORT     = 3000;
const DB_PATH       = path.join(__dirname, 'data.db');
const HTML_PATH     = path.join(__dirname, 'index.html');

// ── SQLite setup ───────────────────────────────────────────
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');            // faster concurrent reads
db.pragma('synchronous = NORMAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS readings (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id     TEXT    NOT NULL,
    sequence      INTEGER NOT NULL,
    timestamp     TEXT    NOT NULL,
    temperature_c REAL,
    humidity_rh   REAL,
    co2_ppm       INTEGER,
    voc_index     INTEGER,
    temp_sources  INTEGER,
    hum_sources   INTEGER,
    received_at   TEXT    DEFAULT (datetime('now','localtime')),
    UNIQUE(device_id, sequence)
  );
`);

// Prepared statements (reused on every insert / query — faster than ad-hoc)
const insertStmt = db.prepare(`
  INSERT OR IGNORE INTO readings
    (device_id, sequence, timestamp, temperature_c, humidity_rh,
     co2_ppm, voc_index, temp_sources, hum_sources)
  VALUES
    (@device_id, @sequence, @timestamp, @temperature_c, @humidity_rh,
     @co2_ppm, @voc_index, @temp_sources, @hum_sources)
`);

const queryByHours = db.prepare(`
  SELECT * FROM readings
  WHERE received_at >= datetime('now', 'localtime', '-' || ? || ' hours')
  ORDER BY id ASC
`);

const queryLatest = db.prepare(`
  SELECT * FROM readings ORDER BY id DESC LIMIT 1
`);

// ── HTTP server ────────────────────────────────────────────
const server = http.createServer((req, res) => {
  // REST API: readings for a time window
  if (req.url.startsWith('/api/readings')) {
    const url    = new URL(req.url, `http://${req.headers.host}`);
    const hours  = parseInt(url.searchParams.get('hours') || '1', 10);
    const rows   = queryByHours.all(Math.max(1, Math.min(hours, 168))); // cap at 7 days
    res.writeHead(200, {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*'
    });
    res.end(JSON.stringify(rows));
    return;
  }

  // REST API: single latest reading
  if (req.url === '/api/latest') {
    const row = queryLatest.get();
    res.writeHead(200, {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*'
    });
    res.end(JSON.stringify(row || null));
    return;
  }

  // Everything else: serve the dashboard HTML
  if (req.url === '/' || req.url === '/index.html') {
    fs.readFile(HTML_PATH, (err, data) => {
      if (err) {
        res.writeHead(500);
        res.end('Could not load dashboard.');
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(data);
    });
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

server.listen(HTTP_PORT, '0.0.0.0', () => {
  console.log(`[HTTP]  Dashboard on http://0.0.0.0:${HTTP_PORT}`);
});

// ── WebSocket server (attached to the HTTP server) ─────────
const wss = new WebSocketServer({ server });
const wsClients = new Set();

wss.on('connection', (ws) => {
  wsClients.add(ws);
  console.log(`[WS]   Client connected  (${wsClients.size} total)`);

  // Send the latest reading immediately so the page isn't blank
  const latest = queryLatest.get();
  if (latest) {
    ws.send(JSON.stringify({ type: 'reading', data: latest }));
  }

  ws.on('close', () => {
    wsClients.delete(ws);
    console.log(`[WS]   Client disconnected (${wsClients.size} total)`);
  });
});

function broadcast(record) {
  const msg = JSON.stringify({ type: 'reading', data: record });
  for (const ws of wsClients) {
    if (ws.readyState === 1) {   // OPEN
      ws.send(msg);
    }
  }
}

// ── MQTT subscriber ────────────────────────────────────────
const mqttClient = mqtt.connect(MQTT_BROKER);

mqttClient.on('connect', () => {
  console.log(`[MQTT] Connected to ${MQTT_BROKER}`);
  mqttClient.subscribe(MQTT_TOPIC, (err) => {
    if (err) console.error('[MQTT] Subscribe error:', err);
    else     console.log(`[MQTT] Subscribed to ${MQTT_TOPIC}`);
  });
});

mqttClient.on('error', (err) => {
  console.error('[MQTT] Error:', err.message);
});

mqttClient.on('message', (topic, payload) => {
  let record;
  try {
    record = JSON.parse(payload.toString());
  } catch (e) {
    console.warn('[MQTT] Malformed JSON, skipping:', payload.toString().slice(0, 80));
    return;
  }

  // Normalise nulls: JSON null → JS null → SQLite NULL
  const row = {
    device_id:     record.device_id     ?? 'unknown',
    sequence:      record.sequence      ?? 0,
    timestamp:     record.timestamp     ?? '',
    temperature_c: record.temperature_c ?? null,
    humidity_rh:   record.humidity_rh   ?? null,
    co2_ppm:       record.co2_ppm       ?? null,
    voc_index:     record.voc_index     ?? null,
    temp_sources:  record.temp_sources  ?? 0,
    hum_sources:   record.hum_sources   ?? 0,
  };

  // INSERT OR IGNORE: the UNIQUE(device_id, sequence) constraint
  // silently skips duplicates — that's the dedup, in one line.
  const info = insertStmt.run(row);

  if (info.changes === 0) {
    // Duplicate — row already existed, nothing stored
    console.log(`[DEDUP] Duplicate skipped: ${row.device_id} seq=${row.sequence}`);
    return;
  }

  // New record stored — log and push to all browsers
  console.log(`[DB]   Stored: seq=${row.sequence}  T=${row.temperature_c ?? '--'}  RH=${row.humidity_rh ?? '--'}  CO2=${row.co2_ppm ?? '--'}  VOC=${row.voc_index ?? '--'}`);

  // Attach the auto-generated id for the frontend
  row.id = info.lastInsertRowid;
  broadcast(row);
});

// ── Graceful shutdown ──────────────────────────────────────
process.on('SIGINT', () => {
  console.log('\nShutting down...');
  mqttClient.end();
  db.close();
  process.exit(0);
});

console.log('[BOOT] Environmental Station Dashboard server starting...');
