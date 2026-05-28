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
const MQTT_BROKER   = 'mqtt://localhost:1883';
const MQTT_TOPIC    = 'sensors/xiao01/readings';
const MQTT_ACK_TOPIC = 'sensors/xiao01/ack';
const SAMPLE_INTERVAL_SEC = 60;
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
    replayed      INTEGER DEFAULT 0,
    estimated_measured_at TEXT,
    received_at   TEXT    DEFAULT (datetime('now','localtime')),
    UNIQUE(device_id, sequence)
  );
`);

function ensure_column(table_name, column_name, column_definition) {
  const columns = db.pragma(`table_info(${table_name})`);
  const exists = columns.some((column) => column.name === column_name);
  if (!exists) {
    db.exec(`ALTER TABLE ${table_name} ADD COLUMN ${column_definition}`);
    console.log(`[DB]   Added column ${column_name} to ${table_name}`);
  }
}

ensure_column('readings', 'replayed', 'replayed INTEGER DEFAULT 0');
ensure_column('readings', 'estimated_measured_at', 'estimated_measured_at TEXT');

// Prepared statements (reused on every insert / query — faster than ad-hoc)
const insertStmt = db.prepare(`
  INSERT OR IGNORE INTO readings
    (device_id, sequence, timestamp, temperature_c, humidity_rh,
     co2_ppm, voc_index, temp_sources, hum_sources, replayed)
  VALUES
    (@device_id, @sequence, @timestamp, @temperature_c, @humidity_rh,
     @co2_ppm, @voc_index, @temp_sources, @hum_sources, @replayed)
`);

const queryByHours = db.prepare(`
  SELECT *, COALESCE(estimated_measured_at, received_at) AS display_time
  FROM readings
  WHERE COALESCE(estimated_measured_at, received_at) >= datetime('now', 'localtime', '-' || ? || ' hours')
  ORDER BY COALESCE(estimated_measured_at, received_at) ASC, id ASC
`);

const queryLatest = db.prepare(`
  SELECT *, COALESCE(estimated_measured_at, received_at) AS display_time
  FROM readings
  ORDER BY id DESC LIMIT 1
`);

const updateLiveMeasuredAt = db.prepare(`
  UPDATE readings
  SET estimated_measured_at = ?
  WHERE id = ?
`);

const backfillReplayedRows = db.prepare(`
  UPDATE readings
  SET estimated_measured_at = datetime(?, '-' || ((? - sequence) * ?) || ' seconds')
  WHERE device_id = ?
    AND replayed = 1
    AND estimated_measured_at IS NULL
    AND sequence < ?
`);

// ── HTTP server ────────────────────────────────────────────
const server = http.createServer((req, res) => {
  // REST API: readings for a time window
  if (req.url.startsWith('/api/readings')) {
    const url    = new URL(req.url, `http://${req.headers.host}`);
    const requested_hours = parseFloat(url.searchParams.get('hours') || '1');
    const safe_hours = Number.isFinite(requested_hours)
      ? Math.max(0.1, Math.min(requested_hours, 168))
      : 1;

    const rows = queryByHours.all(safe_hours); // cap at 7 days
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

function send_ws_message(message) {
  const msg = JSON.stringify(message);
  for (const ws of wsClients) {
    if (ws.readyState === 1) {   // OPEN
      ws.send(msg);
    }
  }
}

function broadcast(record) {
  send_ws_message({ type: 'reading', data: record });
}

function broadcast_history_refresh() {
  send_ws_message({ type: 'history_refresh' });
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


function publish_ack(row, duplicate = false) {
  const ack = {
    device_id: row.device_id,
    sequence: row.sequence,
    stored: true
  };

  const payload = JSON.stringify(ack);
  mqttClient.publish(MQTT_ACK_TOPIC, payload, (err) => {
    if (err) {
      console.error(`[ACK] Publish failed for seq=${row.sequence}:`, err.message);
      return;
    }

    if (duplicate) {
      console.log(`[ACK] Published ACK for duplicate seq=${row.sequence}`);
    } else {
      console.log(`[ACK] Published ACK for seq=${row.sequence}`);
    }
  });
}

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
    replayed:      record.replayed ? 1 : 0,
  };

  // INSERT OR IGNORE: the UNIQUE(device_id, sequence) constraint
  // silently skips duplicates — that's the dedup, in one line.
  const info = insertStmt.run(row);

  if (info.changes === 0) {
    // Duplicate — row already existed, so the firmware can safely consider it stored.
    console.log(`[DEDUP] Duplicate skipped: ${row.device_id} seq=${row.sequence}`);
    publish_ack(row, true);
    return;
  }

  // New record stored — log and push to all browsers
  console.log(`[DB]   Stored: seq=${row.sequence}  T=${row.temperature_c ?? '--'}  RH=${row.humidity_rh ?? '--'}  CO2=${row.co2_ppm ?? '--'}  VOC=${row.voc_index ?? '--'}  replayed=${row.replayed}`);

  // Attach the auto-generated id and server-side timestamp for the frontend
  row.id = info.lastInsertRowid;

  const stored = db.prepare('SELECT received_at FROM readings WHERE id = ?').get(row.id);
  if (stored) {
    row.received_at = stored.received_at;
  }

  publish_ack(row, false);

  if (row.replayed) {
    // Replayed SD records are reliable after ACK, but their display time is
    // estimated only after the next live record provides an anchor. Avoid
    // broadcasting a temporary vertical wall to the charts.
    console.log(`[WS]   Replayed seq=${row.sequence} stored; waiting for live anchor before dashboard refresh.`);
    return;
  }

  // Live records are their own measurement-time anchor. Backfill any replayed
  // rows before this live sequence using the known commit interval.
  row.estimated_measured_at = row.received_at;
  row.display_time = row.estimated_measured_at;
  updateLiveMeasuredAt.run(row.estimated_measured_at, row.id);

  const backfill = backfillReplayedRows.run(row.received_at, row.sequence, SAMPLE_INTERVAL_SEC, row.device_id, row.sequence);
  if (backfill.changes > 0) {
    console.log(`[TIME] Backfilled ${backfill.changes} replayed row(s) using live seq=${row.sequence} as anchor.`);
    console.log('[WS]   Requesting dashboard history refresh after replay backfill.');
    broadcast_history_refresh();
    return;
  }

  console.log(`[WS]   Broadcast: seq=${row.sequence} received_at=${row.received_at ?? '--'} display_time=${row.display_time ?? '--'} firmware_ts=${row.timestamp || '--'}`);
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
