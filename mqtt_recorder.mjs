#!/usr/bin/env node
/**
 * HKJC racing push recorder (Phase 0, self use).
 *
 * Connects to HKJC's public no-login Solace MQTT-over-WebSocket broker, subscribes
 * to the whole-meeting topic tree for one (date, venue), sends the mandatory
 * recovery request, and appends every message as one gzip JSONL line:
 *
 *     {"recv":"2026-11-04T19:12:03+08:00","topic":"...","data":{...}}
 *
 * Credentials are read at runtime from https://bet.hkjc.com/Config/GlobalConfig.js
 * (PUSH_NO_LOGIN_SECRET is a public constant in the site bundle, not an account).
 *
 *   node mqtt_recorder.mjs --date 20261104 --venue hv --out data/push
 *   node mqtt_recorder.mjs --date 20261104 --venue hv --seconds 30 --verbose
 */

import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import process from 'node:process';

const args = process.argv.slice(2);
const argOf = (name, dflt) => {
  const i = args.indexOf('--' + name);
  return i >= 0 && args[i + 1] && !args[i + 1].startsWith('--') ? args[i + 1] : dflt;
};
const DATE = argOf('date', new Date().toISOString().slice(0, 10).replace(/-/g, ''));
const VENUE = argOf('venue', 'hv').toLowerCase();
const OUT = argOf('out', 'data/push');
const SECONDS = Number(argOf('seconds', '0')); // 0 = run until killed
const VERBOSE = args.includes('--verbose');

const BET = 'https://bet.hkjc.com';
const UA = 'Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Mobile Safari/537.36';
const log = (...m) => console.log(new Date().toISOString(), ...m);

/* ---------------------------------------------------------------- runtime config */
async function loadConfig() {
  const res = await fetch(`${BET}/Config/GlobalConfig.js`, { headers: { 'user-agent': UA } });
  const js = await res.text();
  const pick = (key) => {
    const m = js.match(new RegExp(key + "\\s*:\\s*'([^']+)'"));
    if (!m) throw new Error(`config key not found: ${key}`);
    return m[1];
  };
  return {
    url: pick('PUSH_URL_NOLOGIN'),
    secret: pick('PUSH_NO_LOGIN_SECRET'),
    user: 'jcbw2',
  };
}

/* ------------------------------------------------------------------ mqtt encoding */
const enc = (s) => {
  const b = Buffer.from(s, 'utf8');
  const l = Buffer.alloc(2);
  l.writeUInt16BE(b.length, 0);
  return Buffer.concat([l, b]);
};
const varint = (n) => {
  const out = [];
  do { let d = n % 128; n = Math.floor(n / 128); if (n > 0) d |= 0x80; out.push(d); } while (n > 0);
  return Buffer.from(out);
};
const packet = (h, body) => Buffer.concat([Buffer.from([h]), varint(body.length), body]);

const clientId = `jcbw2_4e_${Math.random().toString(16).slice(2, 8)}_${Date.now()}`;

const connectPacket = (cfg) => packet(0x10, Buffer.concat([
  enc('MQTT'), Buffer.from([0x05, 0xc2, 0x00, 0x3c, 0x00]), // v5, user+pass+cleanStart, keepalive 60, no props
  enc(clientId), enc(cfg.user), enc(cfg.secret),
]));

const subscribePacket = (pid, topics) => {
  const parts = [Buffer.from([pid >> 8, pid & 0xff]), Buffer.from([0x00])];
  for (const t of topics) parts.push(enc(t), Buffer.from([0x00]));
  return packet(0x82, Buffer.concat(parts));
};

const publishPacket = (topic, obj) => packet(0x30, Buffer.concat([
  enc(topic), Buffer.from([0x00, 0x00]), Buffer.from(JSON.stringify(obj), 'utf8'),
]));

const PINGREQ = Buffer.from([0xc0, 0x00]);

/* ----------------------------------------------------------------------- recorder */
const day = DATE.slice(0, 4) + '-' + DATE.slice(4, 6) + '-' + DATE.slice(6, 8);
fs.mkdirSync(OUT, { recursive: true });
const file = path.join(OUT, `${day}_${VENUE}.jsonl.gz`);
const stream = fs.createWriteStream(file, { flags: 'a' });

function writeLine(obj) {
  stream.write(zlib.gzipSync(Buffer.from(JSON.stringify(obj) + '\n', 'utf8')));
}

let cfg, ws, msgCount = 0, lastMsg = 0;
const hkt = () => new Date(Date.now() + 8 * 3600e3).toISOString().replace('Z', '+08:00');

const topics = [
  `hk/d/evnt/racing/evt/02/upd/racing/${DATE}/${VENUE}`,          // ALL_MEE
  `hk/d/prdt/wager/evt/02/upd/racing/${DATE}/${VENUE}/+/+`,       // every pool of every race
];

async function connect() {
  cfg = cfg || await loadConfig();
  ws = new WebSocket(cfg.url, ['mqtt']);
  ws.binaryType = 'arraybuffer';

  ws.onopen = () => { log('ws open ->', cfg.url); ws.send(connectPacket(cfg)); };
  ws.onerror = (e) => log('ws error', e?.message || e?.type || 'unknown');
  ws.onclose = (e) => {
    log('ws closed', e.code, e.reason || '', '- reconnecting in 5s');
    setTimeout(connect, 5000);
  };

  ws.onmessage = async (ev) => {
    const buf = ev.data instanceof ArrayBuffer ? Buffer.from(ev.data)
      : typeof ev.data === 'string' ? Buffer.from(ev.data, 'binary')
      : Buffer.from(await ev.data.arrayBuffer());
    const type = buf[0] >> 4;

    if (type === 2) {                        // CONNACK
      const rc = buf[3];                     // v5: reason code is byte 3
      log('CONNACK reason =', rc);
      if (rc === 0) {
        ws.send(subscribePacket(1, topics));
        ws.send(publishPacket('hk/d/prdt/wager/req/02/recovery/client', {
          systemCode: 'JCBW2_4E', senderId: clientId,
          topics: [`hk/d/evnt/racing/evt/02/upd/racing/${DATE}/${VENUE}`],
          latestMessageInSeconds: 120,
        }));
        log('subscribed + recovery request sent for', DATE, VENUE);
      } else {
        log('connect refused; backing off 30s');
        try { ws.close(); } catch {}
      }
      return;
    }
    if (type === 9) { log('SUBACK'); return; }
    if (type === 13) { return; }             // PINGRESP
    if (type === 3) {                        // PUBLISH
      let i = 1;
      const tlen = buf.readUInt16BE(i); i += 2;
      const topic = buf.slice(i, i + tlen).toString('utf8'); i += tlen;
      const propLen = buf[i]; i += 1 + propLen;
      let payload = buf.slice(i);
      if (payload[0] === 0x1f && payload[1] === 0x8b) {
        try { payload = zlib.gunzipSync(payload); } catch { /* keep raw */ }
      }
      let data;
      try { data = JSON.parse(payload.toString('utf8')); }
      catch { data = { _raw: payload.toString('base64') }; }
      msgCount++; lastMsg = Date.now();
      writeLine({ recv: hkt(), topic, data });
      if (VERBOSE || msgCount <= 20 || msgCount % 500 === 0) {
        log(`#${msgCount}`, topic, JSON.stringify(data).slice(0, 220));
      }
      return;
    }
    if (VERBOSE) log('packet type', type, buf.slice(0, 16).toString('hex'));
  };
}

const ping = setInterval(() => {
  try { if (ws && ws.readyState === 1) ws.send(PINGREQ); } catch {}
}, 30000);

const status = setInterval(() => {
  log(`alive msgs=${msgCount} last=${lastMsg ? ((Date.now() - lastMsg) / 1000).toFixed(0) + 's ago' : 'none'} file=${file}`);
}, 300000);

const shutdown = () => {
  clearInterval(ping); clearInterval(status);
  log(`shutdown msgs=${msgCount}`);
  try { ws?.close(); } catch {}
  stream.end(() => process.exit(0));
  setTimeout(() => process.exit(0), 1500);
};
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

if (SECONDS > 0) setTimeout(shutdown, SECONDS * 1000);

log(`recorder start date=${DATE} venue=${VENUE} out=${file}`);
connect();
