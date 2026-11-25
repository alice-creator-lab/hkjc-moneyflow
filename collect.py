#!/usr/bin/env python3
"""HKJC money-flow Phase 0 collector (self use, read-only).

Records pool investment (投注額) + runner odds for every meeting HKJC currently
serves, with an adaptive cadence:

  * off-meeting / not yet selling    : 600 s
  * meeting today, not selling yet   : 120 s
  * meeting tomorrow, selling        :  60 s
  * meeting today, selling           :  20 s
  * today, within 15 min of post time:  10 s  (floor)

Two record layers:
  * SQLite  data/moneyflow.db  -> change log (only writes when a value moves)
  * compact gzip JSONL         -> data/snap/<date>.jsonl.gz (one line per poll)

Query text is extracted from bet.hkjc.com's own bundle (the gateway only accepts
the exact registered query); it self-heals on WHITELIST/schema errors.

Usage:
    python3 collect.py --once                 # one poll, print summary
    python3 collect.py --daemon               # adaptive loop (long running)
    python3 collect.py --summary              # what has been recorded so far
    python3 collect.py --once --targets 2026-11-08:ST   # force extra target
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import dist as distmod

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
SNAP = DATA / "snap"
LOGS = ROOT / "logs"
DB = DATA / "moneyflow.db"
QUERY_FILE = DATA / "query.txt"
QUERY_META = DATA / "query.meta.json"
TARGETS_FILE = DATA / "targets.json"
ODDS_FILE = DATA / "odds.txt"
PIDFILE = DATA / "collector.pid"
MQTT_PIDFILE = DATA / "mqtt.pid"

BET = "https://bet.hkjc.com"
GQL = "https://info.cld.hkjc.com/graphql/base-get/"
UA = ("Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/149.0.0.0 Mobile Safari/537.36")
HDRS = {
    "accept": "*/*",
    "accept-language": "zh-HK,zh;q=0.9,en;q=0.8",
    "content-type": "application/json",
    "origin": BET,
    "referer": BET + "/",
    "user-agent": UA,
    "accept-encoding": "gzip",
}
HKT = timezone(timedelta(hours=8))

# cadence (seconds)
HOLD_BEFORE_MIN = 20   # do not load a meeting until 20 min before its first race
RESULT_DUE_S = 600      # 賽果 一般開跑後 10 分鐘才出
RESULT_BACKOFF_S = 120  # 未出就每 2 分鐘再問一次

INTV_IDLE = 3600        # nothing near: one load an hour is plenty
INTV_TODAY_PRESALE = 600
INTV_TOMORROW_SELLING = 3600
INTV_TODAY_SELLING = 20
INTV_HOT = 10
INTV_FINAL5 = 10      # every 10 s inside the last 5 minutes before the off
INTV_SPRINT = 2       # every 2 s inside the last 60 s: the bands are 10-20 s wide, so 2 s loses nothing

SELLING_STATUS = {"SELLING", "OPEN", "RACING", "IN_PROGRESS", "BETTING", "STARTED"}
ODDS_CURSOR: dict[str, int] = {}
DIST_LAST: dict[tuple, float] = {}
ODDS_TYPES = ["WIN", "PLA", "QIN", "QPL", "FCT", "TRI", "FF", "TCE",
              "QTT", "DBL", "TBL", "DT", "TT", "SixUP"]
ODDS_TYPES_SPRINT = ["WIN", "PLA"]   # keep the 1 s cycle inside the request budget


# --------------------------------------------------------------------------- utils
def now() -> datetime:
    return datetime.now(HKT)


def iso(dt: datetime | None = None) -> str:
    return (dt or now()).isoformat(timespec="seconds")


def log(msg: str) -> None:
    line = f"{iso()} {msg}"
    print(line, flush=True)
    LOGS.mkdir(parents=True, exist_ok=True)
    with open(LOGS / "collector.log", "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def http(url: str, data: bytes | None = None, timeout: int = 30) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=data, headers=HDRS,
                                 method="POST" if data is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw, enc, code = r.read(), r.headers.get("Content-Encoding"), r.status
    except urllib.error.HTTPError as e:
        raw, enc, code = e.read(), e.headers.get("Content-Encoding"), e.code
    if enc == "gzip":
        raw = gzip.decompress(raw)
    return code, raw


def js_string_at(src: str, idx: int) -> str:
    j = idx - 1
    while j > 0 and src[j] not in "\"'`":
        j -= 1
    quote, k, out = src[j], idx, []
    while k < len(src):
        c = src[k]
        if c == "\\":
            out.append(src[k:k + 2]); k += 2; continue
        if c == quote:
            break
        out.append(c); k += 1
    raw = "".join(out)
    return (raw.replace("\\n", "\n").replace('\\"', '"')
               .replace("\\'", "'").replace("\\\\", "\\"))


# ------------------------------------------------------------------ query lifecycle
def extract_query() -> str:
    _, page = http(BET + "/racing/?lang=ch")
    m = re.search(r"/static/js/main\.[0-9a-f]+\.js", page.decode("utf-8", "ignore"))
    if not m:
        raise RuntimeError("main bundle not found on /racing/")
    bundle_url = BET + m.group(0)
    _, bundle_raw = http(bundle_url, timeout=90)
    bundle = bundle_raw.decode("utf-8", "ignore")
    parts = []
    for marker in ("query raceMeetings($date: String, $venueCode: String) {",
                   "fragment raceFragment",
                   "fragment racingBlockFragment"):
        i = bundle.find(marker)
        if i < 0:
            raise RuntimeError(f"marker not found in bundle: {marker!r}")
        parts.append(js_string_at(bundle, i))
    query = "\n".join(parts)
    DATA.mkdir(parents=True, exist_ok=True)
    QUERY_FILE.write_text(query, encoding="utf-8")
    QUERY_META.write_text(json.dumps({
        "bundle": bundle_url, "extracted_at": iso(),
        "query_sha": __import__("hashlib").sha256(query.encode()).hexdigest()[:16],
    }), encoding="utf-8")
    log(f"query extracted ({len(query)} chars) from {bundle_url}")
    return query


def get_query(force: bool = False) -> str:
    if not force and QUERY_FILE.exists():
        age_h = (time.time() - QUERY_FILE.stat().st_mtime) / 3600
        if age_h < 12:
            return QUERY_FILE.read_text(encoding="utf-8")
    return extract_query()


# ---------------------------------------------------------------------- datastore
SCHEMA = """
CREATE TABLE IF NOT EXISTS meeting (
  meeting_id TEXT PRIMARY KEY, date TEXT, venue TEXT, first_seen TEXT, last_seen TEXT
);
CREATE TABLE IF NOT EXISTS poll (
  ts TEXT, meeting_id TEXT, status TEXT, total_inv REAL, cur_race INTEGER,
  n_pools INTEGER, n_runners INTEGER, ms REAL, bytes INTEGER, ok INTEGER, err TEXT
);
CREATE TABLE IF NOT EXISTS pool_change (
  ts TEXT, meeting_id TEXT, odds_type TEXT, leg TEXT, status TEXT, sell TEXT,
  inv REAL, last_upd TEXT
);
CREATE TABLE IF NOT EXISTS odds_change (
  ts TEXT, meeting_id TEXT, race INTEGER, runner INTEGER, win_odds REAL
);
CREATE TABLE IF NOT EXISTS event (
  ts TEXT, meeting_id TEXT, kind TEXT, detail TEXT
);
CREATE TABLE IF NOT EXISTS race_meta (
  meeting_id TEXT, race INTEGER, post_time TEXT, distance INTEGER, race_name_ch TEXT,
  race_class_ch TEXT, track_ch TEXT, going_ch TEXT, n_runners INTEGER,
  PRIMARY KEY (meeting_id, race)
);
CREATE TABLE IF NOT EXISTS runner_meta (
  meeting_id TEXT, race INTEGER, runner INTEGER, name_ch TEXT, name_en TEXT,
  jockey_ch TEXT, trainer_ch TEXT, draw INTEGER, weight INTEGER, rating INTEGER,
  last6run TEXT, gear TEXT, PRIMARY KEY (meeting_id, race, runner)
);
CREATE TABLE IF NOT EXISTS odds_extra (
  ts TEXT, meeting_id TEXT, race INTEGER, runner INTEGER,
  pla REAL, drop_v REAL, hot INTEGER,
  PRIMARY KEY (meeting_id, race, runner)
);
CREATE INDEX IF NOT EXISTS ix_odds_mrt ON odds_change(meeting_id, race, runner, ts);
CREATE TABLE IF NOT EXISTS race_result (
  meeting_id TEXT, race INTEGER, ts TEXT, payload TEXT,
  PRIMARY KEY (meeting_id, race)
);
CREATE TABLE IF NOT EXISTS baseline (
  meeting_id TEXT, race INTEGER, ts TEXT, win_inv REAL, payload TEXT,
  PRIMARY KEY (meeting_id, race)
);
CREATE TABLE IF NOT EXISTS combo_odds (
  ts TEXT, meeting_id TEXT, race INTEGER, pool TEXT, combo TEXT, odds REAL,
  n_legs INTEGER DEFAULT 1, leg_index INTEGER DEFAULT 0,
  PRIMARY KEY (meeting_id, race, pool, combo)
);
CREATE TABLE IF NOT EXISTS dist_snapshot (
  ts TEXT, meeting_id TEXT, race INTEGER, total REAL, payload TEXT
);
CREATE INDEX IF NOT EXISTS ix_dist ON dist_snapshot(meeting_id, race, ts);
CREATE TABLE IF NOT EXISTS app_state (k TEXT PRIMARY KEY, v TEXT);
CREATE INDEX IF NOT EXISTS ix_poll_m   ON poll(meeting_id, ts);
CREATE INDEX IF NOT EXISTS ix_pool_m   ON pool_change(meeting_id, ts);
CREATE INDEX IF NOT EXISTS ix_odds_m   ON odds_change(meeting_id, ts);
CREATE INDEX IF NOT EXISTS ix_event_m  ON event(meeting_id, ts);
"""


def db() -> sqlite3.Connection:
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA busy_timeout=20000")   # never spin on "database is locked"; wait, then fail cleanly
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    # lightweight migrations for tables created before a column existed
    cols = {r[1] for r in conn.execute("PRAGMA table_info(combo_odds)")}
    if "n_legs" not in cols:
        conn.execute("ALTER TABLE combo_odds ADD COLUMN n_legs INTEGER DEFAULT 1")
    if "leg_index" not in cols:
        conn.execute("ALTER TABLE combo_odds ADD COLUMN leg_index INTEGER DEFAULT 0")
    conn.commit()
    return conn


class LastSeen:
    """In-memory last values so we only write rows when something actually changed."""

    def __init__(self, conn: sqlite3.Connection):
        self.pools: dict[tuple, tuple] = {}
        self.odds: dict[tuple, float] = {}
        self.meetings: set[str] = set()
        self.extra: dict[tuple, tuple] = {}
        self.combos: dict[tuple, float] = {}
        for mid, race, pool, combo, odds in conn.execute(
                "SELECT meeting_id, race, pool, combo, odds FROM combo_odds"):
            self.combos[(mid, race, pool, combo)] = odds
        for mid, race, runner, pla, drop_v, hot in conn.execute(
                "SELECT meeting_id, race, runner, pla, drop_v, hot FROM odds_extra"):
            self.extra[(mid, race, runner)] = (pla, drop_v, hot)
        self.meta: dict[tuple, tuple] = {}
        for mid, race, runner, sig in conn.execute(
                "SELECT meeting_id, race, runner, "
                "name_ch||'|'||IFNULL(jockey_ch,'')||'|'||IFNULL(draw,-1)||'|'||"
                "IFNULL(weight,-1)||'|'||IFNULL(rating,-1) FROM runner_meta"):
            self.meta[(mid, race, runner)] = (sig,)
        for mid, t, leg, st, sell, inv, upd in conn.execute(
                "SELECT meeting_id, odds_type, leg, status, sell, inv, last_upd FROM pool_change "
                "ORDER BY ts"):
            self.pools[(mid, t, leg)] = (st, sell, inv, upd)
        for mid, race, runner, o in conn.execute(
                "SELECT meeting_id, race, runner, win_odds FROM odds_change ORDER BY ts"):
            self.odds[(mid, race, runner)] = o
        for (mid,) in conn.execute("SELECT meeting_id FROM meeting"):
            self.meetings.add(mid)


# ------------------------------------------------------------------------ parsing
def num(v):
    """HKJC sends money as strings (sometimes with commas); normalise to float."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(",", "").strip())
    except ValueError:
        return None


def leg_str(leg: dict | None) -> str:
    races = (leg or {}).get("races") or []
    return "-".join(str(r) for r in races)


def parse_meeting(m: dict) -> dict:
    date = m.get("date") or ""
    venue = (m.get("venueCode") or "").upper()
    races = m.get("races") or []
    pools = []
    for p in m.get("poolInvs") or []:
        pools.append({
            "t": p.get("oddsType"),
            "leg": leg_str(p.get("leg")),
            "st": p.get("status"),
            "sell": p.get("sellStatus"),
            "inv": num(p.get("investment")),
            "upd": p.get("lastUpdateTime"),
        })
    odds = []
    for r in races:
        no = r.get("no")
        for run in (r.get("runners") or []):
            odds.append((int(no) if str(no).isdigit() else no,
                         run.get("no"),
                         num(run.get("winOdds"))))
    return {
        "post_times": [r.get("postTime") for r in races if r.get("postTime")],
        "meeting_id": f"{date}_{venue}",
        "date": date, "venue": venue,
        "status": m.get("status"),
        "total_inv": num(m.get("totalInvestment")),
        "cur_race": m.get("currentNumberOfRace"),
        "races": races, "pools": pools, "odds": odds,
    }


def selling(p: dict) -> bool:
    return (p.get("sell") or "").upper() not in ("", "STOP_SELL", "CLOSED", "SUSPENDED")


def record(conn: sqlite3.Connection, last: LastSeen, parsed: dict, ts: str,
           ms: float, nbytes: int, skip_races: set | None = None) -> dict:
    mid = parsed["meeting_id"]
    date, venue = parsed["date"], parsed["venue"]
    stats = {"pool_changes": 0, "odds_changes": 0, "new_sale": None, "selling": False}

    if mid not in last.meetings:
        conn.execute("INSERT OR IGNORE INTO meeting VALUES (?,?,?,?,?)",
                     (mid, date, venue, ts, ts))
        last.meetings.add(mid)
        conn.execute("INSERT INTO event VALUES (?,?,?,?)",
                     (ts, mid, "meeting_first_seen", parsed["status"]))
    else:
        conn.execute("UPDATE meeting SET last_seen=? WHERE meeting_id=?", (ts, mid))

    any_open = any(selling(p) for p in parsed["pools"]) or (parsed["total_inv"] or 0) > 0
    stats["selling"] = any_open
    prev_open = conn.execute(
        "SELECT 1 FROM event WHERE meeting_id=? AND kind='sale_open' LIMIT 1", (mid,)).fetchone()
    if any_open and not prev_open:
        conn.execute("INSERT INTO event VALUES (?,?,?,?)",
                     (ts, mid, "sale_open", f"total_inv={parsed['total_inv']}"))
        stats["new_sale"] = True
        log(f"SALE OPEN {mid} total_inv={parsed['total_inv']}")

    skip_races = skip_races or set()

    def skipped(leg: str) -> bool:
        parts = [x for x in str(leg).split("-") if x.isdigit()]
        return bool(parts) and all(int(x) in skip_races for x in parts)

    # --- pools: write only on change
    for p in parsed["pools"]:
        if skipped(p["leg"]):
            continue
        key = (mid, p["t"], p["leg"])
        val = (p["st"], p["sell"], p["inv"], p["upd"])
        if last.pools.get(key) != val:
            conn.execute("INSERT INTO pool_change VALUES (?,?,?,?,?,?,?,?)",
                         (ts, mid, p["t"], p["leg"], p["st"], p["sell"], p["inv"], p["upd"]))
            last.pools[key] = val
            stats["pool_changes"] += 1

    # --- runner odds: write only on change
    for race_no, runner_no, odds_v in parsed["odds"]:
        if race_no in skip_races:      # race is over: its odds are frozen, do not store repeats
            continue
        key = (mid, race_no, runner_no)
        try:
            val = float(odds_v) if odds_v not in (None, "", "-") else None
        except (TypeError, ValueError):
            val = None
        if val is None:
            continue
        if last.odds.get(key) != val:
            conn.execute("INSERT INTO odds_change VALUES (?,?,?,?,?)",
                         (ts, mid, race_no, runner_no, val))
            last.odds[key] = val
            stats["odds_changes"] += 1

    # --- race / runner metadata (names, jockey, draw...) so the monitor can render cards
    for race in parsed["races"]:
        rno = race.get("no")
        try:
            rno = int(rno)
        except (TypeError, ValueError):
            continue
        runners = race.get("runners") or []
        track = (race.get("raceTrack") or {}).get("description_ch")
        going = race.get("go_ch")
        conn.execute(
            "INSERT INTO race_meta VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(meeting_id, race) DO UPDATE SET "
            "post_time=excluded.post_time, distance=excluded.distance, "
            "race_name_ch=excluded.race_name_ch, race_class_ch=excluded.race_class_ch, "
            "track_ch=excluded.track_ch, going_ch=excluded.going_ch, n_runners=excluded.n_runners",
            (mid, rno, race.get("postTime"), race.get("distance"), race.get("raceName_ch"),
             race.get("raceClass_ch"), track, going, len(runners)))
        for run in runners:
            try:
                ono = int(run.get("no"))
            except (TypeError, ValueError):
                continue
            jockey = (run.get("jockey") or {}).get("name_ch")
            trainer = (run.get("trainer") or {}).get("name_ch")
            name = run.get("name_ch")
            sig = (f"{name}|{jockey or ''}|{run.get('barrierDrawNumber')}|"
                   f"{run.get('handicapWeight')}|{run.get('currentRating')}")
            key = (mid, rno, ono)
            if last.meta.get(key) == (sig,) or last.meta.get(key) == sig:
                continue
            conn.execute(
                "INSERT INTO runner_meta VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(meeting_id, race, runner) DO UPDATE SET "
                "name_ch=excluded.name_ch, name_en=excluded.name_en, jockey_ch=excluded.jockey_ch, "
                "trainer_ch=excluded.trainer_ch, draw=excluded.draw, weight=excluded.weight, "
                "rating=excluded.rating, last6run=excluded.last6run, gear=excluded.gear",
                (mid, rno, ono, name, run.get("name_en"), jockey, trainer,
                 run.get("barrierDrawNumber"), run.get("handicapWeight"),
                 run.get("currentRating"), run.get("last6run"), run.get("gearInfo")))
            last.meta[key] = (sig,)

    n_runners = sum(1 for r in parsed["races"] for _ in (r.get("runners") or []))
    conn.execute("INSERT INTO poll VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                 (ts, mid, parsed["status"], parsed["total_inv"], parsed["cur_race"],
                  len(parsed["pools"]), n_runners, round(ms, 1), nbytes, 1, None))
    return stats


RESULT_TRIED: dict[tuple, float] = {}
GQL_RESULTS: dict[str, tuple[float, dict]] = {}


def fetch_one_result(mid: str, date: str, venue: str, no: int) -> dict:
    """GraphQL first (covers simulcast), HTML fallback (local meetings drop out of the GQL window)."""
    import results_gql

    key = f"{date}_{venue}"
    hit = GQL_RESULTS.get(key)
    if not hit or time.time() - hit[0] > 120:
        hit = (time.time(), results_gql.fetch_meeting(date, venue))
        GQL_RESULTS[key] = hit
    table = hit[1] or {}
    if isinstance(table.get(no if isinstance(no, int) else str(no)), dict):
        return table[no]
    try:
        import backfill_results as br
        return br.fetch_result(str(date).replace("-", "/"), no)
    except Exception:  # noqa: BLE001
        return {"ok": False, "reason": "no local results page"}


def fetch_result_if_due(conn: sqlite3.Connection, mid: str, date: str,
                        races: list[dict], ts: str) -> None:
    """Store the 賽果 once a race is over so the app can show it and stop refreshing that race.

    One race per cycle keeps the poll budget intact; a failed fetch is retried after 15 min
    (simulcast meetings have no local results page at all).
    """
    finished: list[tuple[int, float]] = []
    for rc in races or []:
        try:
            no = int(rc.get("no"))
            pt = datetime.fromisoformat(str(rc.get("post_time") or rc.get("postTime")))
            if pt.tzinfo is None:
                pt = pt.replace(tzinfo=HKT)
        except (TypeError, ValueError):
            continue
        if conn.execute("SELECT 1 FROM race_result WHERE meeting_id=? AND race=?",
                        (mid, no)).fetchone():
            continue
        age = (now() - pt).total_seconds()
        # 賽果 通常要開跑後 10 分鐘才出，太早問只會白花一個 request
        if age < RESULT_DUE_S:
            continue
        if time.time() - RESULT_TRIED.get((mid, no), 0.0) < RESULT_BACKOFF_S:
            continue
        finished.append((no, age))
    if not finished:
        return

    # one GraphQL call returns every race of the meeting (local *and* simulcast)
    venue = str(races[0].get("venue") or mid.split("_")[-1]) if races else mid.split("_")[-1]
    try:
        import results_gql
        table = results_gql.fetch_meeting(date, venue)
    except Exception as exc:  # noqa: BLE001
        table = {}
        log(f"gql results failed {mid}: {type(exc).__name__}: {exc}")

    for no, age_s in finished:
        RESULT_TRIED[(mid, no)] = time.time()   # retry no sooner than RESULT_BACKOFF_S
        res = table.get(no) if isinstance(table.get(no), dict) and table[no].get("ok") else None
        if res:
            conn.execute("INSERT OR REPLACE INTO race_result VALUES (?,?,?,?)",
                         (mid, no, ts, json.dumps(res, ensure_ascii=False)))
            conn.commit()
            log(f"result stored {mid} R{no}: {len(res['rows'])} finishers (gql)")
            continue
        # fallback: the local results page (the GQL window only covers live meetings)
        if age_s < 1800:
            continue
        try:
            import backfill_results as br
            res = br.fetch_result(str(date).replace("-", "/"), no)
        except Exception as exc:  # noqa: BLE001
            log(f"result fallback failed {mid} R{no}: {type(exc).__name__}: {exc}")
            continue
        conn.execute("INSERT OR REPLACE INTO race_result VALUES (?,?,?,?)",
                     (mid, no, ts, json.dumps(res, ensure_ascii=False)))
        conn.commit()
        log(f"result stored {mid} R{no}: html fallback ok={res.get('ok')}")


def poll_odds(conn: sqlite3.Connection, last: LastSeen, mid: str, date: str, venue: str,
              race_no: int, ts: str, pool_inv: dict | None = None,
              baseline_due: bool = False, types: list | None = None,
              dist_every: float = 55.0) -> int:
    """Fetch WIN/PLA odds for one race and record changes. Returns rows written."""
    if not ODDS_FILE.exists():
        return 0
    query = ODDS_FILE.read_text(encoding="utf-8")
    doc, ms, nbytes = fetch_doc(query, {"date": date, "venueCode": venue,
                                        "raceNo": race_no,
                                        "oddsTypes": types or ODDS_TYPES},
                                op="racing")
    if doc.get("errors"):
        log(f"odds {mid} R{race_no}: {str(doc['errors'])[:200]}")
        return 0
    rows: dict[int, dict] = {}
    combos: dict[tuple, tuple] = {}
    for m in (doc.get("data") or {}).get("raceMeetings") or []:
        for pool in m.get("pmPools") or []:
            typ = pool.get("oddsType")
            legs = (pool.get("leg") or {}).get("races") or [race_no]
            try:
                n_legs = max(1, len(legs))
                leg_index = [int(x) for x in legs].index(race_no)
            except (TypeError, ValueError):
                n_legs, leg_index = 1, 0
            for node in pool.get("oddsNodes") or []:
                combo = str(node.get("combString") or "")
                price = num(node.get("oddsValue"))
                if not combo or price is None:
                    continue
                if typ not in ("WIN", "PLA"):
                    combos[(typ, combo)] = (price, n_legs, leg_index)
                    continue
                try:
                    rno = int(combo)
                except ValueError:
                    continue
                slot = rows.setdefault(rno, {})
                if typ == "WIN":
                    slot["win"] = price
                    slot["drop"] = num(node.get("oddsDropValue"))
                    slot["hot"] = 1 if node.get("hotFavourite") else 0
                elif typ == "PLA":
                    slot["pla"] = price

    # ---- distribution snapshot (at most once a minute per race)
    if pool_inv:
        key = (mid, race_no)
        if time.time() - DIST_LAST.get(key, 0) >= dist_every:
            try:
                d = distmod.compute(
                    [{"no": no, "odds": v.get("win"), "pla": v.get("pla")}
                     for no, v in rows.items()],
                    pool_inv, combos)
                if d["total"] > 0:
                    conn.execute("INSERT INTO dist_snapshot VALUES (?,?,?,?,?)",
                                 (ts, mid, race_no, round(d["total"], 1),
                                  json.dumps([[h["no"], round(h["win"]), round(h["pla"]),
                                               round(h["qin"]), round(h["qpl"])]
                                              for h in d["horses"]],
                                             separators=(",", ":"))))
                    DIST_LAST[key] = time.time()
            except Exception as exc:  # noqa: BLE001 - never kill the cycle for a chart
                log(f"dist {mid} R{race_no} failed: {type(exc).__name__}: {exc}")

    for (pool_t, combo), meta in combos.items():
        price, n_legs, leg_index = meta
        key = (mid, race_no, pool_t, combo)
        if last.combos.get(key) != price:
            conn.execute(
                "INSERT INTO combo_odds VALUES (?,?,?,?,?,?,?,?) "
                "ON CONFLICT(meeting_id, race, pool, combo) DO UPDATE SET "
                "ts=excluded.ts, odds=excluded.odds, n_legs=excluded.n_legs, "
                "leg_index=excluded.leg_index",
                (ts, mid, race_no, pool_t, combo, price, n_legs, leg_index))
            last.combos[key] = price

    if baseline_due and rows:
        exists = conn.execute("SELECT ts FROM baseline WHERE meeting_id=? AND race=?",
                              (mid, race_no)).fetchone()
        # upgrade a pre-race-day baseline (e.g. taken at declaration) to the race-day morning one,
        # which is what 隔夜飛 actually measures from
        if exists:
            try:
                old_day = datetime.fromisoformat(exists[0]).date().isoformat()
                race_day = str(mid).split("_")[0]
                if old_day < race_day and ts[:10] == race_day:
                    exists = None
            except (ValueError, TypeError):
                pass
        if not exists:
            win_inv = 0.0
            for (typ, _leg), inv in (pool_inv or {}).items():
                if typ == "WIN":
                    win_inv = max(win_inv, inv or 0.0)
            payload = [[rno_, v.get("win"), v.get("pla")] for rno_, v in sorted(rows.items())]
            conn.execute("INSERT OR REPLACE INTO baseline VALUES (?,?,?,?,?)",
                         (mid, race_no, ts, round(win_inv), json.dumps(payload, separators=(",", ":"))))
            log(f"baseline {mid} R{race_no}: win={round(win_inv)} horses={len(payload)}")

    written = 0
    for rno, v in rows.items():
        win, pla = v.get("win"), v.get("pla")
        key = (mid, race_no, rno)
        if win is not None and last.odds.get(key) != win:
            conn.execute("INSERT INTO odds_change VALUES (?,?,?,?,?)",
                         (ts, mid, race_no, rno, win))
            last.odds[key] = win
            written += 1
        sig = (pla, v.get("drop"), v.get("hot"))
        if last.extra.get(key) != sig:
            conn.execute(
                "INSERT INTO odds_extra VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(meeting_id, race, runner) DO UPDATE SET "
                "ts=excluded.ts, pla=excluded.pla, drop_v=excluded.drop_v, hot=excluded.hot",
                (ts, mid, race_no, rno, pla, v.get("drop"), v.get("hot")))
            last.extra[key] = sig
    return written


def write_snapshot(parsed: dict, ts: str) -> None:
    SNAP.mkdir(parents=True, exist_ok=True)
    day = now().strftime("%Y-%m-%d")
    line = json.dumps({
        "ts": ts,
        "d": parsed["date"], "v": parsed["venue"], "st": parsed["status"],
        "tot": parsed["total_inv"], "cur": parsed["cur_race"],
        "p": [[p["t"], p["leg"], p["inv"], p["sell"], p["upd"]] for p in parsed["pools"]],
        "o": [[r, n, o] for r, n, o in parsed["odds"] if o is not None],
    }, ensure_ascii=False, separators=(",", ":"))
    with gzip.open(SNAP / f"{day}.jsonl.gz", "at", encoding="utf-8") as fh:
        fh.write(line + "\n")


# --------------------------------------------------------------------- collection
def fetch_doc(query: str, variables: dict, op: str = "raceMeetings") -> tuple[dict, float, int]:
    body = json.dumps({"operationName": op, "variables": variables,
                       "query": query}).encode()
    t0 = time.time()
    code, raw = http(GQL, body)
    ms = (time.time() - t0) * 1000
    try:
        doc = json.loads(raw.decode("utf-8", "ignore"))
    except json.JSONDecodeError:
        doc = {"errors": [{"message": f"HTTP {code} non-json"}]}
    return doc, ms, len(raw)


def needs_reextract(doc: dict) -> bool:
    err = json.dumps(doc.get("errors") or [])
    return "WHITELIST" in err or "match the schema" in err


def poll_once(extra_targets: list[tuple[str, str]] | None = None,
              verbose: bool = False, force_baseline: bool = False) -> list[dict]:
    conn = db()
    last = LastSeen(conn)
    query = get_query()
    ts = iso()
    out: list[dict] = []

    def run(variables: dict, label: str, allow_refresh: bool = True) -> list[dict]:
        nonlocal query
        doc, ms, nbytes = fetch_doc(query, variables)
        if needs_reextract(doc) and allow_refresh:
            log(f"query rejected ({label}) -> re-extracting from live bundle")
            query = get_query(force=True)
            doc, ms, nbytes = fetch_doc(query, variables)
        if doc.get("errors"):
            conn.execute("INSERT INTO poll VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                         (ts, label, None, None, None, 0, 0, round(ms, 1), nbytes, 0,
                          json.dumps(doc["errors"])[:500]))
            log(f"ERROR {label}: {doc['errors']}")
            return [], ms, nbytes
        return (doc.get("data") or {}).get("raceMeetings") or [], ms, nbytes

    today, tomorrow = now().date().isoformat(), (now().date() + timedelta(days=1)).isoformat()

    # inside a race window only the meeting that is about to jump matters; the rest of the
    # calendar can wait for the hourly sweep, and dropping them keeps the payload small
    live_mid = None
    try:
        row = conn.execute(
            "SELECT meeting_id FROM race_meta WHERE post_time IS NOT NULL"
            " AND post_time BETWEEN ? AND ? ORDER BY post_time LIMIT 1",
            ((now() - timedelta(seconds=180)).isoformat(),
             (now() + timedelta(minutes=15)).isoformat())).fetchone()
        if row and row[0].startswith(today):
            live_mid = tuple(row[0].split("_", 1))
    except sqlite3.Error:
        live_mid = None

    if live_mid:
        meetings, ms, nbytes = run({"date": live_mid[0], "venueCode": live_mid[1]},
                                   f"{live_mid[0]}_{live_mid[1]}")
    else:
        meetings, ms, nbytes = run({}, "active")
    for m in meetings:
        parsed = parse_meeting(m)
        if not parsed["date"] or not parsed["venue"]:
            continue
        nowdt = now()

        def over(rno) -> bool:
            """a race more than 10 minutes past its advertised time is done racing"""
            for rc in parsed["races"]:
                if str(rc.get("no")) != str(rno):
                    continue
                try:
                    pt = datetime.fromisoformat(str(rc.get("postTime")))
                except (TypeError, ValueError):
                    return False
                return (nowdt - pt).total_seconds() > 600
            return False

        over_now = {int(rc["no"]) for rc in parsed["races"]
                    if str(rc.get("no", "")).isdigit() and over(int(rc["no"]))}

        # a meeting is only worth loading inside its own window: tonight's S3 (first race 20:20)
        # has no business pulling pools all afternoon
        held = False
        hold_until = None
        first_pt = None
        for rc in parsed["races"]:
            try:
                pt = datetime.fromisoformat(str(rc.get("postTime")))
            except (TypeError, ValueError):
                continue
            if pt.tzinfo is None:
                pt = pt.replace(tzinfo=HKT)
            first_pt = pt if first_pt is None or pt < first_pt else first_pt
        if first_pt is not None and nowdt < first_pt - timedelta(minutes=HOLD_BEFORE_MIN):
            has_baseline = bool(conn.execute(
                "SELECT 1 FROM baseline WHERE meeting_id=? LIMIT 1",
                (parsed["meeting_id"],)).fetchone())
            held = meeting_hold(first_pt, parsed["date"], today, nowdt.hour, has_baseline)
            hold_until = first_pt - timedelta(minutes=HOLD_BEFORE_MIN)
            if held:
                # a held meeting must not write pool/odds rows at all; when the 09:00 baseline
                # exception unholds it, writes stay enabled or the baseline can never be built
                over_now = {int(rc["no"]) for rc in parsed["races"]
                            if str(rc.get("no", "")).isdigit()}

        stats = record(conn, last, parsed, ts, ms, nbytes, skip_races=over_now)
        if held:
            # a held meeting is still reported to the sleep calculator: it carries the moment
            # the hold expires. Without it the meeting is invisible to next_interval(), which
            # falls back to a one-hour idle sleep and swallows the whole T-15 window.
            out.append({**{k: parsed[k] for k in ("meeting_id", "date", "venue", "status",
                                                  "total_inv", "cur_race", "post_times")},
                        "pools": len(parsed["pools"]), "held": True,
                        "hold_until": hold_until.isoformat() if hold_until else None, **stats})
            continue
        write_snapshot(parsed, ts)
        if stats["selling"] and parsed["date"] in (today, tomorrow):
            open_races = sorted({int(p["leg"]) for p in parsed["pools"]
                                 if p["leg"].isdigit() and selling(p)})

            open_races = [r for r in open_races if r not in over_now]
            baseline_due = force_baseline or (parsed["date"] == today and now().hour <= 9)
            # imminent race: inside the last 5 minutes there is no time to rotate, and inside
            # the last 30 seconds we drop to WIN/PLA so the 1 s cycle still fits the request budget
            pick, sprint, final5 = pick_odds_races(
                open_races, parsed["races"], parsed["meeting_id"])
            # a race whose 賽果 is stored is settled: no point polling its odds any more
            done = {row[0] for row in conn.execute(
                "SELECT race FROM race_result WHERE meeting_id=?",
                (parsed["meeting_id"],)).fetchall()}
            if done or open_races:
                pick = [r for r in pick if r not in done and r not in over_now]
            if open_races:
                for rno in pick:
                    # per-race pools only (single-race legs equal the race number)
                    def in_race(leg: str, race_no: int) -> bool:
                        parts = [x for x in str(leg).split("-") if x.isdigit()]
                        return str(race_no) in parts

                    pool_inv = {(p["t"], p["leg"]): (p["inv"] or 0.0)
                                for p in parsed["pools"] if in_race(p["leg"], rno)}
                    poll_odds(conn, last, parsed["meeting_id"], parsed["date"],
                              parsed["venue"], rno, ts, pool_inv,
                              baseline_due=baseline_due,
                              types=ODDS_TYPES_SPRINT if sprint else None,
                              dist_every=5.0 if sprint else (10.0 if final5 else 55.0))
            fetch_result_if_due(conn, parsed["meeting_id"], parsed["date"], parsed["races"], ts)
        out.append({**{k: parsed[k] for k in ("meeting_id", "date", "venue", "status",
                                              "total_inv", "cur_race", "post_times")},
                    "pools": len(parsed["pools"]), **stats})

    for target in extra_targets or []:
        date, venue = target
        if any(o["date"] == date and o["venue"] == venue.upper() for o in out):
            continue
        want = (date, venue.upper())
        got, ms2, nb2 = run({"date": date, "venueCode": want[1]}, f"{date}_{venue}")
        matched = 0
        for m in got:
            parsed = parse_meeting(m)
            # the gateway falls back to the currently-live meeting for dates it does not
            # serve yet -- only keep the meeting we actually asked for
            if (parsed["date"], parsed["venue"]) != want:
                continue
            matched += 1
            stats = record(conn, last, parsed, ts, ms2, nb2)
            write_snapshot(parsed, ts)
            out.append({**{k: parsed[k] for k in ("meeting_id", "date", "venue", "status",
                                                  "total_inv", "cur_race", "post_times")},
                        "pools": len(parsed["pools"]), **stats})
        if matched == 0:
            log(f"target {want[0]} {want[1]}: not served yet (gateway returned a different meeting)")


    conn.commit()
    conn.close()
    if verbose:
        for o in out:
            log(f"  {o['meeting_id']:<14} {o['status']:<9} inv={str(o['total_inv']):>10} "
                f"pools={o['pools']:>3} chg(pool={o['pool_changes']},odds={o['odds_changes']})"
                + (f"  [held until {o['hold_until'][11:16]}]" if o.get("held") else ""))
    return out


# ------------------------------------------------------------------------ cadence
def imminent_race(races: list[dict]) -> tuple[int | None, float | None]:
    """(race_no, seconds_to_post) for the next race about to jump, within a 5-minute window."""
    best_no, best_left = None, None
    for rc in races or []:
        try:
            pt = rc.get("post_time")
            if not pt:
                continue
            d = datetime.fromisoformat(str(pt))
            if d.tzinfo is None:
                d = d.replace(tzinfo=HKT)
            left = (d - now()).total_seconds()
        except (ValueError, TypeError):
            continue
        # keep sprinting for up to 3 minutes past the advertised time: a late jump is exactly
        # the window where the last money lands, and the pool closes only when betting stops
        if -180 <= left <= 300 and (best_left is None or left < best_left):
            best_no, best_left = int(rc["no"]), left
    return best_no, best_left


def minutes_to_next_post(post_times: list[str]) -> float | None:
    best = None
    for p in post_times:
        try:
            dt = datetime.fromisoformat(str(p))
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=HKT)
        delta = (dt - now()).total_seconds()
        if delta >= -180 and (best is None or delta < best):   # keep the overrun race "next"
            best = delta
    return None if best is None else best / 60


def meeting_hold(first_pt, mdate: str, today: str, hour: int, has_baseline: bool) -> bool:
    """True when a meeting is too far out to be worth loading.

    Exception: the 09:00 pass of a same-day meeting that has no overnight baseline yet — without
    it the 隔夜飛 band would be empty for every evening meeting.
    """
    if mdate == today and hour <= 9 and not has_baseline:
        return False
    return True


def next_result_wake(conn: sqlite3.Connection) -> float | None:
    """Seconds until the next 賽果 is due, so a long idle sleep never misses one."""
    nowdt = now()
    waits: list[float] = []
    rows = conn.execute(
        "SELECT rm.meeting_id, rm.race, rm.post_time FROM race_meta rm "
        "WHERE NOT EXISTS (SELECT 1 FROM race_result rr "
        "  WHERE rr.meeting_id=rm.meeting_id AND rr.race=rm.race) "
        "AND rm.post_time <= ?", ((nowdt + timedelta(minutes=40)).isoformat(),)).fetchall()
    for mid, rno, pt in rows:
        try:
            p = datetime.fromisoformat(str(pt))
        except (TypeError, ValueError):
            continue
        if p.tzinfo is None:
            p = p.replace(tzinfo=HKT)
        due = (p - nowdt).total_seconds() + RESULT_DUE_S
        if due > RESULT_DUE_S * 6 or due < -3600:
            continue          # far in the future, or long past: not worth waking for
        if due > 0:
            waits.append(due)
            continue
        left = RESULT_BACKOFF_S - (time.time() - RESULT_TRIED.get((mid, rno), 0.0))
        waits.append(left if left > 0 else 2)
    return min(waits) if waits else None


def next_interval(results: list[dict]) -> tuple[int, str]:
    today = now().date()
    interval, why = INTV_IDLE, "idle"
    for r in results:
        if r.get("held"):
            # wake exactly when the hold expires (plus a small buffer so the next cycle is
            # already past first_post - HOLD_BEFORE_MIN) instead of idling for an hour
            try:
                wait = (datetime.fromisoformat(str(r["hold_until"])) - now()).total_seconds() + 2
            except (KeyError, TypeError, ValueError):
                continue
            interval, why = min(interval, max(2, int(wait))), "hold-expiry"
            continue
        try:
            d = datetime.strptime(r["date"], "%Y-%m-%d").date()
        except (KeyError, ValueError):
            continue
        selling_any = bool(r.get("selling")) or (r.get("total_inv") or 0) > 0
        soon = minutes_to_next_post(r.get("post_times") or [])
        if d == today:
            if selling_any:
                # only the race that is about to jump needs a tight cadence; between races
                # everything is idle, so one load an hour is enough
                in_window = soon is not None and -3 <= soon <= 15
                if not in_window:
                    interval, why = min(interval, INTV_IDLE), "between-races"
                    if soon is not None and soon > 15:
                        # never sleep through the start of the next race window
                        interval = min(interval, max(2, int((soon - 15) * 60)))
                else:
                    interval, why = min(interval, INTV_TODAY_SELLING), "today+near-post-15m"
                    if soon is not None and -3 <= soon <= 5:
                        interval, why = min(interval, INTV_FINAL5), "final-5min"
                    if soon is not None and -180 <= soon * 60 <= 70:
                        interval, why = min(interval, INTV_SPRINT), "final-60s"
            else:
                interval, why = min(interval, INTV_TODAY_PRESALE), "today+presale"
        elif d == today + timedelta(days=1) and selling_any:
            interval, why = min(interval, INTV_TOMORROW_SELLING), "tomorrow+selling"
    return interval, why


# -------------------------------------------------------------------- mqtt helper
def mqtt_should_run(results: list[dict]) -> tuple[bool, str, str]:
    """Record the push for any meeting on sale today or tomorrow (odds ticks matter pre-race)."""
    today = now().date().isoformat()
    tomorrow = (now().date() + timedelta(days=1)).isoformat()
    for r in sorted(results, key=lambda x: x.get("date") or ""):
        if r.get("held"):
            continue
        if r.get("date") in (today, tomorrow) and (r.get("selling") or (r.get("total_inv") or 0) > 0):
            return True, r["venue"], r["date"]
    return False, "", ""


def mqtt_running() -> bool:
    if not MQTT_PIDFILE.exists():
        return False
    try:
        pid = int(MQTT_PIDFILE.read_text().strip())
        os.kill(pid, 0)
        return True
    except (ValueError, ProcessLookupError, PermissionError):
        return False


def node_bin() -> str | None:
    cfg = ROOT / "config.env"
    if cfg.exists():
        for line in cfg.read_text(encoding="utf-8", errors="ignore").splitlines():
            k, _, v = line.partition("=")
            if k.strip() == "NODE" and v.strip() and Path(v.strip()).exists():
                return v.strip()
    return shutil_which("node")


def mqtt_start(date: str, venue: str) -> None:
    script = ROOT / "mqtt_recorder.mjs"
    node = node_bin()
    if not script.exists() or not node:
        log("mqtt recorder skipped (node not found)")
        return
    out = open(LOGS / "mqtt.log", "a", encoding="utf-8")
    proc = subprocess.Popen(
        [node, str(script), "--date", date.replace("-", ""),
         "--venue", venue.lower(), "--out", str(DATA / "push")],
        stdout=out, stderr=out, cwd=str(ROOT), start_new_session=True)
    MQTT_PIDFILE.write_text(str(proc.pid))
    log(f"mqtt recorder started pid={proc.pid} for {date} {venue}")


def shutil_which(binary: str) -> str | None:
    for p in os.environ.get("PATH", "").split(":"):
        cand = Path(p) / binary
        if cand.exists():
            return str(cand)
    return None


def mqtt_stop() -> None:
    if not mqtt_running():
        MQTT_PIDFILE.unlink(missing_ok=True)
        return
    pid = int(MQTT_PIDFILE.read_text().strip())
    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        pass
    MQTT_PIDFILE.unlink(missing_ok=True)
    log("mqtt recorder stopped")


# ------------------------------------------------------------------------- daemon
def pick_odds_races(open_races: list[int], races: list[dict],
                    mid: str) -> tuple[list[int], bool, bool]:
    """Which races to poll this cycle, and whether we are in the final-5-min / final-30-s phase."""
    imminent, secs_left = imminent_race(races)
    sprint = imminent is not None and secs_left <= 70
    final5 = imminent is not None and secs_left <= 300
    if final5 and imminent in open_races:
        return [imminent], sprint, final5
    if not open_races:          # every race is closed/finished - nothing to poll this cycle
        return [], sprint, final5
    cursor = ODDS_CURSOR.get(mid, 0)
    pick = [open_races[(cursor + i) % len(open_races)] for i in range(min(3, len(open_races)))]
    ODDS_CURSOR[mid] = (cursor + len(pick)) % len(open_races)
    return pick, sprint, final5


def daemon(extra_targets: list[tuple[str, str]] | None) -> None:
    DATA.mkdir(parents=True, exist_ok=True)
    PIDFILE.write_text(str(os.getpid()))
    log(f"collector daemon start pid={os.getpid()}")
    stop = {"flag": False}

    def handle(signum, _frame):
        stop["flag"] = True
        log(f"signal {signum} -> shutting down")
        mqtt_stop()

    signal.signal(signal.SIGTERM, handle)
    signal.signal(signal.SIGINT, handle)
    last_purge = 0.0
    while not stop["flag"]:
        # User pause switch: `touch data/.nocollect` stops collection for good; the flag is kept
        # (unlike `.stop`) so the watchdog will not restart us until it is removed.
        if (DATA / ".nocollect").exists():
            mqtt_stop()
            log("collect pause file present -> shutting down (rm data/.nocollect to resume)")
            break
        try:
            if time.time() - last_purge > 6 * 3600:      # keep only the last 2 days of past meetings
                try:
                    import retention
                    res = retention.purge(2, verbose=False)
                    if res["meetings"]:
                        log(f"retention: dropped {len(res['meetings'])} old meetings "
                            f"({', '.join(res['meetings'])})")
                except Exception as exc:  # noqa: BLE001 - never kill the cycle
                    log(f"retention failed: {type(exc).__name__}: {exc}")
                last_purge = time.time()
            t0 = time.time()
            results = poll_once(extra_targets, verbose=True)
            interval, why = next_interval(results)
            try:
                rconn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=20)
                try:
                    wake = next_result_wake(rconn)
                finally:
                    rconn.close()
                if wake is not None and wake < interval:
                    interval, why = max(2, int(wake)), f"{why}+result-due"
            except sqlite3.Error:
                pass
            want_mqtt, venue, mdate = mqtt_should_run(results)
            if want_mqtt and not mqtt_running():
                mqtt_start(mdate, venue)
            elif not want_mqtt and mqtt_running():
                mqtt_stop()
            elapsed = time.time() - t0
            floor = 0.2 if interval <= 2 else 5      # a 1 s sprint must not be floored to 5 s
            sleep_for = max(floor, interval - elapsed)
            log(f"cycle done in {elapsed:.1f}s ({why}); sleeping {sleep_for:.0f}s")
            deadline = time.time() + sleep_for
            while time.time() < deadline and not stop["flag"]:
                if (DATA / ".nocollect").exists():
                    mqtt_stop()
                    log("collect pause file seen -> shutting down")
                    stop["flag"] = True
                    break
                if (DATA / ".stop").exists():
                    (DATA / ".stop").unlink(missing_ok=True)
                    stop["flag"] = True
                    log("stop file seen -> shutting down")
                    break
                time.sleep(min(2, deadline - time.time()))
        except Exception as exc:  # keep the recorder alive no matter what
            log(f"cycle failed: {type(exc).__name__}: {exc}")
            try:
                # a half-finished transaction would keep the DB locked for every other reader
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            time.sleep(30)
    try:
        if PIDFILE.exists() and PIDFILE.read_text().strip() == str(os.getpid()):
            PIDFILE.unlink()
    except OSError:
        pass
    log("collector daemon exit")


# ------------------------------------------------------------------------ summary
def summary() -> None:
    if not DB.exists():
        print("no database yet")
        return
    conn = db()
    print(f"db: {DB}  ({DB.stat().st_size/1024:.0f} KB)")
    for mid, date, venue, first, last_seen in conn.execute(
            "SELECT meeting_id, date, venue, first_seen, last_seen FROM meeting ORDER BY date"):
        n_poll, = conn.execute("SELECT COUNT(*) FROM poll WHERE meeting_id=? AND ok=1",
                               (mid,)).fetchone()
        n_pool, = conn.execute("SELECT COUNT(*) FROM pool_change WHERE meeting_id=?",
                               (mid,)).fetchone()
        n_odds, = conn.execute("SELECT COUNT(*) FROM odds_change WHERE meeting_id=?",
                               (mid,)).fetchone()
        tot = conn.execute("SELECT MIN(total_inv), MAX(total_inv) FROM poll WHERE meeting_id=?",
                           (mid,)).fetchone()
        sale = conn.execute("SELECT ts FROM event WHERE meeting_id=? AND kind='sale_open' "
                            "ORDER BY ts LIMIT 1", (mid,)).fetchone()
        print(f"\n{mid}  ({date} {venue})")
        print(f"  first poll {first}   last poll {last_seen}")
        print(f"  polls={n_poll}  pool_changes={n_pool}  odds_changes={n_odds}")
        print(f"  totalInvestment min/max = {tot[0]} / {tot[1]}")
        print(f"  sale open: {sale[0] if sale else 'not yet'}")
        top = conn.execute(
            "SELECT odds_type, leg, COUNT(*) c FROM pool_change WHERE meeting_id=? "
            "GROUP BY odds_type, leg ORDER BY c DESC LIMIT 5", (mid,)).fetchall()
        if top:
            print("  busiest pools: " + ", ".join(f"{t}[{l}]x{c}" for t, l, c in top))
    conn.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--daemon", action="store_true")
    ap.add_argument("--summary", action="store_true")
    ap.add_argument("--targets", action="append", default=[],
                    help="extra DATE:VENUE target, e.g. 2026-11-08:ST")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--baseline", action="store_true",
                    help="force-write the overnight baseline now (test/backfill)")
    a = ap.parse_args()

    extra = []
    for t in a.targets:
        date, _, venue = t.partition(":")
        if not date or not venue:
            sys.exit(f"bad --targets value: {t!r} (want DATE:VENUE)")
        extra.append((date, venue.upper()))
    if TARGETS_FILE.exists() and not extra:
        try:
            extra = [(d, v) for d, v in json.loads(TARGETS_FILE.read_text())]
        except (json.JSONDecodeError, TypeError):
            extra = []

    if a.summary:
        summary(); return
    if a.daemon:
        daemon(extra); return
    if a.once:
        res = poll_once(extra, verbose=True, force_baseline=a.baseline)
        if not res:
            print("no meetings returned")
        else:
            interval, why = next_interval(res)
            print(f"adaptive interval now: {interval}s ({why})")
        return
    ap.print_help()


if __name__ == "__main__":
    main()
