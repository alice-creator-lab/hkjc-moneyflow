#!/usr/bin/env python3
"""Phase 0 monitor: read-only JSON API + static web UI over data/moneyflow.db.

    python3 serve.py --port 8787 --host 0.0.0.0

Endpoints
    GET /                      web/index.html  (optional ?k=<token>)
    GET /api/summary           meetings + collector health
    GET /api/meeting/<id>      races, runners, pools, series for one meeting
    GET /api/ping              {"ok": true}
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import dist as distmod

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
WEB = ROOT / "web"
DB = DATA / "moneyflow.db"
TOKEN_FILE = DATA / "webapp_token.txt"
HKT = timezone(timedelta(hours=8))


def now() -> datetime:
    return datetime.now(HKT)


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def token() -> str:
    return TOKEN_FILE.read_text().strip() if TOKEN_FILE.exists() else ""


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError, ValueError):
        return False


def read_pid(name: str) -> int | None:
    p = DATA / name
    try:
        return int(p.read_text().strip())
    except (OSError, ValueError):
        return None


# ------------------------------------------------------------------ data shaping
def pool_series(conn, mid: str, odds_type: str, leg: str, limit: int = 400):
    rows = conn.execute(
        "SELECT ts, inv FROM pool_change WHERE meeting_id=? AND odds_type=? AND leg=? "
        "AND inv IS NOT NULL ORDER BY ts DESC LIMIT ?", (mid, odds_type, leg, limit)).fetchall()
    return [[r["ts"], r["inv"]] for r in reversed(rows)]


def latest_pools(conn, mid: str) -> dict:
    """Latest row per (type, leg) plus the previous non-null investment for a delta."""
    out = {}
    rows = conn.execute(
        "SELECT odds_type, leg, status, sell, inv, last_upd, ts FROM pool_change "
        "WHERE meeting_id=? ORDER BY ts", (mid,)).fetchall()
    for r in rows:
        key = (r["odds_type"], r["leg"])
        prev = out.get(key)
        d = dict(r)
        d["prev_inv"] = None
        if prev is not None:
            if r["inv"] is not None and prev["inv"] is not None and r["inv"] != prev["inv"]:
                d["prev_inv"] = prev["inv"]
            elif prev.get("prev_inv") is not None:
                d["prev_inv"] = prev["prev_inv"]
        out[key] = d
    return out


def runner_extra(conn, mid: str) -> dict:
    out = {}
    for r in conn.execute(
            "SELECT race, runner, pla, drop_v, hot FROM odds_extra WHERE meeting_id=?", (mid,)):
        out[(r["race"], r["runner"])] = dict(r)
    return out


def runner_odds(conn, mid: str) -> dict:
    """latest + previous odds per (race, runner), plus change count.

    The table now grows by thousands of rows per meeting (1 s polling at the off), so the
    ranking is done in SQL and only the last 12 h are read instead of the whole history.
    """
    cut = (datetime.now() - timedelta(hours=12)).isoformat(timespec="seconds")
    rows = conn.execute(
        "SELECT race, runner, win_odds, ts, rn, n FROM ("
        "  SELECT race, runner, win_odds, ts,"
        "         ROW_NUMBER() OVER (PARTITION BY race, runner ORDER BY ts DESC) rn,"
        "         COUNT(*) OVER (PARTITION BY race, runner) n"
        "  FROM odds_change WHERE meeting_id=? AND ts>=?"
        ") WHERE rn<=2 ORDER BY race, runner, rn DESC", (mid, cut)).fetchall()
    out: dict[tuple[int, int], dict] = {}
    for r in rows:
        key = (r["race"], r["runner"])
        if r["rn"] == 1:
            out[key] = {"odds": r["win_odds"], "prev": None, "ts": r["ts"], "n": r["n"]}
        else:
            if key in out:
                out[key]["prev"] = r["win_odds"]
    return out


def race_distribution(conn, mid: str, race_no: int, runners: list, pools: list) -> dict:
    """Cash-per-horse for one race + the move since the last snapshot 3+ minutes old."""
    pool_inv = {(p["type"], p["leg"]): (p["inv"] or 0.0)
                for p in pools if p["leg"] == str(race_no)}
    combos = {}
    for r in conn.execute(
            "SELECT pool, combo, odds, n_legs, leg_index FROM combo_odds "
            "WHERE meeting_id=? AND race=?", (mid, race_no)):
        combos[(r["pool"], r["combo"])] = (r["odds"], r["n_legs"], r["leg_index"])

    d = distmod.compute(runners, pool_inv, combos)
    by_no = {h["no"]: h for h in d["horses"]}
    for r in runners:
        h = by_no.get(r.get("no"))
        if h:
            h["name"] = r.get("name")
            h["jockey"] = r.get("jockey")
            h["odds"] = h["odds"] or r.get("odds")
            h["pla_odds"] = h["pla_odds"] or r.get("pla")

    snaps = conn.execute(
        "SELECT ts, total, payload FROM dist_snapshot WHERE meeting_id=? AND race=? "
        "ORDER BY ts DESC LIMIT 40", (mid, race_no)).fetchall()
    prev = None
    if snaps:
        try:
            newest = datetime.fromisoformat(snaps[0]["ts"])
        except ValueError:
            newest = now()
        for s in snaps[1:]:
            try:
                ts = datetime.fromisoformat(s["ts"])
            except ValueError:
                continue
            if (newest - ts).total_seconds() >= 180:
                prev = s
                break

    # WQQP-style expectation: money share vs the win-odds implied share
    ip_total = sum(1.0 / h["odds"] for h in d["horses"] if h.get("odds"))
    for h in d["horses"]:
        exp = (1.0 / h["odds"]) / ip_total if (h.get("odds") and ip_total) else None
        h["expected"] = round(exp * 100, 1) if exp else None
        h["ratio"] = round(h["share"] / exp, 2) if exp else None

    if prev and d["horses"]:
        try:
            old = {row[0]: sum(row[1:]) for row in json.loads(prev["payload"])}
        except (json.JSONDecodeError, TypeError):
            old = {}
        for h in d["horses"]:
            base = old.get(h["no"])
            h["delta"] = (h["total"] - base) if base is not None else None
    for h in d["horses"]:
        h.setdefault("delta", None)

    return {
        "total": round(d["total"]),
        "horses": d["horses"],
        "since": prev["ts"] if prev else None,
        "latest_snapshot": snaps[0]["ts"] if snaps else None,
    }


_LS_CACHE: dict[tuple, tuple[float, dict | None]] = {}


def last_start(conn, meet_date: str, horse_name: str) -> dict | None:
    """Most recent previous start: closing (開跑) odds plus, when we watched that race live,
    the odds move over its final five minutes."""
    ck = (meet_date, horse_name)
    hit = _LS_CACHE.get(ck)
    if hit and time.time() - hit[0] < 600:
        return hit[1]
    val = _last_start(conn, meet_date, horse_name)
    _LS_CACHE[ck] = (time.time(), val)
    return val


def _last_start(conn, meet_date: str, horse_name: str) -> dict | None:
    row = conn.execute(
        "SELECT meeting_date, race, finish, odds, jockey, trainer FROM hist_start "
        "WHERE horse_name=? AND meeting_date < ? ORDER BY meeting_date DESC, race DESC LIMIT 1",
        (horse_name, meet_date)).fetchone()
    if not row:
        return None
    out = {"date": row["meeting_date"], "race": row["race"], "finish": row["finish"],
           "odds": row["odds"], "jockey": row["jockey"], "trainer": row["trainer"],
           "series": None, "t5": None, "close": None}
    # our own recording of that race (only meetings the collector covered)
    mid_row = conn.execute(
        "SELECT meeting_id FROM race_meta WHERE meeting_id LIKE ? LIMIT 1",
        (row["meeting_date"] + "%",)).fetchone()
    if not mid_row:
        return out
    mid = mid_row["meeting_id"]
    meta = conn.execute("SELECT post_time FROM race_meta WHERE meeting_id=? AND race=?",
                        (mid, row["race"])).fetchone()
    if not meta or not meta["post_time"]:
        return out
    try:
        post = datetime.fromisoformat(meta["post_time"])
    except (ValueError, TypeError):
        return out
    lo = (post - timedelta(minutes=5)).isoformat()
    hi = post.isoformat()
    rows = conn.execute(
        "SELECT ts, win_odds FROM odds_change WHERE meeting_id=? AND race=? AND runner=? "
        "AND ts >= ? AND ts <= ? ORDER BY ts",
        (mid, row["race"], 0, lo, hi)).fetchall()
    if not rows:
        # runner number is not stored by name; match via runner_meta
        rm = conn.execute("SELECT runner FROM runner_meta WHERE meeting_id=? AND race=? AND name_ch=?",
                          (mid, row["race"], horse_name)).fetchone()
        if rm:
            rows = conn.execute(
                "SELECT ts, win_odds FROM odds_change WHERE meeting_id=? AND race=? AND runner=? "
                "AND ts >= ? AND ts <= ? ORDER BY ts",
                (mid, row["race"], rm["runner"], lo, hi)).fetchall()
    if rows:
        out["series"] = [[r["ts"][11:16], r["win_odds"]] for r in rows]
        out["t5"] = rows[0]["win_odds"]
        out["close"] = rows[-1]["win_odds"]
    return out


def win_flow(conn, mid: str, race_no: int, runners: list, post_time: str | None) -> dict | None:
    """WIN-pool money per horse, split into "before T-15" and the late money added after it.

    WIN stake is derived from the pool investment and the odds (pari-mutuel), so the level is
    the market's own view -- what carries information is how fast a horse's stake grows late.
    """
    # only the last few hours matter for the time bands; the full history can be 100k+ rows
    post_dt = None
    try:
        pt = str(post_time or "")
        post_dt = datetime.fromisoformat(pt) if "T" in pt else None
    except ValueError:
        post_dt = None
    now_dt = datetime.now(post_dt.tzinfo) if (post_dt and post_dt.tzinfo) else datetime.now()
    if post_dt and post_dt > now_dt:          # still selling: keep the whole overnight window
        cut = (post_dt - timedelta(hours=30)).isoformat(timespec="seconds")
    else:                                      # finished: the last few hours are enough
        cut = ((post_dt or now_dt) - timedelta(hours=6)).isoformat(timespec="seconds")
    pool = conn.execute(
        "SELECT ts, inv FROM pool_change WHERE meeting_id=? AND odds_type='WIN' AND leg=? "
        "AND inv IS NOT NULL AND ts>=? ORDER BY ts", (mid, str(race_no), cut)).fetchall()
    odds = conn.execute(
        "SELECT ts, runner, win_odds FROM odds_change WHERE meeting_id=? AND race=? AND ts>=? "
        "ORDER BY ts", (mid, race_no, cut)).fetchall()
    if not pool or not odds:
        return None

    timeline = []
    oi, cur = 0, {}
    for row in pool:
        while oi < len(odds) and odds[oi]["ts"] <= row["ts"]:
            cur[odds[oi]["runner"]] = odds[oi]["win_odds"]
            oi += 1
        denom = sum(1.0 / v for v in cur.values() if v)
        if not denom:
            continue
        stakes = {no: row["inv"] * (1.0 / v) / denom for no, v in cur.items() if v}
        timeline.append((row["ts"], row["inv"], stakes))
    if not timeline:
        return None

    # one bucket per minute (keep the last reading in each)
    by_min = {}
    for ts, inv, stakes in timeline:
        by_min[ts[:16]] = (ts, inv, stakes)
    series = [by_min[k] for k in sorted(by_min)]

    post = None
    try:
        pt = str(post_time or "")
        post = datetime.fromisoformat(pt) if "T" in pt else None
    except ValueError:
        post = None
    official = post            # 排位時間 T; the bands are measured from here

    # once the pool stops moving the real off has passed; anchor the bands to that moment
    # (a delayed jump would otherwise dump minutes of money into the last 20-second bucket)
    if post:
        try:
            last_dt = datetime.fromisoformat(timeline[-1][0])
            ref = datetime.now(last_dt.tzinfo) if last_dt.tzinfo else datetime.now()
            frozen = (ref - last_dt).total_seconds() > 120
            if frozen:
                final_inv = timeline[-1][1]
                for ts_i, inv_i, _ in timeline:
                    if inv_i >= final_inv * 0.999 and datetime.fromisoformat(ts_i) > post:
                        post = datetime.fromisoformat(ts_i)
                        break
        except ValueError:
            pass

    def sample(target: str):
        """latest (ts, inv, stakes) at or before target (timeline is second resolution)"""
        lo, hi, best = 0, len(timeline) - 1, None
        while lo <= hi:
            i = (lo + hi) // 2
            if timeline[i][0] <= target:
                best, lo = timeline[i], i + 1
            else:
                hi = i - 1
        return best

    now_ts, now_inv, now_s = timeline[-1]

    # short-window inflow per horse: what arrived in the last 3 and last 1 minutes
    def held_back(secs: int) -> dict:
        hit = sample((datetime.fromisoformat(now_ts) - timedelta(seconds=secs)).isoformat())
        if not hit:
            return {}
        span = (datetime.fromisoformat(now_ts) - datetime.fromisoformat(hit[0])).total_seconds()
        # the nearest sample is too old (collector was idling) -> the window is not real
        return hit[2] if span <= secs + 180 else {}

    s3m, s1m = held_back(180), held_back(60)

    # money already standing at each band boundary; missing boundary => band is still empty
    marks = {}
    t15_ts = None
    SLICES = 9                     # T-3 → T, one 20-second slice each
    POST_CELLS = 12                # after T: T+10, T+20 … while the jump is late
    grid: list[tuple[str, float]] = []
    if official:
        t15_ts = (official - timedelta(minutes=15)).isoformat()
        grid.append(("t15", -15 * 60))
        grid.append(("t3", -3 * 60))
        grid += [(f"s{k}", -20 * (SLICES - k)) for k in range(1, SLICES + 1)]
        late = (post - official).total_seconds() if post else 0.0
        grid += [(f"p{k}", 10.0 * k) for k in range(1, 1 + min(POST_CELLS, max(0, int(late // 10))))]
    for name, off in grid:
        hit = sample((official + timedelta(seconds=off)).isoformat())
        marks[name] = hit[2] if hit else None
    cells = [name for name, _ in grid if name not in ("t15", "t3")]

    # overnight baseline: pool + odds as they stood at the overnight capture
    ov_s: dict[int, float] = {}
    b = conn.execute("SELECT win_inv, payload FROM baseline WHERE meeting_id=? AND race=?",
                     (mid, race_no)).fetchone()
    if b and b["payload"]:
        try:
            arr = json.loads(b["payload"])
            den = sum(1.0 / x[1] for x in arr if x and x[1])
            if den:
                ov_s = {int(x[0]): (b["win_inv"] or 0.0) * (1.0 / x[1]) / den
                        for x in arr if x and x[1]}
        except (ValueError, TypeError, json.JSONDecodeError):
            ov_s = {}

    # a real five-minute window; "six samples back" was only five minutes while we polled
    # constantly, and turns into hours once the collector idles between races
    tgt5 = (datetime.fromisoformat(now_ts) - timedelta(minutes=5)).isoformat()
    prev_hit = sample(tgt5)
    if prev_hit:
        _pts, prev_inv, prev_s = prev_hit
        span_min = (datetime.fromisoformat(now_ts) - datetime.fromisoformat(_pts)).total_seconds() / 60
    else:
        _pts, prev_inv, prev_s, span_min = None, 0.0, {}, 0.0
    total_now = sum(now_s.values()) or 1.0
    total_prev = sum(prev_s.values()) or 1.0
    base_s = marks.get("t15") or {}
    total_base = sum(base_s.values()) or total_now

    out = []
    for r in runners:
        no = r["no"]
        stake = now_s.get(no, 0.0)
        prev = prev_s.get(no, 0.0)

        def at(name):
            m = marks.get(name)
            return stake if m is None else m.get(no, 0.0)

        # cumulative, clamped so every band is >= 0 even if the series is patchy
        c_ov = min(ov_s.get(no, 0.0), stake)
        c15 = max(c_ov, min(at("t15"), stake))
        c3 = max(c15, min(at("t3"), stake))

        # walk the T-relative grid; clamping keeps every cell >= 0 and the row summing to stake
        row = {"no": no, "stake": round(stake), "overnight": round(c_ov),
               "day": round(c15 - c_ov), "p153": round(c3 - c15)}
        prev_c = c3
        for name in cells:
            c = max(prev_c, min(at(name), stake))
            row[name] = round(c - prev_c)
            prev_c = c
        row["tail"] = round(stake - prev_c)

        d3 = max(0.0, stake - s3m.get(no, 0.0)) if s3m else None
        d1 = max(0.0, stake - s1m.get(no, 0.0)) if s1m else None

        out.append({
            **row,
            "d3": round(d3) if d3 is not None else None,
            "d1": round(d1) if d1 is not None else None,
            "d3_pct": round(d3 / stake * 100, 1) if stake and d3 is not None else None,
            "d1_pct": round(d1 / stake * 100, 1) if stake and d1 is not None else None,
            "base": round(at("t15")),
            "late": round(max(0.0, stake - at("t15"))),
            "late_pct": round(max(0.0, stake - at("t15")) / stake * 100, 1) if stake else 0.0,
            "share": round(stake / total_now * 100, 2),
            "share_prev": round(prev / total_prev * 100, 2) if prev and 4 <= span_min <= 12 else None,
            "delta_pct": round((stake - prev) / prev * 100, 1) if prev > 0 and 4 <= span_min <= 12 else None,
        })
    out.sort(key=lambda h: -h["stake"])

    spark, spikes = [], []
    fair = 100.0 / max(1, len(runners))
    for ts, _inv, st in series[-12:]:
        tot = sum(st.values()) or 1.0
        spark.append({"t": ts[11:16],
                      "cells": {str(no): round(v / tot * 100, 1) for no, v in st.items()}})
    for r_ in runners:
        no = str(r_["no"])
        vals = [b["cells"].get(no, 0.0) for b in spark]
        if len(vals) < 3:
            continue
        med = sorted(vals)[len(vals) // 2]
        if med <= 0:
            continue
        for b in spark:
            v = b["cells"].get(no, 0.0)
            if v >= med * 1.5 and v >= fair:
                spikes.append({"no": r_["no"], "t": b["t"], "share": v})

    return {
        "spark": spark, "spikes": spikes[:20],
        "total": round(total_now), "base_total": round(total_base),
        "late_total": round(max(0.0, total_now - total_base)),
        "t15": t15_ts, "now": now_ts, "prev": _pts,
        "bands": {
            "overnight": round(sum(h["overnight"] for h in out)),
            "day": round(sum(h["day"] for h in out)),
            "p153": round(sum(h["p153"] for h in out)),
            **{name: round(sum(h.get(name) or 0 for h in out)) for name in cells},
            "tail": round(sum(h["tail"] for h in out)),
            # rolling windows, independent of the band grid
            "last3": round(sum(h["d3"] or 0 for h in out)) if s3m else None,
            "last1": round(sum(h["d1"] or 0 for h in out)) if s1m else None,
            "top3": max(out, key=lambda h: h["d3"] or 0)["no"] if s3m else None,
            "top1": max(out, key=lambda h: h["d1"] or 0)["no"] if s1m else None,
        },
        "minutes": len(series), "horses": out,
    }


BLOCK_LABELS = {"QIN": "連贏", "QPL": "位置Q", "FCT": "二重彩", "TRI": "單T",
                "FF": "四連環", "TCE": "四重彩"}


def block_trades(conn, mid: str, race_no: int, pools: list) -> list:
    """大戶投注範圍: the biggest combinations in each exotic pool for this race."""
    inv = {}
    for p in pools:
        if p["leg"] == str(race_no):
            inv[p["type"]] = max(inv.get(p["type"], 0.0), p["inv"] or 0.0)
    rows = conn.execute(
        "SELECT pool, combo, odds FROM combo_odds WHERE meeting_id=? AND race=?",
        (mid, race_no)).fetchall()
    grouped = {}
    for r in rows:
        price = r["odds"]
        if not price or price <= 0:
            continue
        grouped.setdefault(r["pool"], []).append((r["combo"], 1.0 / price))

    out = []
    for pool, items in grouped.items():
        money = inv.get(pool, 0.0)
        if money <= 0:
            continue
        total_ip = sum(ip for _c, ip in items)
        if not total_ip:
            continue
        top = sorted(((c, money * ip / total_ip) for c, ip in items), key=lambda x: -x[1])[:3]
        out.append({
            "pool": pool, "label": BLOCK_LABELS.get(pool, pool), "total": round(money),
            "top": [{"c": c.replace(",", "-"), "money": round(m), "pct": round(m / money * 100, 1)}
                    for c, m in top],
        })
    order = ["QIN", "QPL", "FF", "TRI", "FCT", "TCE"]
    out.sort(key=lambda b: order.index(b["pool"]) if b["pool"] in order else 99)
    return out


def race_spectrum(conn, mid: str, race_no: int, horses: list, post_time: str | None = None) -> dict:
    """頻譜: money share per horse per time bucket, plus the '萬綠叢中一點紅' spikes."""
    rows = conn.execute(
        "SELECT ts, total, payload FROM dist_snapshot WHERE meeting_id=? AND race=? "
        "ORDER BY ts", (mid, race_no)).fetchall()
    if not rows:
        return {"buckets": [], "horses": [], "spikes": []}

    # one bucket per ~2 minutes; keep the last 30
    buckets = {}
    for r in rows:
        try:
            ts = datetime.fromisoformat(r["ts"])
        except ValueError:
            continue
        key = ts.replace(second=0, microsecond=0)
        key = key.replace(minute=key.minute - key.minute % 2)
        buckets[key] = r
    keys = sorted(buckets)[-12:]

    nos = [h["no"] for h in horses]
    out = []
    for k in keys:
        r = buckets[k]
        try:
            arr = json.loads(r["payload"])
        except (json.JSONDecodeError, TypeError):
            continue
        tot = {row[0]: sum(row[1:]) for row in arr}
        grand = sum(tot.values()) or 1.0
        mins = None
        if post_time:
            try:
                pt = str(post_time)
                if "T" in pt:
                    post = datetime.fromisoformat(pt)
                else:
                    hh, mm = [int(x) for x in pt.split(":")[:2]]
                    post = k.replace(hour=hh, minute=mm, second=0, microsecond=0)
                mins = int((k - post).total_seconds() // 60)
            except (ValueError, TypeError):
                mins = None
        out.append({
            "t": k.strftime("%H:%M"), "m": mins,
            "total": round(grand),
            "cells": {str(no): round(tot.get(no, 0.0) / grand * 100, 1) for no in nos},
        })

    # spike = a cell well above that horse's own median across the window
    spikes = []
    fair = 100.0 / max(1, len(nos))
    if len(out) >= 3:
        for no in nos:
            series = [b["cells"].get(str(no), 0.0) for b in out]
            med = sorted(series)[len(series) // 2]
            for b in out:
                v = b["cells"].get(str(no), 0.0)
                if v / fair >= 2 and v >= med * 1.5 and med > 0:
                    spikes.append({"no": no, "t": b["t"], "share": v,
                                   "base": round(med, 1), "ratio": round(v / fair, 1)})
    return {"buckets": out, "horses": nos, "spikes": spikes[:40]}


_MP_CACHE: dict[str, tuple[float, dict]] = {}


def meeting_payload(conn, mid: str, ttl: float = 1.5) -> dict:
    hit = _MP_CACHE.get(mid)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    out = _meeting_payload(conn, mid)
    _MP_CACHE[mid] = (time.time(), out)
    return out


def race_phase(post_time: str | None, has_pool: bool, meet_phase: str) -> str:
    if meet_phase == "finished":
        return "finished"
    if not post_time:
        return "presale" if not has_pool else "selling"
    try:
        pt = datetime.fromisoformat(str(post_time))
    except ValueError:
        return "presale"
    now_dt = datetime.now(pt.tzinfo) if pt.tzinfo else datetime.now()
    left = (pt - now_dt).total_seconds()
    if left <= 0:
        return "jumped"
    if left <= 900:
        return "imminent"
    return "selling" if has_pool else "presale"


def _meeting_payload(conn, mid: str) -> dict:
    meet_date = str(mid).split("_")[0]
    m = conn.execute("SELECT * FROM meeting WHERE meeting_id=?", (mid,)).fetchone()
    if not m:
        return {"error": "unknown meeting"}
    polls = conn.execute(
        "SELECT ts, status, total_inv, cur_race, n_pools, n_runners FROM poll "
        "WHERE meeting_id=? AND ok=1 ORDER BY ts", (mid,)).fetchall()
    sale = conn.execute(
        "SELECT ts FROM event WHERE meeting_id=? AND kind='sale_open' ORDER BY ts LIMIT 1",
        (mid,)).fetchone()

    pools = [{"type": k[0], "leg": k[1], **v} for k, v in latest_pools(conn, mid).items()]
    pools.sort(key=lambda p: (p["type"], p["leg"]))

    ODDS_ALL = runner_odds(conn, mid)      # once per meeting, not per race
    EXTRA_ALL = runner_extra(conn, mid)
    _posts = [r["post_time"] for r in conn.execute(
        "SELECT post_time FROM race_meta WHERE meeting_id=? ORDER BY race", (mid,)) if r["post_time"]]
    meet_phase = meeting_phase(polls[-1]["status"] if polls else None,
                               sale["ts"] if sale else None,
                               min(_posts) if _posts else None,
                               max(_posts) if _posts else None)
    races = []
    for rm in conn.execute("SELECT * FROM race_meta WHERE meeting_id=? ORDER BY race", (mid,)):
        rno = rm["race"]
        rmeta = conn.execute(
            "SELECT * FROM runner_meta WHERE meeting_id=? AND race=? ORDER BY runner",
            (mid, rno)).fetchall()
        odds = ODDS_ALL
        extra = EXTRA_ALL
        win_pool = conn.execute(
            "SELECT inv FROM pool_change WHERE meeting_id=? AND odds_type='WIN' AND leg=? "
            "ORDER BY ts DESC LIMIT 1", (mid, str(rno))).fetchone()
        win_inv = (win_pool["inv"] if win_pool else None) or 0

        base = conn.execute(
            "SELECT ts, win_inv, payload FROM baseline WHERE meeting_id=? AND race=?",
            (mid, rno)).fetchone()
        ov = {}
        if base:
            try:
                ov = {int(n): (w, p) for n, w, p in json.loads(base["payload"])}
            except (json.JSONDecodeError, TypeError, ValueError):
                ov = {}

        runners, inv_sum = [], 0.0
        for r in rmeta:
            o = odds.get((rno, r["runner"])) or {}
            price = o.get("odds")
            ip = (1.0 / price) if price and price > 0 else 0.0
            inv_sum += ip
            runners.append({
                "no": r["runner"], "name": r["name_ch"], "name_en": r["name_en"],
                "jockey": r["jockey_ch"], "trainer": r["trainer_ch"],
                "draw": r["draw"], "weight": r["weight"], "rating": r["rating"],
                "last6": r["last6run"], "gear": r["gear"], "odds": price, "odds_prev": o.get("prev"),
                "pla": (extra.get((rno, r["runner"])) or {}).get("pla"),
                "drop": (extra.get((rno, r["runner"])) or {}).get("drop_v"),
                "hot": bool((extra.get((rno, r["runner"])) or {}).get("hot")),
                "ip": ip,
                "last_start": last_start(conn, meet_date, r["name_ch"]),
                "ov_odds": ov.get(r["runner"], (None, None))[0],
                "ov_pla": ov.get(r["runner"], (None, None))[1],
            })
        for r in runners:
            share = (r["ip"] / inv_sum) if inv_sum else 0.0
            r["share"] = round(share, 5)
            r["stake"] = round(win_inv * share) if win_inv else None
        res_row = conn.execute(
            "SELECT payload FROM race_result WHERE meeting_id=? AND race=?", (mid, rno)).fetchone()
        result = None
        if res_row and res_row["payload"]:
            try:
                result = json.loads(res_row["payload"])
            except ValueError:
                result = None
        rphase = race_phase(rm["post_time"], bool(win_pool), meet_phase)
        if result and result.get("ok"):
            rphase = "finished"          # a stored 賽果 settles the race regardless of the clock
        elif rphase == "jumped":
            # 賽果 未到，但開跑超過 10 分鐘就當完咗，唔好一路卡住「已開跑」
            try:
                pt = datetime.fromisoformat(str(rm["post_time"]))
                now_dt = datetime.now(pt.tzinfo) if pt.tzinfo else datetime.now()
                if (now_dt - pt).total_seconds() >= 600:
                    rphase = "finished"
            except (TypeError, ValueError):
                pass
        races.append({
            "result": result,
            "phase": rphase,
            "no": rno, "post_time": rm["post_time"], "name": rm["race_name_ch"],
            "cls": rm["race_class_ch"], "distance": rm["distance"], "track": rm["track_ch"],
            "going": rm["going_ch"], "n_runners": rm["n_runners"],
            "win_pool": win_inv or None, "runners": runners,
            "overnight": ({
                "ts": base["ts"], "win_inv": base["win_inv"], "flow": round(win_inv - base["win_inv"]),
            } if base else None),
            "dist": race_distribution(conn, mid, rno, runners, pools),
            "spectrum": race_spectrum(conn, mid, rno, runners, rm["post_time"]),
            "blocks": block_trades(conn, mid, rno, pools),
            "winflow": win_flow(conn, mid, rno, runners, rm["post_time"]),
        })

    total_series = [[p["ts"], p["total_inv"]] for p in polls if p["total_inv"] is not None]
    win_by_race, pla_by_race = {}, {}
    for rm in conn.execute("SELECT race FROM race_meta WHERE meeting_id=? ORDER BY race", (mid,)):
        rno = str(rm["race"])
        win_by_race[rno] = pool_series(conn, mid, "WIN", rno)
        pla_by_race[rno] = pool_series(conn, mid, "PLA", rno)

    return {
        "id": mid, "date": m["date"], "venue": m["venue"],
        "status": polls[-1]["status"] if polls else None,
        "phase": meeting_phase(polls[-1]["status"] if polls else None,
                               sale["ts"] if sale else None,
                               min((r["post_time"] for r in races if r.get("post_time")), default=None),
                               max((r["post_time"] for r in races if r.get("post_time")), default=None)),
        "total_inv": polls[-1]["total_inv"] if polls else 0,
        "cur_race": polls[-1]["cur_race"] if polls else 0,
        "sale_open": sale["ts"] if sale else None,
        "last_poll": polls[-1]["ts"] if polls else m["last_seen"],
        "n_polls": len(polls),
        "races": races,
        "pools": pools,
        "series": {"total": total_series[-500:], "win": win_by_race, "pla": pla_by_race},
    }


FINISHED_STATUS = {"CLOSED", "FINAL", "RESULT", "FINISHED", "ABANDONED"}


def meeting_phase(status: str | None, sale_open: str | None, first_post: str | None,
                  last_post: str | None) -> str:
    """One word the UI can trust; HKJC's own status stays DEFINED while a meeting is selling."""
    now_dt = datetime.now()
    if status and str(status).upper() in FINISHED_STATUS:
        return "finished"
    try:
        if last_post:
            lp = datetime.fromisoformat(str(last_post))
            if lp.tzinfo:
                lp = lp.astimezone().replace(tzinfo=None)
            if now_dt > lp + timedelta(minutes=10):
                return "finished"
    except ValueError:
        pass
    if not sale_open:
        return "presale"
    try:
        if first_post:
            fp = datetime.fromisoformat(str(first_post))
            if fp.tzinfo:
                fp = fp.astimezone().replace(tzinfo=None)
            if now_dt >= fp:
                return "racing"
    except ValueError:
        pass
    return "selling"


def summary_payload() -> dict:
    if not DB.exists():
        return {"meetings": [], "health": {"collector": False, "error": "no database"}}
    conn = db()
    meetings = []
    for m in conn.execute("SELECT * FROM meeting ORDER BY date"):
        mid = m["meeting_id"]
        last = conn.execute(
            "SELECT ts, status, total_inv, n_pools, n_runners FROM poll "
            "WHERE meeting_id=? AND ok=1 ORDER BY ts DESC LIMIT 1", (mid,)).fetchone()
        sale = conn.execute(
            "SELECT ts FROM event WHERE meeting_id=? AND kind='sale_open' LIMIT 1", (mid,)
        ).fetchone()
        changes = conn.execute(
            "SELECT COUNT(*) c FROM pool_change WHERE meeting_id=?", (mid,)).fetchone()["c"]
        rmeta = conn.execute(
            "SELECT COUNT(*) c FROM runner_meta WHERE meeting_id=?", (mid,)).fetchone()["c"]
        times = conn.execute(
            "SELECT MIN(post_time) a, MAX(post_time) b FROM race_meta WHERE meeting_id=?",
            (mid,)).fetchone()
        phase = meeting_phase(last["status"] if last else None,
                              sale["ts"] if sale else None,
                              times["a"] if times else None,
                              times["b"] if times else None)
        meetings.append({
            "id": mid, "date": m["date"], "venue": m["venue"],
            "phase": phase,
            "status": last["status"] if last else None,
            "total_inv": last["total_inv"] if last else 0,
            "last_poll": last["ts"] if last else m["last_seen"],
            "pools": last["n_pools"] if last else 0,
            "runners": rmeta, "changes": changes,
            "sale_open": sale["ts"] if sale else None,
        })

    last_poll = max((x["last_poll"] for x in meetings), default=None)
    age = None
    if last_poll:
        try:
            age = round((now() - datetime.fromisoformat(last_poll)).total_seconds())
        except ValueError:
            age = None
    meta = {}
    if (DATA / "query.meta.json").exists():
        try:
            meta = json.loads((DATA / "query.meta.json").read_text())
        except json.JSONDecodeError:
            meta = {}
    return {
        "now": now().isoformat(timespec="seconds"),
        "meetings": meetings,
        "health": {
            "collector": pid_alive(read_pid("collector.pid") or 0),
            "mqtt": pid_alive(read_pid("mqtt.pid") or 0),
            "last_poll": last_poll, "last_poll_age_s": age,
            "query_extracted_at": meta.get("extracted_at"),
            "db_kb": round(DB.stat().st_size / 1024) if DB.exists() else 0,
        },
    }


# ----------------------------------------------------------------------- server
class Handler(BaseHTTPRequestHandler):
    server_version = "moneyflow-phase0"

    def log_message(self, fmt, *args):  # quieter logs
        if "/api/" not in fmt % args:
            return

    def _send(self, code: int, body: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code: int = 200):
        self._send(code, json.dumps(obj, ensure_ascii=False).encode(), "application/json; charset=utf-8")

    def _authed(self, qs: dict) -> bool:
        want = token()
        if not want:
            return True
        given = (qs.get("k", [""])[0] or self.headers.get("X-Token", ""))
        if given == want:
            return True
        cookie = self.headers.get("Cookie", "")
        return f"k={want}" in cookie

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path, qs = parsed.path, urllib.parse.parse_qs(parsed.query)
        try:
            if path == "/api/ping":
                return self._json({"ok": True})

            if not self._authed(qs) and path != "/api/ping":
                return self._send(401, b"unauthorized", "text/plain; charset=utf-8")

            if path == "/api/summary":
                return self._json(summary_payload())

            if path.startswith("/api/meeting/"):
                mid = urllib.parse.unquote(path[len("/api/meeting/"):])
                return self._json(meeting_payload(db(), mid))

            if path in ("/", "/index.html"):
                body = (WEB / "index.html").read_bytes()
                resp = self._send
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                if token() and qs.get("k"):
                    self.send_header("Set-Cookie", f"k={token()}; Path=/; Max-Age=31536000; SameSite=Lax")
                self.end_headers()
                self.wfile.write(body)
                return

            if path.startswith("/static/"):
                f = (WEB / path[len("/static/"):]).resolve()
                if not str(f).startswith(str(WEB)) or not f.is_file():
                    return self._send(404, b"not found", "text/plain")
                ctype = {"js": "application/javascript", "css": "text/css"}.get(
                    f.suffix.lstrip("."), "application/octet-stream")
                return self._send(200, f.read_bytes(), ctype + "; charset=utf-8")

            return self._send(404, b"not found", "text/plain")
        except BrokenPipeError:
            pass
        except Exception as exc:  # noqa: BLE001
            self._json({"error": f"{type(exc).__name__}: {exc}"}, 500)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--host", default="127.0.0.1")
    a = ap.parse_args()
    if not DB.exists():
        raise SystemExit(f"no database at {DB} - run collect.py first")
    srv = ThreadingHTTPServer((a.host, a.port), Handler)
    print(f"monitor on http://{a.host}:{a.port}/  (db={DB})", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
