#!/usr/bin/env python3
"""Backfill last-start closing (開跑) win odds from HKJC results pages.

The results page carries the final win odds for every runner, which is the number we need
for a horse's previous start.  Last-5-minute odds are not published historically -- those
only exist for meetings our own collector watched.
"""
from __future__ import annotations

import datetime as dt
import re
import sqlite3
import sys
import time
import urllib.request

DB = "/home/admin/.hermes/profiles/friend/workspace/hkjc-moneyflow/data/moneyflow.db"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
URL = ("https://racing.hkjc.com/racing/information/Chinese/Racing/LocalResults.aspx"
       "?RaceDate={date}&RaceNo={race}")

SCHEMA = """
CREATE TABLE IF NOT EXISTS hist_start (
  meeting_date TEXT, race INTEGER, horse_name TEXT, horse_id TEXT,
  finish TEXT, odds REAL, jockey TEXT, trainer TEXT, draw INTEGER, weight INTEGER,
  dist TEXT, going TEXT, cls TEXT, fetched_at TEXT,
  PRIMARY KEY (meeting_date, race, horse_name)
);
CREATE INDEX IF NOT EXISTS ix_hist_horse ON hist_start(horse_name);
"""

TAG = re.compile(r"<[^>]+>")


def clean(s: str) -> str:
    s = s.replace("&nbsp;", " ").replace("&amp;", "&")
    return re.sub(r"\s+", " ", TAG.sub(" ", s)).strip()


def fetch(date: str, race: int) -> str | None:
    req = urllib.request.Request(URL.format(date=date, race=race),
                                 headers={"User-Agent": UA, "Accept-Language": "zh-HK,zh;q=0.9"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.read().decode("utf-8", "ignore")
    except Exception:
        return None


def parse_rows(html: str) -> list[dict]:
    m = re.search(r'<div class="performance">(.*?)</table>', html, re.S)
    if not m:
        return []
    body = m.group(1)
    out = []
    for tr in re.findall(r"<tr>(.*?)</tr>", body, re.S):
        tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
        if len(tds) < 8:
            continue
        name_cell = tds[2]
        a = re.search(r">([^<]+)</a>", name_cell)
        name = (a.group(1) if a else clean(name_cell)).strip()
        hid = re.search(r"horseid=([A-Za-z0-9_]+)", name_cell)
        odds_txt = clean(tds[-1])
        try:
            odds = float(odds_txt)
        except ValueError:
            odds = None
        try:
            draw = int(clean(tds[7]))
        except (ValueError, IndexError):
            draw = None
        try:
            weight = int(clean(tds[5]))
        except (ValueError, IndexError):
            weight = None
        out.append({
            "finish": clean(tds[0])[:6], "no": clean(tds[1]),
            "name": name, "horse_id": hid.group(1) if hid else None,
            "jockey": clean(tds[3]), "trainer": clean(tds[4]),
            "weight": weight, "draw": draw,
            "dist": clean(tds[8])[:10], "going": clean(tds[9])[:16],
            "time": clean(tds[10])[:12], "odds": odds,
        })
    return out


DIV_TR = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S)


def parse_dividends(html: str) -> list[dict]:
    """派彩 table → [{pool, combo, dividend}] (rows after the first omit the pool name)."""
    i = html.find("派彩")
    if i < 0:
        return []
    out, pool = [], ""
    for tr in DIV_TR.findall(html[i:i + 14000]):
        cells = [clean(c) for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)]
        if not cells or cells[0] == "彩池":
            continue
        if len(cells) >= 3:
            pool, combo, div = cells[0], cells[1], cells[2]
        elif len(cells) == 2 and pool:
            combo, div = cells[0], cells[1]
        else:
            continue
        try:
            amount = float(div.replace(",", ""))
        except ValueError:
            amount = None
        out.append({"pool": pool, "combo": combo, "dividend": amount})
    return out


def fetch_result(date: str, race: int) -> dict:
    """Full result for one race: finishing order plus 派彩."""
    html = fetch(date, race)
    if not html:
        return {"ok": False, "reason": "fetch failed"}
    rows = parse_rows(html)
    if not rows:
        return {"ok": False, "reason": "no rows"}
    return {"ok": True, "rows": rows, "dividends": parse_dividends(html)}


def candidate_dates(days_back: int) -> list[str]:
    today = dt.date.today()
    out = []
    for i in range(1, days_back + 1):
        d = today - dt.timedelta(days=i)
        if d.weekday() in (2, 5, 6):          # Wed / Sat / Sun
            out.append(d.isoformat())
    return out


def main() -> None:
    days_back = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    conn = sqlite3.connect(DB)
    conn.executescript(SCHEMA)
    now = dt.datetime.now().isoformat(timespec="seconds")
    dates = candidate_dates(days_back)
    total_rows = 0
    for date in dates:
        if conn.execute("SELECT 1 FROM hist_start WHERE meeting_date=? LIMIT 1", (date,)).fetchone():
            print(f"{date}: cached", flush=True)
            continue
        first = fetch(date, 1)
        rows = parse_rows(first) if first else []
        if not rows:
            print(f"{date}: no meeting", flush=True)
            continue
        got = 0
        for race in range(1, 12):
            html = first if race == 1 else fetch(date, race)
            if not html:
                continue
            rs = parse_rows(html)
            if not rs:
                break
            for r in rs:
                if not r["name"] or r["name"] == "—":
                    continue
                conn.execute(
                    "INSERT OR REPLACE INTO hist_start VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (date, race, r["name"], r["horse_id"], r["finish"], r["odds"],
                     r["jockey"], r["trainer"], r["draw"], r["weight"],
                     r["dist"], r["going"], None, now))
                got += 1
            time.sleep(0.4)
        conn.commit()
        total_rows += got
        print(f"{date}: {got} runners", flush=True)
    print(f"done, {total_rows} rows", flush=True)


if __name__ == "__main__":
    main()
