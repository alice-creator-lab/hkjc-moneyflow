#!/usr/bin/env python3
"""Freeze the per-horse money-flow features of every finished race into a permanent table.

`collect.py` keeps only 2 days of raw odds/pool rows (retention.py), which is enough for the
dashboard but not for long-run backtesting. This job runs before the purge window closes and
stores one row per (race, horse) — bands, share, delta, odds, finishing position — in
`race_signal`, which retention never touches. The web UI is unchanged.
"""
from __future__ import annotations

import json
import sqlite3
import time
import urllib.request
from datetime import datetime, timezone, timedelta

DB = "/home/admin/.hermes/profiles/friend/workspace/hkjc-moneyflow/data/moneyflow.db"
TOKEN_FILE = "/home/admin/.hermes/profiles/friend/workspace/hkjc-moneyflow/data/webapp_token.txt"
API = "http://127.0.0.1:8787"

SCHEMA = """
CREATE TABLE IF NOT EXISTS race_signal (
  meeting_id TEXT, race INTEGER, horse_no INTEGER,
  ts TEXT, name TEXT, finish INTEGER, n_runners INTEGER,
  odds REAL, stake REAL, share REAL, share_prev REAL,
  late_pct REAL, delta_pct REAL,
  overnight REAL, day REAL, p153 REAL, m31 REAL, s40m1 REAL, s20_40 REAL, tail REAL,
  s1 REAL, s2 REAL, s3 REAL, s4 REAL, s5 REAL, s6 REAL, s7 REAL, s8 REAL, s9 REAL,
  in_top3_money INTEGER, in_top3_late INTEGER,
  PRIMARY KEY (meeting_id, race, horse_no)
);
"""


COLS = ("meeting_id", "race", "horse_no", "ts", "name", "finish", "n_runners", "odds",
        "stake", "share", "share_prev", "late_pct", "delta_pct",
        "overnight", "day", "p153", "m31", "s40m1", "s20_40", "tail",
        "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9",
        "in_top3_money", "in_top3_late")


def api(path: str, token: str) -> dict:
    req = urllib.request.Request(f"{API}{path}?k={token}", headers={"User-Agent": "moneyflow-signal"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def main() -> None:
    token = open(TOKEN_FILE, encoding="utf-8").read().strip()
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")
    for attempt in range(6):                       # collector writes every second during a sprint
        try:
            conn.execute(SCHEMA)
            have = {row[1] for row in conn.execute("PRAGMA table_info(race_signal)")}
            for k in range(1, 10):
                if f"s{k}" not in have:
                    conn.execute(f"ALTER TABLE race_signal ADD COLUMN s{k} REAL")
            conn.commit()
            break
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc) or attempt == 5:
                raise
            time.sleep(3)

    # rows stored before the 20-second slices existed have s1..s9 empty - recompute those
    done = {(m, r) for m, r in conn.execute(
        "SELECT DISTINCT meeting_id, race FROM race_signal WHERE "
        + " + ".join(f"COALESCE(s{k}, 0)" for k in range(1, 10)) + " > 0")}
    todo = [row for row in conn.execute(
        "SELECT DISTINCT meeting_id, race FROM race_result "
        "WHERE payload LIKE '%\"ok\": true%' ORDER BY meeting_id, race") if row not in done]
    if not todo:
        print("nothing new")
        return

    by_mid: dict[str, list[int]] = {}
    for mid, rno in todo:
        by_mid.setdefault(mid, []).append(rno)

    now = datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
    written = 0
    for mid, races in by_mid.items():
        try:
            payload = api(f"/api/meeting/{mid}", token)
        except Exception as exc:  # noqa: BLE001
            print(f"{mid}: api error {type(exc).__name__}: {exc}")
            continue
        for race in payload.get("races") or []:
            if race.get("no") not in races:
                continue
            horses = ((race.get("winflow") or {}).get("horses") or [])
            rows = {int(h["no"]): h for h in ((race.get("result") or {}).get("rows") or [])
                    if str(h.get("no", "")).isdigit()}
            if not horses or not rows:
                print(f"{mid} R{race['no']}: no features yet")
                continue
            n = len(horses)
            fair = 100.0 / n
            by_share = sorted(horses, key=lambda h: -(h.get("share") or 0))[:3]
            by_late = sorted(horses, key=lambda h: -(
                (h.get("m31") or 0) + (h.get("s40m1") or 0) + (h.get("s20_40") or 0) + (h.get("tail") or 0)))[:3]
            top_share = {h["no"] for h in by_share}
            top_late = {h["no"] for h in by_late}
            batch = []
            for h in horses:
                no = h["no"]
                rr = rows.get(no) or {}
                try:
                    pos = int(str(rr.get("finish")).split("(")[0])
                except (TypeError, ValueError):
                    pos = None
                try:
                    odds = float(rr.get("odds")) if rr.get("odds") else None
                except (TypeError, ValueError):
                    odds = None
                batch.append((
                    mid, race["no"], no, now, rr.get("name") or h.get("name"), pos, n, odds,
                    h.get("stake") or 0, h.get("share") or 0, h.get("share_prev"),
                    h.get("late_pct") or 0, h.get("delta_pct") or 0,
                    h.get("overnight") or 0, h.get("day") or 0, h.get("p153") or 0,
                    h.get("m31") or 0, h.get("s40m1") or 0, h.get("s20_40") or 0, h.get("tail") or 0,
                    *[h.get(f"s{k}") or 0 for k in range(1, 10)],
                    1 if no in top_share and (h.get("share") or 0) / fair >= 1 else 0,
                    1 if no in top_late else 0,
                ))
            conn.executemany(
                "INSERT OR REPLACE INTO race_signal (" + ",".join(COLS) + ") VALUES (" +
                ",".join("?" * len(COLS)) + ")", batch)
            conn.commit()
            written += len(batch)
            print(f"{mid} R{race['no']}: {len(batch)} rows")
    print(f"done, {written} rows")


if __name__ == "__main__":
    main()
