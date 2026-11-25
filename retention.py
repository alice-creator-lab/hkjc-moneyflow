#!/usr/bin/env python3
"""Keep only the last N days of *past* meetings in the live tables.

hist_start (past results / 開跑賠率 used for 上仗) is deliberately kept: it is form history,
not monitoring data.  Everything else for a meeting older than the cutoff goes, so the DB
stops growing without bound as second-level odds accumulate.
"""
from __future__ import annotations

import datetime as dt
import sqlite3
import sys

DB = "/home/admin/.hermes/profiles/friend/workspace/hkjc-moneyflow/data/moneyflow.db"
KEEP_DAYS = 2
TABLES = ("poll", "event", "pool_change", "odds_change", "odds_extra", "combo_odds",
          "dist_snapshot", "runner_meta", "race_meta", "baseline", "meeting")


def purge(keep_days: int = KEEP_DAYS, db: str = DB, verbose: bool = True) -> dict:
    cutoff = (dt.date.today() - dt.timedelta(days=keep_days)).isoformat()
    conn = sqlite3.connect(db, timeout=30)
    conn.row_factory = sqlite3.Row
    mids = [r[0] for r in conn.execute("SELECT meeting_id FROM meeting WHERE date < ?", (cutoff,))]
    removed = {}
    for t in TABLES:
        n = 0
        for mid in mids:
            cur = conn.execute(f"DELETE FROM {t} WHERE meeting_id=?", (mid,))
            n += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        removed[t] = n
    conn.commit()
    if mids and verbose:
        print(f"purged {len(mids)} meetings older than {cutoff}: {mids}")
        print("  rows:", {k: v for k, v in removed.items() if v})
    conn.close()
    return {"cutoff": cutoff, "meetings": mids, "rows": removed}


if __name__ == "__main__":
    days = int(sys.argv[1]) if len(sys.argv) > 1 else KEEP_DAYS
    purge(days)
