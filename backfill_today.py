import sys, sqlite3, json, time
from datetime import datetime, timedelta
sys.path.insert(0, '/home/admin/.hermes/profiles/friend/workspace/hkjc-moneyflow')
import collect as C

DB = '/home/admin/.hermes/profiles/friend/workspace/hkjc-moneyflow/data/moneyflow.db'
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
conn.execute("""CREATE TABLE IF NOT EXISTS race_result (
  meeting_id TEXT, race INTEGER, ts TEXT, payload TEXT, PRIMARY KEY (meeting_id, race))""")
conn.commit()

now = C.now()
rows = conn.execute("""SELECT m.meeting_id, m.date, r.race, r.post_time
                      FROM race_meta r JOIN meeting m ON m.meeting_id=r.meeting_id
                      WHERE r.post_time IS NOT NULL ORDER BY m.date, r.race""").fetchall()
for r in rows:
    try:
        pt = datetime.fromisoformat(r['post_time'])
        if pt.tzinfo is None:
            pt = pt.replace(tzinfo=C.HKT)
    except ValueError:
        continue
    if (now - pt).total_seconds() < 120:
        continue
    if conn.execute("SELECT 1 FROM race_result WHERE meeting_id=? AND race=?",
                    (r['meeting_id'], r['race'])).fetchone():
        continue
    import backfill_results as br
    res = br.fetch_result(r['date'].replace('-', '/'), r['race'])
    conn.execute("INSERT OR REPLACE INTO race_result VALUES (?,?,?,?)",
                 (r['meeting_id'], r['race'], now.isoformat(timespec='seconds'),
                  json.dumps(res, ensure_ascii=False)))
    conn.commit()
    print(r['meeting_id'], 'R%s' % r['race'], 'ok' if res.get('ok') else 'miss', res.get('reason', ''))
print('done')
