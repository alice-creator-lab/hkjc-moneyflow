#!/usr/bin/env python3
"""Build a static snapshot of the monitor for GitHub Pages.

    python3 build_pages.py            # writes .pages/ (index.html, static/*, data.js)

The snapshot is self-contained: data.js sets window.MF_STATIC with the same
payloads /api/summary and /api/meeting/<id> return, so the UI runs with no server.
Refresh = rebuild + commit (the cron job on this box can do that on race days).
"""

from __future__ import annotations

import json
import shutil
import time
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import serve  # noqa: E402  (reuse the API shaping code)

OUT = ROOT / ".pages"
HKT = timezone(timedelta(hours=8))


def main() -> None:
    if not serve.DB.exists():
        raise SystemExit("no database yet")

    summary = serve.summary_payload()
    conn = serve.db()
    meetings = {}
    for m in summary["meetings"]:
        meetings[m["id"]] = serve.meeting_payload(conn, m["id"])
    conn.close()

    payload = {"summary": summary, "meetings": meetings, "generated_at": summary["now"]}

    # keep .git / README alive: delete only the generated artefacts
    (OUT / "static").mkdir(parents=True, exist_ok=True)
    for old in (OUT / "static").glob("*"):
        old.unlink()
    for name in ("index.html", "data.js"):
        (OUT / name).unlink(missing_ok=True)

    shutil.copy(ROOT / "web" / "style.css", OUT / "static" / "style.css")
    shutil.copy(ROOT / "web" / "app.js", OUT / "static" / "app.js")

    html = (ROOT / "web" / "index.html").read_text(encoding="utf-8")
    bust = str(int(time.time()))
    html = html.replace('static/style.css', f'static/style.css?v={bust}')
    html = html.replace(
        '<script src="static/app.js"></script>',
        f'<script src="data.js?v={bust}"></script>\n<script src="static/app.js?v={bust}"></script>')
    html = html.replace(
        '<p class="mono" id="boot"></p>',
        f'<p class="mono" id="boot"></p>\n<p class="mono">靜態快照 {payload["generated_at"]} · '
        f'由本機 collector 產生</p>')
    (OUT / "index.html").write_text(html, encoding="utf-8")

    (OUT / "data.js").write_text(
        "window.MF_STATIC = " + json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + ";\n",
        encoding="utf-8")
    (OUT / ".nojekyll").write_text("", encoding="utf-8")

    size = (OUT / "data.js").stat().st_size / 1024
    print(f"built {OUT}  meetings={len(meetings)}  data.js={size:.0f} KB  at "
          f"{datetime.now(HKT).isoformat(timespec='seconds')}")


if __name__ == "__main__":
    main()
