#!/usr/bin/env python3
"""Race results via HKJC's public GraphQL (works for local *and* simulcast meetings).

The LocalResults HTML only covers local races; the overseas results page renders client-side and
is fed by this same endpoint (POST /graphql/base/ with query RaceResultsProfile).
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path
import urllib.error
import urllib.request

GQL = "https://info.cld.hkjc.com/graphql/base/"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0 Safari/537.36"),
    "Origin": "https://racing.hkjc.com",
    "Referer": "https://racing.hkjc.com/",
    "Accept-Language": "zh-HK,zh;q=0.9",
}

QUERY = """
query RaceResultsProfile($date: String, $venueCode: String) {
  raceMeetingProfile(date: $date, venueCode: $venueCode) {
    date
    venueCode
    status
    races {
      no
      status
      postTime
      distance
      go_ch
      raceName_ch
      raceClass_ch
      runners {
        no
        id
        handicapWeight
        barrierDrawNumber
        winOdds
        finalPosition
        deadHeat
        margin
        lbwNm { chinese }
        marginNm { chinese }
        status
        horse { id name_ch name_en }
        jockey { name_ch }
        trainer { name_ch }
      }
      raceResults { status raceTime }
    }
    pmPools {
      oddsType
      name_ch
      status
      dividends(officialOnly: true) { winComb type div seq status }
    }
  }
}
"""


QUERY_FILE = Path(__file__).resolve().parent / "data" / "results_query.json"


def _query_text() -> str:
    """The endpoint whitelists queries by exact text, so replay the browser's query verbatim."""
    try:
        saved = json.loads(QUERY_FILE.read_text(encoding="utf-8"))
        if saved.get("query"):
            return saved["query"]
    except (OSError, ValueError):
        pass
    return QUERY


def _post(query: str, variables: dict | None) -> dict:
    body = json.dumps({"variables": variables, "query": query}).encode()
    req = urllib.request.Request(GQL, data=body, headers=HEADERS, method="POST")
    with urllib.request.urlopen(req, timeout=25) as r:
        raw = r.read()
    if raw[:2] == b"\x1f\x8b":                      # the gateway answers gzipped
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8", "ignore"))


def fetch_meeting(date: str, venue: str) -> dict:
    """{race_no: {ok, rows, dividends}} for every race that already has a result."""
    date = str(date).replace("/", "-")
    try:
        data = _post(_query_text(), {"date": date, "venueCode": venue})
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        return {"_error": f"{type(exc).__name__}: {exc}"}

    prof = (data.get("data") or {}).get("raceMeetingProfile") or {}
    if isinstance(prof, list):
        prof = prof[0] if prof else {}
    if not prof:
        return {"_error": (data.get("errors") or "no profile")}

    # every pool carries leg.number → the race it pays on
    divs_by_race: dict[int, list[dict]] = {}
    for p in prof.get("pmPools") or []:
        divs = p.get("dividends") or []
        if not divs:
            continue
        leg = p.get("leg") or {}
        try:
            rno = int(leg.get("number") or 0)
        except (TypeError, ValueError):
            rno = 0
        name = p.get("name_ch") or POOL_NAME.get(p.get("oddsType"), p.get("oddsType"))
        divs_by_race.setdefault(rno, []).extend(
            {"pool": name, "combo": d.get("winComb"), "dividend": d.get("div")} for d in divs)

    out = {}
    for race in prof.get("races") or []:
        runners = race.get("runners") or []
        rows = []
        for r in runners:
            pos = r.get("finalPosition")
            if pos in (None, "", 0):
                continue
            rows.append({
                "finish": str(pos),
                "no": str(r.get("no") or ""),
                "name": ((r.get("horse") or {}).get("name_ch") or "").strip(),
                "horse_id": (r.get("horse") or {}).get("id"),
                "jockey": ((r.get("jockey") or {}).get("name_ch") or "").strip(),
                "trainer": ((r.get("trainer") or {}).get("name_ch") or "").strip(),
                "weight": r.get("handicapWeight"),
                "draw": r.get("barrierDrawNumber"),
                "odds": r.get("winOdds"),
                "dist": ((r.get("lbwNm") or {}).get("chinese") or r.get("margin") or "").strip(),
                "dead_heat": bool(r.get("deadHeat")),
            })
        if not rows:
            continue
        rows.sort(key=lambda x: (int("".join(c for c in x["finish"] if c.isdigit()) or 99),
                                 int(x["no"] or 0)))
        res = race.get("raceResults") or {}
        if isinstance(res, list):
            res = res[0] if res else {}
        out[int(race.get("no") or 0)] = {
            "ok": True,
            "rows": rows,
            "dividends": divs_by_race.get(int(race.get("no") or 0), []),
            "race_time": res.get("raceTime"),
            "source": "graphql",
        }
    return out


POOL_NAME = {"WIN": "獨贏", "PLA": "位置", "QIN": "連贏", "QPL": "位置Q", "FCT": "二重彩",
             "TCE": "三重彩", "TRI": "單T", "FF": "四連環", "QTT": "四重彩", "DBL": "孖寶",
             "TBL": "三寶", "DT": "三T", "SixUP": "六環彩"}


def _unused_dividends_for(prof: dict, race: dict) -> list[dict]:
    """All pools' dividends for this race (pools carry their own race list)."""
    n = race.get("no")
    out = []
    for p in prof.get("pmPools") or []:
        legs = (p.get("leg") or [{}])
        races_list = []
        for leg in (legs if isinstance(legs, list) else [legs]):
            races_list.extend(leg.get("races") or [])
        divs = p.get("dividends") or []
        if not divs:
            continue
        # single-race pools: keep when the race number is in the pool's race list
        if races_list and int(n or 0) not in races_list:
            continue
        for d in divs:
            out.append({"pool": p.get("name_ch") or p.get("oddsType"),
                        "combo": d.get("winComb"),
                        "dividend": d.get("div")})
    return out
