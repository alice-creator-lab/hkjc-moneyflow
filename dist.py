"""Bet-distribution maths shared by the collector and the monitor.

Turns pool investment + odds into "how much money sits on each horse":

    WIN / PLA       pool money  x  implied-prob share (1/odds, normalised)
    QIN / QPL       each combo's money = pool x normalised 1/odds, split between its legs
    FCT / TRI / FF  same idea for forecast / trio / first-4 combination pools
    TCE / QTT       quartet / triple-trio, ditto
    multi-race      DBL / TBL / DT / TT / SixUP: combo money is divided per leg, then among
                    that leg's selections; only this race's leg is credited here

A horse's total is the sum of its segments — the number the 熱錢分佈 bars draw.
"""

from __future__ import annotations

COMBO_FIELDS = {
    "QIN": "qin", "QPL": "qpl",
    "FCT": "fct", "TRI": "tri", "FF": "ff", "TCE": "tce",
}
MULTI_POOLS = {"DBL", "TBL", "DT", "TT", "SixUP", "QTT"}


def _num(v):
    try:
        f = float(str(v).replace(",", ""))
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def compute(runners, pool_inv, combos):
    """
    runners : [{no, name, odds, pla}]
    pool_inv: {(odds_type, leg): investment}
    combos  : {(pool, "01,02"): (odds, n_legs, leg_index)}   (a bare number is accepted too)
    """
    horses = {}
    for r in runners:
        try:
            no = int(r["no"])
        except (TypeError, ValueError):
            continue
        horses[no] = {
            "no": no, "name": r.get("name"), "jockey": r.get("jockey"),
            "odds": _num(r.get("odds")), "pla_odds": _num(r.get("pla")),
            "win": 0.0, "pla": 0.0, "qin": 0.0, "qpl": 0.0,
            "fct": 0.0, "tri": 0.0, "ff": 0.0, "tce": 0.0, "multi": 0.0,
        }
    if not horses:
        return {"horses": [], "total": 0.0}

    def pool_money(key):
        best = 0.0
        for (typ, _leg), inv in pool_inv.items():
            if typ == key:
                best = max(best, inv or 0.0)
        return best

    # ---- single-runner pools
    for field, pool_key, target in (("odds", "WIN", "win"), ("pla_odds", "PLA", "pla")):
        ip = {no: 1.0 / h[field] for no, h in horses.items() if h[field]}
        money = pool_money(pool_key)
        if not ip or money <= 0:
            continue
        s = sum(ip.values())
        for no, val in ip.items():
            horses[no][target] = money * val / s

    # ---- combination pools
    grouped: dict[str, list] = {}
    for (pool, combo), meta in combos.items():
        if isinstance(meta, (list, tuple)):
            meta = list(meta) + [1, 0]
            price, n_legs, leg_index = meta[0], meta[1], meta[2]
        else:
            price, n_legs, leg_index = meta, 1, 0
        price = _num(price)
        if not price:
            continue
        positions = [int(x) for x in str(combo).split(",") if str(x).strip().isdigit()]
        if not positions:
            continue
        grouped.setdefault(pool, []).append(
            (positions, price, max(1, int(n_legs)), max(0, int(leg_index))))

    for pool, entries in grouped.items():
        money = pool_money(pool)
        if money <= 0:
            continue
        target = COMBO_FIELDS.get(pool, "multi")
        total_ip = sum(1.0 / price for _p, price, _n, _i in entries)
        if not total_ip:
            continue
        for positions, price, n_legs, leg_index in entries:
            share = money * (1.0 / price) / total_ip
            if n_legs > 1 and len(positions) % n_legs == 0:
                chunk = len(positions) // n_legs
                mine = positions[leg_index * chunk:(leg_index + 1) * chunk] or positions
                per_leg = share / n_legs
            else:
                mine = positions
                per_leg = share
            # multi-race combos carry runners from other races too: keep only ours
            mine = [n for n in mine if n in horses]
            if not mine:
                continue
            for no in mine:
                horses[no][target] += per_leg / len(mine)

    out = []
    grand = 0.0
    for h in horses.values():
        h["total"] = sum(h[k] for k in ("win", "pla", "qin", "qpl", "fct", "tri", "ff",
                                        "tce", "multi"))
        grand += h["total"]
        out.append(h)
    for h in out:
        h["share"] = (h["total"] / grand) if grand else 0.0
    out.sort(key=lambda h: -h["total"])
    return {"horses": out, "total": grand}
