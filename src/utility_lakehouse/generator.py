"""Synthetic source-file generator for a North American electric utility.

Writes the kind of extracts a utility data platform receives every day:

  SAP IS-U style master and billing data (technical SAP field names, SAP date
  format yyyyMMdd, SAP 'X' flags, SAP trailing-minus amounts):
    sap_bp/                 business partners (customers)         CSV, full + change files
    sap_premise/            premises (service locations)          CSV
    sap_contract_account/   contract accounts                     CSV
    sap_meter/              meters (equipment)                    CSV
  SAP source tables (stand-ins for the SAP system itself, with SAP's changed-on date AEDAT).
  They are NOT landing folders: notebook 01b extracts only changed rows from them into
  sap_billing_doc/ and sap_payment/ (see sap_extract.py):
    _sap_system/billing_doc.csv   billing documents (full table)
    _sap_system/payment.csv       payments (full table)
  Other systems:
    ami_meter_reads/        smart-meter daily reads (AMI head-end) JSON lines, one file per week
    call_center_calls/      contact-centre calls                  CSV, one file per month
    oms_outages/            outage management system events       CSV, one file per month

Data-quality problems are planted on purpose so the silver layer has work to do:
duplicate reads, negative or missing kWh, null keys, late-arriving reads, a bad date,
and schema drift (a new 'voltage_v' field appears in later AMI files).

Pure Python (no pandas), deterministic for a given seed. Works locally and on
Databricks, where out_dir is a Unity Catalog volume path such as
/Volumes/utility_lab/landing/raw.
"""
from __future__ import annotations

import csv
import json
import math
import os
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta

REGIONS = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
CITIES = {"NORTH": "Northfield", "SOUTH": "Southport", "EAST": "Eastvale", "WEST": "Westbrook", "CENTRAL": "Centreville"}
STREETS = ["Maple Ave", "Oak St", "Pine Rd", "Cedar Blvd", "Birch Lane", "Elm St", "Willow Dr", "Spruce Cres"]
FIRST = ["Alex", "Sam", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Jamie", "Avery", "Quinn", "Drew", "Robin"]
LAST = ["Smith", "Brown", "Tremblay", "Martin", "Roy", "Wilson", "Lee", "Patel", "Singh", "Gagnon", "Clark", "Young"]
ORG = ["Northern Foods", "Lakeside Clinic", "Metro Storage", "Prairie Plastics", "Harbour Hotel", "Summit Steel",
       "Riverside School", "Greenleaf Grocers", "Bluewater Logistics", "Granite Works"]
SEGMENTS = [("RES", 0.85), ("COM", 0.13), ("IND", 0.02)]
RATE_CAT = {"RES": ["RES-TOU", "RES-TIER"], "COM": ["COM-GS"], "IND": ["IND-LG"]}
BASE_KWH = {"RES": 24.0, "COM": 260.0, "IND": 2100.0}
RATE = {"RES-TOU": 0.128, "RES-TIER": 0.121, "COM-GS": 0.114, "IND-LG": 0.092}
FIXED = {"RES": 28.50, "COM": 65.00, "IND": 240.00}
CHANNELS = [("ONLINE", 0.45), ("PAP", 0.30), ("BANK", 0.18), ("IN_PERSON", 0.07)]
CALL_REASONS = [("BILLING", 0.38), ("PAYMENT", 0.22), ("MOVE", 0.12), ("OUTAGE", 0.10), ("OTHER", 0.18)]
CAUSES = ["WEATHER", "EQUIPMENT", "TREE_CONTACT", "ANIMAL", "PLANNED_MAINTENANCE"]
BILLING_COLS = ["BELNR", "VKONT", "BUDAT", "BEGABRPE", "ENDABRPE", "KWH", "BETRW", "FAEDN", "BILL_STATUS",
                "ESTIMATED_FLAG", "AEDAT"]
PAYMENT_COLS = ["OPBEL", "VKONT", "BELNR", "BUDAT", "BETRZ", "CHANNEL", "AEDAT"]


@dataclass
class Customer:
    partner: str
    bu_type: str          # 1 = person, 2 = organization (SAP BU_TYPE)
    first: str
    last: str
    org: str
    segment: str
    region: str
    email: str
    premise: str
    account: str
    meter: str
    rate_cat: str
    move_in: date
    install: date
    meter_type: str


def _pick(rng: random.Random, weighted):
    r, acc = rng.random(), 0.0
    for value, w in weighted:
        acc += w
        if r <= acc:
            return value
    return weighted[-1][0]


def sap_date(d: date | None) -> str:
    return d.strftime("%Y%m%d") if d else ""


def sap_amount(x: float) -> str:
    """SAP prints negative amounts with a trailing minus, e.g. 125.40-"""
    return f"{abs(x):.2f}-" if x < 0 else f"{x:.2f}"


def _write_csv(path: str, header: list[str], rows: list[list]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def _write_jsonl(path: str, records: list[dict]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def build_customers(n_customers: int, start: date, seed: int) -> list[Customer]:
    rng = random.Random(seed)
    out = []
    for i in range(1, n_customers + 1):
        seg = _pick(rng, SEGMENTS)
        region = REGIONS[(i * 7 + rng.randint(0, 4)) % len(REGIONS)]
        first, last = rng.choice(FIRST), rng.choice(LAST)
        org = f"{rng.choice(ORG)} {i}" if seg != "RES" else ""
        # most customers moved in before the window; about 6% move in during it
        move_in = start + timedelta(days=rng.randint(10, 150)) if rng.random() < 0.06 else start - timedelta(days=rng.randint(60, 3000))
        out.append(Customer(
            partner=f"{1000000000 + i}", bu_type="1" if seg == "RES" else "2",
            first=first if seg == "RES" else "", last=last if seg == "RES" else "", org=org,
            segment=seg, region=region,
            email=f"{(first + '.' + last).lower()}{i}@example.com" if seg == "RES" else f"ap{i}@example.com",
            premise=f"P{i:07d}", account=f"{200000000 + i}", meter=f"M{i:07d}",
            rate_cat=rng.choice(RATE_CAT[seg]), move_in=move_in,
            install=start - timedelta(days=rng.randint(100, 2500)),
            meter_type="AMI" if rng.random() < 0.92 else "MANUAL",
        ))
    return out


def daily_kwh(rng: random.Random, c: Customer, d: date) -> float:
    season = 1.0 + 0.25 * math.cos((d.timetuple().tm_yday - 15) / 365.0 * 2 * math.pi) \
        + 0.18 * max(0.0, math.sin((d.timetuple().tm_yday - 120) / 365.0 * 2 * math.pi))
    weekday = 0.82 if (c.segment != "RES" and d.weekday() >= 5) else 1.0
    return max(0.0, BASE_KWH[c.segment] * season * weekday * rng.uniform(0.75, 1.25))


def generate(out_dir: str, start: date = date(2026, 5, 1), days: int = 184,
             n_customers: int = 1200, seed: int = 42) -> dict:
    """Generate all source files. Returns a summary with row counts per source."""
    rng = random.Random(seed + 1)
    end = start + timedelta(days=days - 1)
    customers = build_customers(n_customers, start, seed)
    by_partner = {c.partner: c for c in customers}
    summary: dict[str, int] = {}

    # ---- SAP master data -------------------------------------------------------------
    bp_header = ["PARTNER", "BU_TYPE", "NAME_FIRST", "NAME_LAST", "NAME_ORG1", "CUST_SEGMENT", "REGION", "SMTP_ADDR", "CHDAT"]
    bp_rows = [[c.partner, c.bu_type, c.first, c.last, c.org, c.segment, c.region, c.email, sap_date(start - timedelta(days=1))]
               for c in customers]
    _write_csv(f"{out_dir}/sap_bp/bp_full_{sap_date(start)}.csv", bp_header, bp_rows)
    summary["sap_bp"] = len(bp_rows)

    # two change files during the window -> SCD Type 2 history in silver
    for k, change_day in enumerate([start + timedelta(days=days // 3), start + timedelta(days=2 * days // 3)]):
        changed = rng.sample(customers, max(3, n_customers // 25))
        rows = []
        for c in changed:
            if c.segment == "RES" and rng.random() < 0.4:
                c.last = rng.choice(LAST)                     # name change
            elif c.segment == "COM" and rng.random() < 0.3:
                c.segment = "IND"                             # reclassified as industrial
                c.rate_cat = "IND-LG"
            else:
                c.email = f"updated{k}.{c.partner}@example.com"
            rows.append([c.partner, c.bu_type, c.first, c.last, c.org, c.segment, c.region, c.email, sap_date(change_day)])
        _write_csv(f"{out_dir}/sap_bp/bp_changes_{sap_date(change_day)}.csv", bp_header, rows)
        summary["sap_bp"] += len(rows)

    _write_csv(f"{out_dir}/sap_premise/premise_full_{sap_date(start)}.csv",
               ["VSTELLE", "STREET", "CITY1", "REGION", "PREMISE_TYPE"],
               [[c.premise, f"{rng.randint(1, 999)} {rng.choice(STREETS)}", CITIES[c.region], c.region,
                 {"RES": rng.choice(["HOUSE", "APARTMENT"]), "COM": "COMMERCIAL", "IND": "INDUSTRIAL"}[c.segment]]
                for c in customers])
    summary["sap_premise"] = n_customers

    _write_csv(f"{out_dir}/sap_contract_account/account_full_{sap_date(start)}.csv",
               ["VKONT", "GPART", "VSTELLE", "RATE_CAT", "MOVE_IN_DATE", "MOVE_OUT_DATE"],
               [[c.account, c.partner, c.premise, c.rate_cat, sap_date(c.move_in), ""] for c in customers])
    summary["sap_contract_account"] = n_customers

    _write_csv(f"{out_dir}/sap_meter/meter_full_{sap_date(start)}.csv",
               ["EQUNR", "VSTELLE", "METER_TYPE", "INSTALL_DATE"],
               [[c.meter, c.premise, c.meter_type, sap_date(c.install)] for c in customers])
    summary["sap_meter"] = n_customers

    # ---- AMI daily reads, one JSON-lines file per week --------------------------------
    usage: dict[tuple[str, int, int], float] = {}           # (account, year, month) -> kWh
    est_days: dict[tuple[str, int, int], int] = {}
    late: list[dict] = []
    total_reads = 0
    week_start = start
    drift_from = start + timedelta(days=int(days * 0.6))      # schema drift: new field from here on
    while week_start <= end:
        week_end = min(week_start + timedelta(days=6), end)
        records = list(late)
        late = []
        d = week_start
        while d <= week_end:
            for c in customers:
                if d < c.move_in:
                    continue
                kwh = round(daily_kwh(rng, c, d), 3)
                quality = "ESTIMATED" if (c.meter_type == "MANUAL" or rng.random() < 0.03) else "ACTUAL"
                key = (c.account, d.year, d.month)
                usage[key] = usage.get(key, 0.0) + kwh
                if quality == "ESTIMATED":
                    est_days[key] = est_days.get(key, 0) + 1
                rec = {"meter_id": c.meter, "premise_id": c.premise, "read_date": d.isoformat(),
                       "kwh": kwh, "read_quality": quality}
                if d >= drift_from:
                    rec["voltage_v"] = round(rng.gauss(240 if c.segment == "RES" else 600, 3), 1)
                r = rng.random()
                if r < 0.002:
                    rec["kwh"] = -abs(rec["kwh"])               # bad: negative usage
                elif r < 0.003:
                    rec["meter_id"] = None                      # bad: missing key
                elif r < 0.004:
                    rec["kwh"] = None                           # bad: missing value
                if rng.random() < 0.01 and d != week_end:
                    late.append(rec)                            # arrives a week late
                    continue
                records.append(rec)
                if rng.random() < 0.003:
                    records.append(dict(rec))                   # exact duplicate
            d += timedelta(days=1)
        _write_jsonl(f"{out_dir}/ami_meter_reads/reads_{week_start.isoformat()}_{week_end.isoformat()}.json", records)
        total_reads += len(records)
        week_start = week_end + timedelta(days=1)
    summary["ami_meter_reads"] = total_reads

    # ---- SAP billing documents (monthly) and payments ---------------------------------
    bills_by_month: dict[str, list[list]] = {}
    payments_by_month: dict[str, list[list]] = {}
    bill_no, pay_no = 4000000000, 9000000000
    month = date(start.year, start.month, 1)
    while month <= end:
        nxt = date(month.year + (month.month == 12), month.month % 12 + 1, 1)
        period_end = nxt - timedelta(days=1)
        bill_date = period_end + timedelta(days=3)
        if bill_date > end:
            break
        rows = bills_by_month.setdefault(bill_date.strftime("%Y-%m"), [])
        for c in customers:
            kwh = usage.get((c.account, month.year, month.month), 0.0)
            if kwh == 0:
                continue
            amount = round(kwh * RATE[c.rate_cat] + FIXED[c.segment], 2)
            estimated = "X" if est_days.get((c.account, month.year, month.month), 0) > 10 else ""
            due = bill_date + timedelta(days=21)
            bill_no += 1
            status = "BILLED"
            if rng.random() < 0.01:                              # reversed and re-billed
                rows.append([str(bill_no), c.account, sap_date(bill_date), sap_date(month), sap_date(period_end),
                             f"{kwh:.3f}", sap_amount(amount), sap_date(due), "REVERSED", estimated,
                             sap_date(bill_date + timedelta(days=2))])            # changed when reversed
                rows.append([f"R{bill_no}", c.account, sap_date(bill_date + timedelta(days=2)), sap_date(month),
                             sap_date(period_end), f"{kwh:.3f}", sap_amount(-amount), sap_date(due), "REVERSAL", estimated,
                             sap_date(bill_date + timedelta(days=2))])
                bill_no += 1
                bill_date_used = bill_date + timedelta(days=4)
                amount = round(amount * rng.uniform(0.9, 1.0), 2)
            else:
                bill_date_used = bill_date
            rows.append([str(bill_no), c.account, sap_date(bill_date_used), sap_date(month), sap_date(period_end),
                         f"{kwh:.3f}", sap_amount(amount), sap_date(due), status, estimated, sap_date(bill_date_used)])

            # payment behaviour
            p = rng.random()
            if p < 0.80:
                pay_date = bill_date_used + timedelta(days=rng.randint(4, 21))
            elif p < 0.93:
                pay_date = due + timedelta(days=rng.randint(3, 75))
            else:
                pay_date = None                                  # stays unpaid
            if pay_date and pay_date <= end:
                pay_no += 1
                paid = amount if rng.random() < 0.94 else round(amount * rng.uniform(0.3, 0.8), 2)
                payments_by_month.setdefault(pay_date.strftime("%Y-%m"), []).append(
                    [str(pay_no), c.account, str(bill_no), sap_date(pay_date), sap_amount(paid), _pick(rng, CHANNELS),
                     sap_date(pay_date)])
        month = nxt
    # SAP source tables: one full table each, extracted incrementally by notebook 01b
    _write_csv(f"{out_dir}/_sap_system/billing_doc.csv", BILLING_COLS,
               [r for ym in sorted(bills_by_month) for r in bills_by_month[ym]])
    _write_csv(f"{out_dir}/_sap_system/payment.csv", PAYMENT_COLS,
               [r for ym in sorted(payments_by_month) for r in payments_by_month[ym]])
    summary["sap_billing_doc"] = sum(len(r) for r in bills_by_month.values())
    summary["sap_payment"] = sum(len(r) for r in payments_by_month.values())

    # ---- outages (OMS) ----------------------------------------------------------------
    region_counts = {r: sum(1 for c in customers if c.region == r) for r in REGIONS}
    outages_by_month: dict[str, list[list]] = {}
    outage_days: dict[tuple[str, date], int] = {}
    oid = 0
    d = start
    while d <= end:
        for r in REGIONS:
            if rng.random() < 0.09:
                oid += 1
                cause = rng.choice(CAUSES)
                begin = datetime(d.year, d.month, d.day, rng.randint(0, 23), rng.randint(0, 59))
                minutes = rng.randint(20, 90) if cause == "PLANNED_MAINTENANCE" else int(rng.expovariate(1 / 140)) + 15
                affected = max(1, int(region_counts[r] * rng.uniform(0.01, 0.25)))
                outages_by_month.setdefault(d.strftime("%Y-%m"), []).append(
                    [f"OUT{oid:06d}", r, begin.isoformat(), (begin + timedelta(minutes=minutes)).isoformat(),
                     cause, affected, "Y" if cause == "PLANNED_MAINTENANCE" else "N"])
                outage_days[(r, d)] = outage_days.get((r, d), 0) + affected
        d += timedelta(days=1)
    # one bad record: restore before start
    first_month = start.strftime("%Y-%m")
    if outages_by_month.get(first_month):
        bad = list(outages_by_month[first_month][0])
        bad[0], bad[3] = "OUT999999", "2026-04-01T00:00:00"
        outages_by_month[first_month].append(bad)
    for ym, rows in outages_by_month.items():
        _write_csv(f"{out_dir}/oms_outages/outages_{ym}.csv",
                   ["outage_id", "region", "outage_start_ts", "restore_ts", "cause", "customers_affected", "planned"], rows)
    summary["oms_outages"] = sum(len(r) for r in outages_by_month.values())

    # ---- contact centre calls ---------------------------------------------------------
    calls_by_month: dict[str, list[list]] = {}
    cid = 0
    partners = list(by_partner)
    d = start
    while d <= end:
        base = int(n_customers * 0.02)
        for r in REGIONS:
            extra = outage_days.get((r, d), 0) // 6
            region_partners = [p for p in partners if by_partner[p].region == r]
            for _ in range(base // len(REGIONS) + extra):
                cid += 1
                reason = "OUTAGE" if extra and rng.random() < 0.6 else _pick(rng, CALL_REASONS)
                start_ts = datetime(d.year, d.month, d.day, rng.randint(8, 19), rng.randint(0, 59), rng.randint(0, 59))
                wait = int(rng.expovariate(1 / (95 if extra else 45)))
                abandoned = rng.random() < (0.04 + min(0.25, wait / 2000))
                handle = 0 if abandoned else int(rng.gauss(330 if reason == "BILLING" else 260, 90))
                fcr = "" if abandoned else ("Y" if rng.random() < 0.78 else "N")
                calls_by_month.setdefault(d.strftime("%Y-%m"), []).append(
                    [f"C{cid:08d}", rng.choice(region_partners), start_ts.isoformat(), wait, max(handle, 0),
                     reason, "Y" if abandoned else "N", fcr])
        d += timedelta(days=1)
    for ym, rows in calls_by_month.items():
        _write_csv(f"{out_dir}/call_center_calls/calls_{ym}.csv",
                   ["call_id", "partner_id", "call_start_ts", "queue_wait_sec", "handle_sec", "call_reason",
                    "abandoned", "first_call_resolved"], rows)
    summary["call_center_calls"] = sum(len(r) for r in calls_by_month.values())
    summary["end_date"] = end.isoformat()  # type: ignore[assignment]
    return summary


def _read_csv(path: str) -> tuple[list[str], list[list[str]]]:
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    return rows[0], rows[1:]


def sap_changes_for_day(out_dir: str, day: date) -> dict:
    """Change the SAP source tables as SAP would during one business day.

    * one existing bill is corrected (amount +5%), so its AEDAT moves to ``day``
    * two open bills are paid on ``day`` (new payment documents, AEDAT = ``day``)
    Re-running for the same day changes nothing (safe to repeat).
    """
    d = sap_date(day)
    bhdr, bills = _read_csv(f"{out_dir}/_sap_system/billing_doc.csv")
    phdr, pays = _read_csv(f"{out_dir}/_sap_system/payment.csv")
    bi, pi = {c: i for i, c in enumerate(bhdr)}, {c: i for i, c in enumerate(phdr)}
    billed = [b for b in bills if b[bi["BILL_STATUS"]] == "BILLED"]
    changed = 0
    target = billed[day.toordinal() % len(billed)]
    if target[bi["AEDAT"]] != d:
        amt = float(target[bi["BETRW"]].rstrip("-"))
        target[bi["BETRW"]] = sap_amount(round(amt * 1.05, 2))
        target[bi["AEDAT"]] = d
        changed += 1
    paid = {p[pi["BELNR"]] for p in pays}
    existing = {p[pi["OPBEL"]] for p in pays}
    new_pays = 0
    for i, b in enumerate([b for b in billed if b[bi["BELNR"]] not in paid][:2]):
        opbel = f"95{day.strftime('%y%m%d')}{i:02d}"
        if opbel in existing:
            continue
        pays.append([opbel, b[bi["VKONT"]], b[bi["BELNR"]], d, b[bi["BETRW"]], "ONLINE", d])
        new_pays += 1
    _write_csv(f"{out_dir}/_sap_system/billing_doc.csv", bhdr, bills)
    _write_csv(f"{out_dir}/_sap_system/payment.csv", phdr, pays)
    return {"sap_bills_corrected": changed, "sap_new_payments": new_pays}


def generate_increment(out_dir: str, day: date, n_customers: int = 1200, seed: int = 42) -> dict:
    """Generate one extra day of source changes (to practise incremental loads).

    AMI reads include one re-sent duplicate and one late correction for the previous day.
    The SAP source tables get one corrected bill and two new payments (changed-on = day).
    """
    rng = random.Random(seed + day.toordinal())
    customers = build_customers(n_customers, date(2026, 5, 1), seed)
    recs = []
    for c in customers:
        rec = {"meter_id": c.meter, "premise_id": c.premise, "read_date": day.isoformat(),
               "kwh": round(daily_kwh(rng, c, day), 3), "read_quality": "ACTUAL",
               "voltage_v": round(rng.gauss(240, 3), 1)}
        recs.append(rec)
    recs.append(dict(recs[0]))                                              # duplicate
    corr = dict(recs[1]); corr["read_date"] = (day - timedelta(days=1)).isoformat(); corr["kwh"] = 31.5
    recs.append(corr)                                                       # late correction
    _write_jsonl(f"{out_dir}/ami_meter_reads/reads_{day.isoformat()}_{day.isoformat()}.json", recs)
    calls = []
    for i in range(int(n_customers * 0.006)):
        c = rng.choice(customers)
        calls.append([f"C9{day.strftime('%m%d')}{i:04d}", c.partner,
                      datetime(day.year, day.month, day.day, rng.randint(8, 19), rng.randint(0, 59)).isoformat(),
                      int(rng.expovariate(1 / 45)), int(rng.gauss(280, 80)), _pick(rng, CALL_REASONS), "N", "Y"])
    _write_csv(f"{out_dir}/call_center_calls/calls_{day.isoformat()}.csv",
               ["call_id", "partner_id", "call_start_ts", "queue_wait_sec", "handle_sec", "call_reason",
                "abandoned", "first_call_resolved"], calls)
    return {"ami_meter_reads": len(recs), "call_center_calls": len(calls), **sap_changes_for_day(out_dir, day)}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Generate synthetic utility source files")
    ap.add_argument("--out", default="./landing")
    ap.add_argument("--customers", type=int, default=1200)
    ap.add_argument("--days", type=int, default=184)
    args = ap.parse_args()
    print(generate(args.out, days=args.days, n_customers=args.customers))
