#!/usr/bin/env python3
"""Phase 1 export: wealth-management / RIA sponsors from the Form 5500 DB.

Reads the filter definition from app.py (ria_filter_sql) so the offline export
and the web search can never drift apart. Deduplicates to one row per unique
sponsor, keeping that sponsor's most recent filing.

Reads only -- never writes to form5500.db.

Usage:  python3 export_wealth_management.py
"""
import csv
import json
import os
import re
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app import ria_filter_sql  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HERE, "output", "form5500.db")
OUT_DIR = os.path.join(HERE, "output")
OUT_JSON = os.path.join(OUT_DIR, "wealth_management_firms_export.json")
OUT_CSV = os.path.join(OUT_DIR, "wealth_management_firms_export.csv")

# filings_summary is the authoritative table: 103 columns, fully populated.
# The narrower `filings` table has ADMIN_NAME / *_SIGNED_NAME / mailing street
# address present as columns but blank in every one of its 1.18M rows.
SELECT_COLS = """
    f.SPONSOR_DFE_NAME, f.SPONS_DFE_EIN, f.BUSINESS_CODE, f.FORM_YEAR,
    f.SPONS_DFE_MAIL_US_ADDRESS1, f.SPONS_DFE_MAIL_US_CITY,
    f.SPONS_DFE_MAIL_US_STATE, f.SPONS_DFE_MAIL_US_ZIP,
    f.ADMIN_NAME, f.SPONS_SIGNED_NAME, f.ADMIN_SIGNED_NAME,
    f.TOT_PARTCP_BOY_CNT, f.NET_ASSETS_EOY_AMT, f.DATE_RECEIVED
"""

FIELDNAMES = [
    "sponsor_name", "ein", "business_code", "form_year",
    "address_street", "address_city", "address_state", "address_zip",
    "address_full", "plan_administrator_name", "signer_name",
    "total_participants", "net_assets", "match_reason", "filings_seen",
]


def norm(name):
    """Dedup key: upper, collapse whitespace, drop trailing punctuation."""
    return re.sub(r"\s+", " ", (name or "").strip().upper()).rstrip(".,")


def clean(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def num(v):
    if v is None or str(v).strip() == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return int(f) if f == int(f) else f


def main():
    if not os.path.exists(DB):
        sys.exit(f"database not found: {DB}")

    where, params = ria_filter_sql()
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"SELECT {SELECT_COLS} FROM filings_summary f WHERE {where}", params
    )

    codes = set(__import__("app").RIA_BUSINESS_CODES)
    best = {}
    total_filings = 0

    for r in rows:
        total_filings += 1
        sponsor = clean(r["SPONSOR_DFE_NAME"])
        if not sponsor:
            continue
        key = norm(sponsor)
        if not key:
            continue

        year = num(r["FORM_YEAR"]) or 0
        received = clean(r["DATE_RECEIVED"]) or ""
        prev = best.get(key)
        seen = (prev["filings_seen"] + 1) if prev else 1
        if prev and (year, received) <= (prev["_year"], prev["_received"]):
            prev["filings_seen"] = seen
            continue

        code = clean(r["BUSINESS_CODE"])
        by_code = code in codes
        upper = key
        by_name = any(k in upper for k in
                      ("WEALTH", "CAPITAL", "ADVISORS", "ADVISERS",
                       "ASSET MANAGEMENT", "INVESTMENT")) \
            or " RIA " in f" {upper} "
        reason = "code+keyword" if (by_code and by_name) else \
                 "code" if by_code else "keyword-in-523xxx"

        admin = clean(r["ADMIN_NAME"])
        if admin and norm(admin) == key:
            admin = None  # same as sponsor -> not a distinct party

        signer = clean(r["SPONS_SIGNED_NAME"]) or clean(r["ADMIN_SIGNED_NAME"])

        street = clean(r["SPONS_DFE_MAIL_US_ADDRESS1"])
        city = clean(r["SPONS_DFE_MAIL_US_CITY"])
        state = clean(r["SPONS_DFE_MAIL_US_STATE"])
        zipc = clean(r["SPONS_DFE_MAIL_US_ZIP"])
        if zipc and len(zipc) == 9 and zipc.isdigit():
            zipc = f"{zipc[:5]}-{zipc[5:]}"
        line2 = ", ".join(p for p in (city, state) if p)
        full = ", ".join(p for p in (street, line2) if p)
        if zipc:
            full = f"{full} {zipc}".strip()

        best[key] = {
            "sponsor_name": sponsor,
            "ein": clean(r["SPONS_DFE_EIN"]),
            "business_code": code,
            "form_year": year or None,
            "address_street": street,
            "address_city": city,
            "address_state": state,
            "address_zip": zipc,
            "address_full": full or None,
            "plan_administrator_name": admin,
            "signer_name": signer,
            "total_participants": num(r["TOT_PARTCP_BOY_CNT"]),
            "net_assets": num(r["NET_ASSETS_EOY_AMT"]),
            "match_reason": reason,
            "filings_seen": seen,
            "_year": year,
            "_received": received,
        }

    conn.close()

    # Second dedup pass: collapse sponsors that share an EIN under different
    # name spellings ("11 CAPITAL" vs "11 CAPITAL, LLC" -> EIN 823749450).
    # Keep the most recent filing; prefer the longer name, which usually
    # carries the legal suffix.
    by_ein, no_ein, merged = {}, [], 0
    for d in best.values():
        ein = d.get("ein")
        if not ein:
            no_ein.append(d)
            continue
        prev = by_ein.get(ein)
        if prev is None:
            by_ein[ein] = d
            continue
        merged += 1
        keep, drop = ((d, prev)
                      if (d["_year"], d["_received"]) > (prev["_year"], prev["_received"])
                      else (prev, d))
        if len(drop["sponsor_name"]) > len(keep["sponsor_name"]):
            keep["sponsor_name"] = drop["sponsor_name"]
        keep["filings_seen"] += drop["filings_seen"]
        by_ein[ein] = keep

    out = sorted(list(by_ein.values()) + no_ein,
                 key=lambda d: d["sponsor_name"].upper())
    for d in out:
        d.pop("_year", None)
        d.pop("_received", None)

    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2, ensure_ascii=False)
    with open(OUT_CSV, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(out)

    by_reason = {}
    for d in out:
        by_reason[d["match_reason"]] = by_reason.get(d["match_reason"], 0) + 1

    print(f"matching filings      : {total_filings:,}")
    print(f"unique sponsor names  : {len(best):,}")
    print(f"  merged on shared EIN: {merged:,}")
    print(f"unique sponsors (rows): {len(out):,}")
    print(f"  by match_reason     : {by_reason}")
    print(f"with street address   : "
          f"{sum(1 for d in out if d['address_street']):,}")
    print(f"with admin name       : "
          f"{sum(1 for d in out if d['plan_administrator_name']):,}")
    print(f"with signer name      : "
          f"{sum(1 for d in out if d['signer_name']):,}")
    print(f"\nJSON -> {OUT_JSON}")
    print(f"CSV  -> {OUT_CSV}")


if __name__ == "__main__":
    main()
