#!/usr/bin/env python3
"""
Synthetic wealth-management data generator for the Bronze layer.

Writes 12 tables x 3 organisations x 7 dates = 252 ORC files laid out as:
    <bucket>/<source>/<table>/<organisation_id>/<YYYY-MM-DD>/data.orc

organisation_id and date are NOT stored inside the files: they come from the
folder path (that is the whole point of the partitioned Bronze layer).

Usage:
    python generate_data.py --dry-run          # write to ./data_out, no MinIO
    python generate_data.py                    # upload to MinIO (reads .env vars)
"""
import argparse
import io
import os
import random
from datetime import date, datetime, timedelta

from pathlib import Path

import pyarrow as pa
import pyarrow.orc as orc

try:  # load MINIO_ROOT_USER / MINIO_ROOT_PASSWORD from the .env next to this script
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass  # fall back to variables already set in the shell

ORGS = ["org_8812", "org_4410", "org_7305"]
START = date(2026, 8, 25)
DATES = [START + timedelta(days=i) for i in range(7)]
BUCKET = "bronze"

# table -> source system (first folder level)
SOURCES = {
    "households": "salesforce",
    "contacts": "salesforce",
    "accounts": "salesforce",
    "advisors": "salesforce",
    "custodians": "blackdiamond",
    "portfolios": "blackdiamond",
    "securities": "blackdiamond",
    "holdings": "blackdiamond",
    "trades": "orion",
    "transactions": "orion",
    "fee_schedules": "orion",
    "statements": "orion",
}

# Seeded cross-tenant collision: same business key in two different orgs.
COLLISION_KEY = "ACC-9999"
COLLISION_ORGS = ["org_8812", "org_4410"]

FIRST = ["Ada", "Chidi", "Emeka", "Funke", "Grace", "Ibrahim", "Jide", "Kemi",
         "Liam", "Maya", "Noah", "Ola", "Priya", "Sara", "Tunde", "Zara"]
LAST = ["Abasi", "Bello", "Okafor", "Smith", "Johnson", "Patel", "Nwosu",
        "Garcia", "Brown", "Adeyemi", "Khan", "Lee"]
TICKERS = ["AAPL", "MSFT", "VTI", "BND", "AGG", "VXUS", "GOOGL", "AMZN",
           "NVDA", "JNJ", "XOM", "SPY", "QQQ", "TLT", "VNQ"]
STATUSES = ["ACTIVE", "ACTIVE", "ACTIVE", "DORMANT", "CLOSED"]


def at(d, hour=6):
    return datetime(d.year, d.month, d.day, hour)


def build_org(org):
    """Create the initial (mutable) state for one organisation."""
    r = random.Random(org)
    st = {"r": r}
    st["custodians"] = [
        {"custodian_id": f"CUS-{i:02d}", "name": n, "country": "US"}
        for i, n in enumerate(["Schwab", "Fidelity", "Pershing"], 1)
    ]
    st["advisors"] = [
        {"advisor_id": f"ADV-{i:03d}", "full_name": f"{r.choice(FIRST)} {r.choice(LAST)}",
         "license_no": f"LIC{r.randint(100000, 999999)}", "hire_date": date(2015 + i, 1, 15)}
        for i in range(1, 6)
    ]
    st["households"] = [
        {"household_id": f"HH-{i:04d}", "household_name": f"{r.choice(LAST)} Family",
         "advisor_id": r.choice(st["advisors"])["advisor_id"], "updated_at": at(START)}
        for i in range(1, 21)
    ]
    st["contacts"] = []
    for i in range(1, 41):
        fn, ln = r.choice(FIRST), r.choice(LAST)
        st["contacts"].append({
            "contact_id": f"CON-{i:04d}", "household_id": r.choice(st["households"])["household_id"],
            "first_name": fn, "last_name": ln, "email": f"{fn.lower()}.{ln.lower()}@example.com",
            "updated_at": at(START)})
    st["accounts"] = []
    for i in range(1, 41):
        st["accounts"].append({
            "account_id": f"ACC-{i:04d}", "household_id": r.choice(st["households"])["household_id"],
            "custodian_id": r.choice(st["custodians"])["custodian_id"],
            "account_type": r.choice(["IRA", "JOINT", "TRUST", "INDIVIDUAL"]),
            "status": "ACTIVE", "opened_date": date(2018, 1, 1) + timedelta(days=r.randint(0, 2000)),
            "updated_at": at(START)})
    if org in COLLISION_ORGS:  # same key, different data, different tenant
        st["accounts"].append({
            "account_id": COLLISION_KEY, "household_id": st["households"][0]["household_id"],
            "custodian_id": "CUS-01", "account_type": "TRUST" if org == COLLISION_ORGS[0] else "IRA",
            "status": "ACTIVE", "opened_date": date(2020, 1, 1), "updated_at": at(START)})
    st["securities"] = [
        {"security_id": f"SEC-{i:03d}", "ticker": t, "asset_class": "BOND" if t in ("BND", "AGG", "TLT") else "EQUITY",
         "price": round(r.uniform(20, 400), 2), "updated_at": at(START)}
        for i, t in enumerate(TICKERS, 1)
    ]
    st["portfolios"] = [
        {"portfolio_id": f"PF-{a['account_id'][4:]}", "account_id": a["account_id"],
         "model_name": r.choice(["Growth", "Balanced", "Income"]), "updated_at": at(START)}
        for a in st["accounts"]
    ]
    st["holdings_qty"] = {
        (p["portfolio_id"], s["security_id"]): r.randint(10, 500)
        for p in st["portfolios"] for s in r.sample(st["securities"], 3)
    }
    st["fee_schedules"] = [
        {"fee_schedule_id": f"FEE-{i}", "name": n, "bps": b}
        for i, (n, b) in enumerate([("Standard", 100), ("Premium", 75), ("Institutional", 50)], 1)
    ]
    return st


def snapshot(org, st, d, di):
    """Rows for every table on day d. Mutates state to create change history."""
    r, ts = st["r"], at(d)
    out = {}
    out["custodians"] = list(st["custodians"])
    out["advisors"] = list(st["advisors"])
    out["fee_schedules"] = list(st["fee_schedules"])
    out["households"] = [dict(x) for x in st["households"]]

    if di > 0:
        for c in st["contacts"]:
            if r.random() < 0.05:
                c["email"] = f"{c['first_name'].lower()}.{c['last_name'].lower()}{di}@example.com"
                c["updated_at"] = ts
        for a in st["accounts"]:
            if a["account_id"] != COLLISION_KEY and r.random() < 0.06:
                a["status"] = r.choice(STATUSES)
                a["updated_at"] = ts
        for s in st["securities"]:
            s["price"] = round(s["price"] * (1 + r.uniform(-0.03, 0.03)), 2)
            s["updated_at"] = ts
        for k in st["holdings_qty"]:
            st["holdings_qty"][k] = max(0, st["holdings_qty"][k] + r.randint(-20, 20))

    out["contacts"] = [dict(x) for x in st["contacts"]]
    out["accounts"] = [dict(x) for x in st["accounts"]]
    out["securities"] = [dict(x) for x in st["securities"]]
    out["portfolios"] = [dict(x) for x in st["portfolios"]]

    price = {s["security_id"]: s["price"] for s in st["securities"]}
    out["holdings"] = [
        {"portfolio_id": p, "security_id": s, "as_of_date": d, "quantity": q,
         "market_value": round(q * price[s], 2)}
        for (p, s), q in st["holdings_qty"].items()
    ]
    accts = [a["account_id"] for a in st["accounts"]]
    out["transactions"] = [
        {"txn_id": f"TXN-{d:%Y%m%d}-{n:04d}", "account_id": r.choice(accts),
         "txn_type": r.choice(["DEPOSIT", "WITHDRAWAL", "FEE", "DIVIDEND"]),
         "amount": round(r.uniform(50, 25000), 2), "txn_date": d, "created_at": ts}
        for n in range(1, 16)
    ]
    pfs = [p["portfolio_id"] for p in st["portfolios"]]
    secs = [s["security_id"] for s in st["securities"]]
    out["trades"] = []
    for n in range(1, 11):
        sid = r.choice(secs)
        out["trades"].append({
            "trade_id": f"TRD-{d:%Y%m%d}-{n:04d}", "portfolio_id": r.choice(pfs), "security_id": sid,
            "side": r.choice(["BUY", "SELL"]), "quantity": r.randint(1, 100),
            "price": price[sid], "trade_date": d})
    out["statements"] = [
        {"statement_id": f"STM-{a}-{d:%Y%m%d}", "account_id": a, "statement_date": d,
         "ending_balance": round(r.uniform(10000, 2_000_000), 2)}
        for a in accts
    ]

    # Give every table an updated_at so Silver can use one dedup rule everywhere
    for t in ("custodians", "advisors", "fee_schedules"):
        out[t] = [{**x, "updated_at": at(START)} for x in out[t]]
    for t in ("holdings", "trades", "transactions", "statements"):
        for row in out[t]:
            row["updated_at"] = ts

    # Seed duplicates so Silver has something to deduplicate:
    # an older, stale copy of a few rows in the same file.
    for t in ("contacts", "accounts", "securities"):
        for row in r.sample(out[t], 2):
            stale = dict(row)
            stale["updated_at"] = ts - timedelta(days=1)
            out[t].append(stale)
    for row in r.sample(out["transactions"], 1):  # exact duplicate
        out["transactions"].append(dict(row))
    return out


def to_orc_bytes(rows):
    table = pa.Table.from_pylist(rows)
    sink = pa.BufferOutputStream()
    orc.write_table(table, sink)
    return sink.getvalue().to_pybytes()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="write to ./data_out instead of MinIO")
    ap.add_argument("--endpoint", default=os.getenv("MINIO_UPLOAD_ENDPOINT", "localhost:9000"))
    args = ap.parse_args()

    client = None
    if not args.dry_run:
        from minio import Minio
        for var in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"):
            if var not in os.environ:
                raise SystemExit(f"{var} not found. Put it in .env next to this script "
                                 "and run: pip install python-dotenv")
        client = Minio(args.endpoint, access_key=os.environ["MINIO_ROOT_USER"],
                       secret_key=os.environ["MINIO_ROOT_PASSWORD"], secure=False)
        if not client.bucket_exists(BUCKET):
            client.make_bucket(BUCKET)

    count = 0
    for org in ORGS:
        st = build_org(org)
        for di, d in enumerate(DATES):
            for table, rows in snapshot(org, st, d, di).items():
                key = f"{SOURCES[table]}/{table}/{org}/{d:%Y-%m-%d}/data.orc"
                data = to_orc_bytes(rows)
                if args.dry_run:
                    path = os.path.join("data_out", BUCKET, key)
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, "wb") as f:
                        f.write(data)
                else:
                    client.put_object(BUCKET, key, io.BytesIO(data), len(data))
                count += 1
        print(f"{org}: done")
    print(f"Wrote {count} files (expected {len(SOURCES) * len(ORGS) * len(DATES)})")
    print(f"Seeded collision: account_id={COLLISION_KEY} exists in {COLLISION_ORGS} with different data")


if __name__ == "__main__":
    main()