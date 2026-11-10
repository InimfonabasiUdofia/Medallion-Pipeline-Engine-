"""
Gold builder: walks configs/gold/<table>/ (config.yaml + query.sql [+ view.sql]) and creates
one gold.<table> per bundle, with deterministic surrogate keys and multi-tenancy guards.

Surrogate keys: sipHash64(organisation_id, natural_key...). The organisation is part of the hash, so
the same business key in two organisations (e.g. ACC-9999) gets two different surrogate keys, and
facts compute the same key from their own natural keys, so joins need no lookup.

Usage (from ~/medallion, after Silver exists):
    python -m engine.gold_builder --print-sql --table dim_account
    python -m engine.gold_builder                # build all + verify
    python -m engine.gold_builder --engine-docs  # markdown table of engine choices
"""
import argparse
import os
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

DB = "gold"
HASH = "sipHash64"


# ---------------------------------------------------------------- loading
def load_bundles(folder):
    bundles = []
    for d in sorted(Path(folder).iterdir()):
        if (d / "config.yaml").exists():
            cfg = yaml.safe_load((d / "config.yaml").read_text())
            cfg["_query"] = (d / "query.sql").read_text().strip().rstrip(";")
            vf = d / "view.sql"
            cfg["_view"] = vf.read_text().strip().rstrip(";") if vf.exists() else None
            bundles.append(cfg)
    return bundles


# ---------------------------------------------------------------- multi-tenancy guard
def guard_errors(sql):
    """Static checks: every GROUP BY and every JOIN ... ON must include organisation_id."""
    errors = []
    group_re = r"GROUP\s+BY\s+(.*?)(?=\bORDER\s+BY\b|\bHAVING\b|\bLIMIT\b|\)|;|$)"
    for m in re.finditer(group_re, sql, flags=re.I | re.S):
        if not re.search(r"\borganisation_id\b", m.group(1)):
            errors.append("GROUP BY without organisation_id: " + " ".join(m.group(1).split()))
    join_re = (r"\bJOIN\b.*?\bON\b(.*?)(?=\bINNER\b|\bLEFT\b|\bRIGHT\b|\bJOIN\b|\bWHERE\b|"
               r"\bGROUP\b|\bORDER\b|\)|;|$)")
    for m in re.finditer(join_re, sql, flags=re.I | re.S):
        if not re.search(r"\borganisation_id\b", m.group(1)):
            errors.append("JOIN without organisation_id in ON: " + " ".join(m.group(1).split()))
    return errors


# ---------------------------------------------------------------- SQL generation
def key_expr(cols):
    return f"{HASH}({', '.join(cols)})"


def build_statements(cfg):
    name = cfg["name"]
    keys = []
    sk = cfg.get("surrogate_key")
    if sk:
        keys.append(f"{key_expr(sk['from'])} AS {sk['name']}")
    for fk_name, cols in (cfg.get("foreign_keys") or {}).items():
        keys.append(f"{key_expr(cols)} AS {fk_name}")
    if keys:
        key_sql = ",\n    ".join(keys)
        select = f"SELECT\n    {key_sql},\n    *\nFROM (\n{cfg['_query']}\n)"
    else:
        select = cfg["_query"]
    order = ", ".join(cfg["order_by"])
    stmts = [
        f"CREATE DATABASE IF NOT EXISTS {DB}",
        f"DROP VIEW IF EXISTS {DB}.{cfg.get('read_view', name + '_v')}",
        f"DROP TABLE IF EXISTS {DB}.{name}",
        f"CREATE TABLE {DB}.{name}\nENGINE = {cfg['engine']}\nORDER BY ({order})\nAS\n{select}",
    ]
    if cfg.get("_view"):
        stmts.append(f"CREATE VIEW {DB}.{cfg['read_view']} AS\n{cfg['_view']}")
    return stmts


# ---------------------------------------------------------------- verification
def one(client, sql):
    return client.query(sql).result_rows[0][0]


def verify(client, cfg, silver_orgs):
    name = cfg["name"]
    rows = one(client, f"SELECT count() FROM {DB}.{name}")
    orgs = one(client, f"SELECT uniqExact(organisation_id) FROM {DB}.{name}")
    empty = one(client, f"SELECT countIf(organisation_id = '') FROM {DB}.{name}")
    notes, ok = [], rows > 0 and orgs == silver_orgs and empty == 0
    sk = cfg.get("surrogate_key")
    if sk:
        uniq = one(client, f"SELECT uniqExact({sk['name']}) FROM {DB}.{name}")
        ok = ok and uniq == rows
        notes.append(f"unique_{sk['name']}={uniq == rows}")
    rec = cfg.get("reconcile")
    if rec:
        g, s = float(one(client, rec["gold_sql"])), float(one(client, rec["silver_sql"]))
        match = abs(g - s) < 0.01
        ok = ok and match
        notes.append(f"reconciles_to_silver={match}")
    print(f"  {'OK  ' if ok else 'FAIL'} {name:<32} rows={rows:<6} orgs={orgs} {' '.join(notes)}")
    return ok


def surrogate_proof(client):
    rows = client.query(
        f"SELECT organisation_id, account_id, account_key FROM {DB}.dim_account "
        f"WHERE account_id = 'ACC-9999' ORDER BY organisation_id").result_rows
    print("\nSurrogate keys for the seeded collision ACC-9999:")
    for r in rows:
        print(f"  {r[0]}  {r[1]}  account_key={r[2]}")
    ok = len(rows) == 2 and rows[0][2] != rows[1][2]
    print("  PASS: different surrogate key per organisation" if ok else "  FAIL")
    return ok


def engine_docs(bundles):
    lines = ["| Table | Kind | Engine | Why |", "|---|---|---|---|"]
    for b in bundles:
        lines.append(f"| {b['name']} | {b['kind']} | {b['engine']} | {' '.join(b['engine_reason'].split())} |")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--folder", default=str(ROOT / "configs" / "gold"))
    ap.add_argument("--table")
    ap.add_argument("--print-sql", action="store_true")
    ap.add_argument("--engine-docs", action="store_true")
    args = ap.parse_args()

    bundles = load_bundles(args.folder)
    if args.engine_docs:
        print(engine_docs(bundles))
        return
    if args.table:
        bundles = [b for b in bundles if b["name"] == args.table]

    for b in bundles:  # guard runs before anything touches the database
        errs = guard_errors(b["_query"]) + (guard_errors(b["_view"]) if b["_view"] else [])
        if errs:
            sys.exit(f"GUARD FAILED for {b['name']}:\n  " + "\n  ".join(errs))

    if args.print_sql:
        for b in bundles:
            for s in build_statements(b):
                print(s + ";\n")
        return

    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST_LOCAL", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""))

    for b in bundles:
        for s in build_statements(b):
            client.command(s)
        print(f"built {DB}.{b['name']}  ({b['engine']})")

    print("\nVerifying (every table covers all organisations, surrogate keys unique, totals match Silver):")
    silver_orgs = one(client, "SELECT uniqExact(organisation_id) FROM silver.accounts")
    results = [verify(client, b, silver_orgs) for b in bundles]
    if any(b["name"] == "dim_account" for b in bundles):
        results.append(surrogate_proof(client))
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()