"""
Silver builder: reads configs/silver.yaml and, for every table, creates
  silver.<table>          : MergeTree table, 1 row per (organisation_id, business key, processing_date)
  silver.<table>_current  : view with the latest row per (organisation_id, business key)
  silver.<table>_history  : view with only the days where a tracked column changed (if track_history is set)
and loads it from bronze.<table> (cleaned, typed, deduplicated: newest updated_at wins).

Usage (from ~/medallion, after the Bronze layer exists):
    python -m engine.silver_builder --print-sql --table accounts
    python -m engine.silver_builder                  # build all + verify
    python -m engine.silver_builder --table accounts # one table
"""
import argparse
import os
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

ORG, DATE = "organisation_id", "processing_date"


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def select_expr(col):
    """Cleaning rule + type cast for one column."""
    n, t, clean = col["name"], col["type"], col.get("clean")
    if clean == "trim":
        return f"trim({n}) AS {n}"
    if clean == "upper_trim":
        return f"upper(trim({n})) AS {n}"
    if t == "String":
        return n
    return f"CAST({n} AS {t}) AS {n}"


def build_statements(name, tcfg, defaults):
    db, src_db = defaults["database"], defaults["source_database"]
    keys = tcfg["business_key"]
    full_key = ", ".join([ORG] + keys + [DATE])
    part_key = ", ".join([ORG] + keys)
    col_defs = ",\n    ".join([f"{ORG} LowCardinality(String)", f"{DATE} Date"] +
                              [f"{c['name']} {c['type']}" for c in tcfg["columns"]])
    select_cols = ",\n    ".join([ORG, DATE] + [select_expr(c) for c in tcfg["columns"]])
    not_empty = " AND ".join(f"{k} != ''" for k in keys)
    order_col = defaults.get("dedup_order_by", "updated_at")

    stmts = [
        f"CREATE DATABASE IF NOT EXISTS {db}",
        f"DROP VIEW IF EXISTS {db}.{name}_history",
        f"DROP VIEW IF EXISTS {db}.{name}_current",
        f"DROP TABLE IF EXISTS {db}.{name}",
        f"CREATE TABLE {db}.{name}\n(\n    {col_defs}\n)\nENGINE = {defaults['engine']}\nORDER BY ({full_key})",
        (f"INSERT INTO {db}.{name}\nSELECT\n    {select_cols}\nFROM {src_db}.{name}\n"
         f"WHERE {not_empty}\nORDER BY {order_col} DESC\nLIMIT 1 BY {full_key}"),
        (f"CREATE VIEW {db}.{name}_current AS\nSELECT * FROM {db}.{name}\n"
         f"ORDER BY {DATE} DESC\nLIMIT 1 BY {part_key}"),
    ]
    tracked = tcfg.get("track_history")
    if tracked:
        tup = f"tuple({', '.join(tracked)})"
        w = f"PARTITION BY {part_key} ORDER BY {DATE}"
        stmts.append(
            f"CREATE VIEW {db}.{name}_history AS\n"
            f"SELECT * EXCEPT (_cur, _prev, _rn) FROM (\n"
            f"    SELECT *, {tup} AS _cur,\n"
            f"        lagInFrame({tup}, 1) OVER ({w} ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS _prev,\n"
            f"        row_number() OVER ({w}) AS _rn\n"
            f"    FROM {db}.{name}\n)\nWHERE _rn = 1 OR _cur != _prev")
    return stmts


def verify(client, name, tcfg, defaults):
    """Silver rows must equal the number of distinct (org, key, date) in Bronze."""
    db, src = defaults["database"], defaults["source_database"]
    keys = tcfg["business_key"]
    key_list = ", ".join([ORG] + keys + [DATE])
    not_empty = " AND ".join(f"{k} != ''" for k in keys)
    raw = client.query(f"SELECT count() FROM {src}.{name}").result_rows[0][0]
    expected = client.query(
        f"SELECT uniqExact({key_list}) FROM {src}.{name} WHERE {not_empty}").result_rows[0][0]
    silver = client.query(f"SELECT count() FROM {db}.{name}").result_rows[0][0]
    dup_keys = client.query(
        f"SELECT count() FROM (SELECT {key_list} FROM {db}.{name} GROUP BY {key_list} HAVING count() > 1)"
    ).result_rows[0][0]
    ok = silver == expected and dup_keys == 0
    print(f"  {'OK  ' if ok else 'FAIL'} {name:<14} bronze={raw:<6} silver={silver:<6} "
          f"removed={raw - silver:<4} duplicate_keys={dup_keys}")
    return ok


def collision_proof(client, db):
    """Same business key in two organisations must stay as separate rows."""
    rows = client.query(
        f"SELECT {ORG}, account_id, any(account_type), count() AS days "
        f"FROM {db}.accounts WHERE account_id = 'ACC-9999' GROUP BY {ORG}, account_id "
        f"ORDER BY {ORG}").result_rows
    print("\nSeeded collision ACC-9999 in silver.accounts:")
    for r in rows:
        print(f"  {r[0]}  {r[1]}  type={r[2]}  days={r[3]}")
    ok = len(rows) == 2
    print("  PASS: two organisations kept apart" if ok else "  FAIL: expected 2 organisations")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(ROOT / "configs" / "silver.yaml"))
    ap.add_argument("--table")
    ap.add_argument("--print-sql", action="store_true")
    args = ap.parse_args()

    cfg = load_config(args.config)
    defaults, tables = cfg["defaults"], cfg["tables"]
    if args.table:
        tables = {args.table: tables[args.table]}

    if args.print_sql:
        for name, tcfg in tables.items():
            for s in build_statements(name, tcfg, defaults):
                print(s + ";\n")
        return

    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST_LOCAL", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""))

    for name, tcfg in tables.items():
        for s in build_statements(name, tcfg, defaults):
            client.command(s)
        print(f"built {defaults['database']}.{name}")

    print("\nVerifying (silver rows must equal distinct org+key+date in bronze):")
    results = [verify(client, n, t, defaults) for n, t in tables.items()]
    if "accounts" in tables:
        results.append(collision_proof(client, defaults["database"]))
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()