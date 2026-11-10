"""
Bronze builder: reads configs/bronze.yaml and creates, for every table,
  1. bronze.<table>_raw : ClickHouse S3-engine table that reads the ORC files in MinIO in place
  2. bronze.<table>     : view that adds organisation_id and processing_date taken from the file path

Usage (from ~/medallion):
    pip install clickhouse-connect pyyaml python-dotenv
    python -m engine.bronze_builder --print-sql      # only show the SQL
    python -m engine.bronze_builder                  # create everything + verify partitions
    python -m engine.bronze_builder --table accounts # one table only
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


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def build_statements(name, tcfg, defaults, user, password):
    """Return the SQL statements that create the raw S3 table and the partitioned view."""
    db, bucket = defaults["database"], defaults["bucket"]
    p_org = defaults["partitions"]["organisation_id"]["path_index"]
    p_date = defaults["partitions"]["processing_date"]["path_index"]
    url = f"http://{defaults['endpoint']}/{bucket}/{tcfg['source']}/{name}/*/*/data.orc"
    cols = ",\n    ".join(f"{c['name']} {c['type']}" for c in tcfg["columns"])
    return [
        f"CREATE DATABASE IF NOT EXISTS {db}",
        f"DROP TABLE IF EXISTS {db}.{name}",          # view first, because it depends on _raw
        f"DROP TABLE IF EXISTS {db}.{name}_raw",
        (f"CREATE TABLE {db}.{name}_raw\n(\n    {cols}\n)\n"
         f"ENGINE = S3('{url}', '{user}', '{password}', '{defaults['format']}')"),
        (f"CREATE VIEW {db}.{name} AS\n"
         f"SELECT\n"
         f"    splitByChar('/', _path)[{p_org}] AS organisation_id,\n"
         f"    toDate(splitByChar('/', _path)[{p_date}]) AS processing_date,\n"
         f"    *\n"
         f"FROM {db}.{name}_raw"),
    ]


def verify(client, db, name):
    """Proof that partition columns are populated (not empty) for one Bronze table."""
    row = client.query(
        f"SELECT count(), uniqExact(organisation_id), uniqExact(processing_date), "
        f"countIf(organisation_id = ''), countIf(processing_date = toDate('1970-01-01')) "
        f"FROM {db}.{name}").result_rows[0]
    total, orgs, dates, empty_org, bad_dates = row
    ok = total > 0 and orgs == 3 and dates == 7 and empty_org == 0 and bad_dates == 0
    print(f"  {'OK  ' if ok else 'FAIL'} {name:<14} rows={total:<6} orgs={orgs} dates={dates} "
          f"empty_org={empty_org} bad_dates={bad_dates}")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(ROOT / "configs" / "bronze.yaml"))
    ap.add_argument("--table", help="build only this table")
    ap.add_argument("--print-sql", action="store_true")
    args = ap.parse_args()

    cfg = load_config(args.config)
    defaults, tables = cfg["defaults"], cfg["tables"]
    if args.table:
        tables = {args.table: tables[args.table]}

    user = os.environ.get("MINIO_ROOT_USER", "<MINIO_ROOT_USER>")
    password = os.environ.get("MINIO_ROOT_PASSWORD", "<MINIO_ROOT_PASSWORD>")

    if args.print_sql:
        for name, tcfg in tables.items():
            for stmt in build_statements(name, tcfg, defaults, user, "********"):
                print(stmt + ";\n")
        return

    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST_LOCAL", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""))

    for name, tcfg in tables.items():
        for stmt in build_statements(name, tcfg, defaults, user, password):
            client.command(stmt)
        print(f"created {defaults['database']}.{name}")

    print("\nVerifying partition columns:")
    results = [verify(client, defaults["database"], n) for n in tables]
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()