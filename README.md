# Medallion Data Platform (Wealth Management)

A configuration-driven Bronze → Silver → Gold pipeline built from scratch with **MinIO**, **ClickHouse** and **Airflow**. Raw multi-tenant ORC files are read in place, cleaned and deduplicated, then aggregated into analytics tables. Every step keeps customer firms (organisations) strictly apart.

All data is synthetic: 12 tables, 3 organisations, 7 days, 252 ORC files.

> Demo video: _add your Loom link here_

## What it does

```
Raw ORC files (MinIO)  ->  Bronze (read in place)  ->  Silver (clean, deduplicated)  ->  Gold (dimensions, fact, aggregates)
```

| Layer | What happens | Built from |
|---|---|---|
| Bronze | ClickHouse S3-engine tables read the ORC files in MinIO without copying. A view adds `organisation_id` and `processing_date` from the folder path. | `configs/bronze.yaml` |
| Silver | Trim and type the data, drop empty keys, keep the newest row per organisation + key + day. Adds `_current` and `_history` views. | `configs/silver.yaml` |
| Gold | Dimensions with surrogate keys, a daily holdings fact, and two aggregates. | `configs/gold/<table>/` |

Add a table to a YAML file (or a new folder under `configs/gold/`) and the Airflow DAG creates its task automatically. No SQL is copy-pasted per table.

## Project structure

```
.
├── docker-compose.yml        # Airflow, Postgres, MinIO, ClickHouse
├── Dockerfile                # Airflow image
├── .env.example              # copy to .env
├── requirements.txt          # tools that run on your machine
├── configs/
│   ├── bronze.yaml           # 12 raw tables, columns, partition rule
│   ├── silver.yaml           # business keys, cleaning rules, history tracking
│   └── gold/<table>/         # config.yaml + query.sql (+ view.sql)
├── engine/
│   ├── bronze_builder.py     # YAML -> Bronze tables and views
│   ├── silver_builder.py     # YAML -> Silver tables, dedup, current/history views
│   └── gold_builder.py       # bundles -> Gold tables, surrogate keys, tenant guard
├── airflow/dags/
│   └── medallion_pipeline.py # generator DAG (one task per table)
├── tools/
│   └── data.py      # synthetic ORC generator -> MinIO
├── tests/
│   └── test_gold_guard.py    # multi-tenancy guard tests
└── data_catalog.md 
  
```

## Quick start

**Requirements:** Docker with Compose, Python 3.10+.

**1. Configure**

```bash
cp .env.example .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # AIRFLOW_FERNET_KEY
openssl rand -hex 32                                                                        # AIRFLOW_SECRET_KEY
```

Put real values in `.env`. The Airflow image needs `clickhouse-connect`; if the `Dockerfile` does not install it, add `RUN pip install clickhouse-connect`.

**2. Start the stack**

```bash
docker compose up -d --build
docker ps        # wait until the containers show healthy
```

| Service | Address |
|---|---|
| Airflow | http://localhost:8080 |
| MinIO console | http://localhost:9001 |
| ClickHouse HTTP | http://localhost:8123 (needs the password) |

**3. Generate the data and upload it to MinIO**

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python tools/generate_data.py --dry-run     # check first, writes ./data_out, expect 252 files
python tools/generate_data.py               # uploads to MinIO bucket "bronze"
```

Files land at `bronze/<source>/<table>/<organisation_id>/<YYYY-MM-DD>/data.orc`.

**4. Build the layers**

Option A: run the Airflow DAG.

```bash
docker exec airflow-scheduler airflow dags unpause medallion_pipeline
docker exec airflow-scheduler airflow dags trigger medallion_pipeline
```

Option B: run the builders by hand.

```bash
python -m engine.bronze_builder
python -m engine.silver_builder
python -m engine.gold_builder
```

Each builder verifies its own output and exits with an error if a check fails.

**5. Run the tests**

```bash
python -m pytest -q tests/test_gold_guard.py     # no database needed
```

## How to check that it works

| Check | Where | What you should see |
|---|---|---|
| Partition columns filled | Bronze builder output | 3 organisations, 7 dates, no empty values for every table |
| Deduplication | Silver builder output | `bronze` rows vs `silver` rows: contacts 882 → 840, accounts 896 → 854, securities 357 → 315, transactions 336 → 315; zero duplicate keys |
| Cross-tenant collision | Silver builder output | `ACC-9999` exists in `org_8812` and `org_4410`; Silver keeps both (7 days each) |
| Surrogate keys | Gold builder output | `ACC-9999` gets a different `account_key` in each organisation |
| Reconciliation | Gold builder output | Gold totals equal Silver totals |
| Tenant guard | `pytest` | 15 tests pass. They also remove `organisation_id` from each query and assert the guard fails |

## Design decisions

**Multi-tenancy.** `organisation_id` is part of every key. Every `GROUP BY` and every `JOIN ... ON` in Gold must include it. The Gold builder checks this before running anything, the DAG has a safety task for it, and the tests prove the guard fails when the column is removed.

**Seeded collision.** The generator puts the same business key (`ACC-9999`) into two organisations with different data. Silver keeps them as separate rows, and Gold gives them separate surrogate keys.

**Surrogate keys.** Gold dimensions use `sipHash64(organisation_id, business key)`. ClickHouse has no auto-increment sequences, and a hash is stable across rebuilds. Facts compute the same hash from their own keys, so joins need no lookup table.

**Gold table engines.**

| Table | Engine | Why |
|---|---|---|
| `dim_advisor`, `dim_household`, `dim_account`, `dim_security` | MergeTree | Reference tables rebuilt in full, no merge logic needed |
| `fact_holdings_daily` | MergeTree | Detail rows that are never summed or merged |
| `agg_aum_by_org_assetclass_day` | SummingMergeTree | Assets under management and counts are additive |
| `agg_trading_by_org_day` | AggregatingMergeTree | Distinct active portfolios cannot be added across batches, so partial states are stored and read through a view |

Print this table from the configs with `python -m engine.gold_builder --engine-docs`.

**Data models.** Silver is a normalized (3NF, OLTP-style) model. Gold is a snowflake schema: advisor → household → account → fact, with security joined to the fact. See `docs/data_catalog.md`.

## Documentation

- [`docs/data_catalog.md`](docs/data_catalog.md): OLTP and OLAP models first, then every Bronze, Silver and Gold table, lineage, quality rules and glossary.
- [`docs/models/`](docs/models/): Mermaid diagrams (they render on GitHub).

## Troubleshooting

| Problem | Fix |
|---|---|
| `Authentication failed` from ClickHouse | The password in `.env` must match the one the container was created with. Env changes need `docker compose up -d --force-recreate`. For a fresh local setup you can remove the volume (this deletes data). |
| Opening port 8123 in a browser shows an auth error | Normal: a browser sends no password. Use the CLI: `docker exec -it clickhouse clickhouse-client --user default --password '...'` |
| `KeyError: MINIO_ROOT_USER` | `.env` is missing or not in the project root. |
| Bronze partition columns are empty | Check the folder positions in `configs/bronze.yaml` (`path_index`). Run `SELECT _path FROM bronze.accounts_raw LIMIT 1` to see the real path. |
| DAG task fails with `ModuleNotFoundError: clickhouse_connect` | Add the package to the `Dockerfile` and rebuild. |

## Limitations

- Synthetic data only; the 252 files are generated once and loaded in full. There is no incremental load: Bronze, Silver and Gold are rebuilt each run.
- The MinIO credentials are written into the Bronze table definitions inside ClickHouse. This is fine for local development; use named collections or a secrets manager in production.
- Silver deduplication orders by `updated_at` only; ties are not broken further.
- Gold tables wait for all Silver tables in the DAG, which is simpler than per-table dependencies but slower.
- The DAG is triggered by hand (`schedule=None`).
- The schema-helper CLI (draft Bronze YAML from a raw ORC file) is not built yet.

## What I would build next

1. The schema-helper CLI, and a saved test for the Silver collision proof.
2. Incremental loads per day instead of full rebuilds.
3. Per-table DAG dependencies and a daily schedule.
4. Data quality checks as a separate framework (Project 2) and a freshness and lineage console (Project 3).
