#!/usr/bin/env python
"""Generate dataset metadata stubs by introspecting Postgres schemas."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Iterable

import psycopg


DEFAULT_SCHEMAS = ("marts_baseball", "staging_baseball")

# python scripts/generate_dataset_registry.py --database sports_dw_dev --schemas marts_baseball staging_baseball
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Introspect Postgres tables and emit MCP dataset metadata stubs."
    )
    parser.add_argument(
        "--host",
        default=os.getenv("PGHOST", "localhost"),
        help="Postgres host (env: PGHOST).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("PGPORT", 5433)),
        help="Postgres port (env: PGPORT).",
    )
    parser.add_argument(
        "--user",
        default=os.getenv("PGUSER", "sports_admin"),
        help="Postgres user (env: PGUSER).",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("PGPASSWORD", "sports_admin_password"),
        help="Postgres password (env: PGPASSWORD).",
    )
    parser.add_argument(
        "--database",
        default=os.getenv("PGDATABASE", "sports_dw_dev"),
        help="Database name (env: PGDATABASE).",
    )
    parser.add_argument(
        "--schemas",
        nargs="+",
        default=list(DEFAULT_SCHEMAS),
        help="Schemas to introspect.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("dataset_registry.generated"),
        help="Directory to write per-table metadata JSON files.",
    )
    return parser.parse_args()


def friendly_name(table_name: str) -> str:
    return table_name.replace("_", " ").title()


def fetch_tables(conn, schemas: Iterable[str]) -> list[tuple[str, str]]:
    schema_tuple = tuple(schemas)
    if not schema_tuple:
        return []
    placeholders = ", ".join(["%s"] * len(schema_tuple))
    query = f"""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema IN ({placeholders})
        ORDER BY table_schema, table_name
    """
    with conn.cursor() as cur:
        cur.execute(query, schema_tuple)
        return cur.fetchall()


def fetch_columns(conn, schema: str, table: str) -> list[dict[str, str]]:
    query = """
        SELECT
            column_name,
            data_type,
            udt_name,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    columns: list[dict[str, str]] = []
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        for name, data_type, udt_name, is_nullable in cur.fetchall():
            columns.append(
                {
                    "name": name,
                    "dtype": udt_name or data_type,
                    "description": "",
                    "units": None,
                    "nullable": is_nullable == "YES",
                }
            )
    return columns


def main() -> None:
    args = parse_args()
    dsn = (
        f"dbname={args.database} user={args.user} password={args.password} "
        f"host={args.host} port={args.port}"
    )
    with psycopg.connect(dsn) as conn:
        tables = fetch_tables(conn, args.schemas)
        registry: dict[str, dict[str, object]] = {}
        for schema, table in tables:
            dataset_id = f"{schema}.{table}"
            registry[dataset_id] = {
                "dataset_id": dataset_id,
                "name": friendly_name(table),
                "description": "",
                "schema": schema,
                "table": table,
                "primary_key": [],
                "columns": fetch_columns(conn, schema, table),
                "sample_size": None,
            }

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    for dataset_id, payload in registry.items():
        schema_dir = output_dir / payload["schema"]
        schema_dir.mkdir(parents=True, exist_ok=True)
        file_path = schema_dir / f"{payload['table']}.json"
        file_path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    print(f"Wrote {len(registry)} dataset stubs under {output_dir}/<schema>/<table>.json")


if __name__ == "__main__":
    main()
