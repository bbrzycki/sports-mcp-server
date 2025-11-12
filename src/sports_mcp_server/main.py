"""MVP MCP-style data service that returns structured JSON slices."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Literal

from dataclasses import dataclass

from fastapi import FastAPI, HTTPException
from psycopg import sql
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

CURATED_REGISTRY_DIR = Path(
    os.getenv("DATASET_REGISTRY_DIR", "dataset_registry.curated")
).resolve()
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "dbname={db} user={user} password={password} host={host} port={port}".format(
        db=os.getenv("PGDATABASE", "sports_dw_dev"),
        user=os.getenv("PGUSER", "sports_admin"),
        password=os.getenv("PGPASSWORD", "sports_admin_password"),
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
    ),
)

app = FastAPI(title="Sports MCP Server")
POOL: ConnectionPool | None = None


class DatasetColumn(BaseModel):
    name: str
    dtype: str
    description: str | None = None
    units: str | None = None


class DatasetMeta(BaseModel):
    dataset_id: str = Field(..., description="Registry identifier")
    name: str
    description: str
    primary_key: list[str] = Field(default_factory=list)
    columns: list[DatasetColumn]
    sample_size: int | None = None


class QueryFilter(BaseModel):
    column: str
    op: Literal["eq", "gte", "lte"]
    value: Any


class DatasetQuery(BaseModel):
    filters: list[QueryFilter] = Field(default_factory=list)
    columns: list[str] | None = None
    limit: int = Field(default=100, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


class DatasetSlice(BaseModel):
    dataset_id: str
    total: int
    returned: int
    offset: int
    next_offset: int | None
    data: list[dict[str, Any]]


@dataclass
class DatasetEntry:
    meta: DatasetMeta
    schema: str
    table: str
    column_names: set[str]


def _load_dataset_registry(directory: Path) -> dict[str, DatasetEntry]:
    if not directory.exists():
        raise RuntimeError(f"Dataset registry directory '{directory}' does not exist.")

    registry: dict[str, DatasetEntry] = {}
    for json_file in sorted(directory.rglob("*.json")):
        payload = json.loads(json_file.read_text())
        dataset_id = payload.get("dataset_id")
        if not dataset_id:
            raise RuntimeError(f"Dataset file '{json_file}' missing 'dataset_id'.")
        schema = payload.get("schema")
        table = payload.get("table")
        if not schema or not table:
            raise RuntimeError(f"Dataset '{dataset_id}' missing schema/table definitions.")

        columns = [
            DatasetColumn(**column) for column in payload.get("columns", [])
        ]
        if not columns:
            raise RuntimeError(f"Dataset '{dataset_id}' must define at least one column.")

        meta = DatasetMeta(
            dataset_id=dataset_id,
            name=payload.get("name", dataset_id),
            description=payload.get("description", ""),
            primary_key=payload.get("primary_key", []),
            columns=columns,
            sample_size=payload.get("sample_size"),
        )
        registry[dataset_id] = DatasetEntry(
            meta=meta,
            schema=schema,
            table=table,
            column_names={col.name for col in columns},
        )

    if not registry:
        raise RuntimeError(f"No datasets were found under '{directory}'.")
    return registry


def _get_pool() -> ConnectionPool:
    global POOL
    if POOL is None:
        POOL = ConnectionPool(POSTGRES_DSN)
    return POOL


DATASETS: dict[str, DatasetEntry] = _load_dataset_registry(CURATED_REGISTRY_DIR)


@app.get("/healthz")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/datasets", response_model=list[DatasetMeta])
def list_datasets() -> list[DatasetMeta]:
    return [cfg["meta"] for cfg in DATASETS.values()]


@app.get("/datasets/{dataset_id}", response_model=DatasetMeta)
def describe_dataset(dataset_id: str) -> DatasetMeta:
    dataset = DATASETS.get(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset["meta"]


@app.post("/datasets/{dataset_id}/query", response_model=DatasetSlice)
def query_dataset(dataset_id: str, query: DatasetQuery) -> DatasetSlice:
    entry = DATASETS.get(dataset_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    selected_columns = _resolve_columns(entry, query.columns)
    where_sql, params = _build_where_clause(query.filters, entry.column_names)

    total = _count_rows(entry, where_sql, params)
    rows = _fetch_rows(entry, selected_columns, where_sql, params, query.limit, query.offset)
    next_offset = query.offset + query.limit if query.offset + query.limit < total else None
    return DatasetSlice(
        dataset_id=dataset_id,
        total=total,
        returned=len(rows),
        offset=query.offset,
        next_offset=next_offset,
        data=rows,
    )


def _resolve_columns(entry: DatasetEntry, requested: list[str] | None) -> list[str]:
    available = [col.name for col in entry.meta.columns]
    if not requested:
        return available
    unknown = [col for col in requested if col not in entry.column_names]
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"Columns {unknown} are not available on dataset '{entry.meta.dataset_id}'.",
        )
    return requested


def _build_where_clause(
    filters: list[QueryFilter],
    allowed_columns: set[str],
) -> tuple[sql.SQL, list[Any]]:
    if not filters:
        return sql.SQL(""), []

    op_map = {"eq": "=", "gte": ">=", "lte": "<="}
    clauses: list[sql.SQL] = []
    params: list[Any] = []
    for fil in filters:
        if fil.column not in allowed_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{fil.column}' cannot be used for filtering.",
            )
        operator = op_map[fil.op]
        clauses.append(
            sql.SQL("{} {} %s").format(
                sql.Identifier(fil.column),
                sql.SQL(operator),
            )
        )
        params.append(fil.value)

    clause_sql = sql.SQL(" AND ").join(clauses)
    return sql.SQL(" WHERE ") + clause_sql, params


def _count_rows(entry: DatasetEntry, where_sql: sql.SQL, params: list[Any]) -> int:
    base = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
        sql.Identifier(entry.schema),
        sql.Identifier(entry.table),
    )
    query = base + where_sql
    pool = _get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            result = cur.fetchone()
    return int(result[0]) if result else 0


def _fetch_rows(
    entry: DatasetEntry,
    columns: list[str],
    where_sql: sql.SQL,
    params: list[Any],
    limit: int,
    offset: int,
) -> list[dict[str, Any]]:
    select_clause = sql.SQL(", ").join(sql.Identifier(col) for col in columns)
    base = sql.SQL("SELECT {} FROM {}.{}").format(
        select_clause,
        sql.Identifier(entry.schema),
        sql.Identifier(entry.table),
    )
    order_sql = (
        sql.SQL(" ORDER BY ")
        + sql.SQL(", ").join(sql.Identifier(col) for col in entry.meta.primary_key)
        if entry.meta.primary_key
        else sql.SQL("")
    )
    query = base + where_sql + order_sql + sql.SQL(" LIMIT %s OFFSET %s")
    values = list(params) + [limit, offset]
    pool = _get_pool()
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, values)
            return cur.fetchall()
