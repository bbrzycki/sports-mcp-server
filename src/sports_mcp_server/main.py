"""MVP MCP-style data service that returns structured JSON slices."""
from __future__ import annotations

from typing import Any, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="Sports MCP Server")


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
    sample_size: int


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


PITCHING_OUTINGS = [
    {
        "player_id": "mlb-660271",
        "player_name": "Shohei Ohtani",
        "game_date": "2021-04-04",
        "season": 2021,
        "earned_runs": 0,
        "outs_recorded": 10,
    },
    {
        "player_id": "mlb-660271",
        "player_name": "Shohei Ohtani",
        "game_date": "2021-04-12",
        "season": 2021,
        "earned_runs": 4,
        "outs_recorded": 9,
    },
    {
        "player_id": "mlb-660271",
        "player_name": "Shohei Ohtani",
        "game_date": "2022-04-07",
        "season": 2022,
        "earned_runs": 1,
        "outs_recorded": 12,
    },
    {
        "player_id": "mlb-593643",
        "player_name": "Gerrit Cole",
        "game_date": "2021-04-01",
        "season": 2021,
        "earned_runs": 2,
        "outs_recorded": 15,
    },
]


DATASETS: dict[str, dict[str, Any]] = {
    "pitching_outings": {
        "meta": DatasetMeta(
            dataset_id="pitching_outings",
            name="Pitching Outings",
            description=(
                "One row per pitcher appearance with earned runs and outs recorded. "
                "Stubbed sample data for agent development."
            ),
            primary_key=["player_id", "game_date"],
            columns=[
                DatasetColumn(
                    name="player_id",
                    dtype="string",
                    description="Canonical pitcher identifier",
                ),
                DatasetColumn(
                    name="player_name",
                    dtype="string",
                    description="Display name",
                ),
                DatasetColumn(
                    name="game_date",
                    dtype="date",
                    description="Date of the appearance",
                ),
                DatasetColumn(
                    name="season",
                    dtype="int",
                    description="Season year",
                ),
                DatasetColumn(
                    name="earned_runs",
                    dtype="int",
                    description="Earned runs charged to the pitcher",
                ),
                DatasetColumn(
                    name="outs_recorded",
                    dtype="int",
                    description="Number of outs recorded (3 = 1 IP)",
                    units="outs",
                ),
            ],
            sample_size=len(PITCHING_OUTINGS),
        ),
        "data": PITCHING_OUTINGS,
    }
}


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
    dataset = DATASETS.get(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    records: list[dict[str, Any]] = dataset["data"]
    filtered = _apply_filters(records, query.filters)
    projected = _apply_projection(filtered, query.columns)

    total = len(projected)
    start = query.offset
    end = min(start + query.limit, total)
    if start > total:
        page: list[dict[str, Any]] = []
    else:
        page = projected[start:end]

    next_offset = end if end < total else None

    return DatasetSlice(
        dataset_id=dataset_id,
        total=total,
        returned=len(page),
        offset=query.offset,
        next_offset=next_offset,
        data=page,
    )


def _apply_filters(
    rows: list[dict[str, Any]],
    filters: list[QueryFilter],
) -> list[dict[str, Any]]:
    if not filters:
        return rows

    def row_matches(row: dict[str, Any]) -> bool:
        for f in filters:
            value = row.get(f.column)
            if value is None:
                return False
            if f.op == "eq":
                if isinstance(value, str) and isinstance(f.value, str):
                    lhs = _normalize_string(value)
                    rhs = _normalize_string(f.value)
                    if lhs != rhs:
                        return False
                elif value != f.value:
                    return False
            if f.op == "gte" and not value >= f.value:
                return False
            if f.op == "lte" and not value <= f.value:
                return False
        return True

    return [row for row in rows if row_matches(row)]


def _normalize_string(value: str) -> str:
    return " ".join(value.replace(",", " ").lower().split())


def _apply_projection(
    rows: list[dict[str, Any]],
    columns: list[str] | None,
) -> list[dict[str, Any]]:
    if not columns:
        return rows
    return [{col: row.get(col) for col in columns} for row in rows]
