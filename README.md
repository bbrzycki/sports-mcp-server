# sports-mcp-server

Model Context Protocol (MCP) server that exposes curated sports analytics to large language model agents.

## High-level scope

- Provide read-only, intent-focused endpoints (e.g., `get_game_summary`, `compare_pitchers`, `list_active_bet_signals`).
- Wrap warehouse + MLflow + control-plane APIs to keep the agent sandboxed to domain-appropriate data.
- Enforce authentication, rate limits, and response shaping to prevent arbitrary SQL access.

## Skeleton plan

```
app/
  main.py          # MCP entrypoint
  handlers/        # Functions per capability
  clients/         # Warehouse + control-plane adapters
schemas/
  responses/       # Pydantic models describing payloads
```

## MVP status

The current MVP ships a FastAPI service with a stubbed dataset registry:

- `GET /datasets` — enumerate available datasets (currently `pitching_outings`).
- `GET /datasets/{dataset_id}` — describe schema and metadata.
- `POST /datasets/{dataset_id}/query` — return filtered, paginated rows (supports `eq`, `gte`, `lte` filters).

Sample data includes Shohei Ohtani and Gerrit Cole pitching outings so downstream agents can develop ERA-style analytics before the warehouse connectors are wired in.

### Dataset metadata scaffolding

To bootstrap real datasets from Postgres without hand-copying every column, use the introspection script:

```bash
pip install -e .  # ensures psycopg is available
cd scripts
python generate_dataset_registry.py \
  --database sports_dw_dev \
  --schemas marts_baseball staging_baseball \
  --output-dir ../dataset_registry.generated
```

The script connects using the usual `PG*` env vars (or CLI flags), enumerates every base table in the provided schemas, and emits a JSON registry with:

- One file per table at `dataset_registry.generated/<schema>/<table>.json`
- `dataset_id` = `<schema>.<table>`
- Auto-generated display name (`"Pitcher Game Logs"`, etc.)
- Columns with name + Postgres data type and blank description placeholders

You can then fill in dataset/table descriptions, primary keys, and richer column docs directly in the generated file before wiring those datasets into the MCP server.

### Connecting to Postgres

The service expects Postgres connection details via environment variables (defaulting to the `sports-data-platform` warehouse container when both stacks share the `sports_net` network):

```
PGHOST=warehouse
PGPORT=5432
PGDATABASE=sports_dw
PGUSER=sports_admin
PGPASSWORD=sports_admin_password
```

To run locally with Docker Compose, ensure `sports-data-platform` is up first, then launch this service:

```bash
docker compose up --build
```

When running directly on the host, point `PGHOST`/`PGPORT` at the exposed warehouse port (e.g., `localhost:5433`).

## Docker quick start

```bash
docker network create sports_net  # one-time shared network
docker compose up --build
```

The placeholder service is reachable at `http://localhost:9100/healthz`.
