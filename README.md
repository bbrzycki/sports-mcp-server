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

The service now loads every JSON spec under `dataset_registry.curated/`, exposes their metadata via:

- `GET /datasets` — enumerate available datasets (driven entirely by the curated registry).
- `GET /datasets/{dataset_id}` — describe schema, columns, primary keys, and documentation.
- `POST /datasets/{dataset_id}/query` — execute parameterised SQL (eq/gte/lte filters, column projection, limit/offset) against Postgres using those definitions.

As soon as you add or edit a dataset JSON file and restart the server, the endpoints surface the new schema automatically—no more stubbed data living in the codebase.

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

### Auto-annotating curated tables

After copying the tables you plan to expose into `dataset_registry.curated/`, run:

```bash
python scripts/annotate_registry.py
```

The annotator pulls the latest Baseball Savant CSV documentation plus a handful of heuristics to populate dataset descriptions, primary keys, and column blurbs (e.g., counts, percentages, IDs). Review and edit the output as needed, but it should give each dataset enough context for an LLM agent to reason about the available fields.

### Runtime configuration

- `DATASET_REGISTRY_DIR` (default `dataset_registry.curated`) — directory the API scans for dataset JSON specs.
- `POSTGRES_DSN` — override the composed connection string if you prefer to supply a single DSN (otherwise the usual `PG*` env vars are read).

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
