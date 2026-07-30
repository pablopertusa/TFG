# Databricks Genie MCP Tools

MCP server for Databricks Apps that exposes reusable tools for Databricks Genie operations, Atlan business-context lookup, usage monitoring, benchmark inspection, permissions, and Genie lifecycle jobs.

The server is intended to be consumed by MCP-compatible agents and clients, including the Databricks working environment used by business users and developers. In that setup, Genie One can use business-facing tools such as Atlan context lookup and Genie usage summaries, while Genie Code can use technical tools for debugging, benchmark inspection, permissions, serialization, and restore workflows.

## Current Scope

- Exposes a stateless HTTP MCP endpoint at `/mcp` using FastMCP and FastAPI.
- Runs locally through the `mcp-pablo` CLI entrypoint and deploys as a Databricks App.
- Uses Databricks user authentication for user-scoped operations when running in Databricks Apps.
- Uses configured Databricks Jobs for Genie serialization and restore workflows.
- Uses Databricks SQL Statement Execution against `system.access.audit` for Genie usage metrics.
- Uses Atlan API credentials to search Databricks assets and extract glossary/readme context.

## Tool Groups

### Health And Identity

- `health`: checks that the server is reachable.
- `get_current_user`: returns the current Databricks user.
- `get_user_name_from_id`: resolves a Databricks user ID to a username when visible to the caller.

### Genie Discovery

- `list_available_genie_spaces`: lists Genie Spaces available to the authenticated user.
- `get_genie_space_details`: returns details for one Genie Space.
- `list_genie_space_tags`: lists custom tags for one Genie Space.
- `find_genie_spaces_by_tag`: finds Genie Spaces matching a tag key and optional tag value.

### Atlan Context

- `find_atlan_assets_by_databricks_table`: finds Atlan assets matching a Databricks table identifier in `catalog.schema.table` format.
- `get_atlan_context_for_databricks_table`: extracts assigned glossary terms, descriptions, READMEs, and a combined context block from matching Atlan assets.

### Genie Usage And Conversations

- `list_genie_space_conversations`: lists conversations for a Genie Space.
- `list_genie_conversation_messages`: lists messages for one conversation.
- `get_genie_usage_metrics`: aggregates Genie usage and feedback metrics for a configurable recent-day window, 90 days by default.
- `start_genie_usage_metrics_query`: starts the usage metrics query and returns a Databricks SQL `statement_id` without waiting for completion.
- `get_genie_usage_metrics_query_result`: checks a started metrics query and returns metrics once the SQL statement succeeds.

### Benchmarks

- `list_genie_benchmark_runs`: lists benchmark/evaluation runs for a Genie Space.
- `get_genie_benchmark_run`: returns one benchmark run.
- `list_genie_benchmark_run_results`: lists result rows for one benchmark run.
- `get_genie_benchmark_result_details`: returns detailed information for one benchmark result.

### Permissions

- `list_genie_space_permissions`: lists access-control entries for one Genie Space.
- `grant_space_permissions`: grants `CAN_MANAGE`, `CAN_EDIT`, or `CAN_READ` after explicit confirmation.

### Serialization And Restore

- `start_genie_serialization_job`: starts the configured serialization job by tag or space ID after explicit confirmation.
- `get_genie_serialization_job_run`: checks a serialization job run.
- `list_genie_space_restore_points`: runs the configured job that lists restore points for one Genie Space.
- `get_genie_restore_points_job_run`: checks a restore-points job run.
- `start_genie_space_restore_job`: starts the configured restore job for a snapshot date after explicit confirmation.
- `get_genie_space_restore_job_run`: checks a restore job run.

## Configuration

The Databricks App configuration lives in `app.yaml`. Current environment variables are:

- `GENIE_SERIALIZATION_JOB_ID`: Databricks Job used to serialize Genie Spaces.
- `GENIE_RESTORE_POINTS_JOB_ID`: Databricks Job used to list available restore points.
- `GENIE_RESTORE_JOB_ID`: Databricks Job used to restore a Genie Space snapshot.
- `GENIE_SPACE_WAREHOUSE_ID`: warehouse used by Genie-space-related workflows that require a SQL warehouse.
- `DATABRICKS_AUDIT_WAREHOUSE_ID`: warehouse used to query `system.access.audit` for usage metrics.
- `ATLAN_API_KEY`: Atlan API key, normally provided from a Databricks secret.
- `ATLAN_BASE_URL`: Atlan tenant URL.

Local development also requires Databricks SDK authentication through the normal Databricks unified authentication flow, such as a configured Databricks CLI profile.

## Development

Create or synchronize the single project virtual environment:

```bash
uv sync
```

All server, agent, test, and tracing dependencies are declared in `pyproject.toml`, resolved in `uv.lock`, and installed in `.venv`. The development scripts do not create separate environments or install packages themselves.

Run the server locally:

```bash
uv run mcp-pablo --port 8000
```

The MCP endpoint is available at:

```text
http://localhost:8000/mcp
```

The root endpoint `/` serves `static/index.html` when present, otherwise it returns a small health payload.

## Testing

Run the Python integration tests:

```bash
uv run pytest tests/
```

The tests start a local MCP server and use `databricks-mcp` to list and call tools. Tests that require external IDs or credentials skip when the corresponding environment variables are not set.

Useful optional test variables:

- `GENIE_TEST_SPACE_ID`
- `GENIE_TEST_TAG_KEY`
- `GENIE_TEST_TAG_VALUE`
- `GENIE_TEST_CONVERSATION_ID`
- `GENIE_TEST_BENCHMARK_RUN_ID`
- `GENIE_TEST_BENCHMARK_RESULT_ID`
- `DATABRICKS_TEST_USER_ID`
- `DATABRICKS_TEST_JOB_RUN_ID`
- `ATLAN_TEST_TABLE_IDENTIFIER`
- `PRINT_TOOL_RESULTS=1`

For local Atlan calls, set `ATLAN_API_KEY` and `ATLAN_BASE_URL`. For audit metrics, set `DATABRICKS_AUDIT_WAREHOUSE_ID` to a warehouse that can query `system.access.audit`.

### Local Agent

`scripts/dev/local_agent.py` runs a manual agent loop locally, queries a Databricks-hosted model, and connects directly to the local MCP endpoint.

Start the MCP server in one terminal:

```bash
DATABRICKS_CONFIG_PROFILE=<profile> uv run mcp-pablo --port 8000
```

Run the agent in another terminal:

```bash
uv run scripts/dev/local_agent.py --profile <profile> --require-tool
```

All MCP tools are exposed to the model by default. Use `--tools tool_a,tool_b` to restrict the agent to an explicit allowlist. The complete default set includes tools that grant permissions or start serialization and restore jobs; those tools retain their server-side confirmation requirements.

An equivalent implementation using OpenAI Agents SDK for the agent loop and MCP integration is also available:

```bash
uv run scripts/dev/local_agent_agents_sdk.py --profile <profile> --require-tool
```

The Agents SDK implementation enables MLflow tracing by default. It logs the complete agent run, model calls, token usage, estimated model cost, latency, tool definitions, and MCP tool inputs and outputs to Databricks. If no experiment is configured, it creates or uses `/Users/<current-user>/mcp-local-agent`.

Select an existing experiment by ID or path:

```bash
uv run scripts/dev/local_agent_agents_sdk.py \
  --profile <profile> \
  --experiment-id <experiment-id> \
  --require-tool
```

Use `--experiment-name <workspace-path>` instead of `--experiment-id`, or `--no-tracing` to run without MLflow. Costs are estimates derived from token usage and the MLflow model-price catalog, not billing records.

## Authentication Model

- Local development uses the default Databricks SDK authentication resolution.
- In Databricks Apps, user-scoped tools authenticate on behalf of the end user through the `x-forwarded-access-token` header captured by middleware.
- Job-oriented tools use the Databricks app/service-principal context.
- Atlan tools use `ATLAN_API_KEY` and `ATLAN_BASE_URL`.

## Safety Notes

- Mutating or operationally expensive actions require explicit confirmation strings before execution.
- Atlan tools return structured context and avoid modifying Atlan assets.
- Audit metrics query `system.access.audit`; access depends on workspace permissions and warehouse configuration. The metrics tool filters by `event_date` and defaults to the last 90 days to avoid scanning the full audit table. Pass `lookback_days` to expand or reduce the window.
- Secrets should be provided through Databricks secrets or local ignored env files, never committed.
