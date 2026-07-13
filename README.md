# Databricks Genie MCP Tools

MCP server for Databricks Apps that exposes operational tools for Databricks Genie Spaces and Atlan business context lookup.

## Features

- Lists and inspects Databricks Genie Spaces available to the authenticated user.
- Collects Genie usage, feedback, conversation, benchmark, and permission metadata.
- Starts configured Databricks Jobs for Genie serialization, restore-point discovery, and restore execution.
- Searches Atlan for assets matching a Databricks table identifier in `catalog.schema.table` format.
- Returns Atlan glossary/readme context that can be reused by assistants when working with Text-to-SQL assets.

## Key Tools

- `health`: server health check.
- `get_current_user`: current Databricks user information.
- `list_available_genie_spaces`, `get_genie_space_details`, `list_genie_space_tags`: Genie Space discovery.
- `list_genie_space_conversations`, `list_genie_conversation_messages`, `get_genie_history_metrics`: Genie usage/history inspection.
- `list_genie_benchmark_runs`, `get_genie_benchmark_run`, `list_genie_benchmark_run_results`, `get_genie_benchmark_result_details`: benchmark inspection.
- `list_genie_space_permissions`, `grant_space_permissions`: Genie access review and controlled permission grants.
- `start_genie_serialization_job`, `get_genie_serialization_job_run`: serialization workflow.
- `list_genie_space_restore_points`, `get_genie_restore_points_job_run`, `start_genie_space_restore_job`, `get_genie_space_restore_job_run`: restore workflow.
- `find_atlan_assets_by_databricks_table`: find matching Atlan assets for `catalog.schema.table`.
- `get_atlan_context_for_databricks_table`: extract glossary/readme business context from matching Atlan assets.

## Configuration

The Databricks App uses these environment variables:

- `GENIE_SERIALIZATION_JOB_ID`: Databricks Job used to serialize Genie Spaces.
- `GENIE_RESTORE_POINTS_JOB_ID`: Databricks Job used to list restore points.
- `GENIE_RESTORE_JOB_ID`: Databricks Job used to restore a Genie Space snapshot.
- `ATLAN_API_KEY`: Atlan API key, normally provided from a Databricks secret.
- `ATLAN_BASE_URL`: Atlan tenant URL.

## Development

Install dependencies and run locally with `uv`:

```bash
uv sync
uv run custom-mcp-server --port 8000
```

The MCP endpoint is available at `http://localhost:8000/mcp`.

Run tests:

```bash
uv run pytest tests/
```

Optional integration-test environment variables:

- `GENIE_TEST_SPACE_ID`
- `GENIE_TEST_TAG_KEY`
- `GENIE_TEST_TAG_VALUE`
- `GENIE_TEST_CONVERSATION_ID`
- `GENIE_TEST_BENCHMARK_RUN_ID`
- `GENIE_TEST_BENCHMARK_RESULT_ID`
- `DATABRICKS_TEST_USER_ID`
- `DATABRICKS_TEST_JOB_RUN_ID`
- `ATLAN_TEST_TABLE_IDENTIFIER`

## Authentication

- Local development uses the default Databricks SDK authentication profile.
- In Databricks Apps, user-scoped tools call Databricks on behalf of the end user through `x-forwarded-access-token`.
- Job-oriented tools use the app service principal.
- Atlan tools use `ATLAN_API_KEY` and `ATLAN_BASE_URL`.
