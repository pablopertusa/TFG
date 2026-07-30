# Development Scripts

Scripts for testing and developing the MCP server.

## Quick Reference

| Script | Purpose | Environment |
|--------|---------|-------------|
| `local_agent.py` | Run a local agent with a Databricks-hosted model and local MCP | Local + Databricks Model Serving |
| `local_agent_agents_sdk.py` | Run the same local flow using OpenAI Agents SDK | Local + Databricks Model Serving |
| `query_remote.sh` | Test deployed app (interactive OAuth) | Databricks App |
| `start_server.sh` | Start local dev server | `localhost:8000` |
| `generate_oauth_token.py` | Generate OAuth tokens | Any |

## Testing

### Local Testing

Create the shared project environment once, and repeat after dependency changes:

```bash
uv sync
```

Start the MCP server in the first terminal:

```bash
DATABRICKS_CONFIG_PROFILE=<profile> uv run mcp-pablo --port 8000
```

Run the local agent in the second terminal:

```bash
uv run scripts/dev/local_agent.py --profile <profile> --require-tool
```

All MCP tools are available to the model by default. Pass `--tools health,get_current_user` to restrict the agent to a specific subset.

To use the OpenAI Agents SDK implementation instead:

```bash
uv run scripts/dev/local_agent_agents_sdk.py --profile <profile> --require-tool
```

This implementation records MLflow traces by default. The trace contains the complete agent latency, model spans, tokens, estimated cost, and MCP tool calls. It uses `/Users/<current-user>/mcp-local-agent` unless `--experiment-id`, `--experiment-name`, `MLFLOW_EXPERIMENT_ID`, or `MLFLOW_EXPERIMENT_NAME` selects another experiment. Pass `--no-tracing` to disable it.

### Remote Testing

```bash
# Interactive (walks you through OAuth)
./scripts/dev/query_remote.sh
```

Tests all tools with user-level OAuth authentication.

## Development Workflow

1. Add tool to `server/tools.py`
2. Test locally: Either follow the local testing above or run the integration tests
3. Deploy to Databricks Apps
4. Test deployed: `./scripts/dev/query_remote.sh`
