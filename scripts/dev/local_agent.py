import argparse
import asyncio
import os
from typing import Any

import mlflow
from agents import (
    Agent,
    ModelSettings,
    OpenAIChatCompletionsModel,
    Runner,
    ToolCallItem,
    ToolCallOutputItem,
    set_tracing_disabled,
)
from agents.mcp import MCPServerStreamableHttp, create_static_tool_filter
from databricks.sdk import WorkspaceClient
from mlflow.entities import SpanType
from openai import AsyncOpenAI

from server.agent_config import AGENT_INSTRUCTIONS

DEFAULT_PROMPT = "Comprueba mediante la herramienta health que el servidor MCP local funciona."


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run an OpenAI Agents SDK agent with a Databricks model and local MCP."
    )
    parser.add_argument("prompt", nargs="*", help="Question or instruction for the agent")
    parser.add_argument(
        "--model",
        default=os.getenv("DATABRICKS_MODEL_ENDPOINT", "databricks-gpt-5-4"),
        help="Databricks model serving endpoint",
    )
    parser.add_argument(
        "--profile",
        default=os.getenv("DATABRICKS_CONFIG_PROFILE"),
        help="Databricks CLI profile; default authentication is used when omitted",
    )
    parser.add_argument(
        "--mcp-url",
        default=os.getenv("MCP_SERVER_URL", "http://127.0.0.1:8000/mcp"),
        help="Local Streamable HTTP MCP endpoint",
    )
    parser.add_argument(
        "--tools",
        default=os.getenv("MCP_AGENT_TOOLS", "*"),
        help="Comma-separated allowlist of MCP tools, or * for all tools (default)",
    )
    parser.add_argument(
        "--require-tool",
        action="store_true",
        help="Require the model to call a tool on its first turn",
    )
    parser.add_argument("--max-turns", type=int, default=4)
    parser.add_argument(
        "--experiment-id",
        default=os.getenv("MLFLOW_EXPERIMENT_ID"),
        help="Existing Databricks MLflow experiment ID",
    )
    parser.add_argument(
        "--experiment-name",
        default=os.getenv("MLFLOW_EXPERIMENT_NAME"),
        help="Databricks MLflow experiment path; defaults to the current user's home",
    )
    parser.add_argument(
        "--no-tracing",
        action="store_true",
        help="Disable MLflow tracing for this execution",
    )
    return parser.parse_args()


def _workspace_client(profile: str | None) -> WorkspaceClient:
    return WorkspaceClient(profile=profile) if profile else WorkspaceClient()


def _databricks_openai_client(workspace_client: WorkspaceClient) -> AsyncOpenAI:
    authorization = workspace_client.config.authenticate().get("Authorization", "")
    if not authorization.startswith("Bearer "):
        raise RuntimeError("Databricks authentication did not return a Bearer token")

    return AsyncOpenAI(
        api_key=authorization.removeprefix("Bearer "),
        base_url=f"{workspace_client.config.host.rstrip('/')}/serving-endpoints",
        timeout=60,
        max_retries=1,
    )


def _configure_mlflow(
    profile: str | None,
    user_name: str,
    experiment_id: str | None,
    experiment_name: str | None,
) -> None:
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        tracking_uri = f"databricks://{profile}" if profile else "databricks"

    mlflow.set_tracking_uri(tracking_uri)
    if experiment_id:
        experiment = mlflow.set_experiment(experiment_id=experiment_id)
    else:
        experiment = mlflow.set_experiment(
            experiment_name=experiment_name or f"/Users/{user_name}/mcp-local-agent"
        )

    # Replace the default OpenAI exporter with MLflow's Agents SDK trace processor.
    mlflow.openai.autolog(disable_openai_agent_tracer=True)
    print(f"MLflow: {experiment.name} (experiment ID: {experiment.experiment_id})")


def _tool_filter(tool_names: set[str]):
    if "*" in tool_names:
        return None
    return create_static_tool_filter(allowed_tool_names=sorted(tool_names))


def _tool_call_name(item: ToolCallItem) -> str:
    raw_item = item.raw_item
    name = getattr(raw_item, "name", None)
    if name:
        return name

    function = getattr(raw_item, "function", None)
    return getattr(function, "name", "unknown")


def _print_run_items(items: list[Any]) -> None:
    for item in items:
        if isinstance(item, ToolCallItem):
            print(f"\n[MCP call] {_tool_call_name(item)}")
        elif isinstance(item, ToolCallOutputItem):
            print(f"[MCP result] {item.output}")


def _print_trace_summary(trace_id: str | None) -> None:
    if not trace_id:
        print("\nMLflow trace: not available")
        return

    mlflow.flush_trace_async_logging()
    try:
        trace = mlflow.get_trace(trace_id=trace_id)
    except Exception as exc:
        print(f"\nMLflow trace: {trace_id} (summary unavailable: {exc})")
        return

    print(f"\nMLflow trace: {trace.info.trace_id}")
    if trace.info.execution_duration is not None:
        print(f"Latency: {trace.info.execution_duration / 1000:.3f} s")

    if usage := trace.info.token_usage:
        print(
            "Tokens: "
            f"input={usage['input_tokens']}, "
            f"output={usage['output_tokens']}, "
            f"total={usage['total_tokens']}"
        )
    else:
        print("Tokens: not available")

    if cost := trace.info.cost:
        print(
            "Estimated model cost: "
            f"input=${cost['input_cost']:.6f}, "
            f"output=${cost['output_cost']:.6f}, "
            f"total=${cost['total_cost']:.6f}"
        )
    else:
        print("Estimated model cost: not available")

    called_tools = [span.name for span in trace.search_spans(span_type=SpanType.TOOL)]
    print(f"Tools called: {', '.join(called_tools) if called_tools else 'none'}")


async def run_agent(
    prompt: str,
    model: str,
    profile: str | None,
    mcp_url: str,
    allowed_tool_names: set[str],
    require_tool: bool,
    max_turns: int,
    tracing_enabled: bool,
    experiment_id: str | None,
    experiment_name: str | None,
) -> tuple[str, str | None]:
    workspace_client = _workspace_client(profile)
    current_user = workspace_client.current_user.me()
    if tracing_enabled:
        _configure_mlflow(profile, current_user.user_name, experiment_id, experiment_name)
    else:
        set_tracing_disabled(True)

    async with _databricks_openai_client(workspace_client) as model_client:
        agent_model = OpenAIChatCompletionsModel(model=model, openai_client=model_client)
        async with MCPServerStreamableHttp(
            name="local-genie-mcp",
            params={"url": mcp_url, "timeout": 60},
            cache_tools_list=True,
            client_session_timeout_seconds=60,
            tool_filter=_tool_filter(allowed_tool_names),
        ) as mcp_server:
            available_tool_names = {tool.name for tool in await mcp_server.list_tools()}
            if "*" not in allowed_tool_names:
                missing_tools = allowed_tool_names - available_tool_names
                if missing_tools:
                    raise ValueError(f"MCP tools not found: {', '.join(sorted(missing_tools))}")

            agent = Agent(
                name="Local Databricks MCP agent",
                instructions=AGENT_INSTRUCTIONS,
                model=agent_model,
                mcp_servers=[mcp_server],
                model_settings=ModelSettings(tool_choice="required" if require_tool else "auto"),
            )

            print(f"Model: {model}")
            print(f"MCP: {mcp_url}")
            print(
                "Tools: all"
                if "*" in allowed_tool_names
                else f"Tools: {', '.join(sorted(available_tool_names))}"
            )

            result = await Runner.run(agent, prompt, max_turns=max_turns)
            _print_run_items(result.new_items)
            trace_id = mlflow.get_last_active_trace_id() if tracing_enabled else None
            return result.final_output, trace_id


async def _main() -> None:
    args = _parse_args()
    prompt = " ".join(args.prompt).strip() or DEFAULT_PROMPT
    allowed_tools = {name.strip() for name in args.tools.split(",") if name.strip()}
    if not allowed_tools:
        raise ValueError("At least one tool must be provided with --tools")

    answer, trace_id = await run_agent(
        prompt=prompt,
        model=args.model,
        profile=args.profile,
        mcp_url=args.mcp_url,
        allowed_tool_names=allowed_tools,
        require_tool=args.require_tool,
        max_turns=args.max_turns,
        tracing_enabled=not args.no_tracing,
        experiment_id=args.experiment_id,
        experiment_name=args.experiment_name,
    )
    print(f"\nAgent: {answer}")
    if not args.no_tracing:
        _print_trace_summary(trace_id)


if __name__ == "__main__":
    asyncio.run(_main())
