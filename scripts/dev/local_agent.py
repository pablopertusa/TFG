import argparse
import asyncio
import json
import os
from typing import Any

from databricks.sdk import WorkspaceClient
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from openai import AsyncOpenAI

DEFAULT_PROMPT = "Comprueba mediante la herramienta health que el servidor MCP local funciona."


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a local agent against a Databricks model and a local MCP server."
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


def _normalize_schema(value: Any) -> Any:
    if isinstance(value, list):
        return [_normalize_schema(item) for item in value]
    if not isinstance(value, dict):
        return value

    normalized = {key: _normalize_schema(item) for key, item in value.items() if key != "anyOf"}
    if "anyOf" not in value:
        return normalized

    choices = value["anyOf"]
    non_null_choices = [choice for choice in choices if choice.get("type") != "null"]
    if len(non_null_choices) != 1 or len(non_null_choices) == len(choices):
        raise ValueError(f"Unsupported MCP tool schema: {json.dumps(value)}")

    normalized.pop("default", None)
    return {**_normalize_schema(non_null_choices[0]), **normalized}


def _openai_tool(tool: Any) -> dict[str, Any]:
    schema = _normalize_schema(dict(tool.inputSchema or {}))
    schema.setdefault("type", "object")
    schema.setdefault("properties", {})
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description or "",
            "parameters": schema,
        },
    }


def _tool_result_text(result: Any) -> str:
    payload = result.model_dump(mode="json", by_alias=True, exclude_none=True)
    return json.dumps(payload, ensure_ascii=False)


async def run_agent(
    prompt: str,
    model: str,
    profile: str | None,
    mcp_url: str,
    allowed_tool_names: set[str],
    require_tool: bool,
    max_turns: int,
) -> str:
    workspace_client = _workspace_client(profile)
    workspace_client.current_user.me()

    async with (
        _databricks_openai_client(workspace_client) as model_client,
        streamablehttp_client(mcp_url) as (read_stream, write_stream, _),
        ClientSession(read_stream, write_stream) as mcp_session,
    ):
        await mcp_session.initialize()
        listed_tools = (await mcp_session.list_tools()).tools
        if "*" in allowed_tool_names:
            selected_tools = listed_tools
        else:
            selected_tools = [tool for tool in listed_tools if tool.name in allowed_tool_names]
            missing_tools = allowed_tool_names - {tool.name for tool in selected_tools}
            if missing_tools:
                raise ValueError(f"MCP tools not found: {', '.join(sorted(missing_tools))}")

        tool_specs = [_openai_tool(tool) for tool in selected_tools]
        selected_tool_names = {tool.name for tool in selected_tools}
        messages: list[dict[str, Any]] = [
            {
                "role": "system",
                "content": (
                    "You are a local agent with access to MCP tools. Use the tools when needed, "
                    "never invent their results, and answer in the user's language."
                ),
            },
            {"role": "user", "content": prompt},
        ]

        print(f"Model: {model}")
        print(f"MCP: {mcp_url}")
        print(f"Tools: {', '.join(tool.name for tool in selected_tools)}")

        for turn in range(max_turns):
            response = await model_client.chat.completions.create(
                model=model,
                messages=messages,
                tools=tool_specs,
                tool_choice="required" if require_tool and turn == 0 else "auto",
            )
            message = response.choices[0].message
            messages.append(message.model_dump(exclude_none=True))

            if not message.tool_calls:
                return message.content or ""

            for tool_call in message.tool_calls:
                tool_name = tool_call.function.name
                if tool_name not in selected_tool_names:
                    raise RuntimeError(f"Model requested a tool outside the allowlist: {tool_name}")

                arguments = json.loads(tool_call.function.arguments or "{}")
                print(f"\n[MCP call] {tool_name}({json.dumps(arguments, ensure_ascii=False)})")
                result = await mcp_session.call_tool(tool_name, arguments)
                result_text = _tool_result_text(result)
                print(f"[MCP result] {result_text}")
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": result_text,
                    }
                )

    raise RuntimeError(f"Agent did not finish after {max_turns} turns")


async def _main() -> None:
    args = _parse_args()
    prompt = " ".join(args.prompt).strip() or DEFAULT_PROMPT
    allowed_tools = {name.strip() for name in args.tools.split(",") if name.strip()}
    if not allowed_tools:
        raise ValueError("At least one tool must be provided with --tools")

    answer = await run_agent(
        prompt=prompt,
        model=args.model,
        profile=args.profile,
        mcp_url=args.mcp_url,
        allowed_tool_names=allowed_tools,
        require_tool=args.require_tool,
        max_turns=args.max_turns,
    )
    print(f"\nAgent: {answer}")


if __name__ == "__main__":
    asyncio.run(_main())
