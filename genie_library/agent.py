import asyncio
import logging
import json
import os
from urllib.parse import urlparse
from contextlib import AsyncExitStack
from datetime import datetime
from typing import Any, AsyncGenerator

import mlflow
from agents import Agent, Runner, function_tool, set_default_openai_api, set_default_openai_client
from agents.tracing import set_trace_processors
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.tags import TagAssignment
from databricks_openai import AsyncDatabricksOpenAI
from databricks_openai.agents import AsyncDatabricksSession, McpServer
from fastapi import HTTPException
from mlflow.genai.agent_server import get_request_headers, invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

from agent_server.utils import (
    deduplicate_input,
    get_databricks_host_from_env,
    get_lakebase_access_error_message,
    get_session_id,
    lakebase_config,
    process_agent_stream_events,
)
from genie_library.enums import Environment
from genie_library.genie_dashboards import GenieDashboards
from genie_library.genie_library import GenieSpaceController


# NOTE: this will work for all databricks models OTHER than GPT-OSS, which uses a slightly different API
set_default_openai_client(AsyncDatabricksOpenAI())
set_default_openai_api("chat_completions")
set_trace_processors([])  # only use mlflow for trace processing
mlflow.openai.autolog()  # type: ignore
logging.getLogger("mlflow.utils.autologging_utils").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

MAX_TOOL_ITEMS = 100
MAX_TAG_BENCHMARK_SPACES = 20
RUN_BENCHMARK_CONFIRMATION = "CONFIRM RUN BENCHMARK"
RUN_BENCHMARK_BY_TAG_CONFIRMATION = "CONFIRM RUN BENCHMARK BY TAG"
CREATE_TAG_CONFIRMATION = "CONFIRM CREATE GENIE TAG"
UPDATE_TAG_CONFIRMATION = "CONFIRM UPDATE GENIE TAG"
DELETE_TAG_CONFIRMATION = "CONFIRM DELETE GENIE TAG"
RUN_SERIALIZATION_JOB_CONFIRMATION = "CONFIRM RUN GENIE SERIALIZATION JOB"
GRANT_SPACE_PERMISSIONS_CONFIRMATION = "CONFIRM GRANT GENIE SPACE PERMISSIONS"
CREATE_GENIE_SPACE_FROM_DASHBOARD_CONFIRMATION = "CONFIRM CREATE GENIE SPACE FROM DASHBOARD"
GENIE_SERIALIZATION_JOB_ID_ENV = "GENIE_SERIALIZATION_JOB_ID"
DEFAULT_GENIE_SERIALIZATION_JOB_ID = 405956489806901
GENIE_RESTORE_POINTS_JOB_ID_ENV = "GENIE_RESTORE_POINTS_JOB_ID"
GENIE_RESTORE_JOB_ID_ENV = "GENIE_RESTORE_JOB_ID"
DATABRICKS_SQL_WAREHOUSE_HTTP_PATH_ENV = "DATABRICKS_SQL_WAREHOUSE_HTTP_PATH"
UC_QUERY_DEFAULT_LIMIT_ENV = "UC_QUERY_DEFAULT_LIMIT"
GENIE_SPACE_WAREHOUSE_ID_ENV = "GENIE_SPACE_WAREHOUSE_ID"
DEFAULT_GENIE_SPACE_WAREHOUSE_ID = "38cb31e24512fd55"
TERMINAL_JOB_LIFE_CYCLE_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
RESTORE_SNAPSHOT_DATE_FORMAT = "%Y-%m-%d"


def _json_response(payload: dict[str, Any]) -> str:
    return json.dumps(payload, default=str)


def _serialize_databricks_object(obj: Any) -> dict[str, Any]:
    if hasattr(obj, "as_dict"):
        return obj.as_dict()
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if isinstance(obj, dict):
        return obj
    return {"value": str(obj)}


def _limit_items(items: list[Any], limit: int) -> tuple[list[Any], bool]:
    safe_limit = max(1, min(limit, MAX_TOOL_ITEMS))
    return items[:safe_limit], len(items) > safe_limit


def _confirmation_required_payload(
    required_confirmation: str,
    action: str,
    space_id: str | None = None,
    **extra: Any,
) -> str:
    payload = {
        "auth_mode": "service_principal",
        "action": action,
        "error": "confirmation_required",
        "required_confirmation": required_confirmation,
    }
    if space_id:
        payload["space_id"] = space_id
    payload.update(extra)
    return _json_response(payload)


def _space_summary(space: Any) -> dict[str, Any]:
    return {
        "space_id": space.space_id,
        "title": space.title,
        "description": space.description,
    }


def _get_genie_controller(
    space_id: str, include_serialized_space: bool = False
) -> GenieSpaceController:
    return GenieSpaceController.from_id(
        space_id=space_id,
        include_serialized_space=include_serialized_space,
        environment=Environment.databricks_runtime,
    )


def _get_databricks_sql_hostname(client: WorkspaceClient) -> str:
    host = client.config.host or os.getenv("DATABRICKS_HOST", "")
    if not host:
        raise ValueError("Databricks host is not configured")
    parsed = urlparse(host)
    return parsed.netloc or parsed.path


def _get_databricks_access_token(client: WorkspaceClient) -> str:
    headers = client.config.authenticate()
    auth_header = headers.get("Authorization") or headers.get("authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise ValueError("Could not create Databricks access token for SQL connector")
    return auth_header.removeprefix("Bearer ")


def _get_user_access_token() -> str:
    token = get_request_headers().get("x-forwarded-access-token")
    if not token:
        raise ValueError(
            "User authorization token is not available. Enable the Databricks App user authorization "
            "sql scope and call this tool from an authenticated app request."
        )
    return token


def _get_configured_uc_query() -> str:
    return """
        SELECT space_id, space_title, num_correct, num_done, accuracy, execution_timestamp
        FROM lf_udm_stg.silver.genie_benchmarks_history
        WHERE space_id = :space_id
        ORDER BY execution_timestamp DESC
        LIMIT :limit
    """


def _get_uc_query_default_limit() -> int:
    raw_limit = os.getenv(UC_QUERY_DEFAULT_LIMIT_ENV, "50")
    try:
        return max(1, min(int(raw_limit), MAX_TOOL_ITEMS))
    except ValueError:
        return 50


def _execute_configured_uc_query(parameters: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    http_path = os.getenv(DATABRICKS_SQL_WAREHOUSE_HTTP_PATH_ENV, "").strip()
    if not http_path:
        raise ValueError(
            f"SQL Warehouse HTTP path is not configured. Set {DATABRICKS_SQL_WAREHOUSE_HTTP_PATH_ENV}."
        )

    safe_limit = max(1, min(limit, MAX_TOOL_ITEMS))
    query = _get_configured_uc_query()
    query_parameters = {**parameters, "limit": safe_limit}
    if not query_parameters.get("space_id"):
        raise ValueError("space_id is required in query_parameters_json")

    client = WorkspaceClient()
    with sql.connect(
        server_hostname=_get_databricks_sql_hostname(client),
        http_path=http_path,
        access_token=_get_user_access_token(),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, query_parameters)
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description or []]

    return [dict(zip(columns, row)) for row in rows]


def _get_genie_serialization_job_id() -> int:
    raw_job_id = os.getenv(GENIE_SERIALIZATION_JOB_ID_ENV)
    if not raw_job_id:
        return DEFAULT_GENIE_SERIALIZATION_JOB_ID
    return int(raw_job_id)


def _get_configured_job_id(env_name: str, job_description: str) -> int:
    raw_job_id = os.getenv(env_name)
    if not raw_job_id or raw_job_id.strip() in {"", "0"}:
        raise ValueError(f"{job_description} is not configured. Set {env_name} to the Databricks Job ID.")
    return int(raw_job_id)


def _job_run_summary(run: Any) -> dict[str, Any]:
    run_dict = _serialize_databricks_object(run)
    tasks = run_dict.get("tasks") or []
    return {
        "run_id": run_dict.get("run_id"),
        "job_id": run_dict.get("job_id"),
        "run_name": run_dict.get("run_name"),
        "run_page_url": run_dict.get("run_page_url"),
        "state": run_dict.get("state"),
        "start_time": run_dict.get("start_time"),
        "end_time": run_dict.get("end_time"),
        "execution_duration": run_dict.get("execution_duration"),
        "tasks": [
            {
                "task_key": task.get("task_key"),
                "run_id": task.get("run_id"),
                "state": task.get("state"),
                "run_page_url": task.get("run_page_url"),
            }
            for task in tasks
        ],
    }


def _job_life_cycle_state(run_summary: dict[str, Any]) -> str | None:
    state = run_summary.get("state") or {}
    life_cycle_state = state.get("life_cycle_state")
    if life_cycle_state is None:
        return None
    return str(life_cycle_state)


def _job_has_terminal_state(run_summary: dict[str, Any]) -> bool:
    return _job_life_cycle_state(run_summary) in TERMINAL_JOB_LIFE_CYCLE_STATES


def _parse_job_output_result(raw_result: str) -> Any:
    try:
        return json.loads(raw_result)
    except json.JSONDecodeError:
        return {"raw_result": raw_result}


def _job_run_output_payload(
    client: WorkspaceClient,
    run_id: int,
    run_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidate_run_ids = []
    if run_summary:
        for task in run_summary.get("tasks") or []:
            task_run_id = task.get("run_id")
            if task_run_id:
                candidate_run_ids.append(int(task_run_id))
    candidate_run_ids.append(run_id)

    last_error = None
    for candidate_run_id in candidate_run_ids:
        try:
            output = client.jobs.get_run_output(run_id=candidate_run_id)
            output_dict = _serialize_databricks_object(output)
            notebook_output = output_dict.get("notebook_output") or {}
            result = notebook_output.get("result")
            return {
                "output_run_id": candidate_run_id,
                "result": _parse_job_output_result(result) if result else None,
                "raw_output": output_dict,
            }
        except Exception as exc:
            last_error = exc

    return {"error": str(last_error) if last_error else "No run output available"}


async def _wait_for_job_terminal_state(
    client: WorkspaceClient,
    run_id: int,
    timeout_minutes: int,
    poll_interval_seconds: int,
) -> tuple[dict[str, Any], bool]:
    safe_timeout_minutes = max(1, min(timeout_minutes, 120))
    safe_poll_interval_seconds = max(5, min(poll_interval_seconds, 120))
    deadline = asyncio.get_running_loop().time() + safe_timeout_minutes * 60

    while True:
        run = await asyncio.to_thread(client.jobs.get_run, run_id=run_id)
        run_summary = _job_run_summary(run)
        if _job_has_terminal_state(run_summary):
            return run_summary, False
        if asyncio.get_running_loop().time() >= deadline:
            return run_summary, True
        await asyncio.sleep(safe_poll_interval_seconds)


def _validate_restore_inputs(space_id: str, snapshot_date: str) -> tuple[str, str]:
    safe_space_id = space_id.strip()
    safe_snapshot_date = snapshot_date.strip()
    if not safe_space_id:
        raise ValueError("space_id is required")
    try:
        datetime.strptime(safe_snapshot_date, RESTORE_SNAPSHOT_DATE_FORMAT)
    except ValueError as exc:
        raise ValueError("snapshot_date must use YYYY-MM-DD format") from exc
    return safe_space_id, safe_snapshot_date


def _restore_confirmation(space_id: str, snapshot_date: str) -> str:
    return f"CONFIRM RESTORE GENIE SPACE {space_id} {snapshot_date}"


def _genie_serialization_job_parameters(
    tag_key: str | None,
    space_id: str | None,
) -> dict[str, str] | None:
    safe_tag_key = tag_key.strip() if tag_key else ""
    safe_space_id = space_id.strip() if space_id else ""
    if safe_tag_key and safe_space_id:
        raise ValueError("Provide either tag_key or space_id, not both")
    if safe_space_id:
        return {"action": "by_id", "space_id": safe_space_id}
    if safe_tag_key:
        return {"action": "by_tag", "tag_key": safe_tag_key}
    return None


@function_tool
def list_available_genie_spaces() -> str:
    """List the Databricks Genie Spaces available to the app service principal."""
    try:
        spaces = GenieSpaceController.list_genie_spaces(environment=Environment.databricks_runtime) or []
        spaces = [space for space in spaces if space.space_id != "01f16bc8b373197c903d11ee84f092ab"] # this GS is not useful
        auth_mode = "service_principal"

        return _json_response(
            {
                "auth_mode": auth_mode,
                "count": len(spaces),
                "spaces": [_space_summary(space) for space in spaces],
            }
        )
    except Exception as exc:
        logger.exception("Failed to list Genie Spaces with the app service principal")
        return _json_response({"auth_mode": "service_principal", "error": str(exc), "spaces": []})


@function_tool
def get_genie_space_details(space_id: str) -> str:
    """Get details for a Databricks Genie Space by ID using the app service principal."""
    try:
        controller = _get_genie_controller(space_id=space_id, include_serialized_space=False)
        space = _serialize_databricks_object(controller.space)
        space.pop("serialized_space", None)
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space": space,
            }
        )
    except Exception as exc:
        logger.exception("Failed to get Genie Space details")
        return _json_response({"auth_mode": "service_principal", "space_id": space_id, "error": str(exc)})


@function_tool
def list_genie_space_tags(space_id: str) -> str:
    """List workspace tags assigned to a Databricks Genie Space."""
    try:
        controller = _get_genie_controller(space_id=space_id, include_serialized_space=False)
        tags = controller.list_tags()
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space": _space_summary(controller.space),
                "count": len(tags),
                "tags": [_serialize_databricks_object(tag) for tag in tags],
            }
        )
    except Exception as exc:
        logger.exception("Failed to list Genie Space tags")
        return _json_response({"auth_mode": "service_principal", "space_id": space_id, "error": str(exc)})


@function_tool
def find_genie_spaces_by_tag(tag_key: str, tag_value: str | None = None) -> str:
    """Find Databricks Genie Spaces by workspace tag key and optional tag value."""
    try:
        spaces = GenieSpaceController.get_genie_spaces_by_tag(
            tag_key=tag_key,
            tag_value=tag_value,
            include_serialized_space=False,
            enviroment=Environment.databricks_runtime,
        )
        return _json_response(
            {
                "auth_mode": "service_principal",
                "tag_key": tag_key,
                "tag_value": tag_value,
                "count": len(spaces),
                "spaces": [_space_summary(space) for space in spaces],
            }
        )
    except Exception as exc:
        logger.exception("Failed to find Genie Spaces by tag")
        return _json_response(
            {"auth_mode": "service_principal", "tag_key": tag_key, "tag_value": tag_value, "error": str(exc)}
        )


@function_tool
def list_available_dashboards(limit: int = 50) -> str:
    """List Lakeview dashboards accessible to the app service principal."""
    try:
        controller = GenieDashboards(environment=Environment.databricks_runtime)
        dashboards = controller.list_dashboards()
        limited, truncated = _limit_items(dashboards, limit)
        return _json_response(
            {
                "auth_mode": "service_principal",
                "count": len(dashboards),
                "returned_count": len(limited),
                "truncated": truncated,
                "dashboards": [
                    {
                        "dashboard_id": dashboard.dashboard_id,
                        "display_name": dashboard.display_name,
                        "path": dashboard.path,
                        "parent_path": dashboard.parent_path,
                        "warehouse_id": dashboard.warehouse_id,
                        "lifecycle_state": str(dashboard.lifecycle_state) if dashboard.lifecycle_state else None,
                        "create_time": dashboard.create_time,
                        "update_time": dashboard.update_time,
                    }
                    for dashboard in limited
                ],
            }
        )
    except Exception as exc:
        logger.exception("Failed to list Lakeview dashboards")
        return _json_response({"auth_mode": "service_principal", "error": str(exc), "dashboards": []})


@function_tool
def list_genie_space_conversations(
    space_id: str, include_all: bool = True, limit: int = 20
) -> str:
    """List conversations for a Databricks Genie Space, limited to avoid oversized responses."""
    try:
        controller = _get_genie_controller(space_id=space_id, include_serialized_space=False)
        conversations = controller.list_conversations(include_all=include_all) or []
        limited, truncated = _limit_items(conversations, limit)
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space": _space_summary(controller.space),
                "include_all": include_all,
                "count": len(conversations),
                "returned_count": len(limited),
                "truncated": truncated,
                "conversations": [_serialize_databricks_object(conversation) for conversation in limited],
            }
        )
    except Exception as exc:
        logger.exception("Failed to list Genie Space conversations")
        return _json_response({"auth_mode": "service_principal", "space_id": space_id, "error": str(exc)})


@function_tool
def list_genie_conversation_messages(
    space_id: str, conversation_id: str, limit: int | None = 50
) -> str:
    """List messages for a Databricks Genie conversation, limited to avoid oversized responses.
    Set limit to `None` to list all messages.
    """
    try:
        controller = _get_genie_controller(space_id=space_id, include_serialized_space=False)
        messages = controller.list_conversation_messages(conversation_id=conversation_id) or []
        if limit:
            limited, truncated = _limit_items(messages, limit)
        else:
            limited, truncated = messages, False
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space": _space_summary(controller.space),
                "conversation_id": conversation_id,
                "count": len(messages),
                "returned_count": len(limited),
                "truncated": truncated,
                "messages": [_serialize_databricks_object(message) for message in limited],
            }
        )
    except Exception as exc:
        logger.exception("Failed to list Genie conversation messages")
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space_id": space_id,
                "conversation_id": conversation_id,
                "error": str(exc),
            }
        )


@function_tool
def query_unity_catalog_table(query_parameters_json: str = "{}", limit: int | None = None) -> str:
    """Read only Genie Space benchmark result history from Unity Catalog.

    This tool is only for the table that stores Genie Space benchmark results. It must not be used
    to read any other Unity Catalog data. Pass query_parameters_json with space_id and optionally
    pass limit to control how many benchmark result rows are returned.
    """
    try:
        raw_parameters = json.loads(query_parameters_json or "{}")
        if not isinstance(raw_parameters, dict):
            raise ValueError("query_parameters_json must be a JSON object")
        safe_limit = limit if limit is not None else _get_uc_query_default_limit()
        rows = _execute_configured_uc_query(parameters=raw_parameters, limit=safe_limit)
        return _json_response(
            {
                "auth_mode": "on_behalf_of_user",
                "query_parameters": raw_parameters,
                "limit": max(1, min(safe_limit, MAX_TOOL_ITEMS)),
                "count": len(rows),
                "rows": rows,
            }
        )
    except Exception as exc:
        logger.exception("Failed to query Unity Catalog table")
        return _json_response(
            {
                "auth_mode": "on_behalf_of_user",
                "query_parameters_json": query_parameters_json,
                "error": str(exc),
            }
        )


@function_tool
def get_user_name_from_id(user_id: str) -> str:
    """Return user name from a user id"""
    try:
        return GenieSpaceController.get_user_name(user_id=user_id, user_names=GenieSpaceController.get_user_names(environment=Environment.databricks_runtime))

    except Exception as exc:
        return _json_response(
            {
                "auth_mode": "service_principal",
                "user_id": user_id,
                "error": str(exc),
            }
        )

@function_tool
def list_genie_space_permissions(space_id: str) -> str:
    """List users, groups, service principals, and permission levels for a Genie Space."""
    try:
        controller = _get_genie_controller(space_id=space_id, include_serialized_space=False)
        permissions = GenieSpaceController.list_space_permissions(
            space_id=space_id,
            environment=Environment.databricks_runtime,
        )
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space": _space_summary(controller.space),
                "permissions": permissions,
            }
        )
    except Exception as exc:
        logger.exception("Failed to list Genie Space permissions")
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space_id": space_id,
                "error": str(exc),
            }
        )

@function_tool
def grant_space_permissions(space_id: str, user_name_list: list[str], permission_level: str, confirmation: str = "") -> str:
    """Grant a permission level to user names in a Genie Space. Permission level can be CAN_MANAGE, CAN_EDIT, CAN_READ."""
    if confirmation != GRANT_SPACE_PERMISSIONS_CONFIRMATION:
        return _confirmation_required_payload(
            required_confirmation=GRANT_SPACE_PERMISSIONS_CONFIRMATION,
            action="grant_space_permissions",
            space_id=space_id,
            user_name_list=user_name_list,
            permission_level=permission_level,
        )

    try:
        permission = GenieSpaceController.get_permission_level(permission_level=permission_level)
        GenieSpaceController.assign_users_to_space(space_id=space_id, user_list=user_name_list, permission_level=permission)
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space_id": space_id,
                "user_name_list": user_name_list,
                "permission_level": permission_level,
                "success": True
            }
        )

    except Exception as exc:
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space_id": space_id,
                "user_name_list": user_name_list,
                "permission_level": permission_level,
                "error": str(exc),
            }
        )


@function_tool
def create_genie_space_from_dashboard(
    dashboard_id: str,
    genie_space_title: str,
    genie_space_description: str | None = None,
    user_name_list: list[str] | None = None,
    warehouse_id: str | None = None,
    include_atlan_context: bool = False,
    confirmation: str = "",
) -> str:
    """Create a Genie Space from a Lakeview Dashboard. Requires explicit confirmation."""
    safe_dashboard_id = dashboard_id.strip()
    safe_title = genie_space_title.strip()
    safe_warehouse_id = (
        warehouse_id or os.getenv(GENIE_SPACE_WAREHOUSE_ID_ENV) or DEFAULT_GENIE_SPACE_WAREHOUSE_ID
    ).strip()
    users = user_name_list or []

    if confirmation != CREATE_GENIE_SPACE_FROM_DASHBOARD_CONFIRMATION:
        return _confirmation_required_payload(
            required_confirmation=CREATE_GENIE_SPACE_FROM_DASHBOARD_CONFIRMATION,
            action="create_genie_space_from_dashboard",
            dashboard_id=safe_dashboard_id,
            genie_space_title=safe_title,
            genie_space_description=genie_space_description,
            user_name_list=users,
            warehouse_id=safe_warehouse_id,
            include_atlan_context=include_atlan_context,
        )

    try:
        if not safe_dashboard_id:
            raise ValueError("dashboard_id is required")
        if not safe_title:
            raise ValueError("genie_space_title is required")
        if not safe_warehouse_id:
            raise ValueError("warehouse_id is required")

        controller = GenieDashboards(environment=Environment.databricks_runtime)
        dashboard = controller.get_dashboard_by_id(dashboard_id=safe_dashboard_id)
        space = controller.create_genie_space_from_dashboard(
            dashboard_id=safe_dashboard_id,
            genie_space_title=safe_title,
            genie_space_description=genie_space_description,
            user_list=users,
            warehouse_id=safe_warehouse_id,
            write_debug_files=False,
            include_atlan_context=include_atlan_context,
        )
        return _json_response(
            {
                "auth_mode": "service_principal",
                "dashboard": _serialize_databricks_object(dashboard),
                "created_space": _serialize_databricks_object(space),
                "user_name_list": users,
                "warehouse_id": safe_warehouse_id,
                "include_atlan_context": include_atlan_context,
                "success": True,
            }
        )
    except Exception as exc:
        logger.exception("Failed to create Genie Space from dashboard")
        return _json_response(
            {
                "auth_mode": "service_principal",
                "dashboard_id": safe_dashboard_id,
                "genie_space_title": safe_title,
                "error": str(exc),
            }
        )


@function_tool
def get_genie_history_metrics(space_id: str) -> str:
    """Get usage and feedback history metrics for a Databricks Genie Space."""
    try:
        controller = _get_genie_controller(space_id=space_id, include_serialized_space=False)
        metrics = controller.get_history_metrics()
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space": _space_summary(controller.space),
                "metrics": metrics,
            }
        )
    except Exception as exc:
        logger.exception("Failed to get Genie history metrics")
        return _json_response({"auth_mode": "service_principal", "space_id": space_id, "error": str(exc)})


@function_tool
def run_genie_benchmark(space_id: str, confirmation: str) -> str:
    """Start a Genie benchmark run. Call without confirmation first to get the required confirmation phrase."""
    if confirmation != RUN_BENCHMARK_CONFIRMATION:
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space_id": space_id,
                "error": "confirmation_required",
                "required_confirmation": RUN_BENCHMARK_CONFIRMATION,
            }
        )

    try:
        controller = _get_genie_controller(space_id=space_id, include_serialized_space=False)
        run = controller.run_benchmark()
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space": _space_summary(controller.space),
                "benchmark_run": run,
            }
        )
    except Exception as exc:
        logger.exception("Failed to start Genie benchmark")
        return _json_response({"auth_mode": "service_principal", "space_id": space_id, "error": str(exc)})


@function_tool
def get_genie_benchmark_run(space_id: str, run_id: str) -> str:
    """Get the status and result fields for a Genie benchmark run."""
    try:
        controller = _get_genie_controller(space_id=space_id, include_serialized_space=False)
        run = controller.get_benchmark_run(run_id=run_id)
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space": _space_summary(controller.space),
                "benchmark_run": run,
            }
        )
    except Exception as exc:
        logger.exception("Failed to get Genie benchmark run")
        return _json_response(
            {"auth_mode": "service_principal", "space_id": space_id, "run_id": run_id, "error": str(exc)}
        )


@function_tool
def start_genie_serialization_job(
    tag_key: str | None = None,
    space_id: str | None = None,
    confirmation: str = "",
) -> str:
    """Start the Databricks Job that serializes Genie Spaces by tag key or by space ID."""
    job_id = _get_genie_serialization_job_id()
    try:
        job_parameters = _genie_serialization_job_parameters(tag_key=tag_key, space_id=space_id)
    except ValueError as exc:
        return _json_response({"auth_mode": "service_principal", "job_id": job_id, "error": str(exc)})
    if confirmation != RUN_SERIALIZATION_JOB_CONFIRMATION:
        return _confirmation_required_payload(
            required_confirmation=RUN_SERIALIZATION_JOB_CONFIRMATION,
            action="start_genie_serialization_job",
            job_id=job_id,
            job_parameters=job_parameters,
            uses_job_default_serialization_parameters=job_parameters is None,
        )

    try:
        if job_parameters:
            waiter = WorkspaceClient().jobs.run_now(job_id=job_id, job_parameters=job_parameters)
        else:
            waiter = WorkspaceClient().jobs.run_now(job_id=job_id)
        return _json_response(
            {
                "auth_mode": "service_principal",
                "job_id": job_id,
                "job_parameters": job_parameters,
                "uses_job_default_serialization_parameters": job_parameters is None,
                "run_id": waiter.run_id,
                "message": "Genie serialization job started. Use get_genie_serialization_job_run to check status.",
            }
        )
    except Exception as exc:
        logger.exception("Failed to start Genie serialization job")
        return _json_response({"auth_mode": "service_principal", "job_id": job_id, "error": str(exc)})


@function_tool
def get_genie_serialization_job_run(run_id: int) -> str:
    """Get status for a Databricks Job run created by the Genie serialization job."""
    try:
        run = WorkspaceClient().jobs.get_run(run_id=run_id)
        return _json_response(
            {
                "auth_mode": "service_principal",
                "run": _job_run_summary(run),
            }
        )
    except Exception as exc:
        logger.exception("Failed to get Genie serialization job run")
        return _json_response({"auth_mode": "service_principal", "run_id": run_id, "error": str(exc)})


@function_tool
async def start_genie_serialization_job_and_wait(
    tag_key: str | None = None,
    space_id: str | None = None,
    confirmation: str = "",
    timeout_minutes: int = 30,
    poll_interval_seconds: int = 20,
) -> str:
    """Start the Genie serialization Databricks Job by tag key or space ID and wait until it reaches a terminal state."""
    job_id = _get_genie_serialization_job_id()
    try:
        job_parameters = _genie_serialization_job_parameters(tag_key=tag_key, space_id=space_id)
    except ValueError as exc:
        return _json_response({"auth_mode": "service_principal", "job_id": job_id, "error": str(exc)})
    safe_timeout_minutes = max(1, min(timeout_minutes, 120))
    safe_poll_interval_seconds = max(5, min(poll_interval_seconds, 120))

    if confirmation != RUN_SERIALIZATION_JOB_CONFIRMATION:
        return _confirmation_required_payload(
            required_confirmation=RUN_SERIALIZATION_JOB_CONFIRMATION,
            action="start_genie_serialization_job_and_wait",
            job_id=job_id,
            job_parameters=job_parameters,
            uses_job_default_serialization_parameters=job_parameters is None,
            timeout_minutes=safe_timeout_minutes,
            poll_interval_seconds=safe_poll_interval_seconds,
        )

    try:
        client = WorkspaceClient()
        if job_parameters:
            waiter = await asyncio.to_thread(client.jobs.run_now, job_id=job_id, job_parameters=job_parameters)
        else:
            waiter = await asyncio.to_thread(client.jobs.run_now, job_id=job_id)
        run_id = waiter.run_id
        deadline = asyncio.get_running_loop().time() + safe_timeout_minutes * 60
        last_run_summary: dict[str, Any] | None = None

        while True:
            run = await asyncio.to_thread(client.jobs.get_run, run_id=run_id)
            last_run_summary = _job_run_summary(run)
            if _job_has_terminal_state(last_run_summary):
                return _json_response(
                    {
                        "auth_mode": "service_principal",
                        "job_id": job_id,
                        "job_parameters": job_parameters,
                        "uses_job_default_serialization_parameters": job_parameters is None,
                        "run_id": run_id,
                        "completed": True,
                        "timed_out": False,
                        "run": last_run_summary,
                    }
                )

            if asyncio.get_running_loop().time() >= deadline:
                return _json_response(
                    {
                        "auth_mode": "service_principal",
                        "job_id": job_id,
                        "job_parameters": job_parameters,
                        "uses_job_default_serialization_parameters": job_parameters is None,
                        "run_id": run_id,
                        "completed": False,
                        "timed_out": True,
                        "timeout_minutes": safe_timeout_minutes,
                        "run": last_run_summary,
                    }
                )

            await asyncio.sleep(safe_poll_interval_seconds)
    except Exception as exc:
        logger.exception("Failed while running Genie serialization job and waiting")
        return _json_response({"auth_mode": "service_principal", "job_id": job_id, "error": str(exc)})


@function_tool
async def list_genie_space_restore_points(
    space_id: str,
    timeout_minutes: int = 10,
    poll_interval_seconds: int = 10,
) -> str:
    """List exact restore points available in GitHub for a Genie Space ID."""
    try:
        safe_space_id = space_id.strip()
        if not safe_space_id:
            raise ValueError("space_id is required")
        job_id = _get_configured_job_id(GENIE_RESTORE_POINTS_JOB_ID_ENV, "Genie restore points Job")
        client = WorkspaceClient()
        job_parameters = {"space_id": safe_space_id}
        waiter = await asyncio.to_thread(client.jobs.run_now, job_id=job_id, job_parameters=job_parameters)
        run_id = waiter.run_id
        run_summary, timed_out = await _wait_for_job_terminal_state(
            client=client,
            run_id=run_id,
            timeout_minutes=timeout_minutes,
            poll_interval_seconds=poll_interval_seconds,
        )
        output = None if timed_out else _job_run_output_payload(client, run_id=run_id, run_summary=run_summary)
        return _json_response(
            {
                "auth_mode": "service_principal",
                "job_id": job_id,
                "job_parameters": job_parameters,
                "run_id": run_id,
                "completed": not timed_out,
                "timed_out": timed_out,
                "run": run_summary,
                "output": output,
            }
        )
    except Exception as exc:
        logger.exception("Failed to list Genie restore points")
        return _json_response({"auth_mode": "service_principal", "space_id": space_id, "error": str(exc)})


@function_tool
def get_genie_restore_points_job_run(run_id: int) -> str:
    """Get status and output for a Genie restore-points listing Job run."""
    try:
        client = WorkspaceClient()
        run = client.jobs.get_run(run_id=run_id)
        run_summary = _job_run_summary(run)
        output = _job_run_output_payload(client, run_id=run_id, run_summary=run_summary) if _job_has_terminal_state(run_summary) else None
        return _json_response(
            {
                "auth_mode": "service_principal",
                "run_id": run_id,
                "run": run_summary,
                "output": output,
            }
        )
    except Exception as exc:
        logger.exception("Failed to get Genie restore-points Job run")
        return _json_response({"auth_mode": "service_principal", "run_id": run_id, "error": str(exc)})


@function_tool
def start_genie_space_restore_job(space_id: str, snapshot_date: str, confirmation: str = "") -> str:
    """Start the Genie Space restore Job for an exact snapshot date. Requires user confirmation."""
    try:
        safe_space_id, safe_snapshot_date = _validate_restore_inputs(space_id, snapshot_date)
        required_confirmation = _restore_confirmation(safe_space_id, safe_snapshot_date)
        job_id = _get_configured_job_id(GENIE_RESTORE_JOB_ID_ENV, "Genie restore Job")
        job_parameters = {"space_id": safe_space_id, "snapshot_date": safe_snapshot_date}
        if confirmation != required_confirmation:
            return _confirmation_required_payload(
                required_confirmation=required_confirmation,
                action="start_genie_space_restore_job",
                space_id=safe_space_id,
                snapshot_date=safe_snapshot_date,
                job_id=job_id,
                job_parameters=job_parameters,
            )

        waiter = WorkspaceClient().jobs.run_now(job_id=job_id, job_parameters=job_parameters)
        return _json_response(
            {
                "auth_mode": "service_principal",
                "job_id": job_id,
                "job_parameters": job_parameters,
                "run_id": waiter.run_id,
                "message": "Genie restore job started. Use get_genie_space_restore_job_run to check status.",
            }
        )
    except Exception as exc:
        logger.exception("Failed to start Genie Space restore Job")
        return _json_response(
            {"auth_mode": "service_principal", "space_id": space_id, "snapshot_date": snapshot_date, "error": str(exc)}
        )


@function_tool
def get_genie_space_restore_job_run(run_id: int) -> str:
    """Get status and output for a Genie Space restore Job run."""
    try:
        client = WorkspaceClient()
        run = client.jobs.get_run(run_id=run_id)
        run_summary = _job_run_summary(run)
        output = _job_run_output_payload(client, run_id=run_id, run_summary=run_summary) if _job_has_terminal_state(run_summary) else None
        return _json_response(
            {
                "auth_mode": "service_principal",
                "run_id": run_id,
                "run": run_summary,
                "output": output,
            }
        )
    except Exception as exc:
        logger.exception("Failed to get Genie Space restore Job run")
        return _json_response({"auth_mode": "service_principal", "run_id": run_id, "error": str(exc)})


@function_tool
async def start_genie_space_restore_job_and_wait(
    space_id: str,
    snapshot_date: str,
    confirmation: str = "",
    timeout_minutes: int = 30,
    poll_interval_seconds: int = 20,
) -> str:
    """Start the Genie Space restore Job for an exact snapshot date and wait until it reaches a terminal state."""
    try:
        safe_space_id, safe_snapshot_date = _validate_restore_inputs(space_id, snapshot_date)
        required_confirmation = _restore_confirmation(safe_space_id, safe_snapshot_date)
        job_id = _get_configured_job_id(GENIE_RESTORE_JOB_ID_ENV, "Genie restore Job")
        job_parameters = {"space_id": safe_space_id, "snapshot_date": safe_snapshot_date}
        safe_timeout_minutes = max(1, min(timeout_minutes, 120))
        safe_poll_interval_seconds = max(5, min(poll_interval_seconds, 120))

        if confirmation != required_confirmation:
            return _confirmation_required_payload(
                required_confirmation=required_confirmation,
                action="start_genie_space_restore_job_and_wait",
                space_id=safe_space_id,
                snapshot_date=safe_snapshot_date,
                job_id=job_id,
                job_parameters=job_parameters,
                timeout_minutes=safe_timeout_minutes,
                poll_interval_seconds=safe_poll_interval_seconds,
            )

        client = WorkspaceClient()
        waiter = await asyncio.to_thread(client.jobs.run_now, job_id=job_id, job_parameters=job_parameters)
        run_id = waiter.run_id
        run_summary, timed_out = await _wait_for_job_terminal_state(
            client=client,
            run_id=run_id,
            timeout_minutes=safe_timeout_minutes,
            poll_interval_seconds=safe_poll_interval_seconds,
        )
        output = None if timed_out else _job_run_output_payload(client, run_id=run_id, run_summary=run_summary)
        return _json_response(
            {
                "auth_mode": "service_principal",
                "job_id": job_id,
                "job_parameters": job_parameters,
                "run_id": run_id,
                "completed": not timed_out,
                "timed_out": timed_out,
                "run": run_summary,
                "output": output,
            }
        )
    except Exception as exc:
        logger.exception("Failed while running Genie Space restore Job and waiting")
        return _json_response(
            {"auth_mode": "service_principal", "space_id": space_id, "snapshot_date": snapshot_date, "error": str(exc)}
        )


@function_tool
def create_genie_space_tag(
    space_id: str,
    tag_key: str,
    tag_value: str | None = None,
    confirmation: str = "",
) -> str:
    """Create a workspace tag on a Genie Space. Call without confirmation first to get the required confirmation phrase."""
    if confirmation != CREATE_TAG_CONFIRMATION:
        return _confirmation_required_payload(
            required_confirmation=CREATE_TAG_CONFIRMATION,
            action="create_genie_space_tag",
            space_id=space_id,
            tag_key=tag_key,
            tag_value=tag_value,
        )

    try:
        controller = _get_genie_controller(space_id=space_id, include_serialized_space=False)
        tag = TagAssignment(
            entity_id=space_id,
            entity_type="geniespaces",
            tag_key=tag_key,
            tag_value=tag_value,
        )
        created_tag = controller.w.workspace_entity_tag_assignments.create_tag_assignment(tag_assignment=tag)
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space": _space_summary(controller.space),
                "created_tag": _serialize_databricks_object(created_tag),
            }
        )
    except Exception as exc:
        logger.exception("Failed to create Genie Space tag")
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space_id": space_id,
                "tag_key": tag_key,
                "tag_value": tag_value,
                "error": str(exc),
            }
        )


@function_tool
def update_genie_space_tag(
    space_id: str,
    tag_key: str,
    tag_value: str | None = None,
    confirmation: str = "",
) -> str:
    """Update a workspace tag value on a Genie Space. Call without confirmation first to get the required confirmation phrase."""
    if confirmation != UPDATE_TAG_CONFIRMATION:
        return _confirmation_required_payload(
            required_confirmation=UPDATE_TAG_CONFIRMATION,
            action="update_genie_space_tag",
            space_id=space_id,
            tag_key=tag_key,
            tag_value=tag_value,
        )

    try:
        controller = _get_genie_controller(space_id=space_id, include_serialized_space=False)
        tag = TagAssignment(
            entity_id=space_id,
            entity_type="geniespaces",
            tag_key=tag_key,
            tag_value=tag_value,
        )
        updated_tag = controller.w.workspace_entity_tag_assignments.update_tag_assignment(
            entity_type="geniespaces",
            entity_id=space_id,
            tag_key=tag_key,
            tag_assignment=tag,
            update_mask="tag_value",
        )
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space": _space_summary(controller.space),
                "updated_tag": _serialize_databricks_object(updated_tag),
            }
        )
    except Exception as exc:
        logger.exception("Failed to update Genie Space tag")
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space_id": space_id,
                "tag_key": tag_key,
                "tag_value": tag_value,
                "error": str(exc),
            }
        )


@function_tool
def delete_genie_space_tag(space_id: str, tag_key: str, confirmation: str = "") -> str:
    """Delete a workspace tag from a Genie Space. Call without confirmation first to get the required confirmation phrase."""
    if confirmation != DELETE_TAG_CONFIRMATION:
        return _confirmation_required_payload(
            required_confirmation=DELETE_TAG_CONFIRMATION,
            action="delete_genie_space_tag",
            space_id=space_id,
            tag_key=tag_key,
        )

    try:
        controller = _get_genie_controller(space_id=space_id, include_serialized_space=False)
        controller.w.workspace_entity_tag_assignments.delete_tag_assignment(
            entity_type="geniespaces",
            entity_id=space_id,
            tag_key=tag_key,
        )
        return _json_response(
            {
                "auth_mode": "service_principal",
                "space": _space_summary(controller.space),
                "deleted_tag_key": tag_key,
            }
        )
    except Exception as exc:
        logger.exception("Failed to delete Genie Space tag")
        return _json_response(
            {"auth_mode": "service_principal", "space_id": space_id, "tag_key": tag_key, "error": str(exc)}
        )

@function_tool
def run_genie_benchmarks_by_tag(
    tag_key: str,
    tag_value: str | None = None,
    max_spaces: int = 5,
    confirmation: str = "",
) -> str:
    """Start Genie benchmark runs for Spaces matching a tag. Call without confirmation first to get the required confirmation phrase."""
    safe_max_spaces = max(1, min(max_spaces, MAX_TAG_BENCHMARK_SPACES))
    try:
        spaces = GenieSpaceController.get_genie_spaces_by_tag(
            tag_key=tag_key,
            tag_value=tag_value,
            include_serialized_space=False,
            enviroment=Environment.databricks_runtime,
        )
        matched_spaces = [_space_summary(space) for space in spaces]
        if confirmation != RUN_BENCHMARK_BY_TAG_CONFIRMATION:
            return _confirmation_required_payload(
                required_confirmation=RUN_BENCHMARK_BY_TAG_CONFIRMATION,
                action="run_genie_benchmarks_by_tag",
                tag_key=tag_key,
                tag_value=tag_value,
                matched_count=len(matched_spaces),
                max_spaces=safe_max_spaces,
                spaces=matched_spaces[:safe_max_spaces],
                truncated=len(matched_spaces) > safe_max_spaces,
            )

        selected_spaces = spaces[:safe_max_spaces]
        benchmark_runs = []
        errors = []
        for space in selected_spaces:
            try:
                controller = _get_genie_controller(space_id=space.space_id, include_serialized_space=False)
                benchmark_runs.append(
                    {
                        "space": _space_summary(controller.space),
                        "benchmark_run": controller.run_benchmark(),
                    }
                )
            except Exception as exc:
                logger.exception("Failed to start tagged Genie benchmark")
                errors.append({"space": _space_summary(space), "error": str(exc)})

        return _json_response(
            {
                "auth_mode": "service_principal",
                "tag_key": tag_key,
                "tag_value": tag_value,
                "matched_count": len(spaces),
                "started_count": len(benchmark_runs),
                "max_spaces": safe_max_spaces,
                "truncated": len(spaces) > safe_max_spaces,
                "benchmark_runs": benchmark_runs,
                "errors": errors,
            }
        )
    except Exception as exc:
        logger.exception("Failed to start Genie benchmarks by tag")
        return _json_response(
            {"auth_mode": "service_principal", "tag_key": tag_key, "tag_value": tag_value, "error": str(exc)}
        )


async def init_mcp_server(workspace_client: WorkspaceClient):
    return McpServer(
        url=f"{get_databricks_host_from_env()}/api/2.0/mcp/functions/system/ai",
        name="system.ai uc function mcp server",
        workspace_client=workspace_client,
    )


async def connect_healthy_mcp_servers(
    stack: AsyncExitStack, servers: list[McpServer]
) -> tuple[list[McpServer], list[str]]:
    """Connect each MCP server and verify it can actually list its tools.

    The Agents SDK lists each server's tools lazily inside ``Runner.run``, so a server that
    connects but fails at list time (e.g. an unauthorized Genie space) would otherwise crash
    the whole request — including unrelated turns. We list tools here, per server: healthy
    servers are kept; any that fails to connect OR to list is dropped and its name returned,
    so the agent runs with whatever is available instead of erroring out.

    Returns (healthy_servers, unavailable_names).
    """
    healthy: list[McpServer] = []
    unavailable: list[str] = []
    for server in servers:
        name = getattr(server, "name", "MCP server")
        try:
            connected = await stack.enter_async_context(server)
            await connected.list_tools()  # forces the connectivity + authorization check now
            healthy.append(connected)
        except Exception:
            logger.warning("MCP server %r unavailable; continuing without it.", name, exc_info=True)
            unavailable.append(name)
    return healthy, unavailable


def create_agent(mcp_servers: list[McpServer] | None = None) -> Agent:
    return Agent(
        name="Agent",
        instructions=(
            "You are a helpful assistant for administering Databricks Genie Spaces. "
            "Use the Genie tools to list spaces, inspect details, tags, conversations, "
            "messages, usage metrics, permissions, benchmark runs, and run the Genie serialization job. "
            "Summarize tool results clearly and include Genie Space titles and IDs when relevant. "
            "Read-only tools can be used directly. Before creating, updating, or deleting tags, "
            "updating metadata, changing permissions, restoring config, starting benchmarks, or launching the "
            "serialization job, explain what will run "
            "and call the tool without confirmation to get the required confirmation string. "
            "Never fill in a confirmation string yourself; only pass it after the user types it. "
            "List current Genie Space permissions before recommending permission changes. "
            "Do not revoke permissions automatically; explain the ACL entries that would need to remain or change. "
            "For the serialization job, pass tag_key when the user wants to serialize spaces by tag, "
            "or pass space_id when the user wants to serialize one specific Genie Space. "
            "Never pass both tag_key and space_id. If neither is provided by the user, leave both unset "
            "so the Databricks Job uses its configured defaults. "
            "For Genie serialization and restore Jobs, prefer asynchronous execution by default. "
            "After the user provides the required confirmation, use start_genie_serialization_job "
            "or start_genie_space_restore_job, return the job_id and run_id, and explain that the Job "
            "is running asynchronously. Do not use start_genie_serialization_job_and_wait or "
            "start_genie_space_restore_job_and_wait unless the user explicitly asks to wait for completion, "
            "using phrases like 'wait until it finishes', 'run and wait', or 'tell me when it is done'. "
            "After starting an async Job, tell the user they can ask for status using the run_id; "
            "use get_genie_serialization_job_run or get_genie_space_restore_job_run to check progress. "
            "If a Job may take several minutes, prefer start_job plus status polling over blocking waits "
            "to avoid chat or proxy timeouts and duplicate job launches. "
            "Use query_unity_catalog_table only to read the Unity Catalog table that contains Genie Space "
            "benchmark result history. Never use it for any other Unity Catalog data or general data lookup. "
            "Do not invent or execute arbitrary SQL; the tool runs the fixed Genie benchmark results query "
            "and requires query_parameters_json with space_id plus an optional limit. This benchmark history "
            "query runs on behalf of the current app user, so it uses the user's SQL Warehouse and Unity "
            "Catalog permissions rather than the app service principal's permissions. "
            "For requests to create a Genie Space from a dashboard, first explain the dashboard_id, title, "
            "description, users, warehouse_id, and whether Atlan context will be included. Then call "
            "create_genie_space_from_dashboard without confirmation to obtain the exact required confirmation. "
            "Never provide the confirmation yourself. Only create the space after the user explicitly provides "
            "the required confirmation phrase. "
            "Use list_available_dashboards when the user asks which Lakeview dashboards the agent can access, "
            "or when they need help choosing the dashboard_id for creating a Genie Space from a dashboard. "
            "For Genie restore requests, first list available restore points for the space_id. "
            "Restore only exact snapshot dates; never choose a nearest date automatically. "
            "Before starting a restore, explain what will be restored and call the restore tool "
            "without confirmation to get the exact confirmation string. "
            "Provide clean and structured answers, using markdown sintax."
        ),
        model="databricks-gpt-5-4",
        tools=[
            list_available_genie_spaces,
            get_genie_space_details,
            list_genie_space_tags,
            find_genie_spaces_by_tag,
            list_available_dashboards,
            list_genie_space_conversations,
            list_genie_conversation_messages,
            query_unity_catalog_table,
            get_genie_history_metrics,
            run_genie_benchmark,
            get_genie_benchmark_run,
            start_genie_serialization_job,
            get_genie_serialization_job_run,
            start_genie_serialization_job_and_wait,
            list_genie_space_restore_points,
            get_genie_restore_points_job_run,
            start_genie_space_restore_job,
            get_genie_space_restore_job_run,
            start_genie_space_restore_job_and_wait,
            create_genie_space_from_dashboard,
            create_genie_space_tag,
            update_genie_space_tag,
            delete_genie_space_tag,
            run_genie_benchmarks_by_tag,
            get_user_name_from_id,
            list_genie_space_permissions,
            grant_space_permissions,
        ],
        mcp_servers=mcp_servers or [],  # type: ignore
    )


@invoke()
async def invoke_handler(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    try:
        # Create session for stateful, short-term conversation history with your Databricks Lakebase instance
        session_id = get_session_id(request)
        if session_id:
            mlflow.update_current_trace(metadata={"mlflow.trace.session": session_id})
        session = AsyncDatabricksSession(
            session_id=session_id,
            instance_name=lakebase_config.instance_name,
            autoscaling_endpoint=lakebase_config.autoscaling_endpoint,
            project=lakebase_config.autoscaling_project,
            branch=lakebase_config.autoscaling_branch,
            schema=lakebase_config.memory_schema,
            create_tables=False,  # Tables created at startup in start_server.py
        )

        # The agent runs inside an AsyncExitStack so any MCP servers stay open for the whole
        # request. To give the agent MCP tools, connect them with connect_healthy_mcp_servers,
        # which health-checks each server so one unavailable server can't crash the request
        # (the Agents SDK lists each server's tools lazily inside Runner.run):
        #   servers, unavailable = await connect_healthy_mcp_servers(
        #       stack, [await init_mcp_server(WorkspaceClient())])
        #   agent = create_agent(mcp_servers=servers)
        # WorkspaceClient() uses service principal credentials; use get_user_workspace_client()
        # for on-behalf-of user authentication.
        async with AsyncExitStack() as stack:
            agent = create_agent()
            messages = await deduplicate_input(request, session)
            result = await Runner.run(agent, messages, session=session)  # type: ignore
        return ResponsesAgentResponse(
            output=[item.to_input_item() for item in result.new_items],  # type: ignore
            custom_outputs={"session_id": session.session_id},
        )
    except Exception as e:
        error_msg = str(e).lower()
        if any(
            keyword in error_msg
            for keyword in ["lakebase", "pg_hba", "postgres", "database instance", "insufficient privilege"]
        ):
            logger.error("Lakebase access error: %s", e)
            raise HTTPException(
                status_code=503,
                detail=get_lakebase_access_error_message(lakebase_config.description),
            ) from e
        raise


@stream()
async def stream_handler(
    request: ResponsesAgentRequest,
) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    try:
        # Create session for stateful, short-term conversation history with your Databricks Lakebase instance
        session_id = get_session_id(request)
        if session_id:
            mlflow.update_current_trace(metadata={"mlflow.trace.session": session_id})
        session = AsyncDatabricksSession(
            session_id=session_id,
            instance_name=lakebase_config.instance_name,
            autoscaling_endpoint=lakebase_config.autoscaling_endpoint,
            project=lakebase_config.autoscaling_project,
            branch=lakebase_config.autoscaling_branch,
            schema=lakebase_config.memory_schema,
            create_tables=False,  # Tables created at startup in start_server.py
        )

        # The agent runs inside an AsyncExitStack so any MCP servers stay open for the whole
        # request. To give the agent MCP tools, connect them with connect_healthy_mcp_servers,
        # which health-checks each server so one unavailable server can't crash the request
        # (the Agents SDK lists each server's tools lazily inside Runner.run):
        #   servers, unavailable = await connect_healthy_mcp_servers(
        #       stack, [await init_mcp_server(WorkspaceClient())])
        #   agent = create_agent(mcp_servers=servers)
        # WorkspaceClient() uses service principal credentials; use get_user_workspace_client()
        # for on-behalf-of user authentication.
        async with AsyncExitStack() as stack:
            agent = create_agent()
            messages = await deduplicate_input(request, session)
            result = Runner.run_streamed(agent, input=messages, session=session)  # type: ignore

            async for event in process_agent_stream_events(result.stream_events()):
                yield event
    except Exception as e:
        error_msg = str(e).lower()
        if any(
            keyword in error_msg
            for keyword in ["lakebase", "pg_hba", "postgres", "database instance", "insufficient privilege"]
        ):
            logger.error("Lakebase access error: %s", e)
            raise HTTPException(
                status_code=503,
                detail=get_lakebase_access_error_message(lakebase_config.description),
            ) from e
        raise
