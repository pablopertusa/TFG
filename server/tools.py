"""
Tools module for the MCP server.

This module defines all the tools (functions) that the MCP server exposes to clients.
Tools are the core functionality of an MCP server - they are callable functions that
AI assistants and other clients can invoke to perform specific actions.

Each tool should:
- Have a clear, descriptive name
- Include comprehensive docstrings (used by AI to understand when to call the tool)
- Return structured data (typically dict or list)
- Handle errors gracefully
"""

import datetime
import json
import os
import time
from typing import Any

from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
from databricks.sdk.service.sql import (
    Disposition,
    ExecuteStatementRequestOnWaitTimeout,
    Format,
    StatementParameterListItem,
)

from server import utils

MAX_TOOL_ITEMS = 100
MAX_CONVERSATIONS_PER_MESSAGES_REQUEST = 50
RUN_SERIALIZATION_JOB_CONFIRMATION = "CONFIRM RUN GENIE SERIALIZATION JOB"
GRANT_SPACE_PERMISSIONS_CONFIRMATION = "CONFIRM GRANT GENIE SPACE PERMISSIONS"
RESTORE_SNAPSHOT_DATE_FORMAT = "%Y-%m-%d"
TERMINAL_JOB_LIFE_CYCLE_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
GENIE_SERIALIZATION_JOB_ID_ENV = "GENIE_SERIALIZATION_JOB_ID"
GENIE_RESTORE_POINTS_JOB_ID_ENV = "GENIE_RESTORE_POINTS_JOB_ID"
GENIE_RESTORE_JOB_ID_ENV = "GENIE_RESTORE_JOB_ID"
AUDIT_SQL_WAREHOUSE_ID_ENV = "DATABRICKS_AUDIT_WAREHOUSE_ID"
GENIE_SPACE_WAREHOUSE_ID_ENV = "GENIE_SPACE_WAREHOUSE_ID"
ATLAN_API_KEY_ENV = "ATLAN_API_KEY"
ATLAN_BASE_URL_ENV = "ATLAN_BASE_URL"
DEFAULT_GENIE_USAGE_LOOKBACK_DAYS = 90
MAX_GENIE_USAGE_LOOKBACK_DAYS = 3660
ATLAN_GOLD_QUALIFIED_NAME_PREFIX = "default/databricks/1732657096/lf_udm_prod/gold/"
ATLAN_METRIC_VIEWS_QUALIFIED_NAME_PREFIX = (
    "default/databricks/1732657096/lf_udm_prod/gold_metric_views/"
)
GENIE_USAGE_METRICS_QUERY = """
WITH base AS (
  SELECT
    a.event_date,
    CAST(DATE_TRUNC('WEEK', a.event_date) AS DATE) AS week_start,
    CAST(DATE_TRUNC('MONTH', a.event_date) AS DATE) AS month_start,
    CAST(a.request_params.space_id AS STRING) AS space_id,
    COALESCE(a.user_identity.email, a.user_identity.subject_name) AS user_id,
    a.action_name,
    a.request_params.feedback_rating AS feedback_rating
  FROM system.access.audit a
  WHERE a.service_name = 'aibiGenie'
    AND a.event_date >= :start_date
    AND a.event_date <= :end_date
    AND CAST(a.request_params.space_id AS STRING) = :space_id
)

SELECT
  CASE
    WHEN GROUPING(event_date) = 0 THEN 'daily'
    WHEN GROUPING(week_start) = 0 THEN 'weekly'
    WHEN GROUPING(month_start) = 0 THEN 'monthly'
    ELSE 'total'
  END AS grain,
  COALESCE(event_date, week_start, month_start) AS period_start,
  COUNT(DISTINCT user_id) AS users,
  COUNT_IF(action_name = 'createConversationMessage') AS questions_made,
  COUNT(*) AS interactions,
  COUNT_IF(action_name = 'updateConversationMessageFeedback') AS feedback,
  COUNT_IF(
    action_name = 'updateConversationMessageFeedback'
    AND CAST(feedback_rating AS STRING) = 'THUMBS_UP'
  ) AS positive_feedback,
  COUNT_IF(
    action_name = 'updateConversationMessageFeedback'
    AND CAST(feedback_rating AS STRING) = 'THUMBS_DOWN'
  ) AS negative_feedback
FROM base
GROUP BY GROUPING SETS ((), (event_date), (week_start), (month_start))

ORDER BY
  CASE grain
    WHEN 'total' THEN 0
    WHEN 'daily' THEN 1
    WHEN 'weekly' THEN 2
    WHEN 'monthly' THEN 3
  END,
  period_start
"""


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


def _list_genie_conversation_messages(
    client: Any,
    space_id: str,
    conversation_id: str,
) -> list[Any]:
    messages = []
    response = client.genie.list_conversation_messages(
        space_id=space_id,
        conversation_id=conversation_id,
        page_size=50,
    )
    messages.extend(response.messages or [])
    while response.next_page_token:
        response = client.genie.list_conversation_messages(
            space_id=space_id,
            conversation_id=conversation_id,
            page_size=50,
            page_token=response.next_page_token,
        )
        messages.extend(response.messages or [])
    return messages


def _space_summary(space: Any) -> dict[str, Any]:
    return {
        "space_id": space.space_id,
        "title": space.title,
        "description": space.description,
    }


def _datetime_from_timestamp(timestamp_ms: int | None) -> datetime.datetime | None:
    if timestamp_ms is None:
        return None
    return datetime.datetime.fromtimestamp(timestamp_ms / 1000, tz=datetime.timezone.utc)


def _date_from_timestamp(timestamp_ms: int | None) -> str | None:
    timestamp = _datetime_from_timestamp(timestamp_ms)
    if timestamp is None:
        return None
    return timestamp.date().isoformat()


def _serialize_benchmark_run(run: Any) -> dict[str, Any]:
    run_dict = _serialize_databricks_object(run)
    run_dict["created_date"] = _date_from_timestamp(run_dict.get("created_timestamp"))
    return run_dict


def _confirmation_required_payload(
    required_confirmation: str,
    action: str,
    auth_mode: str = "on_behalf_of_user",
    **extra: Any,
) -> dict[str, Any]:
    payload = {
        "auth_mode": auth_mode,
        "action": action,
        "error": "confirmation_required",
        "required_confirmation": required_confirmation,
    }
    payload.update(extra)
    return payload


def _get_configured_job_id(env_name: str, job_description: str) -> int:
    raw_job_id = os.getenv(env_name)
    if not raw_job_id or raw_job_id.strip() in {"", "0"}:
        raise ValueError(f"{job_description} is not configured. Set {env_name}.")
    return int(raw_job_id)


def _permission_level(permission_level: str) -> PermissionLevel:
    if permission_level == "CAN_MANAGE":
        return PermissionLevel.CAN_MANAGE
    if permission_level == "CAN_EDIT":
        return PermissionLevel.CAN_EDIT
    if permission_level == "CAN_READ":
        return PermissionLevel.CAN_READ
    raise ValueError("permission_level must be one of CAN_MANAGE, CAN_EDIT, CAN_READ")


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


def _job_has_terminal_state(run_summary: dict[str, Any]) -> bool:
    state = run_summary.get("state") or {}
    return state.get("life_cycle_state") in TERMINAL_JOB_LIFE_CYCLE_STATES


def _parse_job_output_result(raw_result: str) -> Any:
    try:
        return json.loads(raw_result)
    except json.JSONDecodeError:
        return {"raw_result": raw_result}


def _job_run_output_payload(
    client: Any,
    run_id: int,
    run_summary: dict[str, Any],
) -> dict[str, Any]:
    candidate_run_ids = [
        int(task["run_id"]) for task in run_summary.get("tasks") or [] if task.get("run_id")
    ]
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
        except Exception as e:
            last_error = e

    return {"error": str(last_error) if last_error else "No run output available"}


def _wait_for_job_terminal_state(
    client: Any,
    run_id: int,
    timeout_minutes: int,
    poll_interval_seconds: int,
) -> tuple[dict[str, Any], bool]:
    safe_timeout_minutes = max(1, min(timeout_minutes, 120))
    safe_poll_interval_seconds = max(5, min(poll_interval_seconds, 120))
    deadline = time.monotonic() + safe_timeout_minutes * 60

    while True:
        run = client.jobs.get_run(run_id=run_id)
        run_summary = _job_run_summary(run)
        if _job_has_terminal_state(run_summary):
            return run_summary, False
        if time.monotonic() >= deadline:
            return run_summary, True
        time.sleep(safe_poll_interval_seconds)


def _validate_restore_inputs(space_id: str, snapshot_date: str) -> tuple[str, str]:
    safe_space_id = space_id.strip()
    safe_snapshot_date = snapshot_date.strip()
    if not safe_space_id:
        raise ValueError("space_id is required")
    try:
        datetime.datetime.strptime(safe_snapshot_date, RESTORE_SNAPSHOT_DATE_FORMAT)
    except ValueError as exc:
        raise ValueError("snapshot_date must use YYYY-MM-DD format") from exc
    return safe_space_id, safe_snapshot_date


def _restore_confirmation(space_id: str, snapshot_date: str) -> str:
    return f"CONFIRM RESTORE GENIE SPACE {space_id} {snapshot_date}"


def _remove_html_tags(value: str) -> str:
    from bs4 import BeautifulSoup

    return BeautifulSoup(value, "html.parser").get_text()


def _parse_databricks_table_identifier(table_identifier: str) -> dict[str, str]:
    parts = [part.strip() for part in table_identifier.split(".")]
    if len(parts) != 3 or any(not part for part in parts):
        raise ValueError("table_identifier must use catalog.schema.table format")
    return {
        "catalog": parts[0],
        "schema": parts[1],
        "table": parts[2],
        "identifier": ".".join(parts),
    }


def _get_atlan_client():
    api_key = os.getenv(ATLAN_API_KEY_ENV)
    base_url = os.getenv(ATLAN_BASE_URL_ENV)
    if not api_key:
        raise ValueError(f"{ATLAN_API_KEY_ENV} is not configured")
    if not base_url:
        raise ValueError(f"{ATLAN_BASE_URL_ENV} is not configured")

    from pyatlan.client.atlan import AtlanClient

    return AtlanClient(base_url=base_url, api_key=api_key)


def _atlan_model_classes():
    from pyatlan.model.assets import AtlasGlossaryTerm, DatabricksMetricView, Readme, Table
    from pyatlan.model.fluent_search import CompoundQuery, FluentSearch

    return {
        "AtlasGlossaryTerm": AtlasGlossaryTerm,
        "CompoundQuery": CompoundQuery,
        "DatabricksMetricView": DatabricksMetricView,
        "FluentSearch": FluentSearch,
        "Readme": Readme,
        "Table": Table,
    }


def _atlan_asset_specs(parsed_identifier: dict[str, str]) -> list[dict[str, Any]]:
    models = _atlan_model_classes()
    table_name = parsed_identifier["table"]
    specs = [
        {
            "asset_type": models["Table"],
            "assigned_terms_attr": models["Table"].ASSIGNED_TERMS,
            "qualified_name": ATLAN_GOLD_QUALIFIED_NAME_PREFIX + table_name,
            "expected_schema": "gold",
        },
        {
            "asset_type": models["DatabricksMetricView"],
            "assigned_terms_attr": models["DatabricksMetricView"].ASSIGNED_TERMS,
            "qualified_name": ATLAN_METRIC_VIEWS_QUALIFIED_NAME_PREFIX + table_name,
            "expected_schema": "gold_metric_views",
        },
    ]
    schema = parsed_identifier["schema"]
    matching_schema_specs = [spec for spec in specs if spec["expected_schema"] == schema]
    return matching_schema_specs or specs


def _build_atlan_search_request(
    spec: dict[str, Any],
    qualified_name: str | None = None,
    asset_name: str | None = None,
):
    models = _atlan_model_classes()
    Table = models["Table"]
    CompoundQuery = models["CompoundQuery"]
    FluentSearch = models["FluentSearch"]

    if not qualified_name and not asset_name:
        raise ValueError("qualified_name or asset_name is required")

    name_attr = getattr(spec["asset_type"], "NAME", Table.NAME)
    qualified_name_attr = getattr(spec["asset_type"], "QUALIFIED_NAME", Table.QUALIFIED_NAME)
    request = (
        FluentSearch.select()
        .where(
            qualified_name_attr.eq(qualified_name)
            if qualified_name
            else name_attr.eq(asset_name)
        )
        .where(CompoundQuery.asset_type(spec["asset_type"]))
        .where(CompoundQuery.active_assets())
        .include_relationship_attributes(True)
        .include_on_results(spec["assigned_terms_attr"])
        .include_on_relations(models["AtlasGlossaryTerm"].NAME)
        .include_on_relations(models["AtlasGlossaryTerm"].DESCRIPTION)
        .include_on_relations(models["AtlasGlossaryTerm"].README)
    ).to_request()
    return request


def _asset_qualified_name(asset: Any) -> str | None:
    return getattr(asset, "qualified_name", None) or getattr(asset, "qualifiedName", None)


def _asset_summary(asset: Any, match_type: str) -> dict[str, Any]:
    return {
        "guid": getattr(asset, "guid", None),
        "name": getattr(asset, "name", None),
        "qualified_name": _asset_qualified_name(asset),
        "asset_type": getattr(asset, "type_name", None) or asset.__class__.__name__,
        "match_type": match_type,
    }


def _qualified_name_matches_identifier(
    qualified_name: str | None,
    parsed_identifier: dict[str, str],
) -> bool:
    if not qualified_name:
        return False
    normalized = qualified_name.lower().replace(".", "/")
    expected_suffix = (
        f"/{parsed_identifier['catalog']}/{parsed_identifier['schema']}/{parsed_identifier['table']}"
        .lower()
    )
    return expected_suffix in normalized or normalized.endswith(expected_suffix.lstrip("/"))


def _search_atlan_assets_for_table(
    table_identifier: str,
    limit: int,
) -> tuple[dict[str, str], list[dict[str, Any]]]:
    parsed_identifier = _parse_databricks_table_identifier(table_identifier)
    safe_limit = max(1, min(limit, MAX_TOOL_ITEMS))
    client = _get_atlan_client()
    specs = _atlan_asset_specs(parsed_identifier)
    seen_guids = set()
    matches = []

    for spec in specs:
        request = _build_atlan_search_request(spec, qualified_name=spec["qualified_name"])
        for asset in client.asset.search(criteria=request):
            if not _qualified_name_matches_identifier(_asset_qualified_name(asset), parsed_identifier):
                continue
            guid = getattr(asset, "guid", None)
            if guid in seen_guids:
                continue
            seen_guids.add(guid)
            matches.append({"asset": asset, "match_type": "qualified_name"})
            if len(matches) >= safe_limit:
                return parsed_identifier, matches

    if matches:
        return parsed_identifier, matches

    for spec in specs:
        request = _build_atlan_search_request(spec, asset_name=parsed_identifier["table"])
        for asset in client.asset.search(criteria=request):
            if not _qualified_name_matches_identifier(_asset_qualified_name(asset), parsed_identifier):
                continue
            guid = getattr(asset, "guid", None)
            if guid in seen_guids:
                continue
            seen_guids.add(guid)
            matches.append({"asset": asset, "match_type": "asset_name"})
            if len(matches) >= safe_limit:
                return parsed_identifier, matches

    return parsed_identifier, matches


def _extract_atlan_context_from_asset(asset: Any, match_type: str, table_identifier: str) -> dict:
    client = _get_atlan_client()
    models = _atlan_model_classes()
    AtlasGlossaryTerm = models["AtlasGlossaryTerm"]
    Readme = models["Readme"]

    terms = []
    text_parts = []
    for term_ref in getattr(asset, "assigned_terms", None) or []:
        term = client.asset.get_by_guid(
            guid=term_ref.guid,
            asset_type=AtlasGlossaryTerm,
            ignore_relationships=False,
        )
        description = term.user_description or term.description
        readme = None
        if term.readme:
            readme_asset = client.asset.get_by_guid(
                guid=term.readme.guid,
                asset_type=Readme,
                ignore_relationships=False,
            )
            if readme_asset.description:
                readme = _remove_html_tags(readme_asset.description)
        term_payload = {
            "name": getattr(term, "name", None),
            "description": description,
            "readme": readme,
        }
        terms.append(term_payload)
        if description or readme:
            section = f"- Information from {table_identifier}"
            if term_payload["name"]:
                section += f"\nTerm: {term_payload['name']}"
            if description:
                section += f"\n{description}"
            if readme:
                section += f"\n{readme}"
            text_parts.append(section)

    return {
        "asset": _asset_summary(asset, match_type=match_type),
        "terms_count": len(terms),
        "terms": terms,
        "context_text": "TERMS CONTEXT\n\n" + "\n\n".join(text_parts) if text_parts else None,
    }


def _get_usage_metrics_warehouse_id() -> str:
    warehouse_id = os.getenv(AUDIT_SQL_WAREHOUSE_ID_ENV) or os.getenv(GENIE_SPACE_WAREHOUSE_ID_ENV)
    if not warehouse_id:
        raise ValueError(
            f"Audit SQL warehouse is not configured. Set {AUDIT_SQL_WAREHOUSE_ID_ENV}."
        )
    return warehouse_id


def _genie_usage_date_range(lookback_days: int) -> dict[str, Any]:
    safe_lookback_days = max(1, min(lookback_days, MAX_GENIE_USAGE_LOOKBACK_DAYS))
    end_date = datetime.datetime.now(datetime.timezone.utc).date()
    start_date = end_date - datetime.timedelta(days=safe_lookback_days - 1)
    return {
        "lookback_days": safe_lookback_days,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }


def _statement_rows(response: Any) -> list[dict[str, Any]]:
    columns = response.manifest.schema.columns if response.manifest and response.manifest.schema else []
    column_names = [column.name for column in columns or []]
    data_array = response.result.data_array if response.result else []

    rows = []
    for raw_row in data_array or []:
        row = dict(zip(column_names, raw_row, strict=False))
        for key in [
            "users",
            "questions_made",
            "interactions",
            "feedback",
            "positive_feedback",
            "negative_feedback",
        ]:
            if row.get(key) is not None:
                row[key] = int(row[key])
        rows.append(row)
    return rows


def _statement_state(response: Any) -> str | None:
    if not response.status or not response.status.state:
        return None
    return getattr(response.status.state, "value", response.status.state)


def _wait_for_statement_result(
    client: Any,
    response: Any,
    timeout_seconds: int,
    poll_interval_seconds: int,
) -> tuple[Any, bool]:
    safe_timeout_seconds = max(10, min(timeout_seconds, 600))
    safe_poll_interval_seconds = max(2, min(poll_interval_seconds, 30))
    deadline = time.monotonic() + safe_timeout_seconds

    while True:
        state = _statement_state(response)
        if state in {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}:
            return response, False
        if not response.statement_id:
            return response, False
        if time.monotonic() >= deadline:
            return response, True
        time.sleep(safe_poll_interval_seconds)
        response = client.statement_execution.get_statement(response.statement_id)


def _genie_usage_metrics_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = next((row for row in rows if row.get("grain") == "total"), None)
    daily = [row for row in rows if row.get("grain") == "daily"]
    weekly = [row for row in rows if row.get("grain") == "weekly"]
    monthly = [row for row in rows if row.get("grain") == "monthly"]
    return {
        "total": total,
        "daily": daily,
        "weekly": weekly,
        "monthly": monthly,
    }


def _start_genie_usage_metrics_statement(
    client: Any,
    space_id: str,
    lookback_days: int,
    wait_timeout: str,
) -> tuple[Any, str, dict[str, Any]]:
    warehouse_id = _get_usage_metrics_warehouse_id()
    date_range = _genie_usage_date_range(lookback_days)
    response = client.statement_execution.execute_statement(
        statement=GENIE_USAGE_METRICS_QUERY,
        warehouse_id=warehouse_id,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        wait_timeout=wait_timeout,
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        row_limit=10000,
        parameters=[
            StatementParameterListItem(
                name="start_date",
                value=date_range["start_date"],
                type="DATE",
            ),
            StatementParameterListItem(
                name="end_date",
                value=date_range["end_date"],
                type="DATE",
            ),
            StatementParameterListItem(
                name="space_id",
                value=space_id,
                type="STRING",
            ),
        ],
    )
    return response, warehouse_id, date_range


def _execute_genie_usage_metrics_query(
    client: Any,
    space_id: str,
    lookback_days: int,
    timeout_seconds: int,
    poll_interval_seconds: int,
) -> tuple[Any, list[dict[str, Any]], bool, str, dict[str, Any]]:
    response, warehouse_id, date_range = _start_genie_usage_metrics_statement(
        client=client,
        space_id=space_id,
        lookback_days=lookback_days,
        wait_timeout="10s",
    )
    response, timed_out = _wait_for_statement_result(
        client=client,
        response=response,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
    return response, _statement_rows(response), timed_out, warehouse_id, date_range


def load_tools(mcp_server):
    """
    Register all MCP tools with the server.

    This function is called during server initialization to register all available
    tools with the MCP server instance. Tools are registered using the @mcp_server.tool
    decorator, which makes them available to clients via the MCP protocol.

    Args:
        mcp_server: The FastMCP server instance to register tools with. This is the
                   main server object that handles tool registration and routing.

    Example:
        To add a new tool, define it within this function using the decorator:

        @mcp_server.tool
        def my_new_tool(param: str) -> dict:
            '''Description of what the tool does.'''
            return {"result": f"Processed {param}"}
    """

    @mcp_server.tool
    def health() -> dict:
        """
        Check the health of the MCP server and Databricks connection.

        This is a simple diagnostic tool that confirms the server is running properly.
        It's useful for:
        - Monitoring and health checks
        - Testing the MCP connection
        - Verifying the server is responsive

        Returns:
            dict: A dictionary containing:
                - status (str): The health status ("healthy" if operational)
                - message (str): A human-readable status message

        Example response:
            {
                "status": "healthy",
                "message": "Custom MCP Server is healthy and connected to Databricks Apps."
            }
        """
        return {
            "status": "healthy",
            "message": "Custom MCP Server is healthy and connected to Databricks Apps.",
        }

    @mcp_server.tool
    def get_current_user() -> dict:
        """
        Get information about the current authenticated user.

        This tool retrieves details about the user who is currently authenticated
        with the MCP server. When deployed as a Databricks App, this returns
        information about the end user making the request. When running locally,
        it returns information about the developer's Databricks identity.

        Useful for:
        - Personalizing responses based on the user
        - Authorization checks
        - Audit logging
        - User-specific operations

        Returns:
            dict: A dictionary containing:
                - display_name (str): The user's display name
                - user_name (str): The user's username/email
                - active (bool): Whether the user account is active

        Example response:
            {
                "display_name": "John Doe",
                "user_name": "john.doe@example.com",
                "active": true
            }

        Raises:
            Returns error dict if authentication fails or user info cannot be retrieved.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            user = w.current_user.me()
            return {
                "display_name": user.display_name,
                "user_name": user.user_name,
                "active": user.active,
            }
        except Exception as e:
            return {"error": str(e), "message": "Failed to retrieve user information"}

    @mcp_server.tool
    def list_available_genie_spaces(limit: int = 50) -> dict:
        """
        List Databricks Genie Spaces available to the authenticated user.

        Args:
            limit: Maximum number of Genie Spaces to return. Capped at 100.

        Returns:
            dict: Genie Space summaries visible to the user.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            response = w.genie.list_spaces()
            spaces = response.spaces or []
            limited, truncated = _limit_items(spaces, limit)
            return {
                "auth_mode": "on_behalf_of_user",
                "count": len(spaces),
                "returned_count": len(limited),
                "truncated": truncated,
                "spaces": [_space_summary(space) for space in limited],
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "error": str(e),
                "spaces": [],
            }

    @mcp_server.tool
    def get_genie_space_details(space_id: str) -> dict:
        """
        Get details for a Databricks Genie Space available to the authenticated user.

        Args:
            space_id: Databricks Genie Space ID.

        Returns:
            dict: Genie Space details. The serialized_space field is omitted to avoid oversized responses.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            space = w.genie.get_space(space_id=space_id, include_serialized_space=False)
            serialized_space = _serialize_databricks_object(space)
            serialized_space.pop("serialized_space", None)
            return {
                "auth_mode": "on_behalf_of_user",
                "space": serialized_space,
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
                "error": str(e),
            }

    @mcp_server.tool
    def list_genie_space_tags(space_id: str) -> dict:
        """
        List workspace tags assigned to a Databricks Genie Space.

        Args:
            space_id: Databricks Genie Space ID.

        Returns:
            dict: Workspace tag assignments for the Genie Space.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            space = w.genie.get_space(space_id=space_id, include_serialized_space=False)
            tags = list(
                w.workspace_entity_tag_assignments.list_tag_assignments(
                    entity_id=space_id,
                    entity_type="geniespaces",
                )
            )
            return {
                "auth_mode": "on_behalf_of_user",
                "space": _space_summary(space),
                "count": len(tags),
                "tags": [_serialize_databricks_object(tag) for tag in tags],
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
                "error": str(e),
            }

    @mcp_server.tool
    def find_genie_spaces_by_tag(
        tag_key: str,
        tag_value: str | None = None,
        limit: int = 50,
    ) -> dict:
        """
        Find Databricks Genie Spaces by workspace tag key and optional tag value.

        Args:
            tag_key: Workspace tag key to search for.
            tag_value: Optional workspace tag value to match.
            limit: Maximum number of matching Genie Spaces to return. Capped at 100.

        Returns:
            dict: Genie Spaces matching the provided tag criteria.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            response = w.genie.list_spaces()
            spaces = response.spaces or []
            matches = []
            for space in spaces:
                if not space.space_id:
                    continue
                tags = w.workspace_entity_tag_assignments.list_tag_assignments(
                    entity_id=space.space_id,
                    entity_type="geniespaces",
                )
                for tag in tags:
                    if tag.tag_key != tag_key:
                        continue
                    if tag_value is not None and tag.tag_value != tag_value:
                        continue
                    matches.append(space)
                    break

            limited, truncated = _limit_items(matches, limit)
            return {
                "auth_mode": "on_behalf_of_user",
                "tag_key": tag_key,
                "tag_value": tag_value,
                "count": len(matches),
                "returned_count": len(limited),
                "truncated": truncated,
                "spaces": [_space_summary(space) for space in limited],
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "tag_key": tag_key,
                "tag_value": tag_value,
                "error": str(e),
            }

    @mcp_server.tool
    def find_atlan_assets_by_databricks_table(
        table_identifier: str,
        limit: int = 20,
    ) -> dict:
        """
        Find Atlan assets matching a Databricks table identifier.

        Args:
            table_identifier: Databricks table identifier in catalog.schema.table format.
            limit: Maximum number of matching assets to return. Capped at 100.

        Returns:
            dict: Matching Atlan assets for the provided Databricks table identifier.
        """
        try:
            parsed_identifier, matches = _search_atlan_assets_for_table(
                table_identifier=table_identifier,
                limit=limit,
            )
            return {
                "auth_mode": "atlan_api_key",
                "table_identifier": parsed_identifier["identifier"],
                "count": len(matches),
                "matches": [
                    _asset_summary(match["asset"], match_type=match["match_type"])
                    for match in matches
                ],
            }
        except Exception as e:
            return {
                "auth_mode": "atlan_api_key",
                "table_identifier": table_identifier,
                "error": str(e),
                "matches": [],
            }

    @mcp_server.tool
    def get_atlan_context_for_databricks_table(
        table_identifier: str,
        limit: int = 20,
    ) -> dict:
        """
        Get business context from Atlan for a Databricks table identifier.

        Args:
            table_identifier: Databricks table identifier in catalog.schema.table format.
            limit: Maximum number of matching assets to inspect. Capped at 100.

        Returns:
            dict: Structured Atlan context for each matching asset, including glossary terms and text.
        """
        try:
            parsed_identifier, matches = _search_atlan_assets_for_table(
                table_identifier=table_identifier,
                limit=limit,
            )
            contexts = [
                _extract_atlan_context_from_asset(
                    asset=match["asset"],
                    match_type=match["match_type"],
                    table_identifier=parsed_identifier["identifier"],
                )
                for match in matches
            ]
            context_texts = [context["context_text"] for context in contexts if context["context_text"]]
            return {
                "auth_mode": "atlan_api_key",
                "table_identifier": parsed_identifier["identifier"],
                "count": len(matches),
                "contexts": contexts,
                "combined_context_text": "\n\n".join(context_texts) if context_texts else None,
            }
        except Exception as e:
            return {
                "auth_mode": "atlan_api_key",
                "table_identifier": table_identifier,
                "error": str(e),
                "contexts": [],
            }

    @mcp_server.tool
    def get_user_name_from_id(user_id: str) -> dict:
        """
        Resolve a Databricks user ID to a user name using the authenticated user's permissions.

        Args:
            user_id: Databricks user ID.

        Returns:
            dict: The resolved user name, or unknown if the user ID is not visible.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            users = w.users.list(attributes="id,userName")
            user_names = {
                str(user.id): user.user_name for user in users if user.id and user.user_name
            }
            return {
                "auth_mode": "on_behalf_of_user",
                "user_id": user_id,
                "user_name": user_names.get(str(user_id), "unknown"),
                "found": str(user_id) in user_names,
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "user_id": user_id,
                "error": str(e),
            }

    @mcp_server.tool
    def list_genie_space_conversations(
        space_id: str,
        include_all: bool = True,
        limit: int = 20,
    ) -> dict:
        """
        List conversations for a Databricks Genie Space available to the authenticated user.

        Args:
            space_id: Databricks Genie Space ID.
            include_all: Whether to include all conversations instead of only recent conversations.
            limit: Maximum number of conversations to return. Capped at 100.

        Returns:
            dict: Conversation summaries for the Genie Space.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            space = w.genie.get_space(space_id=space_id, include_serialized_space=False)
            conversations = []
            response = w.genie.list_conversations(
                space_id=space_id,
                include_all=include_all,
                page_size=50,
            )
            conversations.extend(response.conversations or [])
            while response.next_page_token:
                response = w.genie.list_conversations(
                    space_id=space_id,
                    include_all=include_all,
                    page_size=50,
                    page_token=response.next_page_token,
                )
                conversations.extend(response.conversations or [])

            limited, truncated = _limit_items(conversations, limit)
            return {
                "auth_mode": "on_behalf_of_user",
                "space": _space_summary(space),
                "include_all": include_all,
                "count": len(conversations),
                "returned_count": len(limited),
                "truncated": truncated,
                "conversations": [
                    _serialize_databricks_object(conversation) for conversation in limited
                ],
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
                "error": str(e),
            }

    @mcp_server.tool
    def list_genie_conversation_messages(
        space_id: str,
        conversation_id: str,
        limit: int = 50,
    ) -> dict:
        """
        List messages for a Databricks Genie conversation available to the authenticated user.

        Args:
            space_id: Databricks Genie Space ID.
            conversation_id: Databricks Genie conversation ID.
            limit: Maximum number of messages to return. Capped at 100.

        Returns:
            dict: Messages for the Genie conversation.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            space = w.genie.get_space(space_id=space_id, include_serialized_space=False)
            messages = _list_genie_conversation_messages(
                client=w,
                space_id=space_id,
                conversation_id=conversation_id,
            )

            limited, truncated = _limit_items(messages, limit)
            return {
                "auth_mode": "on_behalf_of_user",
                "space": _space_summary(space),
                "conversation_id": conversation_id,
                "count": len(messages),
                "returned_count": len(limited),
                "truncated": truncated,
                "messages": [_serialize_databricks_object(message) for message in limited],
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
                "conversation_id": conversation_id,
                "error": str(e),
            }

    @mcp_server.tool
    def list_genie_messages_for_conversations(
        space_id: str,
        conversation_ids: list[str],
        limit_per_conversation: int = 50,
    ) -> dict:
        """
        List messages for multiple Databricks Genie conversations available to the authenticated user.

        Args:
            space_id: Databricks Genie Space ID.
            conversation_ids: Databricks Genie conversation IDs. Capped at 50 conversations.
            limit_per_conversation: Maximum messages to return per conversation. Capped at 100.

        Returns:
            dict: Messages grouped by conversation, with partial errors per conversation.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            space = w.genie.get_space(space_id=space_id, include_serialized_space=False)
            safe_conversation_ids = conversation_ids[
                :MAX_CONVERSATIONS_PER_MESSAGES_REQUEST
            ]
            results = []
            for conversation_id in safe_conversation_ids:
                try:
                    messages = _list_genie_conversation_messages(
                        client=w,
                        space_id=space_id,
                        conversation_id=conversation_id,
                    )
                    limited, truncated = _limit_items(messages, limit_per_conversation)
                    results.append(
                        {
                            "conversation_id": conversation_id,
                            "count": len(messages),
                            "returned_count": len(limited),
                            "truncated": truncated,
                            "messages": [
                                _serialize_databricks_object(message)
                                for message in limited
                            ],
                        }
                    )
                except Exception as e:
                    results.append(
                        {
                            "conversation_id": conversation_id,
                            "error": str(e),
                        }
                    )

            return {
                "auth_mode": "on_behalf_of_user",
                "space": _space_summary(space),
                "conversation_count": len(conversation_ids),
                "returned_conversation_count": len(safe_conversation_ids),
                "truncated": len(conversation_ids) > len(safe_conversation_ids),
                "limit_per_conversation": max(
                    1, min(limit_per_conversation, MAX_TOOL_ITEMS)
                ),
                "results": results,
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
                "conversation_ids": conversation_ids,
                "error": str(e),
            }

    @mcp_server.tool
    def get_genie_usage_metrics(
        space_id: str,
        lookback_days: int = DEFAULT_GENIE_USAGE_LOOKBACK_DAYS,
        timeout_seconds: int = 180,
        poll_interval_seconds: int = 5,
    ) -> dict:
        """
        Get Genie Space usage metrics and wait for the SQL statement to complete.

        Args:
            space_id: Databricks Genie Space ID.
            lookback_days: Number of recent days to scan. Capped between 1 and 3660.
            timeout_seconds: Maximum time to wait for the SQL statement. Capped between 10 and 600.
            poll_interval_seconds: Polling interval while the SQL statement runs. Capped between 2 and 30.

        Returns:
            dict: Total, daily, and weekly usage metrics for the Genie Space.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            response, rows, timed_out, warehouse_id, date_range = (
                _execute_genie_usage_metrics_query(
                    client=w,
                    space_id=space_id,
                    lookback_days=lookback_days,
                    timeout_seconds=timeout_seconds,
                    poll_interval_seconds=poll_interval_seconds,
                )
            )
            status = _serialize_databricks_object(response.status) if response.status else None
            state = _statement_state(response)
            if timed_out:
                return {
                    "auth_mode": "on_behalf_of_user",
                    "source": "system.access.audit",
                    "space_id": space_id,
                    "warehouse_id": warehouse_id,
                    "date_range": date_range,
                    "statement_id": response.statement_id,
                    "timed_out": True,
                    "status": status,
                    "result_tool": "get_genie_usage_metrics_query_result",
                    "message": "Statement did not complete within timeout_seconds. Use result_tool with statement_id to fetch it later.",
                }
            if state != "SUCCEEDED":
                return {
                    "auth_mode": "on_behalf_of_user",
                    "source": "system.access.audit",
                    "space_id": space_id,
                    "warehouse_id": warehouse_id,
                    "date_range": date_range,
                    "statement_id": response.statement_id,
                    "timed_out": False,
                    "succeeded": False,
                    "status": status,
                    "message": "Statement finished without a successful result.",
                }

            return {
                "auth_mode": "on_behalf_of_user",
                "source": "system.access.audit",
                "space_id": space_id,
                "warehouse_id": warehouse_id,
                "date_range": date_range,
                "statement_id": response.statement_id,
                "timed_out": False,
                "succeeded": True,
                "status": status,
                "metrics": _genie_usage_metrics_payload(rows),
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "source": "system.access.audit",
                "space_id": space_id,
                "error": str(e),
            }

    @mcp_server.tool
    def start_genie_usage_metrics_query(
        space_id: str,
        lookback_days: int = DEFAULT_GENIE_USAGE_LOOKBACK_DAYS,
    ) -> dict:
        """
        Start a Genie Space usage metrics SQL statement and return quickly. This is the recommended way of retrieving usage data. 
        Useful is `get_genie_usage_metrics` times out. 

        Args:
            space_id: Databricks Genie Space ID.
            lookback_days: Number of recent days to scan. Capped between 1 and 3660.

        Returns:
            dict: Statement ID and status. Use get_genie_usage_metrics_query_result to fetch metrics.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            response, warehouse_id, date_range = _start_genie_usage_metrics_statement(
                client=w,
                space_id=space_id,
                lookback_days=lookback_days,
                wait_timeout="5s",
            )
            state = _statement_state(response)
            status = _serialize_databricks_object(response.status) if response.status else None
            payload = {
                "auth_mode": "on_behalf_of_user",
                "source": "system.access.audit",
                "space_id": space_id,
                "warehouse_id": warehouse_id,
                "date_range": date_range,
                "statement_id": response.statement_id,
                "done": state in {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"},
                "succeeded": state == "SUCCEEDED",
                "status": status,
                "result_tool": "get_genie_usage_metrics_query_result",
            }
            if state == "SUCCEEDED":
                payload["metrics"] = _genie_usage_metrics_payload(_statement_rows(response))
            return payload
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "source": "system.access.audit",
                "space_id": space_id,
                "error": str(e),
            }

    @mcp_server.tool
    def get_genie_usage_metrics_query_result(statement_id: str) -> dict:
        """
        Fetch the result of a Genie Space usage metrics SQL statement.

        Args:
            statement_id: Databricks SQL statement ID returned by start_genie_usage_metrics_query
                or by get_genie_usage_metrics when it times out.

        Returns:
            dict: Current statement status, plus metrics when the statement has succeeded.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            response = w.statement_execution.get_statement(statement_id)
            state = _statement_state(response)
            status = _serialize_databricks_object(response.status) if response.status else None
            payload = {
                "auth_mode": "on_behalf_of_user",
                "source": "system.access.audit",
                "statement_id": statement_id,
                "done": state in {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"},
                "succeeded": state == "SUCCEEDED",
                "status": status,
            }
            if state == "SUCCEEDED":
                payload["metrics"] = _genie_usage_metrics_payload(_statement_rows(response))
            elif state in {"FAILED", "CANCELED", "CLOSED"}:
                payload["message"] = "Statement finished without a successful result."
            return payload
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "source": "system.access.audit",
                "statement_id": statement_id,
                "error": str(e),
            }

    @mcp_server.tool
    def list_genie_benchmark_runs(space_id: str, limit: int = 20) -> dict:
        """
        List Genie benchmark evaluation runs for a Databricks Genie Space.

        Args:
            space_id: Databricks Genie Space ID.
            limit: Maximum number of benchmark runs to return. Capped at 100.

        Returns:
            dict: Benchmark evaluation runs for the Genie Space.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            space = w.genie.get_space(space_id=space_id, include_serialized_space=False)
            safe_limit = max(1, min(limit, MAX_TOOL_ITEMS))
            runs = []
            response = w.genie.genie_list_eval_runs(space_id=space_id, page_size=50)
            runs.extend(response.eval_runs or [])
            while response.next_page_token and len(runs) < safe_limit:
                response = w.genie.genie_list_eval_runs(
                    space_id=space_id,
                    page_size=50,
                    page_token=response.next_page_token,
                )
                runs.extend(response.eval_runs or [])

            limited, truncated = _limit_items(runs, safe_limit)
            return {
                "auth_mode": "on_behalf_of_user",
                "space": _space_summary(space),
                "count": len(runs),
                "returned_count": len(limited),
                "truncated": truncated or bool(response.next_page_token),
                "benchmark_runs": [_serialize_benchmark_run(run) for run in limited],
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
                "error": str(e),
            }

    @mcp_server.tool
    def get_genie_benchmark_run(space_id: str, run_id: str) -> dict:
        """
        Get status and summary fields for a Genie benchmark evaluation run.

        Args:
            space_id: Databricks Genie Space ID.
            run_id: Genie benchmark evaluation run ID.

        Returns:
            dict: Benchmark evaluation run details.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            run = w.genie.genie_get_eval_run(space_id=space_id, eval_run_id=run_id)
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
                "run_id": run_id,
                "benchmark_run": _serialize_databricks_object(run),
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
                "run_id": run_id,
                "error": str(e),
            }

    @mcp_server.tool
    def list_genie_benchmark_run_results(
        space_id: str,
        run_id: str,
        limit: int = 50,
    ) -> dict:
        """
        List result rows for a Genie benchmark evaluation run.

        Args:
            space_id: Databricks Genie Space ID.
            run_id: Genie benchmark evaluation run ID.
            limit: Maximum number of benchmark result rows to return. Capped at 100.

        Returns:
            dict: Benchmark evaluation result rows for the run.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            safe_limit = max(1, min(limit, MAX_TOOL_ITEMS))
            results = []
            response = w.genie.genie_list_eval_results(
                space_id=space_id,
                eval_run_id=run_id,
                page_size=50,
            )
            results.extend(response.eval_results or [])
            while response.next_page_token and len(results) < safe_limit:
                response = w.genie.genie_list_eval_results(
                    space_id=space_id,
                    eval_run_id=run_id,
                    page_size=50,
                    page_token=response.next_page_token,
                )
                results.extend(response.eval_results or [])

            limited, truncated = _limit_items(results, safe_limit)
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
                "run_id": run_id,
                "count": len(results),
                "returned_count": len(limited),
                "truncated": truncated or bool(response.next_page_token),
                "results": [_serialize_databricks_object(result) for result in limited],
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
                "run_id": run_id,
                "error": str(e),
            }

    @mcp_server.tool
    def get_genie_benchmark_result_details(
        space_id: str,
        run_id: str,
        result_id: str,
    ) -> dict:
        """
        Get detailed information for a single Genie benchmark evaluation result.

        Args:
            space_id: Databricks Genie Space ID.
            run_id: Genie benchmark evaluation run ID.
            result_id: Genie benchmark evaluation result ID.

        Returns:
            dict: Detailed benchmark evaluation result information.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            result = w.genie.genie_get_eval_result_details(
                space_id=space_id,
                eval_run_id=run_id,
                result_id=result_id,
            )
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
                "run_id": run_id,
                "result_id": result_id,
                "result": _serialize_databricks_object(result),
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
                "run_id": run_id,
                "result_id": result_id,
                "error": str(e),
            }

    @mcp_server.tool
    def list_genie_space_permissions(space_id: str) -> dict:
        """
        List users, groups, service principals, and permission levels for a Genie Space.

        Args:
            space_id: Databricks Genie Space ID.

        Returns:
            dict: Access control entries for the Genie Space.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            space = w.genie.get_space(space_id=space_id, include_serialized_space=False)
            permissions = w.permissions.get(
                request_object_type="genie",
                request_object_id=space_id,
            )
            access_control_list = permissions.access_control_list or []
            return {
                "auth_mode": "on_behalf_of_user",
                "space": _space_summary(space),
                "permissions": [
                    _serialize_databricks_object(permission) for permission in access_control_list
                ],
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
                "error": str(e),
            }

    @mcp_server.tool
    def grant_space_permissions(
        space_id: str,
        user_name_list: list[str],
        permission_level: str,
        confirmation: str = "",
    ) -> dict:
        """
        Grant a permission level to users in a Databricks Genie Space.

        Args:
            space_id: Databricks Genie Space ID.
            user_name_list: User names to grant permissions to.
            permission_level: Permission level. Must be CAN_MANAGE, CAN_EDIT, or CAN_READ.
            confirmation: Must equal CONFIRM GRANT GENIE SPACE PERMISSIONS to perform the grant.

        Returns:
            dict: Confirmation requirement or grant result.
        """
        if confirmation != GRANT_SPACE_PERMISSIONS_CONFIRMATION:
            return _confirmation_required_payload(
                required_confirmation=GRANT_SPACE_PERMISSIONS_CONFIRMATION,
                action="grant_space_permissions",
                space_id=space_id,
                user_name_list=user_name_list,
                permission_level=permission_level,
            )

        try:
            permission = _permission_level(permission_level)
            w = utils.get_user_authenticated_workspace_client()
            access_control_list = [
                AccessControlRequest(user_name=user_name, permission_level=permission)
                for user_name in user_name_list
            ]
            w.permissions.update(
                request_object_type="genie",
                request_object_id=space_id,
                access_control_list=access_control_list,
            )
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
                "user_name_list": user_name_list,
                "permission_level": permission_level,
                "success": True,
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
                "user_name_list": user_name_list,
                "permission_level": permission_level,
                "error": str(e),
            }

    @mcp_server.tool
    def start_genie_serialization_job(
        tag_key: str | None = None,
        space_id: str | None = None,
        confirmation: str = "",
    ) -> dict:
        """
        Start the configured Databricks Job that serializes Genie Spaces by tag or space ID.

        Args:
            tag_key: Optional tag key to serialize spaces by tag.
            space_id: Optional Genie Space ID to serialize a single space.
            confirmation: Must equal CONFIRM RUN GENIE SERIALIZATION JOB to start the job.

        Returns:
            dict: Confirmation requirement or started job run ID.
        """
        try:
            job_id = _get_configured_job_id(
                GENIE_SERIALIZATION_JOB_ID_ENV,
                "Genie serialization Job",
            )
            job_parameters = _genie_serialization_job_parameters(
                tag_key=tag_key,
                space_id=space_id,
            )
        except Exception as e:
            return {"auth_mode": "service_principal", "error": str(e)}

        if confirmation != RUN_SERIALIZATION_JOB_CONFIRMATION:
            return _confirmation_required_payload(
                required_confirmation=RUN_SERIALIZATION_JOB_CONFIRMATION,
                action="start_genie_serialization_job",
                auth_mode="service_principal",
                job_id=job_id,
                job_parameters=job_parameters,
                uses_job_default_serialization_parameters=job_parameters is None,
            )

        try:
            w = utils.get_workspace_client()
            if job_parameters:
                waiter = w.jobs.run_now(job_id=job_id, job_parameters=job_parameters)
            else:
                waiter = w.jobs.run_now(job_id=job_id)
            return {
                "auth_mode": "service_principal",
                "job_id": job_id,
                "job_parameters": job_parameters,
                "uses_job_default_serialization_parameters": job_parameters is None,
                "run_id": waiter.run_id,
                "message": "Genie serialization job started.",
            }
        except Exception as e:
            return {"auth_mode": "service_principal", "job_id": job_id, "error": str(e)}

    @mcp_server.tool
    def get_genie_serialization_job_run(run_id: int) -> dict:
        """
        Get status for a Databricks Job run created by the Genie serialization job.

        Args:
            run_id: Databricks Job run ID.

        Returns:
            dict: Job run summary.
        """
        try:
            w = utils.get_workspace_client()
            run = w.jobs.get_run(run_id=run_id)
            return {"auth_mode": "service_principal", "run": _job_run_summary(run)}
        except Exception as e:
            return {"auth_mode": "service_principal", "run_id": run_id, "error": str(e)}

    @mcp_server.tool
    def list_genie_space_restore_points(
        space_id: str,
        timeout_minutes: int = 10,
        poll_interval_seconds: int = 10,
    ) -> dict:
        """
        List exact restore points available for a Genie Space by running the configured Job.

        Args:
            space_id: Databricks Genie Space ID.
            timeout_minutes: Maximum wait time. Capped at 120 minutes.
            poll_interval_seconds: Poll interval. Capped between 5 and 120 seconds.

        Returns:
            dict: Job run status and output containing restore points when completed.
        """
        try:
            safe_space_id = space_id.strip()
            if not safe_space_id:
                raise ValueError("space_id is required")
            job_id = _get_configured_job_id(
                GENIE_RESTORE_POINTS_JOB_ID_ENV,
                "Genie restore points Job",
            )
            w = utils.get_workspace_client()
            job_parameters = {"space_id": safe_space_id}
            waiter = w.jobs.run_now(job_id=job_id, job_parameters=job_parameters)
            run_id = waiter.run_id
            run_summary, timed_out = _wait_for_job_terminal_state(
                client=w,
                run_id=run_id,
                timeout_minutes=timeout_minutes,
                poll_interval_seconds=poll_interval_seconds,
            )
            output = None if timed_out else _job_run_output_payload(w, run_id, run_summary)
            return {
                "auth_mode": "service_principal",
                "job_id": job_id,
                "job_parameters": job_parameters,
                "run_id": run_id,
                "completed": not timed_out,
                "timed_out": timed_out,
                "run": run_summary,
                "output": output,
            }
        except Exception as e:
            return {"auth_mode": "service_principal", "space_id": space_id, "error": str(e)}

    @mcp_server.tool
    def get_genie_restore_points_job_run(run_id: int) -> dict:
        """
        Get status and output for a Genie restore-points listing Job run.

        Args:
            run_id: Databricks Job run ID.

        Returns:
            dict: Job run summary and output when terminal.
        """
        try:
            w = utils.get_workspace_client()
            run = w.jobs.get_run(run_id=run_id)
            run_summary = _job_run_summary(run)
            output = (
                _job_run_output_payload(w, run_id, run_summary)
                if _job_has_terminal_state(run_summary)
                else None
            )
            return {
                "auth_mode": "service_principal",
                "run_id": run_id,
                "run": run_summary,
                "output": output,
            }
        except Exception as e:
            return {"auth_mode": "service_principal", "run_id": run_id, "error": str(e)}

    @mcp_server.tool
    def start_genie_space_restore_job(
        space_id: str,
        snapshot_date: str,
        confirmation: str = "",
    ) -> dict:
        """
        Start the configured Genie Space restore Job for an exact snapshot date.

        Args:
            space_id: Databricks Genie Space ID.
            snapshot_date: Restore snapshot date in YYYY-MM-DD format.
            confirmation: Must equal CONFIRM RESTORE GENIE SPACE <space_id> <snapshot_date>.

        Returns:
            dict: Confirmation requirement or started job run ID.
        """
        try:
            safe_space_id, safe_snapshot_date = _validate_restore_inputs(space_id, snapshot_date)
            required_confirmation = _restore_confirmation(safe_space_id, safe_snapshot_date)
            job_id = _get_configured_job_id(GENIE_RESTORE_JOB_ID_ENV, "Genie restore Job")
            job_parameters = {"space_id": safe_space_id, "snapshot_date": safe_snapshot_date}
            if confirmation != required_confirmation:
                return _confirmation_required_payload(
                    required_confirmation=required_confirmation,
                    action="start_genie_space_restore_job",
                    auth_mode="service_principal",
                    space_id=safe_space_id,
                    snapshot_date=safe_snapshot_date,
                    job_id=job_id,
                    job_parameters=job_parameters,
                )

            w = utils.get_workspace_client()
            waiter = w.jobs.run_now(job_id=job_id, job_parameters=job_parameters)
            return {
                "auth_mode": "service_principal",
                "job_id": job_id,
                "job_parameters": job_parameters,
                "run_id": waiter.run_id,
                "message": "Genie restore job started.",
            }
        except Exception as e:
            return {
                "auth_mode": "service_principal",
                "space_id": space_id,
                "snapshot_date": snapshot_date,
                "error": str(e),
            }

    @mcp_server.tool
    def get_genie_space_restore_job_run(run_id: int) -> dict:
        """
        Get status and output for a Genie Space restore Job run.

        Args:
            run_id: Databricks Job run ID.

        Returns:
            dict: Job run summary and output when terminal.
        """
        try:
            w = utils.get_workspace_client()
            run = w.jobs.get_run(run_id=run_id)
            run_summary = _job_run_summary(run)
            output = (
                _job_run_output_payload(w, run_id, run_summary)
                if _job_has_terminal_state(run_summary)
                else None
            )
            return {
                "auth_mode": "service_principal",
                "run_id": run_id,
                "run": run_summary,
                "output": output,
            }
        except Exception as e:
            return {"auth_mode": "service_principal", "run_id": run_id, "error": str(e)}

    """
    TODO: Add more tools as necessary
    """
