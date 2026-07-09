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
import re
import time
import uuid
from collections import Counter
from typing import Any

from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

from server import utils


MAX_TOOL_ITEMS = 100
RUN_SERIALIZATION_JOB_CONFIRMATION = "CONFIRM RUN GENIE SERIALIZATION JOB"
GRANT_SPACE_PERMISSIONS_CONFIRMATION = "CONFIRM GRANT GENIE SPACE PERMISSIONS"
RESTORE_SNAPSHOT_DATE_FORMAT = "%Y-%m-%d"
TERMINAL_JOB_LIFE_CYCLE_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
GENIE_SERIALIZATION_JOB_ID_ENV = "GENIE_SERIALIZATION_JOB_ID"
GENIE_RESTORE_POINTS_JOB_ID_ENV = "GENIE_RESTORE_POINTS_JOB_ID"
GENIE_RESTORE_JOB_ID_ENV = "GENIE_RESTORE_JOB_ID"
GENIE_SPACE_WAREHOUSE_ID_ENV = "GENIE_SPACE_WAREHOUSE_ID"
ATLAN_API_KEY_ENV = "ATLAN_API_KEY"
ATLAN_BASE_URL_ENV = "ATLAN_BASE_URL"
CREATE_GENIE_SPACE_FROM_DASHBOARD_CONFIRMATION = "CONFIRM CREATE GENIE SPACE FROM DASHBOARD"
ATLAN_GOLD_QUALIFIED_NAME_PREFIX = "default/databricks/1732657096/lf_udm_prod/gold/"
ATLAN_METRIC_VIEWS_QUALIFIED_NAME_PREFIX = (
    "default/databricks/1732657096/lf_udm_prod/gold_metric_views/"
)


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


def _sorted_counter(counter: Counter) -> dict:
    return dict(sorted(counter.items(), key=lambda item: str(item[0])))


def _serialize_benchmark_run(run: Any) -> dict[str, Any]:
    run_dict = _serialize_databricks_object(run)
    run_dict["created_date"] = _date_from_timestamp(run_dict.get("created_timestamp"))
    return run_dict


def _thumb_key(message: Any) -> str | None:
    feedback = getattr(message, "feedback", None)
    if not feedback:
        return None
    rating = getattr(feedback, "rating", None)
    rating_value = getattr(rating, "value", rating)
    if rating_value == "POSITIVE":
        return "thumbs_up"
    if rating_value == "NEGATIVE":
        return "thumbs_down"
    return None


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


def _clean_complex_params_from_query(query: list[str]) -> list[str]:
    expr = r"""array_contains\(\s*(:[`\w]+)\s*,\s*([`\w.'"]+(?: [`\w.'"]+)*)\s*\)"""
    return [re.sub(expr, r"\2 = \1", line) for line in query]


def _parse_dashboard_serialized_content(serialized_dashboard: str) -> dict[str, Any]:
    serialized_dict = json.loads(serialized_dashboard)
    result: dict[str, Any] = {"data": [], "joins": [], "queries": []}

    for source in serialized_dict.get("datasets", []):
        if "queryLines" in source:
            query = {
                "display_name": source.get("displayName"),
                "query_lines": _clean_complex_params_from_query(source.get("queryLines") or []),
            }
            if "parameters" in source:
                query["parameters"] = source["parameters"]
            result["queries"].append(query)
        elif "asset_name" in source:
            result["data"].append(
                {
                    "type": "metric_view",
                    "display_name": source.get("displayName"),
                    "source": source.get("asset_name"),
                }
            )
        elif "config" in source:
            config = source.get("config") or {}
            table_source = config.get("source")
            data = {
                "type": "table",
                "display_name": source.get("displayName"),
                "source": table_source,
                "dimensions": config.get("dimensions") or [],
            }
            result["data"].append(data)
            for join in config.get("joins") or []:
                join_source = join.get("source")
                join_name = join.get("name")
                join_on = join.get("on") or ""
                if table_source and join_source and join_name:
                    normalized_on = join_on.replace(
                        "source.",
                        table_source.split(".")[2] + ".",
                    ).replace(join_name, join_source.split(".")[2])
                else:
                    normalized_on = join_on
                result["joins"].append(
                    {
                        "name": join_name,
                        "left": table_source,
                        "right": join_source,
                        "on": normalized_on,
                    }
                )

    return result


def _remove_html_tags(value: str) -> str:
    from bs4 import BeautifulSoup

    return BeautifulSoup(value, "html.parser").get_text()


def _get_atlan_context(table_name: str) -> str | None:
    api_key = os.getenv(ATLAN_API_KEY_ENV)
    base_url = os.getenv(ATLAN_BASE_URL_ENV)
    if not api_key:
        raise ValueError(f"{ATLAN_API_KEY_ENV} is not configured")
    if not base_url:
        raise ValueError(f"{ATLAN_BASE_URL_ENV} is not configured")

    from pyatlan.client.atlan import AtlanClient
    from pyatlan.model.assets import AtlasGlossaryTerm, DatabricksMetricView, Readme, Table
    from pyatlan.model.fluent_search import CompoundQuery, FluentSearch

    client = AtlanClient(base_url=base_url, api_key=api_key)
    table_parts = table_name.split(".")
    asset_name = table_parts[-1]
    schema_name = table_parts[1] if len(table_parts) > 1 else ""

    if schema_name == "gold":
        qualified_name = ATLAN_GOLD_QUALIFIED_NAME_PREFIX + asset_name
        asset_type = Table
        assigned_terms_attr = Table.ASSIGNED_TERMS
    else:
        qualified_name = ATLAN_METRIC_VIEWS_QUALIFIED_NAME_PREFIX + asset_name
        asset_type = DatabricksMetricView
        assigned_terms_attr = DatabricksMetricView.ASSIGNED_TERMS

    request = (
        FluentSearch.select()
        .where(Table.QUALIFIED_NAME.eq(qualified_name))
        .where(CompoundQuery.asset_type(asset_type))
        .where(CompoundQuery.active_assets())
        .include_relationship_attributes(True)
        .include_on_results(assigned_terms_attr)
        .include_on_relations(AtlasGlossaryTerm.NAME)
        .include_on_relations(AtlasGlossaryTerm.DESCRIPTION)
        .include_on_relations(AtlasGlossaryTerm.README)
    ).to_request()

    results = list(client.asset.search(criteria=request))
    if len(results) != 1:
        return None

    asset = results[0]
    if not getattr(asset, "assigned_terms", None):
        return None

    text_parts = []
    for term_ref in asset.assigned_terms:
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
        if description or readme:
            section = f"- Information from {table_name}"
            if description:
                section += f"\n{description}"
            if readme:
                section += f"\n{readme}"
            text_parts.append(section)

    if not text_parts:
        return None
    return "TERMS CONTEXT\n\n" + "\n\n".join(text_parts)


def _create_serialized_space_from_parsed_dashboard(
    parsed_dashboard: dict[str, Any],
    include_atlan_context: bool,
) -> str:
    space: dict[str, Any] = {
        "version": 2,
        "data_sources": {},
        "instructions": {
            "example_question_sqls": [],
            "join_specs": [],
            "text_instructions": [],
        },
    }
    content_context = []

    for data in parsed_dashboard.get("data", []):
        source = data.get("source")
        if not source:
            continue
        if "tables" not in space["data_sources"]:
            space["data_sources"]["tables"] = []
        table = {"identifier": source}
        if include_atlan_context:
            context = _get_atlan_context(table_name=source)
            if context:
                content_context.append(context)
        if data.get("type") == "table":
            table["column_configs"] = sorted(
                [
                    {
                        "column_name": dimension.get("name"),
                        "enable_format_assistance": True,
                    }
                    for dimension in data.get("dimensions") or []
                    if dimension.get("name")
                ],
                key=lambda column: column["column_name"],
            )
        space["data_sources"]["tables"].append(table)

    space["instructions"]["text_instructions"] = [
        {"id": uuid.uuid4().hex, "content": content_context if content_context else [""]}
    ]

    if "tables" in space["data_sources"]:
        space["data_sources"]["tables"] = sorted(
            space["data_sources"]["tables"],
            key=lambda table: table["identifier"],
        )

    for query in parsed_dashboard.get("queries", []):
        example_query: dict[str, Any] = {
            "id": uuid.uuid4().hex,
            "question": [query.get("display_name")],
            "sql": query.get("query_lines") or [],
        }
        if query.get("parameters"):
            example_query["parameters"] = [
                {"name": parameter.get("displayName"), "type_hint": parameter.get("dataType")}
                for parameter in query["parameters"]
            ]
        space["instructions"]["example_question_sqls"].append(example_query)
    space["instructions"]["example_question_sqls"] = sorted(
        space["instructions"]["example_question_sqls"],
        key=lambda query: query["id"],
    )

    for join in parsed_dashboard.get("joins", []):
        left = join.get("left")
        right = join.get("right")
        if not left or not right:
            continue
        space["instructions"]["join_specs"].append(
            {
                "id": uuid.uuid4().hex,
                "left": {"identifier": left, "alias": left.split(".")[-1]},
                "right": {"identifier": right, "alias": right.split(".")[-1]},
                "sql": [join.get("on") or "", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"],
            }
        )
    space["instructions"]["join_specs"] = sorted(
        space["instructions"]["join_specs"],
        key=lambda join: join["id"],
    )

    return json.dumps(space)


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
    def list_available_dashboards(limit: int = 50) -> dict:
        """
        List Lakeview dashboards available to the authenticated Databricks user.

        Args:
            limit: Maximum number of dashboards to return. Capped at 100.

        Returns:
            dict: Dashboard summaries visible to the user.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            dashboards = list(w.lakeview.list())
            limited, truncated = _limit_items(dashboards, limit)
            return {
                "auth_mode": "on_behalf_of_user",
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
                        "lifecycle_state": str(dashboard.lifecycle_state)
                        if dashboard.lifecycle_state
                        else None,
                        "create_time": dashboard.create_time,
                        "update_time": dashboard.update_time,
                    }
                    for dashboard in limited
                ],
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "error": str(e),
                "dashboards": [],
            }

    @mcp_server.tool
    def get_dashboard_details(dashboard_id: str) -> dict:
        """
        Get details for a Lakeview dashboard available to the authenticated Databricks user.

        Args:
            dashboard_id: Databricks Lakeview dashboard ID.

        Returns:
            dict: Dashboard details. The serialized_dashboard field is omitted to avoid oversized responses.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            dashboard = w.lakeview.get(dashboard_id=dashboard_id)
            serialized_dashboard = _serialize_databricks_object(dashboard)
            serialized_dashboard.pop("serialized_dashboard", None)
            return {
                "auth_mode": "on_behalf_of_user",
                "dashboard": serialized_dashboard,
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "dashboard_id": dashboard_id,
                "error": str(e),
            }

    @mcp_server.tool
    def preview_genie_space_from_dashboard(
        dashboard_id: str,
        include_atlan_context: bool = False,
    ) -> dict:
        """
        Preview the Genie Space configuration that would be generated from a Lakeview dashboard.

        Args:
            dashboard_id: Databricks Lakeview dashboard ID.
            include_atlan_context: Whether to validate and include Atlan glossary context.

        Returns:
            dict: Parsed dashboard datasets, queries, joins, and serialized Genie Space preview.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            dashboard = w.lakeview.get(dashboard_id=dashboard_id)
            if not dashboard.serialized_dashboard:
                raise ValueError("Selected dashboard has no serialized_dashboard attribute")
            parsed_dashboard = _parse_dashboard_serialized_content(dashboard.serialized_dashboard)
            serialized_space = _create_serialized_space_from_parsed_dashboard(
                parsed_dashboard=parsed_dashboard,
                include_atlan_context=include_atlan_context,
            )
            serialized_space_payload = json.loads(serialized_space)
            return {
                "auth_mode": "on_behalf_of_user",
                "dashboard": {
                    "dashboard_id": dashboard.dashboard_id,
                    "display_name": dashboard.display_name,
                    "path": dashboard.path,
                    "warehouse_id": dashboard.warehouse_id,
                },
                "include_atlan_context": include_atlan_context,
                "parsed_dashboard": parsed_dashboard,
                "serialized_space": serialized_space_payload,
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "dashboard_id": dashboard_id,
                "include_atlan_context": include_atlan_context,
                "error": str(e),
            }

    @mcp_server.tool
    def create_genie_space_from_dashboard(
        dashboard_id: str,
        genie_space_title: str,
        genie_space_description: str | None = None,
        user_name_list: list[str] | None = None,
        warehouse_id: str | None = None,
        user_permission_level: str = "CAN_MANAGE",
        include_atlan_context: bool = False,
        confirmation: str = "",
    ) -> dict:
        """
        Create a Databricks Genie Space from a Lakeview dashboard.

        Args:
            dashboard_id: Databricks Lakeview dashboard ID.
            genie_space_title: Title for the new Genie Space.
            genie_space_description: Optional description for the new Genie Space.
            user_name_list: Optional users to grant permissions to after creation.
            warehouse_id: Optional warehouse ID. Defaults to GENIE_SPACE_WAREHOUSE_ID app env var.
            user_permission_level: Permission granted to user_name_list. CAN_MANAGE, CAN_EDIT, or CAN_READ.
            include_atlan_context: Whether to include Atlan glossary context in text instructions.
            confirmation: Must equal CONFIRM CREATE GENIE SPACE FROM DASHBOARD to create the space.

        Returns:
            dict: Confirmation requirement or created Genie Space details.
        """
        users = user_name_list or []
        safe_dashboard_id = dashboard_id.strip()
        safe_title = genie_space_title.strip()
        safe_warehouse_id = (warehouse_id or os.getenv(GENIE_SPACE_WAREHOUSE_ID_ENV) or "").strip()

        if confirmation != CREATE_GENIE_SPACE_FROM_DASHBOARD_CONFIRMATION:
            return _confirmation_required_payload(
                required_confirmation=CREATE_GENIE_SPACE_FROM_DASHBOARD_CONFIRMATION,
                action="create_genie_space_from_dashboard",
                dashboard_id=safe_dashboard_id,
                genie_space_title=safe_title,
                genie_space_description=genie_space_description,
                user_name_list=users,
                warehouse_id=safe_warehouse_id,
                user_permission_level=user_permission_level,
                include_atlan_context=include_atlan_context,
            )

        try:
            if not safe_dashboard_id:
                raise ValueError("dashboard_id is required")
            if not safe_title:
                raise ValueError("genie_space_title is required")
            if not safe_warehouse_id:
                raise ValueError(f"warehouse_id is required or set {GENIE_SPACE_WAREHOUSE_ID_ENV}")

            permission = _permission_level(user_permission_level)
            w = utils.get_user_authenticated_workspace_client()
            dashboard = w.lakeview.get(dashboard_id=safe_dashboard_id)
            if not dashboard.serialized_dashboard:
                raise ValueError("Selected dashboard has no serialized_dashboard attribute")
            parsed_dashboard = _parse_dashboard_serialized_content(dashboard.serialized_dashboard)
            serialized_space = _create_serialized_space_from_parsed_dashboard(
                parsed_dashboard=parsed_dashboard,
                include_atlan_context=include_atlan_context,
            )
            created_space = w.genie.create_space(
                title=safe_title,
                warehouse_id=safe_warehouse_id,
                description=genie_space_description,
                serialized_space=serialized_space,
            )

            if users:
                w.permissions.update(
                    request_object_type="genie",
                    request_object_id=created_space.space_id,
                    access_control_list=[
                        AccessControlRequest(user_name=user_name, permission_level=permission)
                        for user_name in users
                    ],
                )

            return {
                "auth_mode": "on_behalf_of_user",
                "dashboard": {
                    "dashboard_id": dashboard.dashboard_id,
                    "display_name": dashboard.display_name,
                    "path": dashboard.path,
                },
                "created_space": _serialize_databricks_object(created_space),
                "user_name_list": users,
                "user_permission_level": user_permission_level,
                "warehouse_id": safe_warehouse_id,
                "include_atlan_context": include_atlan_context,
                "success": True,
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "dashboard_id": safe_dashboard_id,
                "genie_space_title": safe_title,
                "error": str(e),
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
            messages = []
            response = w.genie.list_conversation_messages(
                space_id=space_id,
                conversation_id=conversation_id,
                page_size=50,
            )
            messages.extend(response.messages or [])
            while response.next_page_token:
                response = w.genie.list_conversation_messages(
                    space_id=space_id,
                    conversation_id=conversation_id,
                    page_size=50,
                    page_token=response.next_page_token,
                )
                messages.extend(response.messages or [])

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
    def get_genie_history_metrics(
        space_id: str,
        include_all: bool = True,
        max_conversations: int = 100,
        max_messages_per_conversation: int = 100,
    ) -> dict:
        """
        Get usage and feedback history metrics for a Databricks Genie Space.

        Args:
            space_id: Databricks Genie Space ID.
            include_all: Whether to include all conversations instead of only recent conversations.
            max_conversations: Maximum number of conversations to inspect. Capped at 100.
            max_messages_per_conversation: Maximum number of messages per conversation. Capped at 100.

        Returns:
            dict: Aggregated Genie usage and feedback metrics computed on behalf of the user.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            space = w.genie.get_space(space_id=space_id, include_serialized_space=False)
            safe_conversation_limit = max(1, min(max_conversations, MAX_TOOL_ITEMS))
            safe_message_limit = max(1, min(max_messages_per_conversation, MAX_TOOL_ITEMS))

            user_names = {
                str(user.id): user.user_name
                for user in w.users.list(attributes="id,userName")
                if user.id and user.user_name
            }
            conversations = []
            response = w.genie.list_conversations(
                space_id=space_id,
                include_all=include_all,
                page_size=50,
            )
            conversations.extend(response.conversations or [])
            while response.next_page_token and len(conversations) < safe_conversation_limit:
                response = w.genie.list_conversations(
                    space_id=space_id,
                    include_all=include_all,
                    page_size=50,
                    page_token=response.next_page_token,
                )
                conversations.extend(response.conversations or [])

            limited_conversations, conversations_truncated = _limit_items(
                conversations,
                safe_conversation_limit,
            )
            questions_per_user = Counter()
            questions_per_day = Counter()
            questions_per_week = Counter()
            questions_per_month = Counter()
            total_thumbs = {"thumbs_up": 0, "thumbs_down": 0}
            thumbs_per_month = {}
            total_messages = 0
            messages_truncated = False

            for conversation in limited_conversations:
                conversation_id = conversation.conversation_id
                if not conversation_id:
                    continue
                messages = []
                messages_response = w.genie.list_conversation_messages(
                    space_id=space_id,
                    conversation_id=conversation_id,
                    page_size=50,
                )
                messages.extend(messages_response.messages or [])
                while messages_response.next_page_token and len(messages) < safe_message_limit:
                    messages_response = w.genie.list_conversation_messages(
                        space_id=space_id,
                        conversation_id=conversation_id,
                        page_size=50,
                        page_token=messages_response.next_page_token,
                    )
                    messages.extend(messages_response.messages or [])

                limited_messages, truncated = _limit_items(messages, safe_message_limit)
                messages_truncated = messages_truncated or truncated
                total_messages += len(limited_messages)

                for message in limited_messages:
                    user_id = getattr(message, "user_id", None)
                    user_name = (
                        user_names.get(str(user_id), "unknown") if user_id else "unknown"
                    )
                    questions_per_user[user_name] += 1

                    message_datetime = _datetime_from_timestamp(
                        getattr(message, "created_timestamp", None)
                    )
                    month_key = None
                    if message_datetime:
                        questions_per_day[message_datetime.date().isoformat()] += 1
                        iso_year, iso_week, _ = message_datetime.isocalendar()
                        questions_per_week[f"{iso_year}-W{iso_week:02d}"] += 1
                        month_key = message_datetime.strftime("%Y-%m")
                        questions_per_month[month_key] += 1

                    thumb_key = _thumb_key(message)
                    if thumb_key:
                        total_thumbs[thumb_key] += 1
                        if message_datetime and month_key:
                            if month_key not in thumbs_per_month:
                                thumbs_per_month[month_key] = {
                                    "thumbs_up": 0,
                                    "thumbs_down": 0,
                                }
                            thumbs_per_month[month_key][thumb_key] += 1

            return {
                "auth_mode": "on_behalf_of_user",
                "space": _space_summary(space),
                "limits": {
                    "include_all": include_all,
                    "max_conversations": safe_conversation_limit,
                    "max_messages_per_conversation": safe_message_limit,
                },
                "truncated": conversations_truncated or messages_truncated,
                "inspected_conversations": len(limited_conversations),
                "inspected_messages": total_messages,
                "metrics": {
                    "questions_per_user": _sorted_counter(questions_per_user),
                    "questions_per_day": _sorted_counter(questions_per_day),
                    "questions_per_week": _sorted_counter(questions_per_week),
                    "questions_per_month": _sorted_counter(questions_per_month),
                    "total_questions_history": total_messages,
                    "total_thumbs_history": total_thumbs,
                    "thumbs_per_month": dict(sorted(thumbs_per_month.items())),
                },
            }
        except Exception as e:
            return {
                "auth_mode": "on_behalf_of_user",
                "space_id": space_id,
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
    def start_genie_serialization_job_and_wait(
        tag_key: str | None = None,
        space_id: str | None = None,
        confirmation: str = "",
        timeout_minutes: int = 30,
        poll_interval_seconds: int = 20,
    ) -> dict:
        """
        Start the Genie serialization Job and wait until it reaches a terminal state.

        Args:
            tag_key: Optional tag key to serialize spaces by tag.
            space_id: Optional Genie Space ID to serialize a single space.
            confirmation: Must equal CONFIRM RUN GENIE SERIALIZATION JOB to start the job.
            timeout_minutes: Maximum wait time. Capped at 120 minutes.
            poll_interval_seconds: Poll interval. Capped between 5 and 120 seconds.

        Returns:
            dict: Started job run status and terminal/timed-out state.
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

        safe_timeout_minutes = max(1, min(timeout_minutes, 120))
        safe_poll_interval_seconds = max(5, min(poll_interval_seconds, 120))
        if confirmation != RUN_SERIALIZATION_JOB_CONFIRMATION:
            return _confirmation_required_payload(
                required_confirmation=RUN_SERIALIZATION_JOB_CONFIRMATION,
                action="start_genie_serialization_job_and_wait",
                auth_mode="service_principal",
                job_id=job_id,
                job_parameters=job_parameters,
                uses_job_default_serialization_parameters=job_parameters is None,
                timeout_minutes=safe_timeout_minutes,
                poll_interval_seconds=safe_poll_interval_seconds,
            )

        try:
            w = utils.get_workspace_client()
            if job_parameters:
                waiter = w.jobs.run_now(job_id=job_id, job_parameters=job_parameters)
            else:
                waiter = w.jobs.run_now(job_id=job_id)
            run_id = waiter.run_id
            run_summary, timed_out = _wait_for_job_terminal_state(
                client=w,
                run_id=run_id,
                timeout_minutes=safe_timeout_minutes,
                poll_interval_seconds=safe_poll_interval_seconds,
            )
            return {
                "auth_mode": "service_principal",
                "job_id": job_id,
                "job_parameters": job_parameters,
                "uses_job_default_serialization_parameters": job_parameters is None,
                "run_id": run_id,
                "completed": not timed_out,
                "timed_out": timed_out,
                "run": run_summary,
            }
        except Exception as e:
            return {"auth_mode": "service_principal", "job_id": job_id, "error": str(e)}

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

    @mcp_server.tool
    def start_genie_space_restore_job_and_wait(
        space_id: str,
        snapshot_date: str,
        confirmation: str = "",
        timeout_minutes: int = 30,
        poll_interval_seconds: int = 20,
    ) -> dict:
        """
        Start the configured Genie Space restore Job and wait until terminal state.

        Args:
            space_id: Databricks Genie Space ID.
            snapshot_date: Restore snapshot date in YYYY-MM-DD format.
            confirmation: Must equal CONFIRM RESTORE GENIE SPACE <space_id> <snapshot_date>.
            timeout_minutes: Maximum wait time. Capped at 120 minutes.
            poll_interval_seconds: Poll interval. Capped between 5 and 120 seconds.

        Returns:
            dict: Job run status and output when completed.
        """
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
                    auth_mode="service_principal",
                    space_id=safe_space_id,
                    snapshot_date=safe_snapshot_date,
                    job_id=job_id,
                    job_parameters=job_parameters,
                    timeout_minutes=safe_timeout_minutes,
                    poll_interval_seconds=safe_poll_interval_seconds,
                )

            w = utils.get_workspace_client()
            waiter = w.jobs.run_now(job_id=job_id, job_parameters=job_parameters)
            run_id = waiter.run_id
            run_summary, timed_out = _wait_for_job_terminal_state(
                client=w,
                run_id=run_id,
                timeout_minutes=safe_timeout_minutes,
                poll_interval_seconds=safe_poll_interval_seconds,
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
            return {
                "auth_mode": "service_principal",
                "space_id": space_id,
                "snapshot_date": snapshot_date,
                "error": str(e),
            }

    """
    TODO: Add more tools as necessary
    """
