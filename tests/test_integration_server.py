import os
import shlex
import signal
import socket
import subprocess
import time
from contextlib import closing

import pytest
import requests
from databricks_mcp import DatabricksMCPClient


def _print_tool_result(tool_name: str, result):
    if os.getenv("PRINT_TOOL_RESULTS") != "1":
        return
    print(f"\n--- TOOL RESULT: {tool_name} ---")
    print(result)
    content = getattr(result, "content", None)
    if content is not None:
        print("--- CONTENT ---")
        print(content)


def _find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_server_startup(url: str, timeout: int = 60):
    deadline = time.time() + timeout
    last_exc = None

    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=1)
            if 200 <= response.status_code < 400:
                return response
        except Exception as e:
            last_exc = e
        time.sleep(0.1)
    if last_exc:
        raise last_exc

    raise TimeoutError(f"Server at {url} did not respond in {timeout} seconds")


@pytest.fixture(scope="session")
def run_mcp_server():
    host = "127.0.0.1"
    port = _find_free_port()
    url = f"http://{host}:{port}"
    cmd = shlex.split(f"uv run custom-mcp-server --port {port}")

    # Start the process
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        # Start a new process group so we can kill children on teardown
        preexec_fn=os.setsid,
        creationflags=0,
    )

    try:
        _wait_for_server_startup(url)
    except Exception as e:
        proc.terminate()
        raise e

    yield url

    try:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=10)
    except Exception:
        os.killpg(proc.pid, signal.SIGKILL)
    finally:
        proc.wait(timeout=5)


# Test List Tools runs without errors
def test_list_tools(run_mcp_server):
    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    tools = mcp_client.list_tools()
    tool_names = {tool.name for tool in tools}
    assert "health" in tool_names
    assert "get_current_user" in tool_names
    assert "list_available_genie_spaces" in tool_names
    assert "get_genie_space_details" in tool_names
    assert "list_genie_space_tags" in tool_names
    assert "list_available_dashboards" in tool_names
    assert "get_dashboard_details" in tool_names
    assert "preview_genie_space_from_dashboard" in tool_names
    assert "create_genie_space_from_dashboard" in tool_names
    assert "get_user_name_from_id" in tool_names
    assert "get_genie_history_metrics" in tool_names
    assert "list_genie_benchmark_runs" in tool_names
    assert "get_genie_benchmark_run" in tool_names
    assert "list_genie_benchmark_run_results" in tool_names
    assert "get_genie_benchmark_result_details" in tool_names
    assert "grant_space_permissions" in tool_names
    assert "find_genie_spaces_by_tag" in tool_names
    assert "list_genie_space_conversations" in tool_names
    assert "list_genie_conversation_messages" in tool_names
    assert "list_genie_space_permissions" in tool_names
    assert "start_genie_serialization_job" in tool_names
    assert "get_genie_serialization_job_run" in tool_names
    assert "list_genie_space_restore_points" in tool_names
    assert "get_genie_restore_points_job_run" in tool_names
    assert "start_genie_space_restore_job" in tool_names
    assert "get_genie_space_restore_job_run" in tool_names


# Test no-argument tools run without errors
def test_call_no_argument_tools(run_mcp_server):
    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    for tool_name in [
        "health",
        "get_current_user",
        "list_available_genie_spaces",
        "list_available_dashboards",
    ]:
        result = mcp_client.call_tool(tool_name)
        _print_tool_result(tool_name, result)
        assert result is not None


def test_dashboard_detail_tool(run_mcp_server):
    dashboard_id = os.getenv("DASHBOARD_TEST_ID")
    if not dashboard_id:
        pytest.skip("Set DASHBOARD_TEST_ID to run dashboard detail integration tests")

    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    for tool_name in ["get_dashboard_details", "preview_genie_space_from_dashboard"]:
        result = mcp_client.call_tool(tool_name, {"dashboard_id": dashboard_id})
        _print_tool_result(tool_name, result)
        assert result is not None


def test_create_genie_space_from_dashboard_requires_confirmation(run_mcp_server):
    dashboard_id = os.getenv("DASHBOARD_TEST_ID")
    if not dashboard_id:
        pytest.skip("Set DASHBOARD_TEST_ID to run dashboard creation confirmation tests")

    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    result = mcp_client.call_tool(
        "create_genie_space_from_dashboard",
        {
            "dashboard_id": dashboard_id,
            "genie_space_title": "Integration Test Preview Only",
        },
    )
    _print_tool_result("create_genie_space_from_dashboard", result)
    assert result is not None


def test_get_user_name_from_id(run_mcp_server):
    user_id = os.getenv("DATABRICKS_TEST_USER_ID")
    if not user_id:
        pytest.skip("Set DATABRICKS_TEST_USER_ID to run user lookup integration tests")

    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    result = mcp_client.call_tool("get_user_name_from_id", {"user_id": user_id})
    _print_tool_result("get_user_name_from_id", result)
    assert result is not None


def test_genie_space_detail_tools(run_mcp_server):
    space_id = os.getenv("GENIE_TEST_SPACE_ID")
    if not space_id:
        pytest.skip("Set GENIE_TEST_SPACE_ID to run Genie Space detail integration tests")

    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    for tool_name in [
        "get_genie_space_details",
        "list_genie_space_tags",
        "list_genie_space_conversations",
        "list_genie_space_permissions",
        "get_genie_history_metrics",
        "list_genie_benchmark_runs",
    ]:
        result = mcp_client.call_tool(tool_name, {"space_id": space_id})
        _print_tool_result(tool_name, result)
        assert result is not None


def test_find_genie_spaces_by_tag(run_mcp_server):
    tag_key = os.getenv("GENIE_TEST_TAG_KEY")
    if not tag_key:
        pytest.skip("Set GENIE_TEST_TAG_KEY to run Genie tag search integration tests")

    arguments = {"tag_key": tag_key}
    tag_value = os.getenv("GENIE_TEST_TAG_VALUE")
    if tag_value:
        arguments["tag_value"] = tag_value

    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    result = mcp_client.call_tool("find_genie_spaces_by_tag", arguments)
    _print_tool_result("find_genie_spaces_by_tag", result)
    assert result is not None


def test_genie_conversation_messages(run_mcp_server):
    space_id = os.getenv("GENIE_TEST_SPACE_ID")
    conversation_id = os.getenv("GENIE_TEST_CONVERSATION_ID")
    if not space_id or not conversation_id:
        pytest.skip(
            "Set GENIE_TEST_SPACE_ID and GENIE_TEST_CONVERSATION_ID to run Genie message tests"
        )

    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    result = mcp_client.call_tool(
        "list_genie_conversation_messages",
        {"space_id": space_id, "conversation_id": conversation_id},
    )
    _print_tool_result("list_genie_conversation_messages", result)
    assert result is not None


def test_genie_benchmark_run_tools(run_mcp_server):
    space_id = os.getenv("GENIE_TEST_SPACE_ID")
    run_id = os.getenv("GENIE_TEST_BENCHMARK_RUN_ID")
    if not space_id or not run_id:
        pytest.skip("Set GENIE_TEST_SPACE_ID and GENIE_TEST_BENCHMARK_RUN_ID to run tests")

    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    for tool_name in ["get_genie_benchmark_run", "list_genie_benchmark_run_results"]:
        result = mcp_client.call_tool(tool_name, {"space_id": space_id, "run_id": run_id})
        _print_tool_result(tool_name, result)
        assert result is not None


def test_genie_benchmark_result_details(run_mcp_server):
    space_id = os.getenv("GENIE_TEST_SPACE_ID")
    run_id = os.getenv("GENIE_TEST_BENCHMARK_RUN_ID")
    result_id = os.getenv("GENIE_TEST_BENCHMARK_RESULT_ID")
    if not space_id or not run_id or not result_id:
        pytest.skip(
            "Set GENIE_TEST_SPACE_ID, GENIE_TEST_BENCHMARK_RUN_ID, and "
            "GENIE_TEST_BENCHMARK_RESULT_ID to run benchmark result detail tests"
        )

    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    result = mcp_client.call_tool(
        "get_genie_benchmark_result_details",
        {"space_id": space_id, "run_id": run_id, "result_id": result_id},
    )
    _print_tool_result("get_genie_benchmark_result_details", result)
    assert result is not None


def test_confirmation_required_tools(run_mcp_server):
    space_id = os.getenv("GENIE_TEST_SPACE_ID")
    dashboard_id = os.getenv("DASHBOARD_TEST_ID")
    if not space_id or not dashboard_id:
        pytest.skip("Set GENIE_TEST_SPACE_ID and DASHBOARD_TEST_ID to run confirmation tests")

    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    calls = [
        (
            "create_genie_space_from_dashboard",
            {
                "dashboard_id": dashboard_id,
                "genie_space_title": "Integration Test Preview Only",
            },
        ),
        (
            "grant_space_permissions",
            {
                "space_id": space_id,
                "user_name_list": ["nobody@example.com"],
                "permission_level": "CAN_READ",
            },
        ),
        ("start_genie_serialization_job", {"space_id": space_id}),
        (
            "start_genie_space_restore_job",
            {"space_id": space_id, "snapshot_date": "2026-01-01"},
        ),
    ]
    for tool_name, arguments in calls:
        result = mcp_client.call_tool(tool_name, arguments)
        _print_tool_result(tool_name, result)
        assert result is not None


def test_job_status_tools(run_mcp_server):
    run_id = os.getenv("DATABRICKS_TEST_JOB_RUN_ID")
    if not run_id:
        pytest.skip("Set DATABRICKS_TEST_JOB_RUN_ID to run job status integration tests")

    url = run_mcp_server
    mcp_client = DatabricksMCPClient(server_url=f"{url}/mcp")
    for tool_name in [
        "get_genie_serialization_job_run",
        "get_genie_restore_points_job_run",
        "get_genie_space_restore_job_run",
    ]:
        result = mcp_client.call_tool(tool_name, {"run_id": int(run_id)})
        _print_tool_result(tool_name, result)
        assert result is not None
