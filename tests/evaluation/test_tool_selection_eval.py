import asyncio

from tests.evaluation.generate_tool_selection_dataset import build_dataset
from tests.evaluation.run_tool_selection_eval import (
    DEFAULT_DATASET,
    RecordingMCPServer,
    _argument_matches,
    _clarification_detected,
    _confirmation_required_response,
    _load_dataset,
    _score_case,
    _validate_dataset,
)


def test_generated_dataset_is_valid() -> None:
    dataset = build_dataset()
    expected_inventory = [
        "find_atlan_assets_by_databricks_table",
        "get_atlan_context_for_databricks_table",
        "list_genie_space_conversations",
        "list_genie_conversation_messages",
        "list_genie_messages_for_conversations",
        "get_genie_usage_metrics",
        "start_genie_usage_metrics_query",
        "get_genie_usage_metrics_query_result",
        "list_genie_benchmark_runs",
        "get_genie_benchmark_run",
        "list_genie_benchmark_run_results",
        "get_genie_benchmark_result_details",
        "start_genie_serialization_job",
        "get_genie_serialization_job_run",
        "list_genie_space_restore_points",
        "get_genie_restore_points_job_run",
        "start_genie_space_restore_job",
        "get_genie_space_restore_job_run",
    ]
    expected_sequences = {
        "workflow-sequence-atlan-context",
        "workflow-sequence-conversation-review",
        "workflow-sequence-conversation-batch-review",
        "workflow-sequence-usage-async",
        "workflow-sequence-usage-and-conversations",
        "workflow-sequence-benchmark-drilldown",
        "workflow-sequence-benchmark-run-review",
        "workflow-sequence-snapshot-readiness",
        "workflow-sequence-restore-points-status",
        "workflow-sequence-restore-preflight",
    }

    _validate_dataset(dataset)

    assert dataset["tool_inventory"] == expected_inventory
    assert len(dataset["cases"]) == 50
    assert {
        case["id"] for case in dataset["cases"] if case["category"] == "tool_sequence"
    } == expected_sequences

    cases = {case["id"]: case for case in dataset["cases"]}
    assert cases["workflow-disambiguate-atlan-assets"]["expected_arguments"] == {
        "table_identifier": {
            "matcher": "exact",
            "value": "eval_catalog.eval_schema.eval_orders",
        }
    }
    assert cases["workflow-disambiguate-atlan-context"]["expected_arguments"] == {
        "table_identifier": {
            "matcher": "exact",
            "value": "eval_catalog.eval_schema.eval_orders",
        }
    }
    assert cases["workflow-start-genie-serialization-job-direct"][
        "allowed_tool_sequences"
    ] == [[]]
    assert cases["workflow-start-genie-space-restore-job-direct"][
        "allowed_tool_sequences"
    ] == [[]]
    assert cases["workflow-sequence-restore-preflight"]["allowed_tool_sequences"] == [
        ["list_genie_space_restore_points", "get_genie_restore_points_job_run"]
    ]


def test_checked_in_dataset_matches_generator() -> None:
    assert _load_dataset(DEFAULT_DATASET) == build_dataset()


def test_score_case_accepts_expected_tool_and_arguments() -> None:
    case = {
        "should_use_tool": True,
        "expected_tools": ["get_genie_space_details"],
        "forbidden_tools": ["list_genie_space_tags"],
        "expected_tool_sequence": ["get_genie_space_details"],
        "allowed_tool_sequences": [],
        "expected_arguments": {"space_id": {"matcher": "exact", "value": "space-1"}},
        "expected_response_behavior": "answer",
    }
    calls = [
        {
            "name": "get_genie_space_details",
            "arguments": {"space_id": "space-1"},
        }
    ]

    scores = _score_case(case, calls, "Detalles del espacio.", None)

    assert scores["passed"] is True


def test_score_case_accepts_an_alternative_sequence() -> None:
    case = {
        "should_use_tool": True,
        "expected_tools": ["start_query", "get_query"],
        "forbidden_tools": [],
        "expected_tool_sequence": ["start_query", "get_query"],
        "allowed_tool_sequences": [["get_metrics"]],
        "expected_arguments": {
            "start_query": {"space_id": {"matcher": "exact", "value": "space-1"}},
            "get_metrics": {"space_id": {"matcher": "exact", "value": "space-1"}},
        },
        "expected_response_behavior": "answer",
    }

    scores = _score_case(
        case,
        [{"name": "get_metrics", "arguments": {"space_id": "space-1"}}],
        "Metricas obtenidas.",
        None,
    )

    assert scores["selection_pass"] is True
    assert scores["arguments_pass"] is True
    assert scores["passed"] is True


def test_score_case_checks_alternative_sequence_arguments() -> None:
    case = {
        "should_use_tool": True,
        "expected_tools": ["start_query", "get_query"],
        "forbidden_tools": [],
        "expected_tool_sequence": ["start_query", "get_query"],
        "allowed_tool_sequences": [["get_metrics"]],
        "expected_arguments": {
            "start_query": {"space_id": {"matcher": "exact", "value": "space-1"}},
            "get_metrics": {"space_id": {"matcher": "exact", "value": "space-1"}},
        },
        "expected_response_behavior": "answer",
    }

    scores = _score_case(
        case,
        [{"name": "get_metrics", "arguments": {}}],
        "Metricas obtenidas.",
        None,
    )

    assert scores["selection_pass"] is True
    assert scores["arguments_pass"] is False
    assert scores["passed"] is False


def test_score_case_rejects_unexpected_confirmation() -> None:
    case = {
        "should_use_tool": True,
        "expected_tools": ["restore"],
        "forbidden_tools": [],
        "expected_tool_sequence": ["restore"],
        "allowed_tool_sequences": [],
        "expected_arguments": {
            "space_id": {"matcher": "exact", "value": "space-1"},
            "confirmation": {"matcher": "absent"},
        },
        "expected_response_behavior": "answer",
    }

    scores = _score_case(
        case,
        [
            {
                "name": "restore",
                "arguments": {"space_id": "space-1", "confirmation": "CONFIRM RESTORE"},
            }
        ],
        "Restore preparado.",
        None,
    )

    assert scores["arguments_pass"] is False
    assert scores["passed"] is False


def test_score_case_accepts_confirmation_request_without_tool_call() -> None:
    case = {
        "should_use_tool": True,
        "expected_tools": ["start_genie_space_restore_job"],
        "forbidden_tools": [],
        "expected_tool_sequence": ["start_genie_space_restore_job"],
        "allowed_tool_sequences": [[]],
        "expected_arguments": {
            "space_id": {"matcher": "exact", "value": "space-1"},
            "snapshot_date": {"matcher": "exact", "value": "2026-01-15"},
            "confirmation": {"matcher": "absent"},
        },
        "expected_response_behavior": "confirmation_required",
    }

    scores = _score_case(
        case,
        [],
        "The restore was not executed. Explicit confirmation is required.",
        None,
    )

    assert scores["selection_pass"] is True
    assert scores["tool_use_pass"] is True
    assert scores["arguments_pass"] is True
    assert scores["response_behavior_pass"] is True
    assert scores["passed"] is True


def test_score_case_requires_clarification_without_a_tool() -> None:
    case = {
        "should_use_tool": False,
        "expected_tools": [],
        "forbidden_tools": ["restore"],
        "expected_tool_sequence": [],
        "allowed_tool_sequences": [],
        "expected_arguments": {},
        "expected_response_behavior": "ask_clarification",
    }

    scores = _score_case(case, [], "Que space ID debo usar?", None)

    assert scores["response_behavior_pass"] is True
    assert scores["passed"] is True


def test_argument_exact_match_is_type_aware() -> None:
    assert _argument_matches(1, {"matcher": "exact", "value": True}) is False


def test_clarification_detector_rejects_declarative_text() -> None:
    assert _clarification_detected("No se proporciono el space ID.") is False


def test_confirmation_response_rejects_false_execution_claim() -> None:
    false_claims = (
        "Los permisos se han concedido correctamente. Falta la confirmacion.",
        "Permissions were granted. Confirmation is required.",
        "Permisos otorgados; se requiere confirmacion.",
        "Restauracion iniciada; confirmacion pendiente.",
        "No se ha solicitado confirmacion; los permisos se han concedido.",
        "No se ha concedido ningun permiso; despues se concedieron. Falta confirmacion.",
        "No se ha concedido ningun permiso; despues concedimos permisos. Falta confirmacion.",
        "Permissions have not been granted; I then granted them. Confirmation is required.",
    )

    assert all(not _confirmation_required_response(claim) for claim in false_claims)
    assert (
        _confirmation_required_response(
            "No se ha concedido ningun permiso; necesito confirmacion explicita."
        )
        is True
    )


def test_recording_server_returns_fixture_without_a_transport_call() -> None:
    server = RecordingMCPServer(
        synthetic_results={"health": {"status": "healthy"}},
        params={"url": "http://127.0.0.1:1/mcp"},
    )

    result = asyncio.run(server.call_tool("health", {}))

    assert result.structuredContent == {"status": "healthy"}
    assert server.calls[0]["synthetic"] is True
