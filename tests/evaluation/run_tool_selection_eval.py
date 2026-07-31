import argparse
import asyncio
import hashlib
import json
import os
import re
import time
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import mlflow
import yaml
from agents import Agent, ModelSettings, OpenAIChatCompletionsModel, RunConfig, Runner
from agents.mcp import MCPServerStreamableHttp, create_static_tool_filter
from databricks.sdk import WorkspaceClient
from mcp.types import CallToolResult, TextContent
from openai import AsyncOpenAI

from server.agent_config import AGENT_INSTRUCTIONS

DEFAULT_DATASET = Path(__file__).with_name("tool_selection_dataset.yaml")
DEFAULT_RESULTS_DIR = Path(__file__).with_name("results")
MISSING = object()
ARGUMENT_MATCHERS = {"exact", "one_of", "contains", "regex", "present", "absent"}
RESPONSE_BEHAVIORS = {
    "answer",
    "answer_without_tools",
    "ask_clarification",
    "confirmation_required",
}
CONFIRMATION_GATED_TOOLS = {
    "grant_space_permissions",
    "start_genie_serialization_job",
    "start_genie_space_restore_job",
}


class RecordingMCPServer(MCPServerStreamableHttp):
    """Expose live tool definitions while replacing every execution with a fixture."""

    def __init__(self, *, synthetic_results: dict[str, Any], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.synthetic_results = synthetic_results
        self.calls: list[dict[str, Any]] = []

    def reset_calls(self) -> None:
        self.calls.clear()

    async def call_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any] | None,
        meta: dict[str, Any] | None = None,
    ) -> CallToolResult:
        started = time.perf_counter()
        call: dict[str, Any] = {
            "name": tool_name,
            "arguments": deepcopy(arguments or {}),
            "meta": deepcopy(meta),
            "synthetic": True,
        }
        try:
            if tool_name not in self.synthetic_results:
                raise ValueError(f"No synthetic result configured for tool {tool_name!r}")
            payload = deepcopy(self.synthetic_results[tool_name])
            call["result"] = payload
            call["error"] = None
            return CallToolResult(
                content=[
                    TextContent(
                        type="text",
                        text=json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    )
                ],
                structuredContent=payload,
                isError=False,
            )
        except Exception as exc:
            call["error"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            call["latency_ms"] = round((time.perf_counter() - started) * 1000, 3)
            self.calls.append(call)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate MCP tool selection with live schemas and synthetic tool results."
    )
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--model", help="Override the model configured in the dataset")
    parser.add_argument(
        "--profile",
        default=os.getenv("DATABRICKS_CONFIG_PROFILE"),
        help="Databricks CLI profile; default authentication is used when omitted",
    )
    parser.add_argument(
        "--mcp-url",
        default=os.getenv("MCP_SERVER_URL", "http://127.0.0.1:8000/mcp"),
    )
    parser.add_argument("--case-id", action="append", default=[])
    parser.add_argument("--category", action="append", default=[])
    parser.add_argument("--tool", action="append", default=[], help="Filter tool_under_test")
    parser.add_argument("--tag", action="append", default=[])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--max-turns", type=int)
    parser.add_argument("--experiment-id", default=os.getenv("MLFLOW_EXPERIMENT_ID"))
    parser.add_argument("--experiment-name", default=os.getenv("MLFLOW_EXPERIMENT_NAME"))
    parser.add_argument("--no-tracing", action="store_true")
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and select cases without connecting to MCP or the model",
    )
    return parser.parse_args()


def _load_dataset(path: Path) -> dict[str, Any]:
    dataset = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(dataset, dict):
        raise ValueError("Dataset root must be a mapping")
    _validate_dataset(dataset)
    return dataset


def _validate_dataset(dataset: dict[str, Any]) -> None:
    required = {
        "version",
        "name",
        "execution_policy",
        "defaults",
        "tool_inventory",
        "synthetic_results",
        "cases",
    }
    missing_fields = sorted(required - dataset.keys())
    if missing_fields:
        raise ValueError(f"Dataset is missing fields: {', '.join(missing_fields)}")
    if dataset["execution_policy"].get("real_tool_execution") is not False:
        raise ValueError("Evaluation dataset must set real_tool_execution: false")

    inventory = dataset["tool_inventory"]
    if not isinstance(inventory, list) or not all(isinstance(name, str) for name in inventory):
        raise ValueError("tool_inventory must be a list of tool names")
    if len(inventory) != len(set(inventory)):
        raise ValueError("tool_inventory contains duplicates")
    if not isinstance(dataset["synthetic_results"], dict):
        raise ValueError("synthetic_results must be a mapping")
    synthetic_names = set(dataset["synthetic_results"])
    if synthetic_names != set(inventory):
        raise ValueError(
            "Synthetic result inventory mismatch: "
            f"missing={sorted(set(inventory) - synthetic_names)}, "
            f"extra={sorted(synthetic_names - set(inventory))}"
        )

    if not isinstance(dataset["cases"], list):
        raise ValueError("cases must be a list")

    required_case_fields = {
        "id",
        "category",
        "tool_under_test",
        "tags",
        "user_prompt",
        "expected_tools",
        "forbidden_tools",
        "expected_tool_sequence",
        "allowed_tool_sequences",
        "should_use_tool",
        "expected_arguments",
        "expected_response_behavior",
    }
    case_ids: set[str] = set()
    known_tools = set(inventory)
    for case in dataset["cases"]:
        if not isinstance(case, dict):
            raise ValueError("Every evaluation case must be a mapping")
        missing_case_fields = sorted(required_case_fields - case.keys())
        if missing_case_fields:
            raise ValueError(f"Case is missing fields: {', '.join(missing_case_fields)}")
        case_id = case.get("id")
        if not isinstance(case_id, str) or not case_id or case_id in case_ids:
            raise ValueError(f"Missing or duplicate case ID: {case_id!r}")
        case_ids.add(case_id)
        if not isinstance(case["category"], str) or not case["category"]:
            raise ValueError(f"Case {case_id} has an invalid category")
        if not isinstance(case["user_prompt"], str) or not case["user_prompt"].strip():
            raise ValueError(f"Case {case_id} has an invalid user_prompt")
        if case["tool_under_test"] is not None and case["tool_under_test"] not in known_tools:
            raise ValueError(f"Case {case_id} has an unknown tool_under_test")
        if not isinstance(case["should_use_tool"], bool):
            raise ValueError(f"Case {case_id} should_use_tool must be boolean")
        if case["expected_response_behavior"] not in RESPONSE_BEHAVIORS:
            raise ValueError(f"Case {case_id} has an invalid expected_response_behavior")
        for field in (
            "tags",
            "expected_tools",
            "forbidden_tools",
            "expected_tool_sequence",
            "allowed_tool_sequences",
        ):
            if not isinstance(case[field], list):
                raise ValueError(f"Case {case_id} field {field} must be a list")
        if not isinstance(case["expected_arguments"], dict):
            raise ValueError(f"Case {case_id} expected_arguments must be a mapping")
        if not all(isinstance(tag, str) for tag in case["tags"]):
            raise ValueError(f"Case {case_id} tags must contain strings")
        for field in ("expected_tools", "forbidden_tools", "expected_tool_sequence"):
            if not all(isinstance(name, str) for name in case[field]):
                raise ValueError(f"Case {case_id} field {field} must contain tool names")
        if not all(isinstance(sequence, list) for sequence in case["allowed_tool_sequences"]):
            raise ValueError(f"Case {case_id} allowed_tool_sequences must contain lists")
        if not all(
            all(isinstance(name, str) for name in sequence)
            for sequence in case["allowed_tool_sequences"]
        ):
            raise ValueError(f"Case {case_id} allowed sequences must contain tool names")

        primary_sequence = case["expected_tool_sequence"] or case["expected_tools"]
        if bool(primary_sequence) != case["should_use_tool"]:
            raise ValueError(f"Case {case_id} has inconsistent should_use_tool and expectations")
        if any(not sequence for sequence in case["allowed_tool_sequences"]):
            raise ValueError(f"Case {case_id} has an empty allowed tool sequence")
        referenced_tools = set(case.get("expected_tools", []))
        referenced_tools.update(case.get("forbidden_tools", []))
        referenced_tools.update(case.get("expected_tool_sequence", []))
        for sequence in case.get("allowed_tool_sequences", []):
            referenced_tools.update(sequence)
        unknown = sorted(referenced_tools - known_tools)
        if unknown:
            raise ValueError(f"Case {case_id} references unknown tools: {unknown}")

        arguments_by_tool = _expected_arguments_by_tool(case)
        unknown_argument_tools = sorted(set(arguments_by_tool) - known_tools)
        if unknown_argument_tools:
            raise ValueError(
                f"Case {case_id} has argument rules for unknown tools: {unknown_argument_tools}"
            )
        for tool_name, rules in arguments_by_tool.items():
            if not isinstance(rules, dict):
                raise ValueError(f"Case {case_id} argument rules for {tool_name} must be a mapping")
            for argument_name, rule in rules.items():
                _validate_argument_rule(case_id, tool_name, argument_name, rule)
        expected_or_allowed_tools = set(primary_sequence)
        for sequence in case["allowed_tool_sequences"]:
            expected_or_allowed_tools.update(sequence)
        for tool_name in expected_or_allowed_tools.intersection(CONFIRMATION_GATED_TOOLS):
            confirmation_rule = arguments_by_tool.get(tool_name, {}).get("confirmation")
            if confirmation_rule != {"matcher": "absent"}:
                raise ValueError(
                    f"Case {case_id} must require confirmation to be absent for {tool_name}"
                )
            if case["expected_response_behavior"] != "confirmation_required":
                raise ValueError(
                    f"Case {case_id} must expect confirmation_required for {tool_name}"
                )

    for tool_name in inventory:
        tool_cases = [case for case in dataset["cases"] if case.get("tool_under_test") == tool_name]
        missing_coverage = {
            tag: f"{sum(tag in case.get('tags', []) for case in tool_cases)}/{minimum}"
            for tag, minimum in dataset.get("coverage_requirements_per_tool", {}).items()
            if sum(tag in case.get("tags", []) for case in tool_cases) < minimum
        }
        if missing_coverage:
            raise ValueError(f"Coverage missing for {tool_name}: {missing_coverage}")


def _select_cases(cases: list[dict[str, Any]], args: argparse.Namespace) -> list[dict[str, Any]]:
    selected = cases
    if args.case_id:
        wanted = set(args.case_id)
        selected = [case for case in selected if case["id"] in wanted]
        missing = wanted - {case["id"] for case in selected}
        if missing:
            raise ValueError(f"Unknown case IDs: {sorted(missing)}")
    if args.category:
        selected = [case for case in selected if case["category"] in set(args.category)]
    if args.tool:
        selected = [case for case in selected if case["tool_under_test"] in set(args.tool)]
    if args.tag:
        tags = set(args.tag)
        selected = [case for case in selected if tags.issubset(case.get("tags", []))]
    if args.limit is not None:
        if args.limit < 1:
            raise ValueError("--limit must be at least 1")
        selected = selected[: args.limit]
    if not selected:
        raise ValueError("No evaluation cases matched the filters")
    return selected


def _workspace_client(profile: str | None) -> WorkspaceClient:
    return WorkspaceClient(profile=profile) if profile else WorkspaceClient()


def _databricks_openai_client(workspace_client: WorkspaceClient) -> AsyncOpenAI:
    async def api_key() -> str:
        headers = await asyncio.to_thread(workspace_client.config.authenticate)
        authorization = headers.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            raise RuntimeError("Databricks authentication did not return a Bearer token")
        return authorization.removeprefix("Bearer ")

    return AsyncOpenAI(
        api_key=api_key,
        base_url=f"{workspace_client.config.host.rstrip('/')}/serving-endpoints",
        timeout=60,
        max_retries=0,
    )


def _configure_mlflow(
    profile: str | None,
    user_name: str,
    experiment_id: str | None,
    experiment_name: str | None,
) -> dict[str, str]:
    mlflow.set_tracking_uri(
        os.getenv("MLFLOW_TRACKING_URI") or (f"databricks://{profile}" if profile else "databricks")
    )
    if experiment_id:
        experiment = mlflow.set_experiment(experiment_id=experiment_id)
    else:
        experiment = mlflow.set_experiment(
            experiment_name=experiment_name or f"/Users/{user_name}/mcp-tool-selection-eval"
        )
    mlflow.openai.autolog(disable_openai_agent_tracer=True)
    return {"id": experiment.experiment_id, "name": experiment.name}


def _tool_definitions(tools: list[Any]) -> list[dict[str, Any]]:
    return [tool.model_dump(by_alias=True, exclude_none=True, mode="json") for tool in tools]


def _definitions_digest(tool_definitions: list[dict[str, Any]]) -> str:
    encoded = json.dumps(tool_definitions, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _accepted_sequences(case: dict[str, Any]) -> list[list[str]]:
    primary = case.get("expected_tool_sequence") or case.get("expected_tools") or []
    sequences = [primary]
    sequences.extend(case.get("allowed_tool_sequences", []))
    return sequences


def _expected_arguments_by_tool(case: dict[str, Any]) -> dict[str, dict[str, Any]]:
    expected = case.get("expected_arguments") or {}
    if not expected:
        return {}
    if all(isinstance(rule, dict) and "matcher" in rule for rule in expected.values()):
        sequence = case.get("expected_tool_sequence") or case.get("expected_tools") or []
        return {sequence[0]: expected} if sequence else {}
    return expected


def _validate_argument_rule(case_id: str, tool_name: str, argument_name: str, rule: Any) -> None:
    if not isinstance(argument_name, str) or not isinstance(rule, dict):
        raise ValueError(f"Case {case_id} has an invalid argument rule for {tool_name}")
    matcher = rule.get("matcher")
    if matcher not in ARGUMENT_MATCHERS:
        raise ValueError(
            f"Case {case_id} has unsupported matcher {matcher!r} for {tool_name}.{argument_name}"
        )
    if matcher not in {"present", "absent"} and "value" not in rule:
        raise ValueError(f"Case {case_id} matcher {matcher} requires a value")
    if matcher == "one_of" and not isinstance(rule.get("value"), list):
        raise ValueError(f"Case {case_id} one_of value must be a list")
    if matcher == "regex":
        if not isinstance(rule.get("value"), str):
            raise ValueError(f"Case {case_id} regex value must be a string")
        try:
            re.compile(rule["value"])
        except re.error as exc:
            raise ValueError(f"Case {case_id} has an invalid regex: {exc}") from exc


def _json_exact(actual: Any, expected: Any) -> bool:
    if type(actual) is not type(expected):
        return False
    if isinstance(actual, dict):
        return actual.keys() == expected.keys() and all(
            _json_exact(actual[key], expected[key]) for key in actual
        )
    if isinstance(actual, list):
        return len(actual) == len(expected) and all(
            _json_exact(actual_value, expected_value)
            for actual_value, expected_value in zip(actual, expected, strict=True)
        )
    return actual == expected


def _argument_matches(actual: Any, rule: dict[str, Any]) -> bool:
    matcher = rule.get("matcher", "exact")
    expected = rule.get("value")
    if matcher == "present":
        return actual is not MISSING
    if matcher == "absent":
        return actual is MISSING
    if actual is MISSING:
        return False
    if matcher == "exact":
        return _json_exact(actual, expected)
    if matcher == "one_of":
        return any(_json_exact(actual, candidate) for candidate in expected)
    if matcher == "contains":
        if isinstance(actual, dict) and isinstance(expected, dict):
            return all(
                key in actual and _json_exact(actual[key], value) for key, value in expected.items()
            )
        try:
            return expected in actual
        except TypeError:
            return False
    if matcher == "regex":
        return isinstance(actual, str) and re.fullmatch(str(expected), actual) is not None
    raise ValueError(f"Unsupported argument matcher: {matcher!r}")


def _clarification_detected(output: str) -> bool:
    normalized = output.casefold()
    if "?" in normalized or "¿" in normalized:
        return True
    request_patterns = (
        r"\b(?:por favor,?\s+)?(?:indica|proporciona|facilita|especifica|aclara)\b",
        r"\bnecesito que (?:me )?(?:indiques|proporciones|facilites|especifiques|aclares)\b",
        r"\b(?:please provide|please specify|could you provide|could you specify)\b",
        r"\b(?:please send|please share|send me|share the|provide the|specify the)\b",
        r"\bi need (?:the|a|an|your)\b",
    )
    return any(re.search(pattern, normalized) for pattern in request_patterns)


def _confirmation_required_response(output: str) -> bool:
    normalized = output.casefold()
    confirmation_markers = ("confirm", "aprob", "approval")
    non_execution_patterns = (
        r"\bno\s+(?:se\s+)?(?:(?:ha|han|he|hemos|fue|fueron)\s+)?"
        r"(?:conced|otorg|inici|ejecut|restaur|realiz|aplic)\w*",
        r"\bsin\s+(?:conceder|otorgar|iniciar|ejecutar|restaurar|realizar|aplicar)",
        r"\b(?:has|have|was|were|is|are)?\s*not\s+(?:been\s+)?"
        r"(?:granted|started|executed|restored|applied|performed)",
        r"\bwithout\s+(?:granting|starting|executing|restoring|applying|performing)",
        r"\bwill\s+(?:only\s+)?(?:grant|start|execute|restore|apply|perform)\b.*"
        r"\b(?:after|once)\b.*\bconfirm",
    )
    execution_claim_patterns = (
        r"\b(?:se\s+)?(?:ha|han|he|hemos|fue|fueron)\s+"
        r"(?:conced|otorg|inici|ejecut|restaur|realiz|aplic)\w*",
        r"\b(?:permisos?|acceso|job|trabajo|restauraci[oó]n)\s+"
        r"(?:conced|otorg|inici|ejecut|restaur|realiz|aplic)\w*",
        r"\b(?:concedieron|otorgaron|iniciaron|ejecutaron|restauraron|realizaron|aplicaron)\b",
        r"\b(?:conced|otorg|inici|ejecut|restaur|realiz|aplic)"
        r"(?:e|é|amos|o|ó|aron|i|í|imos|io|ió|ieron|ado|ada|ados|adas)\b",
        r"\b(?:was|were|has been|have been)\s+"
        r"(?:granted|started|executed|restored|applied|performed)\b",
        r"\b(?:i|we|the agent|the system|the tool|it|they)\s+(?:then\s+)?"
        r"(?:granted|started|executed|restored|applied|performed)\b",
        r"\bsuccessfully\s+(?:granted|started|executed|restored|applied|performed)\b",
    )
    has_confirmation = any(marker in normalized for marker in confirmation_markers)
    explicitly_not_executed = any(
        re.search(pattern, normalized) for pattern in non_execution_patterns
    )
    text_without_negated_actions = normalized
    for pattern in non_execution_patterns:
        text_without_negated_actions = re.sub(pattern, "", text_without_negated_actions)
    claims_execution = any(
        re.search(pattern, text_without_negated_actions) for pattern in execution_claim_patterns
    )
    return (
        bool(output.strip())
        and has_confirmation
        and explicitly_not_executed
        and not claims_execution
    )


def _score_case(
    case: dict[str, Any],
    calls: list[dict[str, Any]],
    final_output: str,
    error: str | None,
) -> dict[str, Any]:
    actual_sequence = [call["name"] for call in calls]
    accepted_sequences = _accepted_sequences(case)
    selection_pass = actual_sequence in accepted_sequences
    primary_selection_pass = actual_sequence == accepted_sequences[0]
    used_allowed_sequence = selection_pass and not primary_selection_pass
    forbidden_calls = [
        name for name in actual_sequence if name in set(case.get("forbidden_tools", []))
    ]
    forbidden_tools_pass = not forbidden_calls
    tool_use_pass = bool(actual_sequence) is bool(case["should_use_tool"])

    argument_checks: list[dict[str, Any]] = []
    for tool_name, rules in _expected_arguments_by_tool(case).items():
        matching_calls = [call for call in calls if call["name"] == tool_name]
        if not matching_calls and selection_pass and tool_name not in actual_sequence:
            continue
        if not matching_calls:
            argument_checks.append(
                {"tool": tool_name, "argument": None, "passed": False, "reason": "tool_not_called"}
            )
            continue
        actual_arguments = matching_calls[0]["arguments"]
        for argument_name, rule in rules.items():
            actual_value = actual_arguments.get(argument_name, MISSING)
            passed = _argument_matches(actual_value, rule)
            argument_checks.append(
                {
                    "tool": tool_name,
                    "argument": argument_name,
                    "matcher": rule.get("matcher", "exact"),
                    "expected": rule.get("value"),
                    "actual": None if actual_value is MISSING else actual_value,
                    "actual_present": actual_value is not MISSING,
                    "passed": passed,
                }
            )
    arguments_pass = all(check["passed"] for check in argument_checks)

    behavior = case.get("expected_response_behavior", "answer")
    if behavior == "ask_clarification":
        behavior_pass = (
            not calls and bool(final_output.strip()) and _clarification_detected(final_output)
        )
    elif behavior == "answer_without_tools":
        behavior_pass = not calls and bool(final_output.strip())
    elif behavior == "confirmation_required":
        behavior_pass = _confirmation_required_response(final_output)
    else:
        behavior_pass = bool(final_output.strip())

    passed = all(
        (
            error is None,
            selection_pass,
            forbidden_tools_pass,
            tool_use_pass,
            arguments_pass,
            behavior_pass,
        )
    )
    return {
        "passed": passed,
        "selection_pass": selection_pass,
        "primary_selection_pass": primary_selection_pass,
        "used_allowed_sequence": used_allowed_sequence,
        "forbidden_tools_pass": forbidden_tools_pass,
        "tool_use_pass": tool_use_pass,
        "arguments_pass": arguments_pass,
        "response_behavior_pass": behavior_pass,
        "accepted_tool_sequences": accepted_sequences,
        "actual_tool_sequence": actual_sequence,
        "forbidden_calls": forbidden_calls,
        "argument_checks": argument_checks,
    }


def _usage_dict(result: Any) -> dict[str, Any]:
    usage = result.context_wrapper.usage
    return {
        "requests": usage.requests,
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "total_tokens": usage.total_tokens,
    }


def _trace_details(trace_id: str | None) -> dict[str, Any]:
    details: dict[str, Any] = {"trace_id": trace_id}
    if not trace_id:
        return details
    try:
        trace = mlflow.get_trace(trace_id=trace_id, flush=True)
        if trace is None:
            return details
        details["execution_duration_ms"] = trace.info.execution_duration
        details["token_usage"] = trace.info.token_usage
        details["estimated_cost"] = trace.info.cost
    except Exception as exc:
        details["lookup_error"] = f"{type(exc).__name__}: {exc}"
    return details


def _output_path(path: Path | None) -> Path:
    if path:
        return path
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return DEFAULT_RESULTS_DIR / f"tool-selection-{timestamp}.jsonl"


def _aggregate(
    records: list[dict[str, Any]],
    *,
    dataset: dict[str, Any],
    dataset_path: Path,
    output_path: Path,
    tool_definitions: list[dict[str, Any]],
    configuration: dict[str, Any],
    started_at: str,
    latency_ms: float,
    selected_count: int,
) -> dict[str, Any]:
    metric_names = (
        "passed",
        "selection_pass",
        "primary_selection_pass",
        "used_allowed_sequence",
        "forbidden_tools_pass",
        "tool_use_pass",
        "arguments_pass",
        "response_behavior_pass",
    )

    def group_summary(group: list[dict[str, Any]]) -> dict[str, Any]:
        total = len(group)
        return {
            "total": total,
            **{
                name: {
                    "count": sum(bool(record["scores"][name]) for record in group),
                    "rate": round(sum(bool(record["scores"][name]) for record in group) / total, 4)
                    if total
                    else 0.0,
                }
                for name in metric_names
            },
        }

    by_category: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_tool: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        by_category[record["category"]].append(record)
        if record["tool_under_test"]:
            by_tool[record["tool_under_test"]].append(record)

    return {
        "dataset": {
            "name": dataset["name"],
            "version": dataset["version"],
            "review_status": dataset.get("review_status"),
            "path": str(dataset_path),
            "sha256": hashlib.sha256(dataset_path.read_bytes()).hexdigest(),
        },
        "started_at": started_at,
        "completed_at": datetime.now(UTC).isoformat(),
        "latency_ms": round(latency_ms, 3),
        "output_path": str(output_path),
        "configuration": configuration,
        "tool_definitions": tool_definitions,
        "selected_cases": selected_count,
        "executed_cases": len(records),
        "skipped_cases": selected_count - len(records),
        "results": group_summary(records),
        "errors": sum(record["error"] is not None for record in records),
        "tokens": {
            key: sum(record["usage"].get(key, 0) for record in records)
            for key in ("requests", "input_tokens", "output_tokens", "total_tokens")
        },
        "estimated_cost": sum(
            float((record["trace"].get("estimated_cost") or {}).get("total_cost", 0) or 0)
            for record in records
        ),
        "by_category": {name: group_summary(group) for name, group in sorted(by_category.items())},
        "by_tool": {name: group_summary(group) for name, group in sorted(by_tool.items())},
    }


async def _run(args: argparse.Namespace) -> bool:
    dataset = _load_dataset(args.dataset)
    cases = _select_cases(dataset["cases"], args)
    category_counts = dict(Counter(case["category"] for case in cases))
    print(f"Dataset: {dataset['name']} v{dataset['version']} ({len(dataset['cases'])} cases)")
    print(f"Selected: {len(cases)} cases {category_counts}")
    if args.dry_run:
        print("Dry run complete; no MCP or model connection was opened.")
        return True

    defaults = dataset["defaults"]
    model_name = args.model or defaults["model"]
    max_turns = args.max_turns or defaults["max_turns"]
    model_retries = defaults.get("model_retries", 0)
    mcp_retries = defaults.get("mcp_retries", 0)
    output_path = _output_path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    workspace_client = _workspace_client(args.profile)
    current_user = workspace_client.current_user.me()
    experiment = None
    if not args.no_tracing:
        experiment = _configure_mlflow(
            args.profile,
            current_user.user_name,
            args.experiment_id,
            args.experiment_name,
        )

    started_at = datetime.now(UTC).isoformat()
    run_started = time.perf_counter()
    records: list[dict[str, Any]] = []
    async with _databricks_openai_client(workspace_client) as model_client:
        model = OpenAIChatCompletionsModel(model=model_name, openai_client=model_client)
        async with RecordingMCPServer(
            synthetic_results=dataset["synthetic_results"],
            name="local-genie-mcp-synthetic-eval",
            params={"url": args.mcp_url, "timeout": 60},
            cache_tools_list=True,
            client_session_timeout_seconds=60,
            max_retry_attempts=mcp_retries,
            tool_filter=create_static_tool_filter(allowed_tool_names=dataset["tool_inventory"]),
        ) as mcp_server:
            tools = await mcp_server.list_tools()
            tool_definitions = _tool_definitions(tools)
            live_names = [tool["name"] for tool in tool_definitions]
            if set(live_names) != set(dataset["tool_inventory"]):
                raise ValueError(
                    "Live MCP inventory mismatch: "
                    f"missing={sorted(set(dataset['tool_inventory']) - set(live_names))}, "
                    f"extra={sorted(set(live_names) - set(dataset['tool_inventory']))}"
                )
            definitions_digest = _definitions_digest(tool_definitions)
            agent = Agent(
                name="Local Databricks MCP tool-selection evaluator",
                instructions=AGENT_INSTRUCTIONS,
                model=model,
                mcp_servers=[mcp_server],
                model_settings=ModelSettings(
                    tool_choice="auto",
                    parallel_tool_calls=False,
                    include_usage=True,
                    retry={"max_retries": model_retries},
                ),
            )
            configuration = {
                "model": model_name,
                "profile": args.profile,
                "mcp_url": args.mcp_url,
                "max_turns": max_turns,
                "model_retries": model_retries,
                "mcp_retries": mcp_retries,
                "synthetic_only": True,
                "tracing_enabled": not args.no_tracing,
                "mlflow_experiment": experiment,
                "available_tools": live_names,
                "tool_definitions_sha256": definitions_digest,
            }

            with output_path.open("w", encoding="utf-8") as output_file:
                for index, case in enumerate(cases, start=1):
                    mcp_server.reset_calls()
                    case_started = time.perf_counter()
                    result = None
                    error = None
                    trace_id = None
                    previous_trace_id = (
                        mlflow.get_last_active_trace_id() if not args.no_tracing else None
                    )
                    try:
                        result = await Runner.run(
                            agent,
                            case["user_prompt"],
                            max_turns=max_turns,
                            run_config=RunConfig(
                                tracing_disabled=args.no_tracing,
                                workflow_name="MCP tool-selection evaluation",
                                trace_metadata={
                                    "evaluation_dataset": dataset["name"],
                                    "evaluation_case_id": case["id"],
                                },
                            ),
                        )
                    except Exception as exc:
                        error = f"{type(exc).__name__}: {exc}"
                    finally:
                        if not args.no_tracing:
                            latest_trace_id = mlflow.get_last_active_trace_id()
                            if latest_trace_id != previous_trace_id:
                                trace_id = latest_trace_id

                    if trace_id:
                        for key, value in {
                            "evaluation.dataset": dataset["name"],
                            "evaluation.case_id": case["id"],
                        }.items():
                            try:
                                mlflow.set_trace_tag(trace_id, key, value)
                            except Exception as exc:
                                print(f"Warning: could not tag trace {trace_id}: {exc}")

                    final_output = (
                        str(result.final_output)
                        if result is not None and result.final_output is not None
                        else ""
                    )
                    trace = _trace_details(trace_id) if not args.no_tracing else {}
                    usage = _usage_dict(result) if result is not None else {}
                    if not usage and trace.get("token_usage"):
                        trace_usage = trace["token_usage"]
                        usage = {
                            "requests": 0,
                            "input_tokens": trace_usage.get("input_tokens", 0),
                            "output_tokens": trace_usage.get("output_tokens", 0),
                            "total_tokens": trace_usage.get("total_tokens", 0),
                        }
                    calls = deepcopy(mcp_server.calls)
                    scores = _score_case(case, calls, final_output, error)
                    record = {
                        "case_id": case["id"],
                        "category": case["category"],
                        "tool_under_test": case["tool_under_test"],
                        "tags": case.get("tags", []),
                        "prompt": case["user_prompt"],
                        "expectations": {
                            "expected_tools": case.get("expected_tools", []),
                            "forbidden_tools": case.get("forbidden_tools", []),
                            "expected_tool_sequence": case.get("expected_tool_sequence", []),
                            "allowed_tool_sequences": case.get("allowed_tool_sequences", []),
                            "expected_arguments": case.get("expected_arguments", {}),
                            "expected_response_behavior": case.get(
                                "expected_response_behavior", "answer"
                            ),
                        },
                        "available_tools": live_names,
                        "tool_definitions_sha256": definitions_digest,
                        "tool_calls": calls,
                        "final_output": final_output,
                        "error": error,
                        "latency_ms": round((time.perf_counter() - case_started) * 1000, 3),
                        "usage": usage,
                        "model_responses": len(result.raw_responses) if result is not None else 0,
                        "observed_model_retries": max(
                            0, usage.get("requests", 0) - len(result.raw_responses)
                        )
                        if result is not None
                        else 0,
                        "observed_mcp_retries": 0,
                        "configuration": {
                            "model": model_name,
                            "max_turns": max_turns,
                            "model_retries": model_retries,
                            "mcp_retries": mcp_retries,
                            "synthetic_only": True,
                        },
                        "trace": trace,
                        "scores": scores,
                    }
                    records.append(record)
                    output_file.write(json.dumps(record, ensure_ascii=False) + "\n")
                    output_file.flush()
                    outcome = "PASS" if scores["passed"] else "FAIL"
                    print(f"[{index}/{len(cases)}] {outcome} {case['id']}")
                    if not scores["passed"] and args.fail_fast:
                        break

    summary = _aggregate(
        records,
        dataset=dataset,
        dataset_path=args.dataset,
        output_path=output_path,
        tool_definitions=tool_definitions,
        configuration=configuration,
        started_at=started_at,
        latency_ms=(time.perf_counter() - run_started) * 1000,
        selected_count=len(cases),
    )
    summary_path = output_path.with_suffix(".summary.json")
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    passed = summary["results"]["passed"]
    print(f"Passed: {passed['count']}/{summary['results']['total']} ({passed['rate']:.1%})")
    print(f"Results: {output_path}")
    print(f"Summary: {summary_path}")
    return (
        len(records) == len(cases)
        and summary["errors"] == 0
        and passed["count"] == summary["results"]["total"]
    )


def main() -> None:
    if not asyncio.run(_run(_parse_args())):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
