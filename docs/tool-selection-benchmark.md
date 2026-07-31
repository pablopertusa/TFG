# MCP Tool-Selection Benchmark

## Purpose

This benchmark evaluates whether a language model can decide when to use an MCP tool, select the correct tool, construct valid arguments, and execute dependent tools in the correct order. It also measures abstention, clarification behavior when required parameters are missing, and safe handling of operations that require explicit confirmation.

The benchmark does not test the functional availability of the real Databricks or Atlan APIs. Tool definitions are discovered from the live local MCP server, but every tool execution is intercepted and answered with deterministic synthetic fixtures.

The benchmark supports the following goals:

- Compare models against identical prompts and expectations.
- Identify confusion between semantically similar tools.
- Measure argument-generation accuracy.
- Validate multi-step workflows that depend on earlier tool results.
- Detect fabricated identifiers and unsafe confirmation handling.
- Record reproducible accuracy, token, cost, and latency metrics.
- Produce machine-readable evidence for later analysis and academic reporting.

## Evaluated Scope

The active inventory contains 18 tools from the expected operational workflows.

### Atlan

- `find_atlan_assets_by_databricks_table`
- `get_atlan_context_for_databricks_table`

### Conversations and Messages

- `list_genie_space_conversations`
- `list_genie_conversation_messages`
- `list_genie_messages_for_conversations`

### Usage Metrics

- `get_genie_usage_metrics`
- `start_genie_usage_metrics_query`
- `get_genie_usage_metrics_query_result`

### Genie Benchmarks

- `list_genie_benchmark_runs`
- `get_genie_benchmark_run`
- `list_genie_benchmark_run_results`
- `get_genie_benchmark_result_details`

### Snapshots, Serialization, and Restore

- `start_genie_serialization_job`
- `get_genie_serialization_job_run`
- `list_genie_space_restore_points`
- `get_genie_restore_points_job_run`
- `start_genie_space_restore_job`
- `get_genie_space_restore_job_run`

Health, identity, general space discovery, tags, and permissions are outside the benchmark scope. They remain available on the MCP server, but the runner filters them out before exposing tools to the model.

## Components

| File | Responsibility |
|---|---|
| `tests/evaluation/generate_tool_selection_dataset.py` | Defines scope, profiles, fixtures, and initial cases |
| `tests/evaluation/tool_selection_dataset.yaml` | Explicit editable dataset consumed by the runner |
| `tests/evaluation/run_tool_selection_eval.py` | Validation, synthetic execution, scoring, and export |
| `tests/evaluation/test_tool_selection_eval.py` | Dataset, scoring, and interception regression tests |
| `server/agent_config.py` | Shared agent instructions |
| `tests/evaluation/results/` | Versionable JSONL and JSON execution artifacts |

The design history and methodological rationale are documented in [`benchmark-methodology-and-rationale.md`](benchmark-methodology-and-rationale.md).

## Dataset Generation

The generator defines one natural direct workflow request for every active tool and adds curated cross-tool cases. It avoids repetitive theory prompts and synthetic wording that tells the model which internal tool name to use.

| Category | Cases | Purpose |
|---|---:|---|
| `single_tool` | 18 | One representative workplace request per tool |
| `disambiguation` | 8 | Distinguish semantically close operations |
| `ambiguous` | 8 | Request missing identifiers instead of fabricating them |
| `abstention` | 6 | Complete tasks from supplied information without tools |
| `tool_sequence` | 10 | Execute realistic multi-step workflows |

The resulting dataset contains exactly 50 English cases. Every active tool has at least one positive direct case, while additional depth is concentrated on the workflows where tool choice, identifier propagation, and safety matter most.

Regenerate it with:

```bash
uv run python tests/evaluation/generate_tool_selection_dataset.py
```

This command replaces `tool_selection_dataset.yaml`. Permanent manual changes must also be incorporated into the generator before regeneration.

## Case Schema

A simplified case looks like this:

```yaml
- id: get-genie-usage-metrics-positive-clear-01
  category: single_tool
  tool_under_test: get_genie_usage_metrics
  tags:
    - positive
    - clear
  user_prompt: Obtain usage metrics for Genie Space ...
  expected_tools:
    - get_genie_usage_metrics
  allowed_tools: []
  forbidden_tools:
    - start_genie_usage_metrics_query
  expected_tool_sequence:
    - get_genie_usage_metrics
  allowed_tool_sequences: []
  should_use_tool: true
  expected_arguments:
    space_id:
      matcher: exact
      value: 01f00000000000000000000000000000
  expected_response_behavior: answer
```

| Field | Meaning |
|---|---|
| `id` | Stable unique case identifier |
| `category` | Aggregation and filtering category |
| `tool_under_test` | Tool receiving coverage attribution |
| `tags` | Cross-cutting coverage labels and filters |
| `user_prompt` | Input sent to the agent |
| `expected_tools` | Tools expected in the run |
| `forbidden_tools` | Tools that must not be selected |
| `expected_tool_sequence` | Exact preferred sequence |
| `allowed_tool_sequences` | Complete accepted alternatives, including an explicitly safe empty sequence |
| `should_use_tool` | Whether the preferred path uses a tool; an allowed empty sequence may still accept no call |
| `expected_arguments` | Argument validation rules |
| `expected_response_behavior` | Required final-answer behavior |

Supported categories are `single_tool`, `disambiguation`, `abstention`, `negative`, `ambiguous`, and `tool_sequence`.

## Argument Matchers

| Matcher | Meaning |
|---|---|
| `exact` | JSON value and type must match |
| `one_of` | Actual value must match one declared option |
| `contains` | Actual value must contain a value or mapping subset |
| `regex` | String must fully match the regular expression |
| `present` | Argument must exist |
| `absent` | Argument must not exist |

Single-tool cases declare arguments directly. Sequence cases group rules by tool name:

```yaml
expected_arguments:
  start_genie_usage_metrics_query:
    space_id:
      matcher: exact
      value: 01f00000000000000000000000000000
  get_genie_usage_metrics_query_result:
    statement_id:
      matcher: exact
      value: 11111111-2222-4333-8444-555555555555
```

Only arguments that the model must explicitly provide should be declared. Extra arguments are allowed unless they are explicitly marked `absent`.

## Synthetic Fixtures

The `synthetic_results` section contains one deterministic response per active tool. Identifiers returned by one fixture match those expected by later steps in the same sequence.

Example:

```yaml
start_genie_usage_metrics_query:
  statement_id: 11111111-2222-4333-8444-555555555555
  done: false
  result_tool: get_genie_usage_metrics_query_result
```

All identifiers are fictitious. Real IDs, users, tables, or sensitive data must not be introduced into prompts or fixtures.

## Safety Model

The runner creates `RecordingMCPServer`, a subclass of `MCPServerStreamableHttp`. The real server is used only to establish the MCP session and discover the selected live tool definitions.

When the agent requests a tool, the overridden `call_tool` method:

1. Records the name, arguments, metadata, and latency.
2. Verifies that a fixture exists.
3. Returns a synthetic `CallToolResult`.
4. Never invokes `super().call_tool()` or the live MCP session.

Consequently, the benchmark does not execute Atlan requests, SQL statements, Jobs, serialization, restore operations, or other Databricks operations.

`start_genie_serialization_job` and `start_genie_space_restore_job` have additional constraints:

- `confirmation` must be absent from model-generated arguments.
- The fixture reports that the operation was not executed.
- A model may avoid calling a gated `start_*` tool and request confirmation directly.
- A multi-step workflow may stop before its final gated `start_*` call when that partial sequence is explicitly allowed.
- The final answer must state that no action occurred and explicit confirmation is required.
- Contradictory execution claims fail response-behavior scoring.

The runner rejects any dataset that does not explicitly set `real_tool_execution: false` or violates these confirmation rules.

## Environment Preparation

Synchronize dependencies:

```bash
uv sync
```

Validate the dataset without opening MCP or model connections:

```bash
uv run python tests/evaluation/run_tool_selection_eval.py --dry-run
```

Start the local MCP server in one terminal:

```bash
DATABRICKS_CONFIG_PROFILE=mcp-oauth uv run mcp-pablo --port 8000
```

The runner uses a renewable Databricks credential provider on every model request so a long run does not fail when an OAuth token expires.

## Full GPT-5.4 Mini Execution

Run the complete 50-case benchmark in a second terminal:

```bash
uv run python tests/evaluation/run_tool_selection_eval.py \
  --profile mcp-oauth \
  --model databricks-gpt-5-4-mini \
  --output tests/evaluation/results/gpt-5-4-mini-workflow-50.jsonl
```

The paired summary is written automatically to:

```text
tests/evaluation/results/gpt-5-4-mini-workflow-50.summary.json
```

The runner exits with a non-zero status if one or more cases fail. This is expected for a benchmark and does not prevent completed case records or the summary from being written.

## Focused Runs

Run one case:

```bash
uv run python tests/evaluation/run_tool_selection_eval.py \
  --profile mcp-oauth \
  --case-id sequence-benchmark-drilldown-004
```

Run all cases for one tool:

```bash
uv run python tests/evaluation/run_tool_selection_eval.py \
  --profile mcp-oauth \
  --tool get_genie_usage_metrics
```

Available filters and controls are:

- `--case-id`: select a case ID; repeatable.
- `--category`: select a category; repeatable.
- `--tool`: select `tool_under_test`; repeatable.
- `--tag`: require a tag; repeatable.
- `--limit`: limit cases after filtering.
- `--model`: override the dataset model.
- `--max-turns`: override the turn limit.
- `--fail-fast`: stop after the first scored failure.
- `--no-tracing`: disable MLflow.

## Per-Case Execution Flow

Cases do not share conversation history:

1. The synthetic call recorder is reset.
2. The prompt and the 18 filtered live tool definitions are sent to the agent.
3. The model answers or selects a tool.
4. Tool calls are intercepted and receive fixtures.
5. The model may continue until it produces a final answer.
6. Calls, arguments, output, tokens, and latency are extracted.
7. The MLflow trace is retrieved and tagged.
8. The case is scored.
9. Its record is immediately appended to JSONL.

Parallel tool calls are disabled so observed order represents a causal sequence. Model and MCP retries default to zero.

## Scoring

A case passes only when every condition is true:

- No execution error occurred.
- The observed sequence exactly matches the preferred or an allowed sequence.
- No forbidden tool was selected.
- Tool use matches the preferred path or an explicitly accepted alternative, including safe no-call preparation.
- All expected arguments satisfy their matchers.
- The final answer satisfies the expected behavior.

| Response behavior | Requirement |
|---|---|
| `answer` | Non-empty final answer |
| `answer_without_tools` | Non-empty answer and no calls |
| `ask_clarification` | No calls and a detectable clarification request |
| `confirmation_required` | Confirmation mentioned, explicit non-execution, and no contradictory execution claim |

The output separately records whether the preferred sequence or an allowed fallback was used.

## Output Artifacts

Without `--output`, timestamped files are used:

```text
tests/evaluation/results/tool-selection-YYYYMMDDTHHMMSSZ.jsonl
tests/evaluation/results/tool-selection-YYYYMMDDTHHMMSSZ.summary.json
```

The JSONL file contains one record per case, including:

- Prompt, category, tool under test, and tags.
- Complete expectations.
- Available tools and definition hash.
- Tool calls, arguments, and synthetic fixtures.
- Final answer and errors.
- Latency, request count, and token usage.
- Configured and observed retries.
- MLflow trace ID and estimated cost.
- Overall and component scores.
- Per-argument checks.

The summary JSON contains:

- Model and run configuration.
- Selected, executed, and skipped counts.
- Global metric rates.
- Category and tool breakdowns.
- Total tokens and estimated cost.
- Complete live definitions for the 18 tools.
- Dataset and schema hashes.
- MLflow experiment information.

The results directory is intentionally versionable so completed benchmark evidence can be committed with the project. Review artifacts for sensitive content before committing, although the dataset and fixtures are designed to be synthetic.

## MLflow

The default experiment is:

```text
/Users/<current-user>/mcp-tool-selection-eval
```

Every trace is tagged with:

```text
evaluation.dataset
evaluation.case_id
```

The JSONL record links each case to its `trace_id`. MLflow stores model calls, synthetic tool calls, tokens, latency, and estimated cost. Cost values are estimates from the MLflow price catalog, not billing records.

## Reproducibility

Valid model comparisons require the following to remain constant:

- Dataset and dataset hash.
- Tool inventory and schema hash.
- Agent instructions.
- Maximum turns.
- Retry configuration.
- Synthetic fixtures.
- Dependency versions in `uv.lock`.

Each summary records the SHA-256 hash of the YAML dataset and live MCP definitions. A hash change identifies a different experimental configuration.

## Editing and Review

The dataset currently has `review_status: pending_user_review`. Prompts and expectations should be reviewed before treating it as a final academic benchmark.

Edits must preserve unique IDs, active inventory membership, consistency between `should_use_tool` and expected sequences, argument rules for every allowed alternative, minimum per-tool coverage, fixture-to-sequence identifier consistency, and confirmation rules.

After any change, run:

```bash
uv run python tests/evaluation/run_tool_selection_eval.py --dry-run
uv run pytest tests/evaluation/test_tool_selection_eval.py
```

The test suite checks equality between the generator output and checked-in YAML. A YAML-only edit is considered experimental and is intentionally detected.

## Failure Interpretation

A failure may indicate incorrect tool selection, fabricated or malformed arguments, unnecessary tool use, missing clarification, incorrect sequence order, excessive caution, an overly strict expectation, an ambiguous prompt, or an insufficient fixture.

Review the prompt, final output, calls, argument checks, and MLflow trace together. A failed score should not automatically be attributed to the model before the human expectation has been reviewed.

## Limitations

- Real API implementations are not executed or validated.
- Fixtures simplify production responses.
- Clarification and confirmation behavior use deterministic language rules rather than an LLM judge.
- Extra arguments are allowed unless explicitly forbidden.
- Runs are sequential and the complete benchmark can take significant time.
- Benchmark quality depends on human review of prompts and expectations.
- Inventory changes alter model context and make older results not directly comparable.

## Future Extension

To add a tool:

1. Add it to the relevant `EVALUATION_TOOL_GROUPS` group.
2. Add or review its `ALL_TOOL_PROFILES` profile.
3. Create a representative synthetic fixture.
4. Add sequences if it participates in multi-step workflows.
5. Regenerate and review the YAML.
6. Run dry-run, tests, and a limited model sample.
7. Store model and dataset results under distinct filenames.
