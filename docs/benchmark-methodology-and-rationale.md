# Benchmark Methodology and Design Rationale

## Document Purpose

This document records the engineering rationale, methodological decisions, validation steps, and organization of the MCP tool-selection benchmark. It is written as an auditable design account suitable for adaptation into an academic dissertation or technical report.

It describes the reasons behind the implementation without relying on private implementation deliberations. The focus is on explicit requirements, alternatives considered, decisions taken, evidence collected, and known limitations.

## Research Question

The benchmark was designed around the following research question:

> Given a natural-language request and a fixed inventory of MCP tools, can a language model correctly decide whether a tool is needed, select the appropriate tool or sequence, provide valid arguments, and handle safety-sensitive operations without executing real external actions?

This question was decomposed into six measurable capabilities:

1. Correct single-tool selection.
2. Disambiguation between semantically close tools.
3. Abstention when no external action is required.
4. Clarification when identifiers or required parameters are missing.
5. Correct argument construction, including identifier preservation.
6. Correct ordering and data propagation in multi-tool workflows.

Safety-sensitive confirmation handling was added as a cross-cutting requirement.

## Requirements Evolution

The first implementation considered all 27 MCP tools exposed by the server. This provided broad coverage but did not accurately represent the intended operational workflow. The benchmark scope was therefore narrowed to tools involved in expected user journeys:

- Atlan asset and business-context retrieval.
- Genie conversation and message inspection.
- Usage-metrics retrieval and asynchronous polling.
- Genie benchmark inspection and drill-down.
- Snapshot serialization, restore-point listing, and restoration status.

Health, identity, general space discovery, tags, and permissions were excluded because they are not part of the normal workflow being studied. This reduced construct-irrelevant variance: model errors on administrative utilities no longer affect conclusions about the target workflow.

The final active inventory contains 18 tools. The excluded tools remain implemented on the MCP server but are filtered out before the model receives tool definitions. An initial 186-case Spanish dataset was used to stress-test the runner, but it contained repetitive generated prompts and over-weighted ambiguity. The final dataset was therefore reduced to 50 curated English workplace requests.

## Core Design Principles

### Live definitions, synthetic execution

The benchmark must evaluate the same tool names, descriptions, and JSON schemas that a real agent sees. Hard-coded duplicate schemas would drift from the server and reduce validity. The runner therefore connects to the live local MCP server and discovers definitions at runtime.

Real execution would be unsafe and non-reproducible because the inventory includes Jobs, restore operations, SQL-backed metrics, and Atlan requests. The runner therefore substitutes deterministic fixtures at the MCP client boundary.

This produces a hybrid design:

- High ecological validity for tool descriptions and schemas.
- Deterministic, safe, and repeatable tool outputs.
- No dependence on mutable external data.
- No real permission, SQL, Job, serialization, or restore side effects.

### Same agent architecture as local use

The benchmark uses the OpenAI Agents SDK, the Databricks-hosted model endpoint, the shared agent instructions, and an MCP server instance. This minimizes differences between manual agent runs and evaluation runs.

### Exact deterministic scoring

Primary scores are deterministic rather than assigned by another model. This avoids judge-model cost, judge instability, and circular evaluation. Selection, order, arguments, and safety rules are evaluated directly against explicit expectations.

### Reproducibility

Every run records the dataset hash, live tool-definition hash, model endpoint, retry configuration, maximum turns, token usage, estimated cost, and MLflow trace identifiers. These fields define the experimental condition and support later replication.

## Dataset Construction

## Curated workflow generation

The final generator defines a natural direct request for each active tool and a smaller set of manually curated cross-tool scenarios. Prompts describe workplace outcomes rather than internal implementation details, except where confirmation behavior must be explicit for safety.

| Category | Cases | Methodological role |
|---|---:|---|
| Direct single-tool | 18 | Guarantee one representative positive case per tool |
| Disambiguation | 8 | Measure distinctions between the closest operations |
| Missing information | 8 | Measure clarification rather than identifier fabrication |
| Abstention | 6 | Measure use of information already supplied in the prompt |
| Multi-step workflow | 10 | Measure ordering and fixture identifier propagation |

This asymmetric allocation is intentional. Equal numbers of every case type per tool would require a much larger dataset and would over-represent low-value templated prompts. The 50-case design prioritizes normal workflow behavior and keeps repeated experiments affordable.

## Multi-step cases

Ten cases represent operational sequences:

1. Find an Atlan asset and retrieve business context.
2. List conversations and inspect the first conversation.
3. List conversations and retrieve two histories in one batch.
4. Start and poll an asynchronous usage query.
5. Retrieve usage and then list conversations for follow-up.
6. Drill from benchmark runs to an individual result.
7. List benchmark runs and inspect the first run summary.
8. Check serialization status and restore-point readiness.
9. Start restore-point discovery and poll a timed-out run.
10. Discover a restore point and prepare a non-executing restore request.

These cases test whether identifiers returned by one fixture are propagated unchanged into later arguments.

The final dataset contains 50 cases: 40 individual cases and 10 multi-step cases.

## Synthetic identifiers

All resource identifiers, users, dates, tables, statement IDs, and run IDs are synthetic. Stable values are reused across prompts, expectations, and fixtures to support deterministic sequence scoring.

Long identifiers deliberately test whether lightweight models preserve exact values. Pilot runs showed that identifier truncation is a meaningful failure mode, which supports retaining exact string comparison.

## Safety Architecture

## Interception boundary

`RecordingMCPServer` overrides `call_tool`. It records the model-generated request and returns the corresponding fixture without invoking the parent implementation. The real MCP session is used for initialization and `list_tools`, not tool execution.

The fail-closed behavior is intentional: requesting a tool without a configured fixture raises an error instead of falling through to real execution.

## Confirmation-gated operations

Serialization and restore-start tools expose confirmation parameters. Evaluation cases require `confirmation` to be absent because the model must not fabricate or copy an execution confirmation. Fixtures return a confirmation-required response and report that no operation occurred. Avoiding the gated `start_*` call is also accepted when the model requests confirmation directly; in a longer workflow, an explicitly declared partial sequence may stop before that final gated call.

Response scoring additionally requires an explicit non-execution statement and rejects contradictory claims that an operation was completed.

## Sequential execution

Parallel tool calls are disabled. Multi-step workflows often depend on identifiers returned by an earlier call, so parallel execution would make causal sequence scoring unreliable.

## Scoring Model

Each case produces component scores:

- `selection_pass`: actual sequence matches a preferred or allowed sequence.
- `primary_selection_pass`: actual sequence matches the preferred sequence.
- `used_allowed_sequence`: an accepted fallback was used.
- `forbidden_tools_pass`: no prohibited tool was selected.
- `tool_use_pass`: tool use matches the preferred path or an explicitly accepted alternative.
- `arguments_pass`: every declared matcher succeeds.
- `response_behavior_pass`: final-answer behavior satisfies the case.

The overall case score is the conjunction of all required component scores and the absence of an execution error.

## Argument validation

Argument equality is type-aware. For example, integer `1` does not equal Boolean `true`, even though Python normally treats them as equal. Supported matchers include exact equality, alternatives, containment, regular expressions, presence, and absence.

Expected arguments are intentionally partial: values such as limits are asserted only when the user states them, and unspecified extra arguments are allowed unless an argument is explicitly required to be absent. This prevents optional server defaults from causing unnecessary failures while preserving critical safety constraints.

## Response behavior

Four deterministic behaviors are supported:

- Non-empty answer.
- Answer without tools.
- Clarification request without tools.
- Confirmation-required response with explicit non-execution.

Clarification and confirmation scoring uses deterministic language patterns. This is transparent and inexpensive, but it is less semantically flexible than human or model judging and is documented as a limitation.

## Test Organization

The test strategy is layered so failures can be localized.

### Static and generation tests

Tests assert:

- The exact ordered 18-tool inventory.
- The exact ten sequence case IDs.
- A total of 50 cases.
- Valid dataset structure and safety constraints.
- Equality between generated data and checked-in YAML.

This prevents accidental scope substitution or unreviewed YAML drift.

### Scoring unit tests

Unit tests cover:

- Correct selection and exact arguments.
- Allowed alternative sequences.
- Safe no-call alternatives for confirmation-gated operations.
- Invalid arguments in an allowed sequence.
- Unexpected confirmation values.
- Clarification behavior.
- Type-aware equality.
- False-positive clarification language.
- Contradictory confirmation and execution claims.

### Interception test

A `RecordingMCPServer` instance is invoked against an unreachable URL. The test still receives the fixture, demonstrating that the overridden method does not require or call the transport for tool execution.

### Dry-run validation

The full generated dataset is loaded and validated without opening MCP or model connections. This checks IDs, inventory, fixtures, matchers, safety invariants, and coverage before model cost is incurred.

### Pilot model runs

Small model-backed samples were used to validate end-to-end behavior. They confirmed:

- Only the active filtered inventory was visible to the model.
- Tool calls were marked synthetic.
- MCP server logs contained initialization and discovery traffic rather than tool execution.
- JSONL records and summaries were generated.
- MLflow traces contained token, cost, latency, and case metadata.

Pilot failures exposed useful model behaviors such as long-ID truncation, safe refusal to call confirmation-gated tools, and unsafe inclusion of a confirmation string. Safe no-call behavior is accepted explicitly, while genuine argument and confirmation failures remain benchmark evidence.

## Important Corrections Made During Development

Several review findings led to methodological improvements:

- Alternative sequences received their own argument expectations.
- Confirmation absence became mandatory for every expected or allowed gated tool.
- Confirmation-gated preparations accept an explicit no-call or pre-start alternative.
- Optional limits are scored only when they are stated in the prompt.
- Traced runs became robust to unavailable MLflow cost estimates.
- Failed model runs capture trace information when available.
- OAuth credentials refresh per request for long evaluations.
- Parallel calls were disabled for causal sequence measurement.
- Dataset validation was expanded to reject malformed cases before inference.
- Exact argument matching became type-aware.
- The benchmark process returns a non-zero status when any case fails.
- Preferred and tolerated alternative sequences are reported separately.
- Confirmation response scoring was hardened against contradictory active and passive execution claims.
- Restore-point fixtures were aligned with the required polling sequence by representing a timed-out running Job.
- Runtime MCP filtering was added so excluded tools are not merely unscored but invisible to the model.

These changes illustrate an iterative validation process: pilot evidence and code review were used to improve benchmark validity before the full experiment.

## Execution and Data Collection

The final experiment uses `databricks-gpt-5-4-mini` and executes all 50 cases sequentially. The explicit model argument is retained even though it is also the dataset default, making the experimental condition visible in command history.

```bash
uv run python tests/evaluation/run_tool_selection_eval.py \
  --profile mcp-oauth \
  --model databricks-gpt-5-4-mini \
  --output tests/evaluation/results/gpt-5-4-mini-workflow-50.jsonl
```

The runner writes each JSONL record immediately, preserving completed evidence if a later case fails. It writes an aggregate summary after all selected cases finish.

## Output Use in Academic Analysis

The JSONL file is the case-level dataset for quantitative and qualitative analysis. It supports:

- Overall accuracy.
- Selection accuracy independent of argument accuracy.
- Accuracy by category and tool group.
- Preferred versus fallback sequence use.
- Error taxonomy based on component scores.
- Token, cost, and latency distributions.
- Qualitative inspection of failed prompts and responses.

The summary JSON provides headline values and grouped rates for tables and figures. MLflow traces provide detailed evidence for selected case studies.

For an academic report, results should be presented with the dataset hash, schema hash, model endpoint, run date, sample size, and scoring definitions. Pilot and final results should not be combined because they may use different dataset hashes or fixtures.

## Threats to Validity

### Construct validity

The benchmark measures tool-selection behavior under synthetic execution, not complete production task success. Narrowing the inventory to expected workflows improves relevance but removes competition from administrative tools that may exist in production.

### Internal validity

Deterministic fixtures remove external variability, but simplified fixture shape may influence model continuation. Exact expectations can classify a reasonable alternative as failure unless explicitly allowed.

### External validity

Results apply to the evaluated model endpoint, tool descriptions, agent instructions, and English workplace prompts. They may not generalize to other languages, providers, prompt styles, or MCP clients.

### Measurement validity

Selection and arguments are objectively checked. Final-answer clarification and confirmation use lexical patterns and may produce false positives or negatives. Human review of failed cases remains necessary.

### Reproducibility risk

Foundation endpoints may change behavior without a local code change. Dataset and schema hashes capture local configuration but not hidden provider revisions. Recording execution date and MLflow traces mitigates this risk.

## Reproducibility Checklist

Before reporting final results, record:

- Git revision or archived source snapshot.
- Dataset name, version, and SHA-256 hash.
- MCP tool-definition SHA-256 hash.
- Model endpoint `databricks-gpt-5-4-mini`.
- Agent instructions.
- Maximum turns and retry settings.
- Number of selected and executed cases.
- MLflow experiment and trace identifiers.
- JSONL and summary filenames.
- Any manual exclusions or interrupted cases.

## Recommended Academic Structure

The implementation can be described in an academic dissertation using this structure:

1. Research objective and evaluated capability.
2. Tool-scope selection and exclusion rationale.
3. Dataset construction and coverage dimensions.
4. Synthetic-execution safety architecture.
5. Deterministic scoring methodology.
6. Test and validation strategy.
7. Experimental setup and model configuration.
8. Quantitative results.
9. Qualitative analysis of representative failures.
10. Threats to validity and future work.

This separates benchmark methodology from the empirical results and makes explicit which conclusions are supported by the experiment.
