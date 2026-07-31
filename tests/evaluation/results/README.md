# Evaluation Results

This directory stores versionable benchmark artifacts.

Each completed run produces:

- A `.jsonl` file with one complete record per evaluation case.
- A `.summary.json` file with aggregate metrics, tool definitions, hashes, token usage, latency, and estimated cost.

The current dataset-v4 GPT-5.4 workflow run uses:

```text
gpt-5-4-workflow-50.jsonl
gpt-5-4-workflow-50.summary.json
```

A future GPT-5.4 Mini workflow run should use:

```text
gpt-5-4-mini-workflow-50.jsonl
gpt-5-4-mini-workflow-50.summary.json
```

The existing Mini-named pair is a historical dataset-v3 artifact whose recorded model is `databricks-gpt-5-4`; do not report it as a Mini run.

Before committing results, verify that the summary reports all 50 selected cases as executed and that its dataset hash matches the checked-in `tool_selection_dataset.yaml`. Keep pilot runs separate from final experimental results because they may use older dataset versions, languages, or fixtures.
