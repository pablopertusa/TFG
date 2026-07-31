# Evaluation Results

This directory stores versionable benchmark artifacts.

Each completed run produces:

- A `.jsonl` file with one complete record per evaluation case.
- A `.summary.json` file with aggregate metrics, tool definitions, hashes, token usage, latency, and estimated cost.

The canonical 50-case GPT-5.4 Mini workflow run should use:

```text
gpt-5-4-mini-workflow-50.jsonl
gpt-5-4-mini-workflow-50.summary.json
```

Before committing results, verify that the summary reports all 50 selected cases as executed and that its dataset hash matches the checked-in `tool_selection_dataset.yaml`. Keep pilot runs separate from final experimental results because they may use older dataset versions, languages, or fixtures.
