#!/bin/bash

set -euo pipefail

# Run local integration tests with the same non-secret configuration used by the app.
#
# Usage:
#   cp scripts/dev/local_test.env.example scripts/dev/local_test.env
#   # Edit scripts/dev/local_test.env
#   ./scripts/dev/run_tests.sh
#
# Optional pytest args are forwarded:
#   ./scripts/dev/run_tests.sh tests/test_integration_server.py::test_list_tools

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

cd "$PROJECT_ROOT"

LOCAL_ENV_FILE="$SCRIPT_DIR/local_test.env"
if [ -f "$LOCAL_ENV_FILE" ]; then
  # shellcheck disable=SC1090
  source "$LOCAL_ENV_FILE"
fi

# App configuration mirrored from app.yaml. These are not secrets.
export GENIE_SERIALIZATION_JOB_ID="${GENIE_SERIALIZATION_JOB_ID:-405956489806901}"
export GENIE_RESTORE_POINTS_JOB_ID="${GENIE_RESTORE_POINTS_JOB_ID:-191614083444231}"
export GENIE_RESTORE_JOB_ID="${GENIE_RESTORE_JOB_ID:-411926808267885}"
export GENIE_SPACE_WAREHOUSE_ID="${GENIE_SPACE_WAREHOUSE_ID:-38cb31e24512fd55}"
export ATLAN_BASE_URL="${ATLAN_BASE_URL:-https://hp.atlan.com/}"

# Optional integration-test IDs. Leave empty to skip those tests.
export GENIE_TEST_SPACE_ID="${GENIE_TEST_SPACE_ID:-}"
export GENIE_TEST_CONVERSATION_ID="${GENIE_TEST_CONVERSATION_ID:-}"
export GENIE_TEST_TAG_KEY="${GENIE_TEST_TAG_KEY:-}"
export GENIE_TEST_TAG_VALUE="${GENIE_TEST_TAG_VALUE:-}"
export DASHBOARD_TEST_ID="${DASHBOARD_TEST_ID:-}"
export DATABRICKS_TEST_USER_ID="${DATABRICKS_TEST_USER_ID:-}"
export GENIE_TEST_BENCHMARK_RUN_ID="${GENIE_TEST_BENCHMARK_RUN_ID:-}"
export GENIE_TEST_BENCHMARK_RESULT_ID="${GENIE_TEST_BENCHMARK_RESULT_ID:-}"
export DATABRICKS_TEST_JOB_RUN_ID="${DATABRICKS_TEST_JOB_RUN_ID:-}"
export PRINT_TOOL_RESULTS="${PRINT_TOOL_RESULTS:-}"

# Optional secret for local Atlan calls. Set it in local_test.env if needed.
export ATLAN_API_KEY="${ATLAN_API_KEY:-}"

PYTEST_ARGS=(tests/ -v)
if [ "$PRINT_TOOL_RESULTS" = "1" ]; then
  PYTEST_ARGS+=(-s)
fi
PYTEST_ARGS+=("$@")

uv run pytest "${PYTEST_ARGS[@]}"
