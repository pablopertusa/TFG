#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "=========================================="
echo "Starting MCP Server - Local Development"
echo "=========================================="
echo ""

echo "Step 1: Syncing dependencies..."
cd "$PROJECT_ROOT"
uv sync
echo "✓ Dependencies synced"
echo ""

echo "Step 2: Starting MCP server on http://localhost:8000..."
echo "Press Ctrl+C to stop the server"
echo ""

uv run mcp-pablo

