import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient
from dotenv import load_dotenv

dotenv_path = Path("scripts/dev/local_test.env")
if dotenv_path.exists():
    load_dotenv(dotenv_path)
else:
    print(".env no encontrado")

# Initialize the Databricks Workspace client
workspace_client = WorkspaceClient(profile="DEFAULT")

# Initialize the MCP client with your app’s MCP endpoint
mcp_client = DatabricksMCPClient(
    server_url='http://0.0.0.0:8000/mcp',
    workspace_client=workspace_client,
)

# List available MCP tools
#print(mcp_client.list_tools())

# Call a tool
#result = mcp_client.call_tool("get_genie_usage_metrics_from_audit", {"space_id": "01f165805e7219b4b9fafcafb541c7f2"})
#print(result.content)

result = mcp_client.call_tool("get_genie_usage_metrics_from_audit", {"space_id": "01f165805e7219b4b9fafcafb541c7f2", "timeout_seconds": 420})
print(result.content)
