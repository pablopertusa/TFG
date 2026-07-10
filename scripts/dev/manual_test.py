from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient

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
result = mcp_client.call_tool("get_genie_history_metrics", {"space_id": "01f165805e7219b4b9fafcafb541c7f2"})
print(result.content)