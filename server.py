from mcp.server.fastmcp import FastMCP
import json


def start_server(config, mcp_definition):
    mcp = FastMCP(config.server_name)

    # Register tools dynamically from the MCP JSON
    for tool_def in mcp_definition["tools"]:
        register_tool(mcp, config, tool_def)

    mcp.run()


def register_tool(mcp, config, tool_def):
    tool_name = tool_def["name"]
    tool_description = tool_def["description"]
    properties = tool_def["inputSchema"]["properties"]

    # Determine if this is a search tool or a get_by_id tool
    is_search = tool_name.startswith("search_")
    table_name = tool_name.replace("search_", "").replace("get_", "").replace("_by_id", "")

    # Build the list of exposed columns for SELECT
    exposed_columns = [
        entry["column"] for entry in config.schema
        if entry["table"] == table_name
    ]
    select_columns = ", ".join(exposed_columns)

    @mcp.tool(name=tool_name, description=tool_description)
    def dynamic_tool(**kwargs) -> str:
        cursor = config.db_connection.cursor(dictionary=True)

        query = f"SELECT {select_columns} FROM `{table_name}` WHERE 1=1"
        params = []

        for param_name, value in kwargs.items():
            if value is None:
                continue

            col_type = properties[param_name]["type"]

            if col_type == "number":
                query += f" AND `{param_name}` <= %s"
            elif col_type == "boolean":
                query += f" AND `{param_name}` = %s"
            else:
                query += f" AND `{param_name}` = %s"

            params.append(value)

        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()

        return json.dumps(results, default=str)

    return dynamic_tool