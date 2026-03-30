from fastmcp import FastMCP
import json


def start_server(config, mcp_definition):
    mcp = FastMCP(config.server_name)

    for tool_def in mcp_definition["tools"]:
        _register(mcp, config, tool_def)

    mcp.run()


def _register(mcp, config, tool_def):
    tool_name = tool_def["name"]
    table_name = tool_def["table"]
    properties = tool_def["inputSchema"]["properties"]

    exposed_columns = [
        entry["column"] for entry in config.schema
        if entry["table"] == table_name
    ]

    filters_doc = "\n".join(
        f"  - {name} ({prop['type']}): {prop['description']}"
        for name, prop in properties.items()
    )
    full_description = (
        f"{tool_def['description']}\n\n"
        f"Pass a JSON string with any of these filters:\n{filters_doc}"
    )

    _table = table_name
    _columns = exposed_columns
    _props = properties
    _conn = config.db_connection
    _integration = config.db_integration

    def handler(query: str = "{}") -> str:
        parsed = json.loads(query)
        results = _integration.execute_query(_conn, _table, _columns, _props, parsed)
        return json.dumps(results, default=str)

    handler.__name__ = tool_name
    handler.__doc__ = full_description

    mcp.tool()(handler)
