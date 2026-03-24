def mysql_type_to_json(data_type):
    """Convert MySQL data type to JSON Schema type"""
    mapping = {
        "int": "integer",
        "tinyint": "boolean",
        "bigint": "integer",
        "float": "number",
        "double": "number",
        "decimal": "number",
        "varchar": "string",
        "text": "string",
        "date": "string",
        "datetime": "string",
        "timestamp": "string",
    }
    return mapping.get(data_type, "string")


def generate_mcp_json(config):
    tables = {}
    for entry in config.schema:
        table = entry["table"]
        if table not in tables:
            tables[table] = []
        tables[table].append(entry)

    tools = []

    for table_name, columns in tables.items():
        table_conf = config.tables_config[table_name]

        search_properties = {}
        for col in columns:
            if col["is_primary_key"]:
                continue

            prop = {
                "type": mysql_type_to_json(col["data_type"]),
                "description": col["description"]
            }

            enum_key = f"{table_name}.{col['column']}"
            if enum_key in config.enums:
                prop["enum"] = config.enums[enum_key]

            search_properties[col["column"]] = prop

        if search_properties:
            tools.append({
                "name": f"SQL_Database_{table_name}",
                "table": table_name,
                "description": f"Search {table_name}. {table_conf.get('description', '')}",
                "inputSchema": {
                    "type": "object",
                    "properties": search_properties,
                    "required": []
                }
            })

    mcp = {
        "server": {
            "name": config.server_name,
            "description": config.server_description
        },
        "tools": tools
    }

    return mcp