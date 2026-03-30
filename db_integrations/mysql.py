import mysql.connector


def connect(db_config: dict):
    return mysql.connector.connect(
        host=db_config["host"],
        port=db_config["port"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["name"],
    )


def introspect(config):
    cursor = config.db_connection.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """,
        (config.db_connection.database,),
    )
    all_columns = cursor.fetchall()

    for col in all_columns:
        table_name = col["TABLE_NAME"]
        col_name = col["COLUMN_NAME"]

        table_conf = config.tables_config.get(table_name)
        if not table_conf or not table_conf.get("expose", False):
            continue

        col_conf = table_conf.get("columns", {}).get(col_name, {})
        if not col_conf.get("expose", False):
            continue

        config.schema.append({
            "table": table_name,
            "column": col_name,
            "data_type": col["DATA_TYPE"],
            "is_nullable": col["IS_NULLABLE"],
            "is_primary_key": col["COLUMN_KEY"] == "PRI",
            "description": col_conf.get("description", col_name),
        })

    for entry in config.schema:
        if entry["data_type"] == "varchar" and not entry["is_primary_key"]:
            try:
                cursor.execute(
                    f"SELECT DISTINCT `{entry['column']}` FROM `{entry['table']}` LIMIT 50"
                )
                values = [row[entry["column"]] for row in cursor.fetchall() if row[entry["column"]]]
                if 1 < len(values) <= 20:
                    config.enums[f"{entry['table']}.{entry['column']}"] = sorted(values)
            except Exception:
                pass

    cursor.close()


def type_to_json(data_type: str) -> str:
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


def execute_query(conn, table: str, columns: list, properties: dict, parsed: dict) -> list:
    select_columns = ", ".join(f"`{col}`" for col in columns)
    cursor = conn.cursor(dictionary=True)

    sql = f"SELECT {select_columns} FROM `{table}` WHERE 1=1"
    params = []

    for param_name, value in parsed.items():
        if value is not None and param_name in properties:
            sql += f" AND `{param_name}` = %s"
            params.append(value)

    cursor.execute(sql, params)
    results = cursor.fetchall()
    cursor.close()
    return results
