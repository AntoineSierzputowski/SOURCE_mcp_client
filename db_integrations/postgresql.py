import psycopg2
import psycopg2.extras


def connect(db_config: dict):
    return psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        user=db_config["user"],
        password=db_config["password"],
        dbname=db_config["name"],
    )


def introspect(config):
    cursor = config.db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute(
        """
        SELECT
            c.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable,
            CASE WHEN kcu.column_name IS NOT NULL THEN true ELSE false END AS is_primary_key
        FROM information_schema.columns c
        LEFT JOIN information_schema.table_constraints tc
            ON tc.table_name = c.table_name
            AND tc.table_schema = c.table_schema
            AND tc.constraint_type = 'PRIMARY KEY'
        LEFT JOIN information_schema.key_column_usage kcu
            ON kcu.constraint_name = tc.constraint_name
            AND kcu.table_schema = tc.table_schema
            AND kcu.column_name = c.column_name
        WHERE c.table_schema = 'public'
        ORDER BY c.table_name, c.ordinal_position
        """
    )
    all_columns = cursor.fetchall()

    for col in all_columns:
        table_name = col["table_name"]
        col_name = col["column_name"]

        table_conf = config.tables_config.get(table_name)
        if not table_conf or not table_conf.get("expose", False):
            continue

        col_conf = table_conf.get("columns", {}).get(col_name, {})
        if not col_conf.get("expose", False):
            continue

        config.schema.append({
            "table": table_name,
            "column": col_name,
            "data_type": col["data_type"],
            "is_nullable": col["is_nullable"] == "YES",
            "is_primary_key": col["is_primary_key"],
            "description": col_conf.get("description", col_name),
        })

    for entry in config.schema:
        if entry["data_type"] in ("character varying", "text") and not entry["is_primary_key"]:
            try:
                cursor.execute(
                    f'SELECT DISTINCT "{entry["column"]}" FROM "{entry["table"]}" LIMIT 50'
                )
                values = [row[entry["column"]] for row in cursor.fetchall() if row[entry["column"]]]
                if 1 < len(values) <= 20:
                    config.enums[f"{entry['table']}.{entry['column']}"] = sorted(values)
            except Exception:
                config.db_connection.rollback()

    cursor.close()


def type_to_json(data_type: str) -> str:
    mapping = {
        "integer": "integer",
        "bigint": "integer",
        "smallint": "integer",
        "serial": "integer",
        "bigserial": "integer",
        "real": "number",
        "double precision": "number",
        "numeric": "number",
        "decimal": "number",
        "boolean": "boolean",
        "character varying": "string",
        "varchar": "string",
        "text": "string",
        "date": "string",
        "timestamp": "string",
        "timestamp without time zone": "string",
        "timestamp with time zone": "string",
        "uuid": "string",
        "jsonb": "string",
        "json": "string",
    }
    return mapping.get(data_type, "string")


def execute_query(conn, table: str, columns: list, properties: dict, parsed: dict) -> list:
    select_columns = ", ".join(f'"{col}"' for col in columns)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    sql = f'SELECT {select_columns} FROM "{table}" WHERE 1=1'
    params = []

    for param_name, value in parsed.items():
        if value is not None and param_name in properties:
            sql += f' AND "{param_name}" = %s'
            params.append(value)

    cursor.execute(sql, params)
    results = [dict(row) for row in cursor.fetchall()]
    cursor.close()
    return results
