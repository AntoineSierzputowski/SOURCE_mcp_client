def introspect_schema(config):
    cursor = config.db_connection.cursor(dictionary=True)

    # 1. Get all columns from INFORMATION_SCHEMA
    cursor.execute("""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """, (config.db_connection.database,))
    all_columns = cursor.fetchall()

    # 2. Filter: keep only exposed tables and columns based on YAML
    for col in all_columns:
        table_name = col["TABLE_NAME"]
        col_name = col["COLUMN_NAME"]

        # Check if table is exposed
        table_conf = config.tables_config.get(table_name)
        if not table_conf or not table_conf.get("expose", False):
            continue

        # Check if column is exposed
        col_conf = table_conf.get("columns", {}).get(col_name, {})
        if not col_conf.get("expose", False):
            continue

        # Merge SQL info + YAML description
        config.schema.append({
            "table": table_name,
            "column": col_name,
            "data_type": col["DATA_TYPE"],
            "is_nullable": col["IS_NULLABLE"],
            "is_primary_key": col["COLUMN_KEY"] == "PRI",
            "description": col_conf.get("description", col_name)
        })

    # 3. Detect enums: get distinct values for exposed VARCHAR columns
    for entry in config.schema:
        if entry["data_type"] == "varchar" and not entry["is_primary_key"]:
            try:
                cursor.execute(
                    f"SELECT DISTINCT `{entry['column']}` FROM `{entry['table']}` LIMIT 50"
                )
                values = [row[entry["column"]] for row in cursor.fetchall() if row[entry["column"]]]
                if 1 < len(values) <= 20:
                    key = f"{entry['table']}.{entry['column']}"
                    config.enums[key] = sorted(values)
            except:
                pass

    cursor.close()