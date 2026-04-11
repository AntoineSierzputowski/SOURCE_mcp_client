from pymongo import MongoClient


def connect(db_config: dict):
    if db_config.get("user") and db_config.get("password"):
        uri = (
            f"mongodb://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['name']}"
        )
    else:
        uri = f"mongodb://{db_config['host']}:{db_config['port']}"

    client = MongoClient(uri)
    return client[db_config["name"]]


def _get_nested_value(doc: dict, path: str):
    """Traverse dot-notation path into a document, descending into arrays if needed."""
    current = doc
    for part in path.split("."):
        if isinstance(current, list):
            current = current[0] if current else None
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def introspect(config):
    db = config.db_connection

    for collection_name, collection_conf in config.tables_config.items():
        if not collection_conf.get("expose", False):
            continue

        collection = db[collection_name]
        fields_conf = collection_conf.get("fields", {})

        sample_doc = collection.find_one({}, {"_id": 0}) or {}

        for field_name, field_conf in fields_conf.items():
            if not field_conf.get("expose", False):
                continue

            value = _get_nested_value(sample_doc, field_name)
            if isinstance(value, bool):
                data_type = "bool"
            elif isinstance(value, int):
                data_type = "int"
            elif isinstance(value, float):
                data_type = "double"
            else:
                data_type = "string"

            config.schema.append({
                "table": collection_name,
                "column": field_name,
                "data_type": data_type,
                "is_nullable": True,
                "is_primary_key": field_name == "_id",
                "description": field_conf.get("description", field_name),
            })

    for entry in config.schema:
        if entry["data_type"] == "string" and not entry["is_primary_key"]:
            try:
                values = db[entry["table"]].distinct(entry["column"])
                values = [str(v) for v in values if v is not None]
                if 1 < len(values) <= 20:
                    config.enums[f"{entry['table']}.{entry['column']}"] = sorted(values)
            except Exception:
                pass


def type_to_json(data_type: str) -> str:
    mapping = {
        "int": "integer",
        "long": "integer",
        "double": "number",
        "decimal": "number",
        "bool": "boolean",
        "string": "string",
        "date": "string",
        "objectId": "string",
    }
    return mapping.get(data_type, "string")


def execute_query(conn, collection_name: str, fields: list, properties: dict, parsed: dict, limit: int) -> list:
    collection = conn[collection_name]

    mongo_filter = {
        k: v for k, v in parsed.items()
        if v is not None and k in properties
    }

    projection = {field: 1 for field in fields}
    projection["_id"] = 0

    return list(collection.find(mongo_filter, projection).limit(limit))
