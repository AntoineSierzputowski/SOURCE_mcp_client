import yaml
import os
from db_integrations import get_integration

_DIR = os.path.dirname(os.path.abspath(__file__))


class SourceConfig:
    def __init__(self, yaml_path=os.path.join(_DIR, "source.yaml")):
        with open(yaml_path, "r") as f:
            raw = yaml.safe_load(f)

        self.server_name = raw["server"]["name"]
        self.server_description = raw["server"]["description"]
        self.max_rows = raw["server"].get("max_rows", 30)
        # mysql uses "tables", mongoDB uses "collections"
        self.tables_config = raw.get("collections") or raw.get("tables", {})

        self.db_type = raw["database"]["type"]
        self.db_integration = get_integration(self.db_type)
        self.db_connection = self.db_integration.connect(raw["database"])

        self.schema = []
        self.enums = {}
