import yaml
import mysql.connector


class SourceConfig:
    def __init__(self, yaml_path="source.yaml"):
        with open(yaml_path, "r") as f:
            raw = yaml.safe_load(f)

        # server basics info 
        self.server_name = raw["server"]["name"]
        self.server_description = raw["server"]["description"]

        # tables config
        self.tables_config = raw["tables"]

        # connect to the DB (onmy mysql for the moment)
        #  add DB functions integrations to the folder db_integration and import the function here
        self.db_connection = mysql.connector.connect(
            host=raw["database"]["host"],
            port=raw["database"]["port"],
            user=raw["database"]["user"],
            password=raw["database"]["password"],
            database=raw["database"]["name"]
        )

        self.schema = []
        self.enums = {}