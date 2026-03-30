from config import SourceConfig
from introspect import introspect_schema
from generate import generate_mcp_json
from server import start_server
import json
import os

_DIR = os.path.dirname(os.path.abspath(__file__))


def main():
    config = SourceConfig()
    introspect_schema(config)
    mcp = generate_mcp_json(config)

    with open(os.path.join(_DIR, "mcp_description.json"), "w") as f:
        json.dump(mcp, f, indent=2, ensure_ascii=False)

    start_server(config, mcp)
if __name__ == "__main__":
    main()