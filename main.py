# main.py

from config import SourceConfig
from introspect import introspect_schema
from generate import generate_mcp_json
import json


def main():
    # load YAML config
    config = SourceConfig()
    # intropsepct the SQL db
    introspect_schema(config)
    # create the mcp json
    mcp = generate_mcp_json(config)

    # Save output
    with open("mcp_description.json", "w") as f:
        json.dump(mcp, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    main()