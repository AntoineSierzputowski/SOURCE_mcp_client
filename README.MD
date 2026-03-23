# mcp-script

Generates an MCP (Model Context Protocol) server description from a MySQL database.

The script reads a `source.yaml` config file, connects to a MySQL database with a read-only user, introspects the schema, and outputs a `mcp_description.json` file containing the MCP tools definition — ready to be used by any MCP-compatible AI agent.

## How it works

1. **`source.yaml`** — You define your database connection, which tables and columns to expose, and write descriptions for each field. This is your control layer — nothing is exposed unless you explicitly say so.

2. **`main.py`** — Entry point. Loads the config, connects to the DB, introspects the schema, and generates the MCP JSON.

3. **`config.py`** — `SourceConfig` class that loads the YAML, opens the DB connection, and holds all data as it gets enriched through the pipeline.

4. **`introspect.py`** — Queries `INFORMATION_SCHEMA` to get the database structure, filters it against the YAML config (only keeps `expose: true` columns), and detects enum values for categorical columns.

5. **`generate.py`** — Takes the merged schema and builds the MCP tools definition. For each exposed table, generates a `search_{table}` tool (filtered queries) and a `get_{table}_by_id` tool (single record lookup).

## Project structure

```
mcp-script/
├── source.yaml            # Config: DB credentials, exposed tables/columns, descriptions
├── main.py                # Entry point
├── config.py              # SourceConfig class
├── introspect.py          # DB schema introspection
├── generate.py            # MCP JSON generation
├── mcp_description.json   # Output (generated)
└── README.md
```

## Setup

### Prerequisites

- Python 3.10+
- MySQL running locally
- A read-only MySQL user

### Create a read-only user (in MySQL as root)

```sql
CREATE USER 'source_reader'@'127.0.0.1' IDENTIFIED BY 'your_password';
GRANT SELECT ON your_database.* TO 'source_reader'@'127.0.0.1';
FLUSH PRIVILEGES;
```

### Install dependencies

```bash
pip install pyyaml mysql-connector-python
```

## Configuration

Edit `source.yaml` to match your database:

```yaml
database:
  type: mysql
  host: 127.0.0.1
  port: 3306
  name: pharma_lab_db
  user: source_reader
  password: "your_password"

server:
  name: "My Dataset"
  description: "Description of your dataset for AI agents."

tables:
  molecules:
    expose: true
    description: "Description of this table."
    columns:
      molecule_id:
        expose: true
        description: "Unique identifier"
      name:
        expose: true
        description: "Name of the molecule"
      secret_field:
        expose: false          # this column stays hidden
```

Each table and each column has an `expose` flag. Only `expose: true` fields end up in the MCP description. The `description` you write is what AI agents will read to understand your data.

## Run

```bash
python main.py
```

Output:

```
📖 Loading source.yaml...
🔌 Connected to pharma_lab_db
🔍 Introspecting database schema...
   → 6 exposed columns found
   → 1 enums detected
⚙️  Generating MCP description...
   → 2 tools generated

✅ mcp_description.json generated
```

## Output

The generated `mcp_description.json` contains:

- **Server info** — name and description of your dataset
- **Tools** — MCP tool definitions with input schemas that AI agents can discover and call

Example tool:

```json
{
  "name": "search_molecules",
  "description": "Search molecules. Molecules characterized by the laboratory...",
  "inputSchema": {
    "type": "object",
    "properties": {
      "type": {
        "type": "string",
        "description": "Therapeutic family",
        "enum": ["GPCR_agonist", "kinase_inhibitor", "protease_inhibitor"]
      },
      "ic50_nm": {
        "type": "number",
        "description": "Half-maximal inhibitory concentration in nanomolars."
      }
    },
    "required": []
  }
}
```

## Security

- The script connects with a **read-only MySQL user** — it can never modify your data.
- The password is used only to open the connection and is never stored in the application.
- Columns with `expose: false` are completely excluded from the output.
- No LLM is involved — the MCP server is fully deterministic.