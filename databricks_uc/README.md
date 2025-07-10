# Databricks Unity Catalog MCP Integration

This module provides a set of tools for interacting with Databricks Unity Catalog through MCP (Model Control Plane). It enables easy access to catalog, schema, and table information within your Databricks workspace.

## Features

- List all catalogs in a workspace
- Get schemas within specific catalogs
- Retrieve table information and structure
- Search across all tables in the workspace
- Get detailed column information for specific tables

## Prerequisites

- Python 3.11 or higher
- Databricks workspace access
- Environment variables set up for Databricks authentication:
  - `DATABRICKS_HOST`
  - `DATABRICKS_TOKEN`

## Installation

This module is part of the mcp-quickstart-servers package. Install it using:

```bash
pip install mcp-quickstart-servers
```

## Usage

### Basic Examples

```python
from databricks_uc import server

# Get all catalogs
catalogs = await server.get_catalogs()

# Get schemas in a specific catalog
schemas = await server.get_catalogs_and_schemas("my_catalog")

# Get table columns
columns = await server.get_table_columns(
    catalog_name="my_catalog",
    schema_name="my_schema",
    table_name="my_table"
)

# Get all tables in a workspace
all_tables = await server.get_all_tables()
```

### Key Functions

#### `get_catalogs()`
Returns a list of all non-system catalogs in the workspace.

#### `get_catalogs_and_schemas(catalog_name: str)`
Returns all schemas within a specified catalog.

#### `get_tables(catalogs_and_schemas: str)`
Returns all tables within a specified catalog.schema combination.

#### `get_table_columns(catalog_name: str, schema_name: str, table_name: str)`
Returns detailed column information for a specific table, including column names and data types.

#### `get_all_tables()`
Returns a comprehensive list of all tables across all catalogs and schemas in the workspace.

## Environment Setup

Create a `.env` file in your project root with:

```env
DATABRICKS_HOST=your-workspace-url
DATABRICKS_TOKEN=your-access-token
```

## Dependencies

- databricks-sdk>=0.54.0
- async-lru>=2.0.5
- python-dotenv>=1.1.0 (for development)

## Development

To set up the development environment:

1. Clone the repository
2. Install development dependencies:
```bash
pip install -e ".[dev]"
```

## Testing

Basic testing can be performed using the provided testing script:

```python
import asyncio
from databricks_uc import server

async def main():
    tables = await server.get_all_tables()
    print(tables)

asyncio.run(main())
```

## Error Handling

The module includes built-in error handling for common issues such as:
- Invalid table names
- Connection errors
- Authentication failures
- Missing permissions

Errors are returned as formatted error messages for easy debugging.