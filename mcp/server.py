"""
A Simple POC for a Databricks Unity Catalog MCP Server.

It contains the functions for the tools that are used to interact with the Databricks Unity Catalog. It can find tables, and table columns and data types. It can also find tables by name.

The server is initialized in the main function.
"""

import os
from difflib import SequenceMatcher
from typing import Any

from async_lru import (
    alru_cache,  # async_lru is required for async functions otherweise the coroutine will be cached iso the results
)
from databricks.sdk import WorkspaceClient
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("RevoData Databricks Unity Catalog MCP")

# TODO: extract functions to a separate files
# TODO: extract the workspace client into a separate function/file to avoid code duplication

# ? While all functions are async, the Databricks SDK is not async, hence the code is executed synchronously.
# ? A potential solution is to use the Databricks REST API with asyncio.


@alru_cache()
async def request_table_details(full_table_name: str) -> list[dict[str, Any]]:
    """Fetch table columns from Databricks.

    Args:
        full_table_name: The fully qualified table name in format 'catalog.schema.table'

    Returns:
        A list of dictionaries containing column information with keys:
        - name: Column name
        - type: Column data type
    """
    w = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
    )
    table = w.tables.get(full_name=full_table_name)
    columns = [
        {
            "name": column.name,
            "type": column.type_name.value,
            # "comment": column.comment, #TODO: add comments in the future, could provide usefull context for the LLM
        }
        for column in table.columns
    ]
    return columns


@alru_cache()
async def get_catalogs() -> list[dict[str, Any]]:
    """Get all the catalog names in a workspace, excluding the ones created by the system user.

    Returns:
        A list of dictionaries containing catalog information with keys:
        - name: Catalog name
    """
    w = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
    )

    catalogs = w.catalogs.list()
    return [
        {"name": catalog.name}
        for catalog in catalogs
        if not catalog.created_by == "System user"
    ]


@alru_cache()
async def get_catalogs_and_schemas(catalog_name: str) -> list[dict[str, Any]]:
    """Get all the schemas in a Databricks catalog, excluding the ones created by the system user.

    Args:
        catalog_name: The name of the catalog to get schemas from

    Returns:
        A list of dictionaries containing schema information with keys:
        - name: Schema full name (catalog.schema format)
    """
    w = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
    )

    return [
        {"name": schema.full_name}
        for schema in w.schemas.list(catalog_name)
        if schema.created_by != "System user"
    ]


@alru_cache()
async def get_tables(catalogs_and_schemas: str) -> list[dict[str, Any]]:
    """Get all the tables in a Databricks 'catalog.schema'.

    Args:
        catalogs_and_schemas: The full schema name in format "catalog.schema"

    Returns:
        A list of dictionaries containing table information with keys:
        - full_table_name: The full table name (catalog.schema.table format)
    """
    w = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
    )

    # split the single string into 2 parts: needed for the Databricks SDK
    catalog_name, schema_name = catalogs_and_schemas.split(".")
    tables = w.tables.list(catalog_name, schema_name)

    return [{"full_table_name": tables.full_name} for tables in tables]


async def get_all_tables() -> dict[str, Any]:
    """Get all the tables in a Databricks workspace.

    This function retrieves all tables across all catalogs and schemas in the workspace
    by first fetching all catalogs, then all schemas within each catalog, and finally
    all tables within each schema.

    Returns:
        A list of dictionaries containing table information with keys:
        - full_table_name: The full table name (catalog.schema.table format)
    """
    # Since we can not seearch through all catalogs and schemas at once with a singel table name, we do the following:

    # Get all catalogs in the workspace
    catalogs = await get_catalogs()

    # For each catalog, get all schemas (so that we have them in the format of 'catalog.schema')
    catalogs_and_schemas = []
    for catalog in catalogs:
        catalogs_and_schemas.extend(await get_catalogs_and_schemas(catalog["name"]))

    # For each catalog.schema, get all tables
    all_tables = []
    for catalog_and_schema in catalogs_and_schemas:
        all_tables.extend(await get_tables(catalog_and_schema["name"]))

    return all_tables


def format_table_details(columns: list[dict[str, Any]]) -> str:
    """Format table columns into a readable string.

    Args:
        columns: A list of dictionaries containing column information with keys:
            - name: The column name
            - type: The column data type

    Returns:
        A formatted string with each column on a new line in the format "name: type"
    """
    return "\n".join([f"{column['name']}: {column['type']}" for column in columns])


@mcp.tool(
    name="Get table details",
    description="Get the column names and data types for a specific Databricks table.",
)
async def get_table_columns(
    catalog_name: str, schema_name: str, table_name: str
) -> str:
    """Get the column names and data types for a specific Databricks table.

    This tool retrieves detailed schema information for a table in Databricks Unity Catalog.
    It returns a formatted list of all columns with their corresponding data types.
    Use this when you need to understand the structure of a table before writing queries
    or performing data analysis.

    Args:
        catalog_name: The name of the catalog in which the table resides.
        schema_name: The name of the schema in which the table resides.
        table_name: The name of the table to get the columns for.

    Returns:
        A formatted string containing each column name and its data type,
        with one column per line in the format "column_name: data_type".
        Returns an error message if the table cannot be found or accessed.
    """
    try:
        data = await request_table_details(f"{catalog_name}.{schema_name}.{table_name}")

        if data is None:
            return "Unable to fetch table columns data."

        formatted_columns = format_table_details(data)
        return formatted_columns
    except Exception as e:
        return f"Error fetching table columns: {str(e)}"


def format_table_matches(table_matches: list[str]) -> str:
    """Format the table matches into a readable string.

    Args:
        table_matches: A list of full table names that match the search criteria.

    Returns:
        A formatted string containing the count of matching tables and a bulleted
        list of all matching table names, with one table per line prefixed with "- ".
    """
    base_string = (
        f"Found the following {len(table_matches)} tables matching your query:\n"
    )

    return base_string + "\n".join([f"- {table}" for table in table_matches])


@mcp.tool(
    name="Find full table name",
    description="Find the full table name(s) in a Databricks workspace by searching for partial matches.",
)
async def find_full_table_name(table_name: str) -> str:
    """Find the full table name(s) in a Databricks workspace by searching for partial matches.

    This tool searches through all available tables in the Databricks workspace to find
    tables whose names are similar to the provided table name. It uses fuzzy string matching
    to find tables with at least 80% similarity to the search term. This is useful when you
    know part of a table name but need to find the complete catalog.schema.table_name format.

    Args:
        table_name: The partial or complete table name to search for. The search is
                   case-insensitive and matches against the table name portion only
                   (not the full catalog.schema.table_name).

    Returns:
        A formatted string containing the count of matching tables and a list of all
        matching full table names in catalog.schema.table_name format. If no matches
        are found, returns a message indicating no tables were found.
    """

    # Get all tables in the workspace
    try:
        all_tables = await get_all_tables()
    except Exception as e:
        return f"Error fetching all tables: {str(e)}"

    # For each table, check if the table name matches the search criteria with a similarity ratio of 80%
    table_matches = []
    for table in all_tables:
        similarity = SequenceMatcher(
            None, table["full_table_name"].split(".")[-1].lower(), table_name.lower()
        ).quick_ratio()
        if similarity >= 0.8:
            # If the table name matches the search criteria, add the full table name to the list
            table_matches.append(table["full_table_name"])

    # If there are any matches, return the formatted string with all the identified full table names
    if table_matches:
        return format_table_matches(table_matches)
    # If there are no matches, return a message indicating no tables were found
    else:
        return f"No matching tables found for table {table_name}"


if __name__ == "__main__":
    mcp.run()
