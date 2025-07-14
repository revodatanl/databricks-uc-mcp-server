"""
A Simple POC for a Databricks Unity Catalog MCP Server.

It provides tools for:
    - Listing all available Databricks profiles from the user's databrickscfg configuration file
    - Retrieving all tables across all catalogs and schemas in a Databricks workspace
    - Fetching detailed metadata for one or more tables

The server is initialized in the main function.
"""

import os
import configparser
from async_lru import alru_cache
from mcp.server.fastmcp import FastMCP

from modules.get_all_tables import get_all_tables
from modules.get_table_details import get_table_details


# Initialize FastMCP server
mcp = FastMCP("RevoData Databricks Unity Catalog MCP")


@alru_cache()
@mcp.tool(
    name="get-databricks-profiles",
    description="Get all databricks profiles from the user's configuration file to authenticate with Databricks. Always ask which profile the user wants to use. Never print out the tokens to the user.",
)
def get_databricks_profiles():
    """
    Retrieves all available Databricks Workspace profiles (host/token combinations) from the user's .databrickscfg configuration file.

    Returns:
        dict[str, dict[str, str]]: A dictionary mapping profile names to their corresponding configuration key-value pairs.
    """
    config_path = os.path.expanduser("~/.databrickscfg")
    config = configparser.ConfigParser()
    with open(config_path, "r") as f:
        config.read_file(f)
    output_dict = {}
    for section in config.sections():
        items = dict(config.items(section))
        output_dict[section] = items

    return output_dict


@alru_cache()
@mcp.tool(
    name="get-all-catalogs-schemas-tables-in-workspace",
    description="Get all tables in all catalogs and schemas available in all Unity Catalogs assigned to a Databricks Workspace",
)
async def get_all_tables_in_workspace(databricks_host: str, databricks_token: str):
    """
    Asynchronously retrieves all tables from all Catalogs assigned to a Databricks Workspace, returning the results
    in a standardized response dictionary indicating success or error.
    Args:
        databricks_host (str): databricks host
        databricks_token (str): databricks token

    Returns:
        dict[str, any]:
            On success: {"resource": <nested catalog-schema-table dict>, "status": "success"}
            On failure: {"error": {"message": <error message>}}
    """
    result = await get_all_tables(databricks_host, databricks_token)
    return result


@mcp.tool(
    name="get-table-details",
    description="Get table details like description and columns from the full three-level namespace catalog.schema.tablename",
)
async def get_table_details_by_full_tablename(
    databricks_host: str, databricks_token: str, full_table_names: list[str]
):
    """
    Synchronously retrieves table details from multiple supplied table names.

    Args:
        databricks_host (str): databricks host
        databricks_token (str): databricks token
        full_table_names (list[str]): List of tables to get the details for, must be in format catalog.schema.tablename

    Returns:
        dict[str, any]:
            On success: {"resource": <list of dictionaries containing table data>, "status": "success"}
            On failure: {"error": {"message": <error message>}}
    """
    result = await get_table_details(
        databricks_host, databricks_token, full_table_names
    )
    return result


def main():
    # Core application logic
    mcp.run()


if __name__ == "__main__":
    main()
