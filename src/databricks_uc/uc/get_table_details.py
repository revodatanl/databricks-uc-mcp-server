import asyncio
import aiohttp

from uc.utils import fetch_with_backoff

# Adjust this to control concurrency (Databricks recommends being conservative)
MAX_CONCURRENT_REQUESTS = 8
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
MAX_RETRIES = 5
BASE_DELAY = 0.5


async def get_single_table_details(
    session: aiohttp.ClientSession,
    databricks_host: str,
    headers: dict[str, str],
    full_table_name: list[str],
):
    """
    Asynchronously retrieves detailed metadata for a single table from the Databricks Unity Catalog API.

    Args:
        session (aiohttp.ClientSession): The active HTTP session for making requests.
        databricks_host (str): The Databricks workspace host URL.
        headers (dict): HTTP headers to include the authorization.
        full_table_name (str): The fully qualified table name in the format 'catalog.schema.table'.

    Returns:
        dict: The JSON response containing table metadata and details.

    """
    url = f"{databricks_host}/api/2.1/unity-catalog/tables/{full_table_name}"
    data = await fetch_with_backoff(
        session, url, headers, semaphore, MAX_RETRIES, BASE_DELAY
    )
    return data


async def get_all_table_details_asynchronous(
    databricks_host: str, headers: dict, full_table_names: list[str]
) -> list[dict[str, any]]:
    """
    Asynchronously retrieves detailed metadata for multiple tables from the Databricks Unity Catalog API.

    Args:
        databricks_host (str): The Databricks workspace host URL.
        headers (dict): HTTP headers to include the authorization.
        full_table_names (list[str]): A list of fully qualified table names in the format 'catalog.schema.table'.

    Returns:
        list[dict[str, any]]: A list of dictionaries containing selected metadata for each table, including table name, catalog, schema, and columns (with only name and type_text for each column).
    """
    async with aiohttp.ClientSession() as session:
        table_tasks = [
            get_single_table_details(session, databricks_host, headers, full_table_name)
            for full_table_name in full_table_names
        ]
        tables_nested = await asyncio.gather(*table_tasks)
        keys_to_include = [
            "name",
            "catalog_name",
            "schema_name",
            "columns",
            # "comment" Not available in every table
        ]
        result = [
            {
                **t,
                "columns": [
                    {"name": c["name"], "type_text": c["type_text"]}
                    for c in t["columns"]
                ],
            }
            for t in [
                {key: table[key] for key in keys_to_include} for table in tables_nested
            ]
        ]
        return result


async def get_table_details(
    databricks_host, databricks_token, full_table_names
) -> dict[str, any]:
    """
    Asynchronously retrieves metadata for multiple Databricks tables and returns the results in a standardized response format.

    Args:
        databricks_host (str): The Databricks workspace host URL.
        databricks_token (str): The Databricks personal access token for authentication.
        full_table_names (list[str]): A list of fully qualified table names in the format 'catalog.schema.table'.

    Returns:
        dict[str, any]: On success, returns {"resource": <list of table metadata>, "status": "success"}.
                        On failure, returns {"error": {"message": <error message>}}.
    """
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }
    try:
        all_table_details = await get_all_table_details_asynchronous(
            databricks_host, headers, full_table_names
        )
        return {"resource": all_table_details, "status": "success"}
    except Exception as e:
        print(e)
        return {"error": {"message": str(e)}}
