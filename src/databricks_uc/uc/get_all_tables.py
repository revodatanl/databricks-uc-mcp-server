import asyncio
import aiohttp

from uc.utils import fetch_with_backoff

# Adjust this to control concurrency (Databricks recommends being conservative)
MAX_CONCURRENT_REQUESTS = 8
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
MAX_RETRIES = 5
BASE_DELAY = 0.5


async def get_catalogs(session: str, databricks_host: str, headers: dict) -> list[str]:
    url = f"{databricks_host}/api/2.1/unity-catalog/catalogs"
    data = await fetch_with_backoff(
        session, url, headers, semaphore, MAX_RETRIES, BASE_DELAY
    )

    return [
        c["name"] for c in data.get("catalogs", []) if c["created_by"] != "System user"
    ]


async def get_schemas_in_catalog(
    session: str, databricks_host: str, headers: dict, catalog_name: str
) -> list[str]:
    """
    Asynchronously retrieves a list of catalog names from the Databricks Unity Catalog API,
    excluding catalogs created by the System user.

    Args:
        session (aiohttp.ClientSession): The active HTTP session for making requests.

    Returns:
        list[str]: A list of catalog names, excluding those created by "System user".
    """

    url = f"{databricks_host}/api/2.1/unity-catalog/schemas?catalog_name={catalog_name}"
    data = await fetch_with_backoff(
        session, url, headers, semaphore, MAX_RETRIES, BASE_DELAY
    )
    return [
        s["name"] for s in data.get("schemas", []) if s["name"] != "information_schema"
    ]


async def get_tables_in_schema(
    session: str,
    databricks_host: str,
    headers: dict,
    catalog_name: str,
    schema_name: str,
) -> list[str]:
    """
    Asynchronously retrieves all tables from a specified schema within a catalog, in the form of 'catalog.schema.table'.

    Args:
        session (aiohttp.ClientSession): The active HTTP session for making requests.
        catalog_name (str): The name of the catalog to query.
        schema_name (str): The name of the schema within the catalog.

    Returns:
        list[str]: A list of table names in the format 'catalog.schema.table'.
    """
    url = f"{databricks_host}/api/2.1/unity-catalog/tables?catalog_name={catalog_name}&schema_name={schema_name}"
    data = await fetch_with_backoff(
        session, url, headers, semaphore, MAX_RETRIES, BASE_DELAY
    )
    return [f"{catalog_name}.{schema_name}.{t['name']}" for t in data.get("tables", [])]


async def get_all_tables_asynchronous(
    databricks_host: str, headers: dict
) -> dict[str, list]:
    """
    Asynchronously retrieves all tables from all schemas in all catalogs using the Databricks Unity Catalog API,
    organizing the results in a nested dictionary structure.

    The function performs the following steps:
        1. Fetches all available catalogs.
        2. For each catalog, fetches all schemas concurrently.
        3. For each schema in each catalog, fetches all tables concurrently.
        4. Organizes the results into a nested dictionary of the form:
           {catalog_name: {schema_name: [table_name, ...], ...}, ...}

    Returns:
        dict[str, dict[str, list[str]]]:
            A nested dictionary mapping catalog names to schema names to lists of table names.
    """

    async with aiohttp.ClientSession() as session:
        # Step 1: Get all catalogs
        catalogs = await get_catalogs(session, databricks_host, headers)
        print(f"Found catalogs: {catalogs}")

        # Step 2: Get all schemas in all catalogs concurrently
        schema_tasks = [
            get_schemas_in_catalog(session, databricks_host, headers, catalog)
            for catalog in catalogs
        ]
        schemas_per_catalog = await asyncio.gather(*schema_tasks)
        catalog_schema_pairs = [
            (catalog, schema)
            for catalog, schemas in zip(catalogs, schemas_per_catalog)
            for schema in schemas
        ]
        print(f"Found {len(catalog_schema_pairs)} schemas in all catalogs.")

        # Step 3: Get all tables in all schemas in all catalogs concurrently
        table_tasks = [
            get_tables_in_schema(session, databricks_host, headers, catalog, schema)
            for catalog, schema in catalog_schema_pairs
        ]
        tables_nested = await asyncio.gather(*table_tasks)
        all_tables = [tbl for sublist in tables_nested for tbl in sublist]

        print(f"\nFound {len(all_tables)} tables in total:")

        # Return a list of all full tablenames
        # return all_tables

        # Return a dict
        full_catalog_dict = {}
        for table in all_tables:
            catalog_name, schema_name, table_name = table.split(".")
            if catalog_name not in full_catalog_dict:
                full_catalog_dict[catalog_name] = {}
            if schema_name not in full_catalog_dict[catalog_name]:
                full_catalog_dict[catalog_name][schema_name] = []
            full_catalog_dict[catalog_name][schema_name].append(table_name)

        return full_catalog_dict


async def get_all_tables(databricks_host: str, databricks_token: str) -> dict[str, any]:
    """
    Asynchronously retrieves all tables from the Databricks Unity Catalog, returning the results
    in a standardized response dictionary indicating success or error.

    This function wraps `get_all_tables_asynchronous()` with error handling, returning a dictionary
    that contains either the retrieved tables and a success status, or an error message.

    Returns:
        dict[str, any]:
            On success: {"resource": <nested catalog-schema-table dict>, "status": "success"}
            On failure: {"error": {"message": <error message>}}
    """
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }
    try:
        all_tables = await get_all_tables_asynchronous(databricks_host, headers)
        return {"resource": all_tables, "status": "success"}
    except Exception as e:
        return {"error": {"message": str(e)}}
