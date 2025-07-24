import asyncio
import aiohttp
from typing import Optional, List, Dict, Any
from api.utils import (
    fetch_with_backoff,
    format_toolcall_response,
    UCClientConfig,
    get_uc_session,
)


async def _get_catalogs(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, config: UCClientConfig
) -> List[str]:
    """Get list of catalogs excluding system catalogs"""
    data = await fetch_with_backoff(
        session,
        "unity-catalog/catalogs",
        semaphore,
        config.max_retries,
        config.base_delay,
    )
    return [
        c["name"] for c in data.get("catalogs", []) if c["created_by"] != "System user"
    ]


async def _get_schemas_in_catalog(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    config: UCClientConfig,
    catalog_name: str,
) -> List[str]:
    """Get schemas in a catalog excluding information_schema"""
    endpoint = f"unity-catalog/schemas?catalog_name={catalog_name}"
    data = await fetch_with_backoff(
        session, endpoint, semaphore, config.max_retries, config.base_delay
    )
    return [
        s["name"] for s in data.get("schemas", []) if s["name"] != "information_schema"
    ]


async def get_tables_in_schema(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    config: UCClientConfig,
    catalog_name: str,
    schema_name: str,
) -> List[str]:
    """Get tables in a schema"""
    endpoint = (
        f"unity-catalog/tables?catalog_name={catalog_name}&schema_name={schema_name}"
    )
    data = await fetch_with_backoff(
        session, endpoint, semaphore, config.max_retries, config.base_delay
    )
    return [f"{catalog_name}.{schema_name}.{t['name']}" for t in data.get("tables", [])]


async def get_table_details(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    config: UCClientConfig,
    full_table_name: str,
) -> Dict[str, Any]:
    """Get detailed information about a specific table, accepts a list of tables"""
    endpoint = f"unity-catalog/tables/{full_table_name}"
    return await fetch_with_backoff(
        session, endpoint, semaphore, config.max_retries, config.base_delay
    )


async def list_all_tables(config: Optional[UCClientConfig] = None) -> Dict[str, Any]:
    """
    List all tables in all catalogs and schemas
    """
    config = config or UCClientConfig()

    try:
        async with get_uc_session(config) as (session, semaphore, cfg):
            # Get catalogs
            catalogs = await _get_catalogs(session, semaphore, cfg)

            # Get schemas for each catalog
            schema_tasks = [
                _get_schemas_in_catalog(session, semaphore, cfg, catalog)
                for catalog in catalogs
            ]
            schemas_per_catalog = await asyncio.gather(*schema_tasks)

            # Create catalog-schema pairs
            catalog_schema_pairs = [
                (catalog, schema)
                for catalog, schemas in zip(catalogs, schemas_per_catalog)
                for schema in schemas
            ]

            # Get tables for each schema
            table_tasks = [
                get_tables_in_schema(session, semaphore, cfg, catalog, schema)
                for catalog, schema in catalog_schema_pairs
            ]
            tables_nested = await asyncio.gather(*table_tasks)

            # Organize results
            result = {}
            for table in (tbl for sublist in tables_nested for tbl in sublist):
                catalog_name, schema_name, table_name = table.split(".")
                result.setdefault(catalog_name, {}).setdefault(schema_name, []).append(
                    table_name
                )

            return format_toolcall_response(success=True, content=result)

    except Exception as e:
        return format_toolcall_response(success=False, error=e)


async def get_tables_details(
    full_table_names: List[str], config: Optional[UCClientConfig] = None
) -> Dict[str, Any]:
    """
    Get detailed information about multiple tables
    """
    config = config or UCClientConfig()

    try:
        async with get_uc_session(config) as (session, semaphore, cfg):
            # Fetch details for all tables concurrently
            table_tasks = [
                get_table_details(session, semaphore, cfg, table_name)
                for table_name in full_table_names
            ]
            tables_data = await asyncio.gather(*table_tasks)

            # Process and filter the results
            keys_to_include = ["name", "catalog_name", "schema_name", "columns"]
            result = [
                {
                    **t,
                    "columns": [
                        {"name": c["name"], "type_text": c["type_text"]}
                        for c in t["columns"]
                    ],
                }
                for t in [
                    {key: table[key] for key in keys_to_include}
                    for table in tables_data
                ]
            ]

            return format_toolcall_response(success=True, content=result)

    except Exception as e:
        return format_toolcall_response(success=False, error=e)
