import asyncio
from async_lru import alru_cache
from mcp.server import FastMCP

import databricks_uc.api.unity_catalog_client as UCClient
import databricks_uc.api.jobs_client as JobsClient


class DatabricksMCPServer(FastMCP):
    def __init__(self):
        super().__init__("RevoData Databricks MCP")
        self._register_mcp_tools()

    async def run(self):
        """Override the parent's run method to avoid anyio.run() call"""
        return await self.run_stdio_async()

    def _register_mcp_tools(self):
        @self.tool(
            name="get-all-catalogs-schemas-tables-in-workspace",
            description="Get all tables in all catalogs and schemas available in all Unity Catalogs assigned to a Databricks Workspace",
        )
        async def get_all_tables_in_workspace():
            return await UCClient.list_all_tables()

        @self.tool(
            name="get-table-details",
            description="Get table details like description and columns from the full three-level namespace catalog.schema.tablename",
        )
        async def get_table_details_by_full_tablename(full_table_names: list[str]):
            return await UCClient.get_tables_details(full_table_names)

        @self.tool(
            name="get-jobs-in-workspace",
            description="Retrieve a list of all jobs in the workspace including details",
        )
        async def get_jobs_in_workspace():
            return await JobsClient.list_jobs()


async def main():
    server = DatabricksMCPServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
