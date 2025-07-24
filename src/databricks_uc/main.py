import asyncio

from databricks_uc.server.mcp_server import DatabricksMCPServer

async def start_mcp_server():

    databricks_mcp_server = DatabricksMCPServer()
    await databricks_mcp_server.run()


async def main():

    await start_mcp_server()


def entrypoint():
    asyncio.run(main())

if __name__ == "__main__":
    
    entrypoint()