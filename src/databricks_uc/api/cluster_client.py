import asyncio
import aiohttp
from typing import Optional, List, Dict, Any

from databricks_uc.api.utils import (
    fetch_with_backoff,
    format_toolcall_response,
    UCClientConfig,
    get_uc_session,
)


async def fetch_jobs_from_endpoint(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, config: UCClientConfig
) -> List[str]:
    """Retrieves a list of jobs"""
    data = await fetch_with_backoff(
        session, "jobs/list", semaphore, config.max_retries, config.base_delay
    )
    return data


async def fetch_job_details_from_endpoint(
        session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, config: UCClientConfig, job_id: int
) -> Dict[str, Any]:
    """Retrieve details of a single job"""
    data = await fetch_with_backoff(
        session, "jobs/get", semaphore, config.max_retries, config.base_delay
    )
    return data


async def list_jobs(config: Optional[UCClientConfig] = None) -> Dict[str, Any]:
    """
    List all tables in all catalogs and schemas
    """
    config = config or UCClientConfig()
    try:
        async with get_uc_session(config) as (session, semaphore, config):
            # Get catalogs
            jobs = await fetch_jobs_from_endpoint(session, semaphore, config)
            result = jobs["jobs"]

            return format_toolcall_response(success=True, content=result)

    except Exception as e:
        return format_toolcall_response(success=False, error=e)




