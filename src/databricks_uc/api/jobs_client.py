import asyncio
import aiohttp
from typing import List, Dict, Any

from api.utils import (
    fetch_with_backoff,
    format_toolcall_response,
    get_async_session,
)


async def fetch_jobs_from_endpoint(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore
) -> List[str]:
    """Retrieves a list of jobs"""
    data = await fetch_with_backoff(session, "jobs/list", semaphore)
    return data


async def list_jobs() -> Dict[str, Any]:
    """
    List all jobs in the users workspace
    """
    try:
        async with get_async_session() as (session, semaphore):
            # Get catalogs
            jobs = await fetch_jobs_from_endpoint(session, semaphore)
            result = jobs["jobs"]

            return format_toolcall_response(success=True, content=result)

    except Exception as e:
        return format_toolcall_response(success=False, error=e)


