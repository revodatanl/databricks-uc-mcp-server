import os
import asyncio
import aiohttp


async def fetch_with_backoff(
    session: aiohttp.ClientSession,
    endpoint: str,
    semaphore: asyncio.Semaphore,
    max_retries: int = 5,
    base_delay: float = 0.5,
) -> dict:
    """
    Asynchronously fetches JSON data from a given URL using an aiohttp ClientSession,
    with automatic retries and exponential backoff on HTTP 429 (Too Many Requests) responses.
    """
    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_token = os.getenv("DATABRICKS_TOKEN")

    url = f"{databricks_host}/api/2.1/{endpoint}"
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }

    delay = base_delay
    for attempt in range(max_retries):
        async with semaphore:
            async with session.get(url, headers=headers) as response:
                if response.status == 429:
                    print(
                        f"429 Too Many Requests for {url}. Retrying in {delay} seconds..."
                    )
                    await asyncio.sleep(delay)
                    delay *= 2  # Exponential backoff
                    continue
                response.raise_for_status()
                return await response.json()
    raise Exception(f"Max retries {max_retries} exceeded for URL: {url}")
