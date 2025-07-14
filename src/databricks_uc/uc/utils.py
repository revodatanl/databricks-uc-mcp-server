import asyncio
import aiohttp


async def fetch_with_backoff(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict,
    semaphore: asyncio.Semaphore,
    max_retries: int = 5,
    base_delay: float = 0.5,
) -> dict:
    """
    Asynchronously fetches JSON data from a given URL using an aiohttp ClientSession,
    with automatic retries and exponential backoff on HTTP 429 (Too Many Requests) responses.
    """
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
