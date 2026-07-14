import asyncio
import logging
import httpx2 as httpx

logger = logging.getLogger(__name__)

_TERMINAL_STATUSES = ("FAILED", "ABORTED", "TIMED_OUT")


async def validate_instagram_profile(
    username: str, http_client: httpx.AsyncClient
) -> dict:
    response = await http_client.post(
        "https://api.apify.com/v2/acts/apify~instagram-profile-scraper/runs",
        params={"memory": 256},
        json={"usernames": [username]},
    )
    response.raise_for_status()
    run_id = response.json()["data"]["id"]
    logger.info("Profile validation started username=%s run_id=%s", username, run_id)

    while True:
        response = await http_client.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}"
        )
        response.raise_for_status()
        status = response.json()["data"]["status"]
        if status == "SUCCEEDED":
            break
        if status in _TERMINAL_STATUSES:
            raise RuntimeError(f"Scrape failed: {status}")
        await asyncio.sleep(10)

    results = await http_client.get(
        f"https://api.apify.com/v2/actor-runs/{run_id}/dataset/items"
    )
    results.raise_for_status()
    items = results.json()
    if not items:
        raise ValueError("Profile not found")
    profile = items[0]
    if "id" not in profile:
        raise ValueError("Profile not available (private, empty, or restricted)")
    logger.info("Profile validation succeeded username=%s", username)
    return profile
