from reels_trends.settings import secrets
from typing import Any
import httpx2 as httpx
import asyncio
import logging

logger = logging.getLogger(__name__)


async def poll_apify_run(
    http_client: httpx.AsyncClient, run_id: str, account: str
) -> list[Any]:
    """Poll an Apify actor run until it succeeds, then fetch its dataset items.
    Transient 5xx responses from Apify's API are treated as "not ready yet" and
    retried, up to APIFY_POLL_MAX_TRANSIENT_ERRORS times, instead of failing the
    whole pipeline for a momentary upstream blip.
    """
    transient_errors = 0
    while True:
        try:
            response = await http_client.get(
                f"https://api.apify.com/v2/actor-runs/{run_id}",
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code < 500:
                raise
            transient_errors += 1
            if transient_errors >= secrets.APIFY_POLL_MAX_TRANSIENT_ERRORS:
                raise
            logger.warning(
                "apify poll transient error account=%s run_id=%s status=%d attempt=%d/%d",
                account,
                run_id,
                e.response.status_code,
                transient_errors,
                secrets.APIFY_POLL_MAX_TRANSIENT_ERRORS,
            )
            await asyncio.sleep(10)
            continue

        status = response.json()["data"]["status"]
        logger.debug("poll account=%s run_id=%s status=%s", account, run_id, status)

        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "ABORTED", "TIMED_OUT"):
            raise RuntimeError(
                f"run failed account={account} run_id={run_id} status={status}"
            )

        await asyncio.sleep(10)

    transient_errors = 0
    while True:
        try:
            results = await http_client.get(
                f"https://api.apify.com/v2/actor-runs/{run_id}/dataset/items",
            )
            results.raise_for_status()
            break
        except httpx.HTTPStatusError as e:
            transient_errors += 1
            if e.response.status_code < 500 or (
                transient_errors >= secrets.APIFY_POLL_MAX_TRANSIENT_ERRORS
            ):
                raise
            logger.warning(
                "apify dataset fetch transient error account=%s run_id=%s status=%d attempt=%d/%d",
                account,
                run_id,
                e.response.status_code,
                transient_errors,
                secrets.APIFY_POLL_MAX_TRANSIENT_ERRORS,
            )
            await asyncio.sleep(10)

    items = results.json()
    logger.info("fetched account=%s run_id=%s count=%d", account, run_id, len(items))
    return items
