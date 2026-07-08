from reels_trends.pipeline.base import TaskContext, START
from reels_trends.db.utils import upsert_to_db
from reels_trends.db.models import InstagramAccountModel
from typing import TypedDict, Any, cast
import asyncio
import logging

logger = logging.getLogger(__name__)


class ScrapeProfileState(TypedDict, total=False):
    account_name: str
    scrape_profile_apify_task_id: str
    scraped_data: list[Any]


class ScrapeInstagramProfileStep:
    name = "scrape_instagram_profile"
    retry_count = 3
    depends = [START]

    def should_apply(self, state: ScrapeProfileState) -> bool:
        return True

    async def apply(
        self, state: ScrapeProfileState, ctx: TaskContext
    ) -> ScrapeProfileState:
        account = state["account_name"]
        response = await ctx["http_client"].post(
            "https://api.apify.com/v2/acts/apify~instagram-profile-scraper/runs",
            json={"usernames": [account]},
        )
        response.raise_for_status()
        run_id = response.json()["data"]["id"]
        logger.info("run started account=%s run_id=%s", account, run_id)
        return cast(ScrapeProfileState, {"scrape_profile_apify_task_id": run_id})


class FetchInstagramProfileStep:
    name = "fetch_instagram_profile"
    retry_count = 3
    depends = ["scrape_instagram_profile"]

    def should_apply(self, state: ScrapeProfileState) -> bool:
        return bool(state.get("scrape_profile_apify_task_id"))

    async def apply(
        self, state: ScrapeProfileState, ctx: TaskContext
    ) -> ScrapeProfileState:
        account = state["account_name"]
        run_id = state["scrape_profile_apify_task_id"]

        while True:
            response = await ctx["http_client"].get(
                f"https://api.apify.com/v2/actor-runs/{run_id}",
            )
            response.raise_for_status()
            status = response.json()["data"]["status"]
            logger.debug("poll account=%s run_id=%s status=%s", account, run_id, status)

            if status == "SUCCEEDED":
                break
            if status in ("FAILED", "ABORTED", "TIMED_OUT"):
                raise RuntimeError(
                    f"run failed account={account} run_id={run_id} status={status}"
                )

            await asyncio.sleep(10)

        results = await ctx["http_client"].get(
            f"https://api.apify.com/v2/actor-runs/{run_id}/dataset/items",
        )
        results.raise_for_status()
        items = results.json()
        logger.info(
            "fetched account=%s run_id=%s count=%d", account, run_id, len(items)
        )
        return cast(ScrapeProfileState, {"scraped_data": items})


class SaveInstagramProfileStep:
    name = "save_instagram_profile"
    retry_count = 3
    depends = ["fetch_instagram_profile"]

    def should_apply(self, state: ScrapeProfileState) -> bool:
        return bool(state.get("scraped_data"))

    async def apply(
        self, state: ScrapeProfileState, ctx: TaskContext
    ) -> ScrapeProfileState:
        account = state["account_name"]
        data = state["scraped_data"]
        rows = [
            {
                "username": item["username"],
                "url": item["url"],
                "profile_id": item["id"],
                "follower_count": item["followersCount"],
                "total_post_count": item["postsCount"],
                "total_video_count": item.get("igtvVideoCount", 0),
                "full_name": item.get("fullName"),
                "verified": item.get("verified", False),
            }
            for item in data
        ]
        await upsert_to_db(ctx["db_session"], rows, InstagramAccountModel, "username")
        logger.info("saved account=%s followers=%d", account, data[0]["followersCount"])
        return {}
