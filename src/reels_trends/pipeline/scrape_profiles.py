from reels_trends.pipeline.base import TaskContext, START, ApifyBillingError
from reels_trends.pipeline.apify import poll_apify_run
from reels_trends.db.utils import upsert_to_db
from reels_trends.db.models import InstagramAccountModel
from typing import TypedDict, Any, cast
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
            params={"memory": 256},
            json={"usernames": [account]},
        )
        if response.status_code == 403:
            raise ApifyBillingError(f"Apify account out of credits: {response.text}")
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
        items = await poll_apify_run(ctx["http_client"], run_id, account)
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
                "total_post_count": item.get("postsCount", 0),
                "total_video_count": item.get("igtvVideoCount", 0),
                "full_name": item.get("fullName"),
                "verified": item.get("verified", False),
            }
            for item in data
        ]
        await upsert_to_db(ctx["db_session"], rows, InstagramAccountModel, "username")
        logger.info("saved account=%s count=%d", account, len(rows))
        return {}
