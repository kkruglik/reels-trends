from zoneinfo import ZoneInfo
from reels_trends.pipeline.base import TaskContext, START, ApifyBillingError
from reels_trends.db.utils import upsert_to_db
from reels_trends.db.models import ReelsModel
from datetime import datetime, timedelta, UTC, time
from typing import TypedDict, Any, cast
import asyncio
import logging

logger = logging.getLogger(__name__)


class ReelScraperInput(TypedDict, total=False):
    username: list[str]
    resultsLimit: int
    onlyPostsNewerThan: str
    skipPinnedPosts: bool
    skipTrialReels: bool
    includeSharesCount: bool
    includeTranscript: bool
    includeDownloadedVideo: bool


class ScrapePostsParams(TypedDict):
    daily_summary_timezone: str
    scrape_lookback_days: int
    scrape_results_limit: int


class ScrapePostsState(TypedDict, total=False):
    account_name: str
    scrape_posts_apify_task_id: str
    scraped_data: list[Any]
    params: ScrapePostsParams


class ScrapeInstagramPostsStep:
    name = "scrape_instagram_posts"
    retry_count = 3
    depends = [START]

    def should_apply(self, state: ScrapePostsState) -> bool:
        now = datetime.now(ZoneInfo(state["params"]["daily_summary_timezone"])).time()
        if now >= time(22, 0) or now < time(7, 0):
            return False
        return True

    async def apply(
        self, state: ScrapePostsState, ctx: TaskContext
    ) -> ScrapePostsState:
        account = state["account_name"]
        p = state["params"]
        cutoff = (
            datetime.now(UTC) - timedelta(days=p["scrape_lookback_days"])
        ).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        payload: ReelScraperInput = {
            "username": [account],
            "resultsLimit": p["scrape_results_limit"],
            "onlyPostsNewerThan": cutoff,
        }
        response = await ctx["http_client"].post(
            "https://api.apify.com/v2/acts/apify~instagram-reel-scraper/runs",
            # params={"memory": 256},
            json=payload,
        )
        if response.status_code == 403:
            raise ApifyBillingError(f"Apify account out of credits: {response.text}")
        response.raise_for_status()
        run_id = response.json()["data"]["id"]
        logger.info("run started account=%s run_id=%s", account, run_id)
        return cast(ScrapePostsState, {"scrape_posts_apify_task_id": run_id})


class FetchInstagramPostsStep:
    name = "fetch_instagram_posts"
    retry_count = 3
    depends = ["scrape_instagram_posts"]

    def should_apply(self, state: ScrapePostsState) -> bool:
        return bool(state.get("scrape_posts_apify_task_id"))

    async def apply(
        self, state: ScrapePostsState, ctx: TaskContext
    ) -> ScrapePostsState:
        account = state["account_name"]
        run_id = state["scrape_posts_apify_task_id"]

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
        return cast(ScrapePostsState, {"scraped_data": items})


class SaveInstagramPostsStep:
    name = "save_instagram_posts"
    retry_count = 3
    depends = ["fetch_instagram_posts"]

    def should_apply(self, state: ScrapePostsState) -> bool:
        return bool(state.get("scraped_data"))

    async def apply(
        self, state: ScrapePostsState, ctx: TaskContext
    ) -> ScrapePostsState:
        account = state["account_name"]
        data = state["scraped_data"]
        valid = [item for item in data if item.get("id")]
        skipped = len(data) - len(valid)
        if skipped:
            logger.warning("skipped %d item(s) without id account=%s", skipped, account)
        if not valid:
            logger.info("no reels to save account=%s", account)
            return {}
        rows = [
            {
                "instagram_id": item["id"],
                "short_code": item["shortCode"],
                "url": item["url"],
                "owner_id": item["ownerId"],
                "caption": item.get("caption"),
                "hashtags": item.get("hashtags", []),
                "mentions": item.get("mentions", []),
                "likes_count": item["likesCount"],
                "comments_count": item["commentsCount"],
                "video_view_count": item.get("videoViewCount"),
                "video_play_count": item.get("videoPlayCount"),
                "video_duration": item.get("videoDuration"),
                "posted_at": datetime.fromisoformat(item["timestamp"]).replace(
                    tzinfo=UTC
                ),
                "username": item["ownerUsername"],
            }
            for item in valid
        ]
        await upsert_to_db(ctx["db_session"], rows, ReelsModel, "instagram_id")
        logger.info("saved account=%s count=%d", account, len(rows))
        return {}
