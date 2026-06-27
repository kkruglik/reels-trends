from reels_trends.db.models import ReelsModel
from reels_trends.repository.tasks import TaskRepository
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.sqlite import insert
from typing import Protocol, Iterable, TypedDict, cast
from datetime import datetime
import asyncio
import logging
import httpx2 as httpx

logger = logging.getLogger(__name__)


class TaskState(TypedDict, total=False):
    apify_task_id: str
    account_name: str
    scraped_data: list


class TaskContext(TypedDict):
    db_session: AsyncSession
    http_client: httpx.AsyncClient


class PipelineStep(Protocol):
    name: str
    retry_count: int

    async def apply(self, state: TaskState, cntx: TaskContext) -> TaskState: ...
    def should_apply(self, state: TaskState) -> bool: ...


class ApifyStartRunStep:
    name = "start_run"
    retry_count = 3

    def should_apply(self, state: TaskState) -> bool:
        return True

    async def apply(self, state: TaskState, ctx: TaskContext) -> TaskState:
        account = state["account_name"]
        logger.info("Starting Apify run for account=%s", account)
        config = await TaskRepository.get_task(ctx["db_session"], account)

        response = await ctx["http_client"].post(
            "https://api.apify.com/v2/acts/apify~instagram-reel-scraper/runs",
            json={**config.model_dump(by_alias=True, exclude_none=True), "username": [config.username]},
        )
        response.raise_for_status()
        run_id = response.json()["data"]["id"]
        logger.info("Apify run started account=%s run_id=%s", account, run_id)
        return cast(TaskState, {"apify_task_id": run_id})


class ApifyFetchResultsStep:
    name = "fetch_results"
    retry_count = 3

    def should_apply(self, state: TaskState) -> bool:
        return bool(state.get("apify_task_id"))

    async def apply(self, state: TaskState, ctx: TaskContext) -> TaskState:
        run_id = state["apify_task_id"]
        logger.info("Polling run_id=%s", run_id)

        while True:
            response = await ctx["http_client"].get(
                f"https://api.apify.com/v2/actor-runs/{run_id}",
                )
            response.raise_for_status()
            status = response.json()["data"]["status"]
            logger.debug("run_id=%s status=%s", run_id, status)

            if status == "SUCCEEDED":
                break
            if status in ("FAILED", "ABORTED", "TIMED_OUT"):
                logger.error("run_id=%s terminal status=%s", run_id, status)
                raise RuntimeError(f"Apify run {run_id} failed: {status}")

            await asyncio.sleep(10)

        logger.info("Fetching dataset for run_id=%s", run_id)
        results = await ctx["http_client"].get(
            f"https://api.apify.com/v2/actor-runs/{run_id}/dataset/items",
        )
        results.raise_for_status()
        items = results.json()
        logger.info("Fetched %d items for run_id=%s", len(items), run_id)
        return {"scraped_data": items}


class SaveApifyResults:
    name = "save_results"
    retry_count = 3

    def should_apply(self, state: TaskState) -> bool:
        return bool(state.get("scraped_data"))

    async def apply(self, state: TaskState, ctx: TaskContext) -> TaskState:
        data = state["scraped_data"]
        logger.info("Saving %d reels to database", len(data))

        rows = [
            {
                "instagram_id": item["id"],
                "short_code": item["shortCode"],
                "url": item["url"],
                "owner_username": item["ownerUsername"],
                "owner_id": item["ownerId"],
                "caption": item.get("caption"),
                "hashtags": item.get("hashtags", []),
                "mentions": item.get("mentions", []),
                "likes_count": item["likesCount"],
                "comments_count": item["commentsCount"],
                "video_view_count": item.get("videoViewCount"),
                "video_play_count": item.get("videoPlayCount"),
                "video_duration": item.get("videoDuration"),
                "posted_at": datetime.fromisoformat(item["timestamp"]),
            }
            for item in data
        ]

        now = datetime.now()
        stmt = insert(ReelsModel).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["instagram_id"],
            set_={
                **{k: stmt.excluded[k] for k in rows[0] if k != "instagram_id"},
                "updated_at": now,
            },
        )
        await ctx["db_session"].execute(stmt)
        await ctx["db_session"].commit()
        logger.info("Upserted %d reels", len(rows))
        return {}


async def run_pipeline(
    steps: Iterable[PipelineStep], state: TaskState, cntx: TaskContext
):
    for step in steps:
        if not step.should_apply(state):
            logger.debug("Skipping step=%s", step.name)
            continue
        logger.info("Running step=%s", step.name)
        try:
            state |= await step.apply(state, cntx)
        except Exception:
            logger.exception("Step=%s failed", step.name)
            raise
