from reels_trends.db.models import TaskModel
from reels_trends.db.session import get_session
from reels_trends.pipeline import PIPELINE_STEPS
from reels_trends.pipeline.base import TaskContext, run_pipeline
from reels_trends.settings import secrets
from aiogram import Bot
from google.cloud import bigquery
from sqlalchemy import select, distinct
from asyncio import Queue, Semaphore
import httpx2 as httpx
import logging

logger = logging.getLogger(__name__)

queue: Queue[tuple[str, str, dict, str]] = Queue()
in_flight: set[tuple[str, str, str]] = set()
semaphore = Semaphore(3)


async def enqueue_one(
    pipeline: str, instagram_account: str, params: dict, job_id: str = ""
) -> None:
    key = (instagram_account, pipeline, job_id)
    if key not in in_flight:
        in_flight.add(key)
        await queue.put((instagram_account, pipeline, params, job_id))
    logger.info("Enqueued item pipeline=%s job_id=%s", pipeline, job_id)


async def enqueue_all(pipeline: str, params: dict, job_id: str = "") -> None:
    async with get_session() as session:
        result = await session.execute(select(distinct(TaskModel.username)))
        usernames = list(result.scalars().all())
    enqueued = 0
    for username in usernames:
        key = (username, pipeline, job_id)
        if key not in in_flight:
            in_flight.add(key)
            await queue.put((username, pipeline, params, job_id))
            enqueued += 1
    logger.info("Enqueued %d items pipeline=%s job_id=%s", enqueued, pipeline, job_id)


async def worker(bot: Bot, big_query_client: bigquery.Client) -> None:
    while True:
        username, pipeline, params, job_id = await queue.get()
        try:
            async with (
                semaphore,
                get_session() as db_session,
                httpx.AsyncClient(
                    headers={"Authorization": f"Bearer {secrets.APIFY_TOKEN}"},
                    timeout=httpx.Timeout(secrets.WORKER_HTTPX_TIMEOUT),
                ) as http_client,
            ):
                ctx: TaskContext = {
                    "db_session": db_session,
                    "http_client": http_client,
                    "bot": bot,
                    "big_query_client": big_query_client,
                }
                await run_pipeline(
                    PIPELINE_STEPS[pipeline],
                    {"account_name": username, "params": params},
                    ctx,
                )
        except Exception:
            logger.exception(
                "Worker failed username=%s pipeline=%s params=%s",
                username,
                pipeline,
                params,
            )
        finally:
            in_flight.discard((username, pipeline, job_id))
            queue.task_done()
