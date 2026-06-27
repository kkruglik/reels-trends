from sqlalchemy.exc import IntegrityError
from reels_trends.config import TaskConfig, ScrapeTaskConfig
from reels_trends.db.models import Base
from reels_trends.db.session import engine, get_session
from reels_trends.repository.tasks import TaskRepository
from reels_trends.pipeline import (
    run_pipeline,
    TaskState,
    TaskContext,
    ApifyStartRunStep,
    ApifyFetchResultsStep,
    SaveApifyResults,
)
from reels_trends.settings import settings
import asyncio
from asyncio import Semaphore
from asyncio.queues import Queue
import httpx2 as httpx
import logging


logger = logging.getLogger(__name__)

semaphore = Semaphore(3)
queue = Queue()

PIPELINE_STEPS = [
    ApifyStartRunStep(),
    ApifyFetchResultsStep(),
    SaveApifyResults(),
]


async def worker():
    while True:
        account_name = await queue.get()
        async with (
            semaphore,
            get_session() as db_session,
            httpx.AsyncClient(headers={"Authorization": f"Bearer {settings.APIFY_TOKEN}"}) as http_client,
        ):
            state: TaskState = {"account_name": account_name}
            ctx: TaskContext = {"db_session": db_session, "http_client": http_client}
            try:
                await run_pipeline(PIPELINE_STEPS, state, ctx)
            except Exception:
                logger.exception("Failed to process account=%s", account_name)
        queue.task_done()


async def start_workers():
    return [asyncio.create_task(worker()) for _ in range(3)]


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    task = TaskConfig(
        scrape_task_config=ScrapeTaskConfig(
            username="tillo.journal",
            results_limit=20,
        ),
        run_every="0 * * * *",
    )

    try:
        async with get_session() as db_session:
            await TaskRepository.create_task(db_session, task)
    except IntegrityError:
        logger.info("Task for %s already exists, skipping", "tillo.journal")

    workers = await start_workers()
    await queue.put("tillo.journal")
    await queue.join()
    for w in workers:
        w.cancel()
