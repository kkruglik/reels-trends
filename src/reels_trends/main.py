from reels_trends.pipeline.scrape_posts import (
    ScrapeInstagramPostsStep,
    FetchInstagramPostsStep,
    SaveInstagramPostsStep,
)
from reels_trends.pipeline.scrape_profiles import (
    ScrapeInstagramProfileStep,
    FetchInstagramProfileStep,
    SaveInstagramProfileStep,
)
from reels_trends.pipeline.check_trends import (
    FetchTrendingData,
    PredictTrending,
    NotifyTrending,
)
from reels_trends.pipeline.daily_summary import NotifySummary
from reels_trends.pipeline.base import TaskContext, run_pipeline
from reels_trends.bot import create_bot
from aiogram import Bot
from reels_trends.db.models import Base, TaskModel
from reels_trends.db.session import engine, get_session
from reels_trends.settings import settings
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from asyncio import Semaphore, Queue
from sqlalchemy import select, distinct
import asyncio
import httpx2 as httpx
import logging

logger = logging.getLogger(__name__)

PIPELINE_STEPS = {
    "posts": [
        ScrapeInstagramPostsStep(),
        FetchInstagramPostsStep(),
        SaveInstagramPostsStep(),
    ],
    "profile": [
        ScrapeInstagramProfileStep(),
        FetchInstagramProfileStep(),
        SaveInstagramProfileStep(),
    ],
    "trends": [
        FetchTrendingData(),
        PredictTrending(),
        NotifyTrending(),
    ],
    "summary": [
        NotifySummary(),
    ],
}

semaphore = Semaphore(3)
queue: Queue[tuple[str, str]] = Queue()
in_flight: set[tuple[str, str]] = set()
_bot: Bot | None = None


async def worker() -> None:
    if _bot is None:
        raise RuntimeError("Bot is not initialized")
    while True:
        username, pipeline = await queue.get()
        try:
            async with (
                semaphore,
                get_session() as db_session,
                httpx.AsyncClient(
                    headers={"Authorization": f"Bearer {settings.APIFY_TOKEN}"},
                    timeout=httpx.Timeout(200.0),
                ) as http_client,
            ):
                ctx: TaskContext = {
                    "db_session": db_session,
                    "http_client": http_client,
                    "bot": _bot,
                }
                await run_pipeline(
                    PIPELINE_STEPS[pipeline], {"account_name": username}, ctx
                )
        except Exception:
            logger.exception(
                "Worker failed username=%s pipeline=%s", username, pipeline
            )
        finally:
            in_flight.discard((username, pipeline))
            queue.task_done()


async def enqueue(pipeline: str) -> None:
    async with get_session() as session:
        result = await session.execute(select(distinct(TaskModel.username)))
        usernames = list(result.scalars().all())
    enqueued = 0
    for username in usernames:
        item = (username, pipeline)
        if item not in in_flight:
            in_flight.add(item)
            await queue.put(item)
            enqueued += 1
    logger.info("Enqueued %d items pipeline=%s", enqueued, pipeline)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(module)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(enqueue, "interval", hours=2, args=["posts"], id="global_posts")
    scheduler.add_job(enqueue, "cron", minute=15, args=["trends"], id="global_trends")
    scheduler.add_job(
        enqueue, "cron", hour=3, minute=0, args=["profile"], id="global_profile"
    )
    scheduler.add_job(
        enqueue,
        "cron",
        hour=20,
        minute=0,
        args=["summary"],
        id="global_summary",
        timezone="Europe/Tallinn",
    )
    scheduler.start()
    logger.info("Scheduler started with 4 global jobs")

    workers = [asyncio.create_task(worker()) for _ in range(3)]

    bot, dp = create_bot(settings.TELEGRAM_BOT_TOKEN)
    global _bot
    _bot = bot
    logger.info("Starting bot polling")
    try:
        await dp.start_polling(bot)
    finally:
        for w in workers:
            w.cancel()
