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
from aiogram.types import BotCommand
from reels_trends.db.models import Base, TaskModel
from reels_trends.db.session import engine, get_session
from reels_trends.settings import secrets, config, IntervalSchedule
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
queue: Queue[tuple[str, str, dict, str]] = Queue()
in_flight: set[tuple[str, str, str]] = set()
_bot: Bot | None = None


async def worker() -> None:
    if _bot is None:
        raise RuntimeError("Bot is not initialized")
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
                    "bot": _bot,
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


async def enqueue(pipeline: str, params: dict, job_id: str = "") -> None:
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


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(module)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        force=True,
    )
    logging.getLogger("apscheduler").setLevel(logging.WARNING)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    scheduler = AsyncIOScheduler()
    for job_id, job in config.pipelines.items():
        schedule = job.schedule
        common = dict(args=[job.pipeline, job.params, job_id], id=f"global_{job_id}")
        if isinstance(schedule, IntervalSchedule):
            scheduler.add_job(
                enqueue, "interval", minutes=schedule.interval_minutes, **common
            )
        else:
            cron = {
                "hour": schedule.hour,
                "minute": schedule.minute,
                "timezone": schedule.timezone,
            }
            if schedule.day is not None:
                cron["day"] = schedule.day
            if schedule.day_of_week is not None:
                cron["day_of_week"] = schedule.day_of_week
            scheduler.add_job(enqueue, "cron", **cron, **common)
    scheduler.start()
    logger.info("Scheduler started with %d jobs", len(config.pipelines))

    bot, dp = create_bot(secrets.TELEGRAM_BOT_TOKEN)
    global _bot
    _bot = bot

    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Show help"),
            BotCommand(command="add", description="Track a profile: /add @username"),
            BotCommand(command="list", description="Show tracked profiles"),
            BotCommand(command="remove", description="Stop tracking a profile"),
        ]
    )

    def _on_worker_done(task: asyncio.Task) -> None:
        if not task.cancelled() and task.exception():
            logger.error("worker crashed: %s", task.exception())

    workers = [asyncio.create_task(worker()) for _ in range(secrets.WORKER_NUM_WORKERS)]
    for w in workers:
        w.add_done_callback(_on_worker_done)

    first_posts = config.pipelines["posts_history"]
    await enqueue(first_posts.pipeline, first_posts.params, "startup")

    logger.info("Starting bot polling")
    try:
        await dp.start_polling(bot)
    finally:
        for w in workers:
            w.cancel()
