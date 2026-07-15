from reels_trends.workers import enqueue_all, worker
from reels_trends.bot import create_bot
from reels_trends.db.models import Base
from reels_trends.db.session import engine
from reels_trends.settings import secrets, config, IntervalSchedule
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import asyncio
import logging

logger = logging.getLogger(__name__)


async def main() -> None:
    log_dir = Path(secrets.LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    file_handler = TimedRotatingFileHandler(
        log_dir / "reels_trends.log",
        when="midnight",
        backupCount=14,
        utc=False,
        encoding="utf-8",
    )
    file_handler.suffix = "%Y-%m-%d"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(module)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=[logging.StreamHandler(), file_handler],
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
                enqueue_all, "interval", minutes=schedule.interval_minutes, **common
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
            scheduler.add_job(enqueue_all, "cron", **cron, **common)
    scheduler.start()
    logger.info("Scheduler started with %d jobs", len(config.pipelines))

    bot, dp = await create_bot(secrets.TELEGRAM_BOT_TOKEN)

    def _on_worker_done(task: asyncio.Task) -> None:
        if not task.cancelled() and task.exception():
            logger.error("worker crashed: %s", task.exception())

    workers = [
        asyncio.create_task(worker(bot)) for _ in range(secrets.WORKER_NUM_WORKERS)
    ]
    for w in workers:
        w.add_done_callback(_on_worker_done)

    first_posts = config.pipelines["posts_history"]
    await enqueue_all(first_posts.pipeline, first_posts.params, "startup")

    logger.info("Starting bot polling")
    try:
        await dp.start_polling(bot)
    finally:
        for w in workers:
            w.cancel()
