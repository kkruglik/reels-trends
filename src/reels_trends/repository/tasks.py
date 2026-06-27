from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from reels_trends.db.models import TaskModel
from reels_trends.config import TaskConfig, ScrapeTaskConfig


class TaskRepository:
    @classmethod
    async def create_task(cls, session: AsyncSession, task: TaskConfig):
        scrape_task_config = task.scrape_task_config
        model = TaskModel(
            is_active=True,
            username=scrape_task_config.username,
            results_limit=scrape_task_config.results_limit,
            skip_pinned_posts=scrape_task_config.skip_pinned_posts,
            skip_trial_reels=scrape_task_config.skip_trial_reels,
            only_posts_newer_than=scrape_task_config.only_posts_newer_than,
            include_shares_count=scrape_task_config.include_shares_count,
            include_transcript=scrape_task_config.include_transcript,
            include_downloaded_video=scrape_task_config.include_downloaded_video,
        )
        session.add(model)
        await session.flush()
        return model

    @classmethod
    async def get_task(cls, session: AsyncSession, username: str) -> ScrapeTaskConfig:
        result = await session.execute(
            select(TaskModel).where(TaskModel.username == username)
        )
        row = result.scalar_one_or_none()
        if row is None:
            raise ValueError(f"Task not found for username: {username}")
        return ScrapeTaskConfig.model_validate(row)

    @classmethod
    async def get_active_tasks(cls, session: AsyncSession):
        result = await session.execute(select(TaskModel).where(TaskModel.is_active))
        return list(result.scalars().all())
