from asyncio import sleep
from html import escape
from reels_trends.pipeline.base import TaskContext, START
from reels_trends.db.models import ReelsModel, TaskModel
from sqlalchemy import select
from datetime import datetime, timedelta, UTC
from typing import TypedDict
import logging

logger = logging.getLogger(__name__)


class DailySummaryState(TypedDict, total=False):
    account_name: str


class NotifySummary:
    name = "notify_summary"
    retry_count = 3
    depends = [START]

    def should_apply(self, state: DailySummaryState) -> bool:
        return True

    async def apply(
        self, state: DailySummaryState, ctx: TaskContext
    ) -> DailySummaryState:
        account = state["account_name"]
        session = ctx["db_session"]
        cutoff = datetime.now(UTC) - timedelta(days=1)

        reels_result = await session.execute(
            select(ReelsModel)
            .where(
                ReelsModel.username == account,
                ReelsModel.posted_at > cutoff,
            )
            .order_by(ReelsModel.video_view_count.desc())
            .limit(5)
        )
        reels = reels_result.scalars().all()

        if not reels:
            logger.info("no reels account=%s", account)
            return {}

        users_result = await session.execute(
            select(TaskModel.user_id).where(TaskModel.username == account)
        )
        user_ids = users_result.scalars().all()

        for user_id in user_ids:
            message = f"📊 <b>Daily Top 5 from @{account}</b>\n\n"
            for i, reel in enumerate(reels, 1):
                caption = escape((reel.caption or "")[:100])
                views = f"{reel.video_view_count:,}" if reel.video_view_count else "—"
                message += (
                    f"{i}. {caption}\n"
                    f"👍 {reel.likes_count:,} · 💬 {reel.comments_count:,} · 👁 {views}\n"
                    f'<a href="{reel.url}">Watch</a>\n\n'
                )
            await ctx["bot"].send_message(user_id, message, parse_mode="HTML")
            await sleep(0.5)

        logger.info(
            "sent account=%s users=%d posts=%d",
            account,
            len(user_ids),
            len(reels),
        )
        return {}
