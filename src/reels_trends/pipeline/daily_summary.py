from asyncio import sleep
from html import escape
from reels_trends.pipeline.base import TaskContext, START
from reels_trends.db.models import ReelsModel, TaskModel
from reels_trends.telegram_format import (
    TELEGRAM_MAX_MESSAGE_LENGTH,
    utf16_len,
    escape_and_truncate_caption,
)
from sqlalchemy import select
from datetime import datetime, timedelta, UTC
from typing import TypedDict
import logging

logger = logging.getLogger(__name__)

_CAPTION_BUDGET = 100
_MAX_URL_LEN = 200


class DailySummaryParams(TypedDict):
    summary_lookback_days: int
    summary_top_count: int


class DailySummaryState(TypedDict, total=False):
    account_name: str
    params: DailySummaryParams


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
        p = state["params"]
        session = ctx["db_session"]
        cutoff = datetime.now(UTC) - timedelta(days=p["summary_lookback_days"])

        reels_result = await session.execute(
            select(ReelsModel)
            .where(
                ReelsModel.username == account,
                ReelsModel.posted_at > cutoff,
            )
            .order_by(ReelsModel.video_play_count.desc())
            .limit(p["summary_top_count"])
        )
        reels = reels_result.scalars().all()

        if not reels:
            logger.info("no reels account=%s", account)
            return {}

        chat_ids_result = await session.execute(
            select(TaskModel.chat_id).where(TaskModel.username == account)
        )
        chat_ids = chat_ids_result.scalars().all()

        for chat_id in chat_ids:
            message = f"📊 <b>Daily Top 5 from {escape(account)}</b>\n\n"
            for i, reel in enumerate(reels, 1):
                caption = escape_and_truncate_caption(reel.caption or "", _CAPTION_BUDGET)
                play_count = reel.video_play_count or reel.video_view_count
                views = f"{play_count:,}" if play_count else "—"
                url = escape((reel.url or "")[:_MAX_URL_LEN])
                item = (
                    f"{i}. {caption}\n"
                    f"👍 {reel.likes_count:,} · 💬 {reel.comments_count:,} · 👁 {views}\n"
                    f'<a href="{url}">Watch</a>\n\n'
                )
                if utf16_len(message) + utf16_len(item) > TELEGRAM_MAX_MESSAGE_LENGTH:
                    break
                message += item
            await ctx["bot"].send_message(chat_id, message, parse_mode="HTML")
            await sleep(0.5)

        logger.info(
            "sent account=%s chats=%d posts=%d",
            account,
            len(chat_ids),
            len(reels),
        )
        return {}
