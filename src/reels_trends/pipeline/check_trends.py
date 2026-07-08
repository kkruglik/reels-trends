from asyncio import sleep
from html import escape
from reels_trends.pipeline.base import TaskContext, START
from reels_trends.db.models import ReelsModel, TaskModel
from sqlalchemy import select, update
from datetime import datetime, timedelta, UTC
from typing import TypedDict
import logging
import pandas as pd

logger = logging.getLogger(__name__)

FRESHNESS_WINDOW = timedelta(hours=6)
TRENDING_MULTIPLIER = 1.5
BASELINE_QUANTILE = 0.75


def _to_df(posts: list[ReelsModel]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "id": p.id,
                "likes_count": p.likes_count,
                "comments_count": p.comments_count,
                "video_view_count": p.video_view_count or 0,
                "posted_at": p.posted_at,
            }
            for p in posts
        ]
    )


class CheckTrendingState(TypedDict, total=False):
    account_name: str
    candidates: pd.DataFrame
    history: pd.DataFrame
    trending_ids: list


class FetchTrendingData:
    name = "fetch_trending_data"
    retry_count = 3
    depends = [START]

    def should_apply(self, state: CheckTrendingState) -> bool:
        return True

    async def apply(
        self, state: CheckTrendingState, ctx: TaskContext
    ) -> CheckTrendingState:
        account = state["account_name"]
        cutoff = datetime.now(UTC) - FRESHNESS_WINDOW

        candidates_result = await ctx["db_session"].execute(
            select(ReelsModel).where(
                ReelsModel.username == account,
                ReelsModel.posted_at >= cutoff,
                ReelsModel.is_notified.is_(False),
                ReelsModel.is_trending.is_(False),
            )
        )
        candidates = candidates_result.scalars().all()

        if not candidates:
            logger.info("no candidates account=%s", account)
            return {}

        history_result = await ctx["db_session"].execute(
            select(ReelsModel)
            .where(ReelsModel.username == account)
            .order_by(ReelsModel.posted_at.desc())
            .limit(500)
        )
        history = history_result.scalars().all()

        logger.info(
            "fetched account=%s candidates=%d history=%d",
            account,
            len(candidates),
            len(history),
        )
        return {"candidates": _to_df(candidates), "history": _to_df(history)}


class PredictTrending:
    name = "predict_trending"
    retry_count = 3
    depends = ["fetch_trending_data"]

    def should_apply(self, state: CheckTrendingState) -> bool:
        candidates = state.get("candidates")
        return candidates is not None and not candidates.empty

    async def apply(
        self, state: CheckTrendingState, ctx: TaskContext
    ) -> CheckTrendingState:
        account = state["account_name"]
        candidates = state["candidates"].copy()
        history = state["history"].copy()
        now = datetime.now(UTC)

        for df in (candidates, history):
            posted = df["posted_at"]
            if posted.dt.tz is None:
                posted = posted.dt.tz_localize(UTC)
            else:
                posted = posted.dt.tz_convert(UTC)
            df["age_hours"] = ((now - posted).dt.total_seconds() / 3600).clip(lower=1)
            df["likes_per_hour"] = df["likes_count"] / df["age_hours"]
            df["views_per_hour"] = df["video_view_count"] / df["age_hours"]
            df["engagement_rate"] = (df["likes_count"] + df["comments_count"]) / df[
                "video_view_count"
            ].clip(lower=1)

        baseline_lph = history["likes_per_hour"].quantile(BASELINE_QUANTILE)
        baseline_vph = history["views_per_hour"].quantile(BASELINE_QUANTILE)
        baseline_er = history["engagement_rate"].quantile(BASELINE_QUANTILE)

        logger.info(
            "baseline account=%s lph=%.2f vph=%.2f er=%.4f",
            account,
            baseline_lph,
            baseline_vph,
            baseline_er,
        )

        is_trending = (
            (candidates["likes_per_hour"] >= baseline_lph * TRENDING_MULTIPLIER)
            | (candidates["views_per_hour"] >= baseline_vph * TRENDING_MULTIPLIER)
            | (
                (candidates["video_view_count"] >= 1000)
                & (candidates["engagement_rate"] >= baseline_er * TRENDING_MULTIPLIER)
            )
        )

        candidates.loc[is_trending, "is_trending"] = True
        trending = candidates.loc[is_trending]
        trending_ids = list(trending["id"])

        for _, row in trending.iterrows():
            logger.info(
                "hit account=%s lph=%.2f vph=%.2f er=%.4f",
                account,
                row["likes_per_hour"],
                row["views_per_hour"],
                row["engagement_rate"],
            )

        logger.info(
            "predicted account=%s candidates=%d trending=%d",
            account,
            len(candidates),
            len(trending_ids),
        )
        return {"trending_ids": trending_ids}


class NotifyTrending:
    name = "notify_trending"
    retry_count = 3
    depends = ["predict_trending"]

    def should_apply(self, state: CheckTrendingState) -> bool:
        return bool(state.get("trending_ids"))

    async def apply(
        self, state: CheckTrendingState, ctx: TaskContext
    ) -> CheckTrendingState:
        account = state["account_name"]
        trending_ids = state["trending_ids"]
        session = ctx["db_session"]

        reels_result = await session.execute(
            select(ReelsModel).where(ReelsModel.id.in_(trending_ids))
        )
        reels = reels_result.scalars().all()

        await session.execute(
            update(ReelsModel)
            .where(ReelsModel.id.in_(trending_ids))
            .values(is_notified=True, is_trending=True)
        )
        await session.commit()

        users_result = await session.execute(
            select(TaskModel.user_id).where(TaskModel.username == account)
        )
        user_ids = users_result.scalars().all()

        for user_id in user_ids:
            for reel in reels:
                caption = escape((reel.caption or "")[:120])
                views = f"{reel.video_view_count:,}" if reel.video_view_count else "—"
                text = (
                    f"🔥 <b>Trending reel from @{account}</b>\n\n"
                    f"{caption}\n\n"
                    f"👍 {reel.likes_count:,} · 💬 {reel.comments_count:,} · 👁 {views}\n\n"
                    f'<a href="{reel.url}">Watch reel</a>'
                )
                await ctx["bot"].send_message(user_id, text, parse_mode="HTML")
                await sleep(0.5)

        logger.info(
            "notified account=%s users=%d posts=%d", account, len(user_ids), len(reels)
        )
        return {}
