from asyncio import sleep
from html import escape
from reels_trends.pipeline.base import TaskContext, START
from reels_trends.db.models import ReelsModel, TaskModel
from sqlalchemy import select, update
from datetime import datetime, timedelta, UTC
from typing import TypedDict
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _to_df(posts: list[ReelsModel]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "id": p.id,
                "likes_count": p.likes_count,
                "comments_count": p.comments_count,
                "video_view_count": p.video_play_count or p.video_view_count or 0,
                "posted_at": p.posted_at,
            }
            for p in posts
        ]
    )


class CheckTrendingParams(TypedDict):
    trending_freshness_hours: int
    trending_history_limit: int
    trending_baseline_quantile: float
    trending_multiplier: float
    trending_growth_halflife_hours: float
    trending_maturity_hours: float
    trending_min_history: int
    trending_min_age_hours: float
    trending_min_likes: int
    trending_min_views: int


class CheckTrendingState(TypedDict, total=False):
    account_name: str
    candidates: pd.DataFrame
    history: pd.DataFrame
    trending_ids: list
    params: CheckTrendingParams


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
        p = state["params"]
        cutoff = datetime.now(UTC) - timedelta(hours=p["trending_freshness_hours"])

        candidates_result = await ctx["db_session"].execute(
            select(ReelsModel).where(
                ReelsModel.username == account,
                ReelsModel.posted_at >= cutoff,
                ReelsModel.is_notified.is_(False),
                ReelsModel.is_trending.is_(False),
                # ReelsModel.video_view_count > 0,
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
            .limit(p["trending_history_limit"])
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
        p = state["params"]
        candidates = state["candidates"].copy()
        history = state["history"].copy()
        now = datetime.now(UTC)

        for df in (candidates, history):
            posted = df["posted_at"]
            if posted.dt.tz is None:
                posted = posted.dt.tz_localize(UTC)
            else:
                posted = posted.dt.tz_convert(UTC)
            df["age_hours"] = (now - posted).dt.total_seconds() / 3600
            df["engagement_rate"] = (df["likes_count"] + df["comments_count"]) / df[
                "video_view_count"
            ].clip(lower=1)

        q = p["trending_baseline_quantile"]
        multiplier = p["trending_multiplier"]
        halflife = p["trending_growth_halflife_hours"]

        # g(t) = fraction of a reel's lifetime engagement reached by age `t`.
        # Projecting a candidate's partial count to its final (count / g(age)) removes
        # the age bias of raw velocity, so fresh reels compete on projected finals, not
        # on the mechanical head start of a small denominator.
        def growth(age: pd.Series) -> pd.Series:
            return 1.0 - np.power(2.0, -age.clip(lower=0.5) / halflife)

        # Baseline is built from the account's own history, excluding the candidates
        # themselves. Velocity baselines come from mature reels whose stored counts are
        # effectively final; engagement_rate is age-independent so it uses all history.
        hist = history[~history["id"].isin(set(candidates["id"]))]
        mature = hist[hist["age_hours"] >= p["trending_maturity_hours"]]

        baseline_likes = mature["likes_count"].quantile(q)
        baseline_views = mature["video_view_count"].quantile(q)
        baseline_er = hist["engagement_rate"].quantile(q)

        enough_mature = len(mature) >= p["trending_min_history"]
        enough_hist = len(hist) >= p["trending_min_history"]

        logger.info(
            "baseline account=%s mature=%d hist=%d likes=%.0f views=%.0f er=%.4f",
            account,
            len(mature),
            len(hist),
            baseline_likes,
            baseline_views,
            baseline_er,
        )

        thr_likes = baseline_likes * multiplier
        thr_views = baseline_views * multiplier
        thr_er = baseline_er * multiplier

        g_c = growth(candidates["age_hours"])
        candidates["proj_likes"] = candidates["likes_count"] / g_c
        candidates["proj_views"] = candidates["video_view_count"] / g_c

        candidates["gate"] = candidates["age_hours"] >= p["trending_min_age_hours"]
        candidates["likes_hit"] = (
            enough_mature
            & (candidates["likes_count"] >= p["trending_min_likes"])
            & (candidates["proj_likes"] >= thr_likes)
        )
        candidates["views_hit"] = (
            enough_mature
            & (candidates["video_view_count"] >= p["trending_min_views"])
            & (candidates["proj_views"] >= thr_views)
        )
        candidates["er_hit"] = (
            enough_hist
            & (candidates["video_view_count"] >= p["trending_min_views"])
            & (candidates["engagement_rate"] >= thr_er)
        )
        candidates["is_trending"] = candidates["gate"] & (
            candidates["likes_hit"] | candidates["views_hit"] | candidates["er_hit"]
        )

        # Per-reel audit trail: log every evaluated candidate with its computed values
        # and the thresholds it was compared against, so a trending/non-trending
        # decision can be reconstructed after the fact.
        for _, row in candidates.iterrows():
            logger.info(
                "audit account=%s id=%s age=%.1fh gate=%s "
                "likes=%d proj_likes=%.0f thr_likes=%.0f likes_hit=%s "
                "views=%d proj_views=%.0f thr_views=%.0f views_hit=%s "
                "er=%.4f thr_er=%.4f er_hit=%s "
                "enough_mature=%s enough_hist=%s -> trending=%s",
                account,
                row["id"],
                row["age_hours"],
                bool(row["gate"]),
                int(row["likes_count"]),
                row["proj_likes"],
                thr_likes,
                bool(row["likes_hit"]),
                int(row["video_view_count"]),
                row["proj_views"],
                thr_views,
                bool(row["views_hit"]),
                row["engagement_rate"],
                thr_er,
                bool(row["er_hit"]),
                enough_mature,
                enough_hist,
                bool(row["is_trending"]),
            )

        trending = candidates.loc[candidates["is_trending"]]
        trending_ids = list(trending["id"])

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

        chat_ids_result = await session.execute(
            select(TaskModel.chat_id).where(TaskModel.username == account)
        )
        chat_ids = chat_ids_result.scalars().all()

        for chat_id in chat_ids:
            for reel in reels:
                caption = escape((reel.caption or "")[:120])
                play_count = reel.video_play_count or reel.video_view_count
                views = f"{play_count:,}" if play_count else "—"
                text = (
                    f"🔥 <b>Trending reel from @{account}</b>\n\n"
                    f"{caption}\n\n"
                    f"👍 {reel.likes_count:,} · 💬 {reel.comments_count:,} · 👁 {views}\n\n"
                    f'<a href="{reel.url}">Watch reel</a>'
                )
                await ctx["bot"].send_message(chat_id, text, parse_mode="HTML")
                await sleep(0.5)

        logger.info(
            "notified account=%s chats=%d posts=%d", account, len(chat_ids), len(reels)
        )
        return {}
