from asyncio import sleep
from html import escape
from reels_trends.pipeline.base import TaskContext, START
from reels_trends.db.models import (
    ReelsModel,
    ReelSnapshotModel,
    InstagramAccountModel,
    TaskModel,
)
from reels_trends.telegram_format import (
    TELEGRAM_MAX_MESSAGE_LENGTH,
    utf16_len,
    escape_and_truncate_caption,
)
from sqlalchemy import select, update
from datetime import datetime, timedelta, UTC
from typing import TypedDict
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Real Instagram reel URLs are ~40-60 chars; this cap just bounds the fixed
# (non-caption) part of the message so the length budget below can't go negative.
_MAX_URL_LEN = 200


def _format_trending_message(account: str, reel: ReelsModel, views: str) -> str:
    url = escape((reel.url or "")[:_MAX_URL_LEN])
    header = f"🔥 <b>Trending reel from {escape(account)}</b>\n\n"
    tail = (
        f"\n\n👍 {reel.likes_count:,} · 💬 {reel.comments_count:,} · 👁 {views}\n\n"
        f'<a href="{url}">Watch reel</a>'
    )
    budget = max(TELEGRAM_MAX_MESSAGE_LENGTH - utf16_len(header) - utf16_len(tail), 0)
    caption = escape_and_truncate_caption(reel.caption or "", budget)

    return f"{header}{caption}{tail}"


def _to_df(posts: list[ReelsModel]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "id": p.id,
                "instagram_id": p.instagram_id,
                "likes_count": p.likes_count,
                "comments_count": p.comments_count,
                "video_view_count": p.video_play_count or p.video_view_count or 0,
                "posted_at": p.posted_at,
            }
            for p in posts
        ]
    )


def _snapshots_to_df(snapshots: list[ReelSnapshotModel]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "instagram_id": s.instagram_id,
                "captured_at": s.captured_at,
                "likes_count": s.likes_count,
                "comments_count": s.comments_count,
                "video_view_count": s.video_view_count,
            }
            for s in snapshots
        ]
    )


class CheckTrendingParams(TypedDict):
    trending_algorithm: str  # "projection" | "velocity"
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
    # velocity algorithm only
    trending_reach_velocity_min: float
    trending_min_rel_growth: float
    trending_min_snapshot_span: float


class CheckTrendingState(TypedDict, total=False):
    account_name: str
    candidates: pd.DataFrame
    history: pd.DataFrame
    candidate_snapshots: pd.DataFrame
    follower_count: int
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
        out: CheckTrendingState = {
            "candidates": _to_df(candidates),
            "history": _to_df(history),
        }

        # The velocity algorithm additionally needs each candidate's snapshot history
        # (to measure Δviews/Δt) and the account follower count (to normalize reach).
        if p.get("trending_algorithm", "projection") == "velocity":
            candidate_ids = [c.instagram_id for c in candidates]
            snapshots_result = await ctx["db_session"].execute(
                select(ReelSnapshotModel)
                .where(ReelSnapshotModel.instagram_id.in_(candidate_ids))
                .order_by(ReelSnapshotModel.captured_at.asc())
            )
            snapshots = snapshots_result.scalars().all()

            account_row = await ctx["db_session"].execute(
                select(InstagramAccountModel.follower_count).where(
                    InstagramAccountModel.username == account
                )
            )
            follower_count = account_row.scalar_one_or_none() or 0

            out["candidate_snapshots"] = _snapshots_to_df(snapshots)
            out["follower_count"] = follower_count
            logger.info(
                "fetched velocity data account=%s snapshots=%d followers=%d",
                account,
                len(snapshots),
                follower_count,
            )
        return out


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
        algo = state["params"].get("trending_algorithm", "projection")
        if algo == "velocity":
            return await self._predict_velocity(state, ctx)
        return await self._predict_projection(state, ctx)

    async def _predict_projection(
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

    async def _predict_velocity(
        self, state: CheckTrendingState, ctx: TaskContext
    ) -> CheckTrendingState:
        account = state["account_name"]
        p = state["params"]
        candidates = state["candidates"].copy()
        history = state["history"].copy()
        snaps = state.get("candidate_snapshots")
        snaps = snaps.copy() if snaps is not None else pd.DataFrame(
            columns=[
                "instagram_id",
                "captured_at",
                "likes_count",
                "comments_count",
                "video_view_count",
            ]
        )
        follower_count = state.get("follower_count", 0) or 0
        now = datetime.now(UTC)

        def _utc(s: pd.Series) -> pd.Series:
            return s.dt.tz_localize(UTC) if s.dt.tz is None else s.dt.tz_convert(UTC)

        # Candidate current state (level) comes from the reels row; velocity comes from
        # the snapshot pair below. reach/er are age-independent so they need no snapshots.
        candidates["age_hours"] = (
            now - _utc(candidates["posted_at"])
        ).dt.total_seconds() / 3600
        candidates["engagement_rate"] = (
            candidates["likes_count"] + candidates["comments_count"]
        ) / candidates["video_view_count"].clip(lower=1)
        candidates["reach"] = (
            candidates["video_view_count"] / follower_count if follower_count else 0.0
        )

        # Per-account baselines from mature history, excluding the candidates themselves.
        q = p["trending_baseline_quantile"]
        multiplier = p["trending_multiplier"]
        hist = history[~history["id"].isin(set(candidates["id"]))].copy()
        if not hist.empty:
            hist["age_hours"] = (
                now - _utc(hist["posted_at"])
            ).dt.total_seconds() / 3600
            hist["engagement_rate"] = (
                hist["likes_count"] + hist["comments_count"]
            ) / hist["video_view_count"].clip(lower=1)
            hist["reach"] = (
                hist["video_view_count"] / follower_count if follower_count else 0.0
            )
        mature = (
            hist[hist["age_hours"] >= p["trending_maturity_hours"]]
            if not hist.empty
            else hist
        )
        thr_reach = (
            mature["reach"].quantile(q) * multiplier if not mature.empty else float("inf")
        )
        thr_er = (
            hist["engagement_rate"].quantile(q) * multiplier
            if not hist.empty
            else float("inf")
        )
        enough_mature = len(mature) >= p["trending_min_history"]
        enough_hist = len(hist) >= p["trending_min_history"]

        if not snaps.empty:
            snaps["captured_at"] = _utc(snaps["captured_at"])

        logger.info(
            "baseline(velocity) account=%s mature=%d hist=%d followers=%d "
            "thr_reach=%.4f thr_er=%.4f",
            account,
            len(mature),
            len(hist),
            follower_count,
            thr_reach,
            thr_er,
        )

        min_span = p["trending_min_snapshot_span"]
        min_growth = p["trending_min_rel_growth"]
        reach_vel_min = p["trending_reach_velocity_min"]
        trending_ids = []

        for _, c in candidates.iterrows():
            views = int(c["video_view_count"])
            age = float(c["age_hours"])
            er = float(c["engagement_rate"])
            reach = float(c["reach"])

            cs = snaps[snaps["instagram_id"] == c["instagram_id"]].sort_values(
                "captured_at"
            )
            n_snap = len(cs)
            view_velocity = reach_velocity = rel_growth = span = 0.0
            if n_snap >= 2:
                prev, last = cs.iloc[-2], cs.iloc[-1]
                span = (
                    last["captured_at"] - prev["captured_at"]
                ).total_seconds() / 3600
                if span > 0:
                    dviews = last["video_view_count"] - prev["video_view_count"]
                    view_velocity = dviews / span
                    reach_velocity = (
                        view_velocity / follower_count if follower_count else 0.0
                    )
                    prev_v = max(int(prev["video_view_count"]), 1)
                    rel_growth = dviews / prev_v / span

            # Velocity-gated: no measurable velocity (<2 snapshots) => never trending.
            eligible = (
                n_snap >= 2
                and span >= min_span
                and age >= p["trending_min_age_hours"]
                and views >= p["trending_min_views"]
            )
            climbing = rel_growth >= min_growth
            velocity_hit = bool(follower_count) and reach_velocity >= reach_vel_min
            reach_hit = enough_mature and bool(follower_count) and reach >= thr_reach
            er_hit = enough_hist and views >= p["trending_min_views"] and er >= thr_er
            is_trending = eligible and climbing and (
                velocity_hit or reach_hit or er_hit
            )

            logger.info(
                "vaudit account=%s id=%s age=%.1fh snaps=%d span=%.2fh eligible=%s "
                "views=%d view_vel=%.0f reach_vel=%.5f reach_vel_min=%.5f velocity_hit=%s "
                "rel_growth=%.4f min_growth=%.4f climbing=%s "
                "reach=%.4f thr_reach=%.4f reach_hit=%s "
                "er=%.4f thr_er=%.4f er_hit=%s -> trending=%s",
                account,
                c["id"],
                age,
                n_snap,
                span,
                bool(eligible),
                views,
                view_velocity,
                reach_velocity,
                reach_vel_min,
                bool(velocity_hit),
                rel_growth,
                min_growth,
                bool(climbing),
                reach,
                thr_reach,
                bool(reach_hit),
                er,
                thr_er,
                bool(er_hit),
                bool(is_trending),
            )
            if is_trending:
                trending_ids.append(c["id"])

        logger.info(
            "predicted(velocity) account=%s candidates=%d trending=%d",
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
                play_count = reel.video_play_count or reel.video_view_count
                views = f"{play_count:,}" if play_count else "—"
                text = _format_trending_message(account, reel, views)
                await ctx["bot"].send_message(chat_id, text, parse_mode="HTML")
                await sleep(0.5)

        logger.info(
            "notified account=%s chats=%d posts=%d", account, len(chat_ids), len(reels)
        )
        return {}
