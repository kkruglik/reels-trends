"""Backtest the velocity trending algorithm across ~30 checkpoints spanning
yesterday and today, replaying the actual production PredictTrending code
(not a reimplementation) with an injected `now` per checkpoint so each
checkpoint only sees reels/snapshots that would have actually existed at
that moment. Compares against what the OLD (already-live) algorithm actually
marked is_trending in the DB for the same window.

Follower counts are held at their current value throughout (no historical
follower-count history is tracked), which is a minor approximation.

Usage:
    uv run python scripts/backtest_trending.py [path/to/reels_trends.db]
"""

import asyncio
import sys
from datetime import datetime, timedelta, UTC

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from reels_trends.db.models import InstagramAccountModel, ReelSnapshotModel, ReelsModel
from reels_trends.pipeline.check_trends import PredictTrending, _snapshots_to_df, _to_df
from reels_trends.settings import config

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "data/reels_trends.db"
N_WINDOWS = 30

params = config.pipelines["trends"].params
freshness = timedelta(hours=params["trending_freshness_hours"])
history_limit = params["trending_history_limit"]

engine = create_engine(f"sqlite:///{DB_PATH}")


async def main() -> None:
    with Session(engine) as session:
        accounts = session.scalars(select(InstagramAccountModel)).all()
        follower_by_acct = {a.username: a.follower_count for a in accounts}

        latest_snapshot = session.scalars(
            select(ReelSnapshotModel.captured_at).order_by(ReelSnapshotModel.captured_at.desc())
        ).first()
        end = latest_snapshot
        start = datetime.combine((end - timedelta(days=1)).date(), datetime.min.time())
        step = (end - start) / (N_WINDOWS - 1)
        checkpoints = [start + step * i for i in range(N_WINDOWS)]

        print(
            f"backtest window: {start} -> {end}  "
            f"({N_WINDOWS} checkpoints, ~{step.total_seconds() / 60:.0f}min apart)\n"
        )

        predictor = PredictTrending()
        fired: dict[str, tuple] = {}  # instagram_id -> (checkpoint, account, views, caption)

        for account in sorted(follower_by_acct):
            already_fired_ids: set[str] = set()
            for t in checkpoints:
                reels = session.scalars(
                    select(ReelsModel)
                    .where(ReelsModel.username == account, ReelsModel.posted_at <= t)
                    .order_by(ReelsModel.posted_at.desc())
                    .limit(history_limit)
                ).all()
                if not reels:
                    continue
                candidates_orm = [
                    r
                    for r in reels
                    if r.posted_at >= t - freshness and r.instagram_id not in already_fired_ids
                ]
                if not candidates_orm:
                    continue
                candidate_ids = [c.instagram_id for c in candidates_orm]
                snaps_orm = session.scalars(
                    select(ReelSnapshotModel)
                    .where(
                        ReelSnapshotModel.instagram_id.in_(candidate_ids),
                        ReelSnapshotModel.captured_at <= t,
                    )
                    .order_by(ReelSnapshotModel.captured_at.asc())
                ).all()

                state = {
                    "account_name": account,
                    "candidates": _to_df(candidates_orm),
                    "history": _to_df(reels),
                    "candidate_snapshots": _snapshots_to_df(snaps_orm),
                    "follower_count": follower_by_acct[account],
                    "params": params,
                }
                result = await predictor._predict_velocity(state, None, now=t.replace(tzinfo=UTC))
                trending_pk_ids = set(result.get("trending_ids", []))
                for c in candidates_orm:
                    if c.id in trending_pk_ids and c.instagram_id not in already_fired_ids:
                        already_fired_ids.add(c.instagram_id)
                        views = c.video_play_count or c.video_view_count or 0
                        fired[c.instagram_id] = (t, account, views, c.caption or "")

        print(f"NEW algorithm (maturity=24h, velocity_window=4h): {len(fired)} reels would fire\n")
        for iid, (t, acct, views, caption) in sorted(fired.items(), key=lambda kv: kv[1][0]):
            print(f"  {t.strftime('%m-%d %H:%M')}  {acct:<18} {views:>8,} views  {caption[:60]}")

        actual_trending = session.scalars(
            select(ReelsModel).where(ReelsModel.is_trending.is_(True), ReelsModel.posted_at >= start)
        ).all()
        actual_ids = {r.instagram_id for r in actual_trending}
        new_ids = set(fired.keys())

        print(f"\nOLD (live) algorithm already marked trending in this window: {len(actual_ids)} reels")
        print(f"  same in both:                    {len(actual_ids & new_ids)}")
        print(f"  newly caught by NEW algo only:    {len(new_ids - actual_ids)}")
        print(f"  no longer caught (OLD had, NEW misses): {len(actual_ids - new_ids)}")


asyncio.run(main())
