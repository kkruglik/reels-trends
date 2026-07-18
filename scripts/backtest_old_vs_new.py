"""Controlled A/B: replay the same 30 checkpoints (yesterday+today) through the
OLD velocity params (trending_maturity_hours=120, literal last-two-snapshot
velocity) and the NEW ones (24h, fixed 4h lookback window) side by side, using
the current live PredictTrending._predict_velocity for NEW and a frozen copy
of the pre-change logic for OLD. Reports what each catches and the delta.

Usage:
    uv run python scripts/backtest_old_vs_new.py [path/to/reels_trends.db]
"""

import asyncio
import sys
from datetime import datetime, timedelta, UTC

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from reels_trends.db.models import InstagramAccountModel, ReelSnapshotModel, ReelsModel
from reels_trends.pipeline.check_trends import PredictTrending, _snapshots_to_df, _to_df
from reels_trends.settings import config

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "data/reels_trends.db"
N_WINDOWS = 30

live_params = config.pipelines["trends"].params
old_params = dict(live_params) | {"trending_maturity_hours": 120.0}
freshness = timedelta(hours=live_params["trending_freshness_hours"])
history_limit = live_params["trending_history_limit"]

engine = create_engine(f"sqlite:///{DB_PATH}")


async def predict_velocity_old_style(state, now: datetime) -> set:
    """Frozen copy of _predict_velocity as it was before this session's fix:
    literal cs.iloc[-2]/cs.iloc[-1] instead of a fixed lookback window."""
    p = state["params"]
    candidates = state["candidates"].copy()
    history = state["history"].copy()
    snaps = state["candidate_snapshots"].copy()
    follower_count = state["follower_count"] or 0

    def _utc(s):
        return s.dt.tz_localize(UTC) if s.dt.tz is None else s.dt.tz_convert(UTC)

    candidates["age_hours"] = (now - _utc(candidates["posted_at"])).dt.total_seconds() / 3600
    candidates["engagement_rate"] = (candidates["likes_count"] + candidates["comments_count"]) / candidates["video_view_count"].clip(lower=1)
    candidates["reach"] = candidates["video_view_count"] / follower_count if follower_count else 0.0

    q = p["trending_baseline_quantile"]
    multiplier = p["trending_multiplier"]
    hist = history[~history["id"].isin(set(candidates["id"]))].copy()
    if not hist.empty:
        hist["age_hours"] = (now - _utc(hist["posted_at"])).dt.total_seconds() / 3600
        hist["engagement_rate"] = (hist["likes_count"] + hist["comments_count"]) / hist["video_view_count"].clip(lower=1)
        hist["reach"] = hist["video_view_count"] / follower_count if follower_count else 0.0
    mature = hist[hist["age_hours"] >= p["trending_maturity_hours"]] if not hist.empty else hist
    thr_reach = mature["reach"].quantile(q) * multiplier if not mature.empty else float("inf")
    thr_er = hist["engagement_rate"].quantile(q) * multiplier if not hist.empty else float("inf")
    enough_mature = len(mature) >= p["trending_min_history"]
    enough_hist = len(hist) >= p["trending_min_history"]

    if not snaps.empty:
        snaps["captured_at"] = _utc(snaps["captured_at"])

    min_span = p["trending_min_snapshot_span"]
    min_growth = p["trending_min_rel_growth"]
    reach_vel_min = p["trending_reach_velocity_min"]
    trending_ids = set()

    for _, c in candidates.iterrows():
        views = int(c["video_view_count"])
        age = float(c["age_hours"])
        er = float(c["engagement_rate"])
        reach = float(c["reach"])
        cs = snaps[snaps["instagram_id"] == c["instagram_id"]].sort_values("captured_at")
        n_snap = len(cs)
        view_velocity = reach_velocity = rel_growth = span = 0.0
        if n_snap >= 2:
            prev, last = cs.iloc[-2], cs.iloc[-1]  # <-- the OLD, noisy selection
            span = (last["captured_at"] - prev["captured_at"]).total_seconds() / 3600
            if span > 0:
                dviews = last["video_view_count"] - prev["video_view_count"]
                view_velocity = dviews / span
                reach_velocity = view_velocity / follower_count if follower_count else 0.0
                prev_v = max(int(prev["video_view_count"]), 1)
                rel_growth = dviews / prev_v / span
        eligible = n_snap >= 2 and span >= min_span and age >= p["trending_min_age_hours"] and views >= p["trending_min_views"]
        climbing = rel_growth >= min_growth
        velocity_hit = bool(follower_count) and reach_velocity >= reach_vel_min
        reach_hit = enough_mature and bool(follower_count) and reach >= thr_reach
        er_hit = enough_hist and views >= p["trending_min_views"] and er >= thr_er
        is_trending = eligible and (climbing and (velocity_hit or reach_hit) or er_hit)
        if is_trending:
            trending_ids.add(c["id"])
    return trending_ids


async def run_backtest(params: dict, use_old_logic: bool) -> dict:
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

        predictor = PredictTrending()
        fired: dict[str, tuple] = {}

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
                    r for r in reels if r.posted_at >= t - freshness and r.instagram_id not in already_fired_ids
                ]
                if not candidates_orm:
                    continue
                candidate_ids = [c.instagram_id for c in candidates_orm]
                snaps_orm = session.scalars(
                    select(ReelSnapshotModel)
                    .where(ReelSnapshotModel.instagram_id.in_(candidate_ids), ReelSnapshotModel.captured_at <= t)
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
                now = t.replace(tzinfo=UTC)
                if use_old_logic:
                    trending_pk_ids = await predict_velocity_old_style(state, now)
                else:
                    result = await predictor._predict_velocity(state, None, now=now)
                    trending_pk_ids = set(result.get("trending_ids", []))
                for c in candidates_orm:
                    if c.id in trending_pk_ids and c.instagram_id not in already_fired_ids:
                        already_fired_ids.add(c.instagram_id)
                        views = c.video_play_count or c.video_view_count or 0
                        fired[c.instagram_id] = (t, account, views, c.caption or "")
        return fired


async def main() -> None:
    old_fired = await run_backtest(old_params, use_old_logic=True)
    new_fired = await run_backtest(live_params, use_old_logic=False)

    old_ids, new_ids = set(old_fired), set(new_fired)
    print(f"OLD params (120h maturity, last-two-snapshot velocity): {len(old_ids)} reels fired")
    print(f"NEW params (24h maturity, 4h-window velocity):          {len(new_ids)} reels fired\n")
    print(f"same in both:        {len(old_ids & new_ids)}")
    print(f"only under NEW:      {len(new_ids - old_ids)}")
    print(f"only under OLD:      {len(old_ids - new_ids)}")

    if new_ids - old_ids:
        print("\n-- caught by NEW but not OLD --")
        for iid in sorted(new_ids - old_ids, key=lambda i: new_fired[i][0]):
            t, acct, views, cap = new_fired[iid]
            print(f"  {t.strftime('%m-%d %H:%M')}  {acct:<16} {views:>8,}  {cap[:55]}")

    if old_ids - new_ids:
        print("\n-- caught by OLD but not NEW --")
        for iid in sorted(old_ids - new_ids, key=lambda i: old_fired[i][0]):
            t, acct, views, cap = old_fired[iid]
            print(f"  {t.strftime('%m-%d %H:%M')}  {acct:<16} {views:>8,}  {cap[:55]}")


asyncio.run(main())
