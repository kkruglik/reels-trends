import logging
from typing import Any
from sqlalchemy import select, insert, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession
from reels_trends.db.models import (
    Base,
    ReelSnapshotModel,
    ReelsModel,
    InstagramAccountModel,
)


logger = logging.getLogger(__name__)


async def insert_to_db(session: AsyncSession, values: list[Any], model: type[Base]):
    stmt = insert(model).values(values)
    await session.execute(stmt)
    await session.commit()
    logger.info("Inserted %d rows into %s", len(values), model.__tablename__)


async def upsert_to_db[T: Base](
    session: AsyncSession,
    values: list[Any],
    model: type[T],
    conflict_column: str | list[str],
):
    conflict_cols = (
        [conflict_column] if isinstance(conflict_column, str) else conflict_column
    )
    stmt = sqlite_insert(model).values(values)
    update_cols = {k: stmt.excluded[k] for k in values[0] if k not in conflict_cols}
    if update_cols:
        stmt = stmt.on_conflict_do_update(
            index_elements=conflict_cols, set_=update_cols
        )
    else:
        stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
    await session.execute(stmt)
    await session.commit()
    logger.info("Upserted %d rows into %s", len(values), model.__tablename__)


async def get_from_db[T: Base](
    session: AsyncSession, model: type[T], **filters
) -> T | None:
    result = await session.execute(select(model).filter_by(**filters))
    return result.scalar_one_or_none()


async def get_all_from_db[T: Base](
    session: AsyncSession, model: type[T], limit: int | None = None, **filters
) -> list[T]:
    stmt = select(model).filter_by(**filters)
    if limit is not None:
        stmt = stmt.limit(limit)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def reel_snapshots_view(
    session: AsyncSession, username: str
) -> list[dict[str, Any]]:
    stmt = (
        select(
            ReelSnapshotModel.id.label("snapshot_id"),
            ReelSnapshotModel.captured_at,
            ReelSnapshotModel.likes_count.label("snapshot_likes_count"),
            ReelSnapshotModel.comments_count.label("snapshot_comments_count"),
            ReelSnapshotModel.video_view_count.label("snapshot_video_view_count"),
            ReelsModel.id.label("reel_id"),
            ReelsModel.instagram_id,
            ReelsModel.short_code,
            ReelsModel.url.label("reel_url"),
            ReelsModel.owner_id,
            ReelsModel.caption,
            ReelsModel.hashtags,
            ReelsModel.mentions,
            ReelsModel.likes_count.label("reel_likes_count"),
            ReelsModel.comments_count.label("reel_comments_count"),
            ReelsModel.video_view_count.label("reel_video_view_count"),
            ReelsModel.video_play_count,
            ReelsModel.video_duration,
            ReelsModel.posted_at,
            ReelsModel.is_notified,
            ReelsModel.is_trending,
            ReelsModel.username,
            InstagramAccountModel.url.label("account_url"),
            InstagramAccountModel.profile_id,
            InstagramAccountModel.follower_count,
            InstagramAccountModel.total_video_count,
            InstagramAccountModel.total_post_count,
            InstagramAccountModel.full_name,
            InstagramAccountModel.verified,
        )
        .join(ReelsModel, ReelSnapshotModel.instagram_id == ReelsModel.instagram_id)
        .join(
            InstagramAccountModel, ReelsModel.username == InstagramAccountModel.username
        )
        .where(
            ReelsModel.username == username,
            ReelSnapshotModel.uploaded_to_bigquery.is_(False),
        )
    )
    result = await session.execute(stmt)
    return result.mappings().all()


async def mark_snapshots_uploaded(session: AsyncSession, snapshot_ids: list[int]) -> None:
    if not snapshot_ids:
        return
    stmt = (
        update(ReelSnapshotModel)
        .where(ReelSnapshotModel.id.in_(snapshot_ids))
        .values(uploaded_to_bigquery=True)
    )
    result = await session.execute(stmt)
    await session.commit()
    logger.info("marked %d snapshot(s) uploaded_to_bigquery", result.rowcount)
