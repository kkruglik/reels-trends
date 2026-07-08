import logging
from typing import Any
from sqlalchemy import select, insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession
from reels_trends.db.models import Base


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
