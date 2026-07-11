from sqlalchemy.ext.asyncio import AsyncSession
from typing import Protocol, Iterable, TypedDict, Any
from aiogram import Bot
import logging
import httpx2 as httpx

logger = logging.getLogger(__name__)

START = "__start__"


class ApifyBillingError(Exception):
    pass


class TaskContext(TypedDict):
    db_session: AsyncSession
    http_client: httpx.AsyncClient
    bot: Bot


class PipelineStep(Protocol):
    name: str
    retry_count: int
    depends: list[str]

    async def apply(
        self, state: dict[str, Any], ctx: TaskContext
    ) -> dict[str, Any]: ...
    def should_apply(self, state: dict[str, Any]) -> bool: ...


async def run_pipeline(
    steps: Iterable[PipelineStep], state: dict[str, Any], ctx: TaskContext
):
    for step in steps:
        if not step.should_apply(state):
            logger.debug("skip step=%s", step.name)
            continue
        logger.info("start step=%s", step.name)
        try:
            state |= await step.apply(state, ctx)
        except ApifyBillingError as e:
            logger.warning("billing error step=%s: %s", step.name, e)
            return
        except Exception:
            logger.exception("error step=%s", step.name)
            raise
