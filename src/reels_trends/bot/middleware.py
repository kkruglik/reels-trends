from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery
from reels_trends.settings import settings
import logging

logger = logging.getLogger(__name__)

_allowed: frozenset[int] = frozenset(settings.TELEGRAM_ALLOWED_USERS)


class WhitelistMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data: dict):
        if not _allowed:
            return await handler(event, data)

        user = data.get("event_from_user")
        if user is None or user.id not in _allowed:
            logger.warning("access denied user_id=%s", user.id if user else "unknown")
            if isinstance(event, Message):
                await event.answer("You are not authorized to use this bot.")
            elif isinstance(event, CallbackQuery):
                await event.answer("You are not authorized.", show_alert=True)
            return

        return await handler(event, data)
