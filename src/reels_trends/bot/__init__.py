from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from reels_trends.bot.handlers import router
from reels_trends.bot.middleware import WhitelistMiddleware


def create_bot(token: str) -> tuple[Bot, Dispatcher]:
    bot = Bot(token=token)
    dp = Dispatcher(storage=MemoryStorage())
    dp.update.outer_middleware(WhitelistMiddleware())
    dp.include_router(router)
    return bot, dp
