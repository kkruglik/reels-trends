from aiogram import Bot, Dispatcher
from reels_trends.bot.handlers import router


def create_bot(token: str) -> tuple[Bot, Dispatcher]:
    bot = Bot(token=token)
    dp = Dispatcher()
    dp.include_router(router)
    return bot, dp
