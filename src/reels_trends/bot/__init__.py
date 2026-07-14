from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BotCommand
from reels_trends.bot.handlers import router
from reels_trends.bot.middleware import WhitelistMiddleware

BOT_COMMANDS = [
    BotCommand(command="start", description="Show help"),
    BotCommand(command="add", description="Track a profile: /add @username"),
    BotCommand(command="list", description="Show tracked profiles"),
    BotCommand(command="remove", description="Stop tracking a profile"),
]


async def create_bot(token: str) -> tuple[Bot, Dispatcher]:
    bot = Bot(token=token)
    dp = Dispatcher(storage=MemoryStorage())
    dp.update.outer_middleware(WhitelistMiddleware())
    dp.include_router(router)
    await bot.set_my_commands(BOT_COMMANDS)
    return bot, dp
