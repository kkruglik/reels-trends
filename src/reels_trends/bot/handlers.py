from urllib.parse import urlparse
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from sqlalchemy import delete, select
import httpx2 as httpx
import logging

from reels_trends.bot.utils import validate_instagram_profile
from reels_trends.db.models import UserModel, TaskModel, InstagramAccountModel
from reels_trends.db.session import get_session
from reels_trends.db.utils import upsert_to_db, get_all_from_db
from reels_trends.settings import settings

router = Router()
logger = logging.getLogger(__name__)


class Form(StatesGroup):
    waiting_for_profile = State()


def _parse_instagram_username(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("@"):
        return raw[1:]
    if "instagram.com" in raw:
        return urlparse(raw).path.strip("/").split("/")[0]
    return raw


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    logger.info("start user_id=%s", message.from_user.id)
    await message.reply(
        "Instagram Reels Trends tracker\n\n"
        "/add — start tracking an Instagram profile\n"
        "/list — show your tracked profiles\n"
        "/remove — stop tracking a profile"
    )


@router.message(Command("add"))
async def cmd_add(message: Message, state: FSMContext) -> None:
    logger.info("add user_id=%s", message.from_user.id)
    await state.set_state(Form.waiting_for_profile)
    await message.reply(
        "Send me the Instagram profile URL or username.\n"
        "Examples: <code>@username</code> or <code>instagram.com/username</code>",
        parse_mode="HTML",
    )


@router.message(Form.waiting_for_profile, F.text, ~F.text.startswith("/"))
async def receive_profile(message: Message, state: FSMContext) -> None:
    await state.clear()
    username = _parse_instagram_username(message.text or "")
    if not username:
        await message.reply("Please send a valid Instagram username or URL.")
        await state.set_state(Form.waiting_for_profile)
        return
    logger.info("add profile user_id=%s username=%s", message.from_user.id, username)

    await message.reply(f"Validating @{username}...")

    async with httpx.AsyncClient(
        headers={"Authorization": f"Bearer {settings.APIFY_TOKEN}"},
        timeout=httpx.Timeout(settings.HTTPX_TIMEOUT),
    ) as http_client:
        try:
            profile = await validate_instagram_profile(username, http_client)
        except (ValueError, RuntimeError) as e:
            logger.warning(
                "add profile failed user_id=%s username=%s error=%s",
                message.from_user.id,
                username,
                e,
            )
            await message.reply(f"Could not validate profile: {e}")
            return

    tg_user = message.from_user
    async with get_session() as db_session:
        await upsert_to_db(
            db_session,
            [{"id": tg_user.id, "username": tg_user.username}],
            UserModel,
            "id",
        )
        await upsert_to_db(
            db_session,
            [
                {
                    "username": profile["username"],
                    "url": profile["url"],
                    "profile_id": profile["id"],
                    "follower_count": profile["followersCount"],
                    "total_post_count": profile["postsCount"],
                    "total_video_count": profile.get("igtvVideoCount", 0),
                    "full_name": profile.get("fullName"),
                    "verified": profile.get("verified", False),
                }
            ],
            InstagramAccountModel,
            "username",
        )
        await upsert_to_db(
            db_session,
            [{"username": profile["username"], "user_id": tg_user.id}],
            TaskModel,
            ["user_id", "username"],
        )

    full_name = profile.get("fullName") or username
    followers = profile.get("followersCount", 0)
    posts = profile.get("postsCount", 0)
    logger.info(
        "add profile done user_id=%s username=%s", message.from_user.id, username
    )
    await message.reply(
        f"Added @{username}\n"
        f"{full_name} · {followers:,} followers · {posts} posts\n"
        "Scraping posts every 2 hours, profile daily."
    )


@router.message(Command("list"))
async def cmd_list(message: Message) -> None:
    logger.info("list user_id=%s", message.from_user.id)
    tg_user = message.from_user
    async with get_session() as db_session:
        tasks = await get_all_from_db(db_session, TaskModel, user_id=tg_user.id)

    if not tasks:
        await message.reply("No profiles tracked yet. Use /add to start.")
        return

    lines = [f"@{t.username}" for t in tasks]
    await message.reply("Tracked profiles:\n" + "\n".join(lines))


@router.message(Command("remove"))
async def cmd_remove(message: Message) -> None:
    logger.info("remove user_id=%s", message.from_user.id)
    tg_user = message.from_user
    async with get_session() as db_session:
        tasks = await get_all_from_db(db_session, TaskModel, user_id=tg_user.id)

    if not tasks:
        await message.reply("No profiles tracked. Use /add to start.")
        return

    buttons = [
        [
            InlineKeyboardButton(
                text=f"@{t.username}", callback_data=f"remove:{t.username}"
            )
        ]
        for t in tasks
    ]
    buttons.append(
        [InlineKeyboardButton(text="Cancel", callback_data="remove:__cancel__")]
    )
    await message.reply(
        "Select a profile to stop tracking:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(lambda c: c.data and c.data.startswith("remove:"))
async def cb_remove(callback: CallbackQuery) -> None:
    username = callback.data.removeprefix("remove:")

    if username == "__cancel__":
        logger.info("remove cancelled user_id=%s", callback.from_user.id)
        await callback.message.edit_text("Cancelled.")
        await callback.answer()
        return

    logger.info(
        "remove profile user_id=%s username=%s", callback.from_user.id, username
    )
    tg_user = callback.from_user
    async with get_session() as db_session:
        await db_session.execute(
            delete(TaskModel).where(
                TaskModel.username == username,
                TaskModel.user_id == tg_user.id,
            )
        )
        await db_session.commit()

        remaining = await db_session.execute(
            select(TaskModel).where(TaskModel.username == username).limit(1)
        )
        if remaining.scalar_one_or_none() is None:
            await db_session.execute(
                delete(InstagramAccountModel).where(
                    InstagramAccountModel.username == username
                )
            )
            await db_session.commit()

    logger.info(
        "remove profile done user_id=%s username=%s", callback.from_user.id, username
    )
    await callback.message.edit_text(f"Stopped tracking @{username}.")
    await callback.answer()
