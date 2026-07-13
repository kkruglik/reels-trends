import re
from urllib.parse import urlparse
from aiogram import Router
from aiogram.filters import Command
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
from reels_trends.settings import secrets

router = Router()
logger = logging.getLogger(__name__)


def _parse_instagram_username(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("@"):
        return raw[1:]
    if "instagram.com" in raw:
        return urlparse(raw).path.strip("/").split("/")[0]
    return raw


_USERNAME_RE = re.compile(r"^[\w.]{1,30}$")


def _parse_usernames(text: str) -> list[str]:
    tokens = re.split(r"[\s,]+", text.strip())
    seen = set()
    result = []
    for token in tokens:
        if not token or token.startswith("/"):
            continue
        u = _parse_instagram_username(token)
        if u and _USERNAME_RE.match(u) and u not in seen:
            seen.add(u)
            result.append(u)
    return result


async def _add_profile(
    username: str,
    chat_id: int,
    tg_user,
    http_client: httpx.AsyncClient,
) -> str:
    async with get_session() as db_session:
        task_result = await db_session.execute(
            select(TaskModel).where(
                TaskModel.username == username,
                TaskModel.chat_id == chat_id,
            )
        )
        if task_result.scalar_one_or_none() is not None:
            return f"@{username} — already tracking"

        account_result = await db_session.execute(
            select(InstagramAccountModel).where(
                InstagramAccountModel.username == username
            )
        )
        existing_account = account_result.scalar_one_or_none()

    if existing_account is not None:
        profile = {
            "username": existing_account.username,
            "url": existing_account.url,
            "id": existing_account.profile_id,
            "followersCount": existing_account.follower_count,
            "postsCount": existing_account.total_post_count,
            "igtvVideoCount": existing_account.total_video_count,
            "fullName": existing_account.full_name,
            "verified": existing_account.verified,
        }
        logger.info("add profile cache hit chat_id=%s username=%s", chat_id, username)
    else:
        try:
            profile = await validate_instagram_profile(username, http_client)
        except (ValueError, RuntimeError) as e:
            logger.warning("add profile failed chat_id=%s username=%s error=%s", chat_id, username, e)
            return f"@{username} — failed: {e}"

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
            [{"username": profile["username"], "user_id": tg_user.id, "chat_id": chat_id}],
            TaskModel,
            ["chat_id", "username"],
        )

    followers = profile.get("followersCount", 0)
    logger.info("add profile done chat_id=%s username=%s", chat_id, username)
    return f"@{username} — added ({followers:,} followers)"


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    logger.info("start chat_id=%s", message.chat.id)
    await message.reply(
        "Instagram Reels Trends tracker\n\n"
        "/add @username — track one or multiple profiles\n"
        "/list — show tracked profiles\n"
        "/remove — stop tracking a profile"
    )


@router.message(Command("add"))
async def cmd_add(message: Message) -> None:
    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2 or not args[1].strip():
        await message.reply(
            "Usage: <code>/add @username1 @username2 ...</code>",
            parse_mode="HTML",
        )
        return

    usernames = _parse_usernames(args[1])
    if not usernames:
        await message.reply("Please send valid Instagram usernames or URLs.")
        return

    if len(usernames) > 1:
        await message.reply(f"Adding {len(usernames)} profiles...")

    async with httpx.AsyncClient(
        headers={"Authorization": f"Bearer {secrets.APIFY_TOKEN}"},
        timeout=httpx.Timeout(secrets.WORKER_HTTPX_TIMEOUT),
    ) as http_client:
        results = []
        for username in usernames:
            logger.info("add profile chat_id=%s username=%s", message.chat.id, username)
            result = await _add_profile(username, message.chat.id, message.from_user, http_client)
            results.append(result)

    await message.reply("\n".join(results))


@router.message(Command("list"))
async def cmd_list(message: Message) -> None:
    logger.info("list chat_id=%s", message.chat.id)
    async with get_session() as db_session:
        tasks = await get_all_from_db(db_session, TaskModel, chat_id=message.chat.id)

    if not tasks:
        await message.reply("No profiles tracked yet. Use /add @username to start.")
        return

    lines = [f"@{t.username}" for t in tasks]
    await message.reply("Tracked profiles:\n" + "\n".join(lines))


@router.message(Command("remove"))
async def cmd_remove(message: Message) -> None:
    logger.info("remove chat_id=%s", message.chat.id)
    async with get_session() as db_session:
        tasks = await get_all_from_db(db_session, TaskModel, chat_id=message.chat.id)

    if not tasks:
        await message.reply("No profiles tracked. Use /add @username to start.")
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
        logger.info("remove cancelled chat_id=%s", callback.message.chat.id)
        await callback.message.edit_text("Cancelled.")
        await callback.answer()
        return

    logger.info(
        "remove profile chat_id=%s username=%s", callback.message.chat.id, username
    )
    async with get_session() as db_session:
        await db_session.execute(
            delete(TaskModel).where(
                TaskModel.username == username,
                TaskModel.chat_id == callback.message.chat.id,
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
        "remove profile done chat_id=%s username=%s", callback.message.chat.id, username
    )
    await callback.message.edit_text(f"Stopped tracking @{username}.")
    await callback.answer()
