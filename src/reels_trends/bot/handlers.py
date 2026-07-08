from urllib.parse import urlparse
from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
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


def _parse_instagram_username(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("@"):
        return raw[1:]
    if "instagram.com" in raw:
        return urlparse(raw).path.strip("/").split("/")[0]
    return raw


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    await message.reply(
        "Instagram Reels Trends tracker\n\n"
        "/add <profile> — start tracking an Instagram profile\n"
        "/list — show your tracked profiles\n"
        "/remove <username> — stop tracking a profile"
    )


@router.message(Command("add"))
async def cmd_add(message: Message) -> None:
    raw = (message.text or "").removeprefix("/add").strip()
    if not raw:
        await message.reply("Usage: /add <instagram_url_or_username>")
        return

    username = _parse_instagram_username(raw)
    await message.reply(f"Validating @{username}...")

    async with httpx.AsyncClient(
        headers={"Authorization": f"Bearer {settings.APIFY_TOKEN}"},
        timeout=httpx.Timeout(200.0),
    ) as http_client:
        try:
            profile = await validate_instagram_profile(username, http_client)
        except (ValueError, RuntimeError) as e:
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
    await message.reply(
        f"Added @{username}\n"
        f"{full_name} · {followers:,} followers · {posts} posts\n"
        "Scraping posts every 2 hours, profile daily."
    )


@router.message(Command("list"))
async def cmd_list(message: Message) -> None:
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
    username = (message.text or "").removeprefix("/remove").strip().lstrip("@")
    if not username:
        await message.reply("Usage: /remove <username>")
        return

    tg_user = message.from_user
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

    await message.reply(f"Stopped tracking @{username}.")
