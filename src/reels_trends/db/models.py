from typing import List
import uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import (
    BigInteger,
    Uuid,
    Integer,
    Float,
    DateTime,
    Boolean,
    String,
    JSON,
    ForeignKey,
    UniqueConstraint,
)
from datetime import datetime, UTC


class Base(DeclarativeBase):
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )


class UserModel(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)  # Telegram user ID
    username: Mapped[str | None] = mapped_column(String, nullable=True)
    tasks: Mapped[List["TaskModel"]] = relationship(back_populates="user")


class InstagramAccountModel(Base):
    __tablename__ = "instagram_accounts"

    username: Mapped[str] = mapped_column(String, primary_key=True)
    url: Mapped[str] = mapped_column(String, nullable=False)
    profile_id: Mapped[str] = mapped_column(String, nullable=False)
    follower_count: Mapped[int] = mapped_column(Integer, nullable=False)
    total_video_count: Mapped[int] = mapped_column(Integer, nullable=False)
    total_post_count: Mapped[int] = mapped_column(Integer, nullable=False)
    full_name: Mapped[str | None] = mapped_column(String, nullable=True)
    verified: Mapped[bool] = mapped_column(Boolean, nullable=False)

    tasks: Mapped[List["TaskModel"]] = relationship(back_populates="account")
    reels: Mapped[List["ReelsModel"]] = relationship(
        back_populates="account", cascade="all, delete-orphan"
    )


class TaskModel(Base):
    __tablename__ = "tasks"

    __table_args__ = (UniqueConstraint("chat_id", "username"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    username: Mapped[str] = mapped_column(
        ForeignKey("instagram_accounts.username"), nullable=False
    )
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    user: Mapped["UserModel"] = relationship("UserModel", back_populates="tasks")
    account: Mapped["InstagramAccountModel"] = relationship(
        "InstagramAccountModel", back_populates="tasks"
    )


class ReelsModel(Base):
    __tablename__ = "reels"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    instagram_id: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    short_code: Mapped[str] = mapped_column(String, nullable=False)
    url: Mapped[str] = mapped_column(String, nullable=False)
    owner_id: Mapped[str] = mapped_column(String, nullable=False)
    caption: Mapped[str | None] = mapped_column(String, nullable=True)
    hashtags: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    mentions: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    likes_count: Mapped[int] = mapped_column(Integer, nullable=False)
    comments_count: Mapped[int] = mapped_column(Integer, nullable=False)
    video_view_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    video_play_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    video_duration: Mapped[float | None] = mapped_column(Float, nullable=True)
    posted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_notified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_trending: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    username: Mapped[str] = mapped_column(
        ForeignKey("instagram_accounts.username", ondelete="CASCADE"), nullable=False
    )
    account: Mapped["InstagramAccountModel"] = relationship(
        "InstagramAccountModel", back_populates="reels"
    )
