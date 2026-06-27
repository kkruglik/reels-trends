import uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import Uuid, Integer, Float, DateTime, Boolean, String, JSON
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


class TaskModel(Base):
    __tablename__ = "tasks"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    username: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    results_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    only_posts_newer_than: Mapped[str | None] = mapped_column(String, nullable=True)
    skip_pinned_posts: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    skip_trial_reels: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    include_shares_count: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    include_transcript: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    include_downloaded_video: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class ReelsModel(Base):
    __tablename__ = "reels"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    instagram_id: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    short_code: Mapped[str] = mapped_column(String, nullable=False)
    url: Mapped[str] = mapped_column(String, nullable=False)
    owner_username: Mapped[str] = mapped_column(String, nullable=False)
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
    is_trending: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class AlertsModel(Base):
    __tablename__ = "alerts"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    is_notified: Mapped[bool] = mapped_column(Boolean)
