from pathlib import Path
from typing import Any, Literal
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
import yaml


class Secrets(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    APIFY_TOKEN: str
    DATABASE_URL: str
    TELEGRAM_BOT_TOKEN: str
    TELEGRAM_ALLOWED_USERS: list[int] = []
    WORKER_HTTPX_TIMEOUT: float = 200.0
    WORKER_NUM_WORKERS: int = 4
    LOG_DIR: str = "logs"


class IntervalSchedule(BaseModel):
    kind: Literal["interval"]
    interval_minutes: int


class CronSchedule(BaseModel):
    kind: Literal["cron"]
    minute: int | str = 0
    hour: int | str = 0
    day: int | str | None = None
    day_of_week: str | None = None
    timezone: str = "Europe/Tallinn"


class PipelineJobConfig(BaseModel):
    pipeline: str
    schedule: IntervalSchedule | CronSchedule = Field(discriminator="kind")
    params: dict[str, Any] = {}


class AppConfig(BaseModel):
    pipelines: dict[str, PipelineJobConfig]


def _load_config(path: str = "config/config.yaml") -> AppConfig:
    return AppConfig(**yaml.safe_load(Path(path).read_text()))


secrets = Secrets()
config = _load_config()
