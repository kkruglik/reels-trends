from pathlib import Path
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
import yaml


class Secrets(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    APIFY_TOKEN: str
    DATABASE_URL: str
    TELEGRAM_BOT_TOKEN: str
    TELEGRAM_ALLOWED_USERS: list[int] = []


class WorkerConfig(BaseModel):
    httpx_timeout: float
    num_workers: int


class ScrapeShortConfig(BaseModel):
    interval_minutes: int
    lookback_days: int
    results_limit: int


class ScrapeHistoryConfig(BaseModel):
    lookback_days: int
    results_limit: int
    cron_day_of_week: str


class ScrapeConfig(BaseModel):
    short: ScrapeShortConfig
    history: ScrapeHistoryConfig


class TrendsConfig(BaseModel):
    check_interval_minutes: int
    freshness_hours: int
    history_limit: int
    multiplier: float
    baseline_quantile: float


class ProfileConfig(BaseModel):
    cron_hour: int


class SummaryConfig(BaseModel):
    cron_hour: int
    timezone: str
    lookback_days: int
    top_count: int


class AppConfig(BaseModel):
    worker: WorkerConfig
    scrape: ScrapeConfig
    trends: TrendsConfig
    profile: ProfileConfig
    summary: SummaryConfig


def _load_config(path: str = "config/config.yaml") -> AppConfig:
    return AppConfig(**yaml.safe_load(Path(path).read_text()))


secrets = Secrets()
config = _load_config()
