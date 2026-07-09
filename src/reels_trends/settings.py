from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    APIFY_TOKEN: str
    DATABASE_URL: str
    TELEGRAM_BOT_TOKEN: str

    # Worker / HTTP
    HTTPX_TIMEOUT: float
    NUM_WORKERS: int

    # Scheduler cadence
    SCRAPE_POSTS_INTERVAL_HOURS: int
    CHECK_TRENDS_CRON_MINUTE: int
    SCRAPE_PROFILE_CRON_HOUR: int
    DAILY_SUMMARY_CRON_HOUR: int
    DAILY_SUMMARY_TIMEZONE: str

    # Apify scraping
    SCRAPE_RESULTS_LIMIT: int
    SCRAPE_LOOKBACK_DAYS: int

    # Trend detection
    TRENDING_FRESHNESS_HOURS: int
    TRENDING_HISTORY_LIMIT: int
    TRENDING_MULTIPLIER: float
    TRENDING_BASELINE_QUANTILE: float

    # Daily summary
    SUMMARY_LOOKBACK_DAYS: int
    SUMMARY_TOP_COUNT: int

    # Access control (empty = allow all)
    TELEGRAM_ALLOWED_USERS: list[int] = []


settings = Settings()
