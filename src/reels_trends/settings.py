from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    APIFY_TOKEN: str
    DATABASE_URL: str
    TELEGRAM_BOT_TOKEN: str


settings = Settings()
