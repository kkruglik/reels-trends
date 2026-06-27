from pydantic.alias_generators import to_camel
from pydantic import BaseModel, ConfigDict


class ScrapeTaskConfig(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        from_attributes=True,
    )

    username: str
    results_limit: int
    only_posts_newer_than: str | None = None
    skip_pinned_posts: bool = False
    skip_trial_reels: bool = False
    include_shares_count: bool = False
    include_transcript: bool = False
    include_downloaded_video: bool = False


class TaskConfig(BaseModel):
    scrape_task_config: ScrapeTaskConfig
    run_every: str  # or cron
