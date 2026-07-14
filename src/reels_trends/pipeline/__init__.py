from reels_trends.pipeline.scrape_posts import (
    ScrapeInstagramPostsStep,
    FetchInstagramPostsStep,
    SaveInstagramPostsStep,
)
from reels_trends.pipeline.scrape_profiles import (
    ScrapeInstagramProfileStep,
    FetchInstagramProfileStep,
    SaveInstagramProfileStep,
)
from reels_trends.pipeline.check_trends import (
    FetchTrendingData,
    PredictTrending,
    NotifyTrending,
)
from reels_trends.pipeline.daily_summary import NotifySummary
from reels_trends.pipeline.base import PipelineStep

PIPELINE_STEPS: dict[str, list[PipelineStep]] = {
    "posts": [
        ScrapeInstagramPostsStep(),
        FetchInstagramPostsStep(),
        SaveInstagramPostsStep(),
    ],
    "profile": [
        ScrapeInstagramProfileStep(),
        FetchInstagramProfileStep(),
        SaveInstagramProfileStep(),
    ],
    "trends": [
        FetchTrendingData(),
        PredictTrending(),
        NotifyTrending(),
    ],
    "summary": [
        NotifySummary(),
    ],
}
