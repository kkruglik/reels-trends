# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run locally (requires .env)
uv run reels-trends

# Install dependencies
uv sync

# Run with Docker
docker compose up --build

# Type check (no pytest configured; check with pyright/mypy if needed)
uv run python -c "from reels_trends.main import main"
```

## Architecture

This is a Telegram bot that monitors Instagram Reels for trending content using the Apify scraping API.

**Entry point**: `src/reels_trends/__init__.py` → `main.py::main()`

**Three concurrent systems run in `main()`**:
1. **APScheduler** — fires 4 global cron/interval jobs (`posts`, `trends`, `profile`, `summary`)
2. **Worker pool** — `NUM_WORKERS` async workers pull `(username, pipeline)` tuples from an asyncio `Queue` and execute them with a `Semaphore(3)` concurrency cap
3. **Aiogram bot** — polls Telegram for user commands (`/add`, `/list`, `/remove`, `/start`)

**Pipeline system** (`pipeline/base.py`): Each pipeline is a list of `PipelineStep` objects with `name`, `depends`, `retry_count`, `should_apply()`, and `apply()`. `run_pipeline()` iterates steps, merging returned dicts into shared `state`. Steps use `TaskContext` (db session, HTTP client, bot).

**Four named pipelines** (defined in `main.py::PIPELINE_STEPS`):
- `posts` — scrape reels via Apify actor `apify~instagram-reel-scraper`, poll until complete, upsert to DB. Skips scraping between 22:00–07:00 local time.
- `profile` — scrape and update `InstagramAccountModel` metadata
- `trends` — fetch unnotified recent reels, compute `likes_per_hour`/`views_per_hour`/`engagement_rate` vs. historical baseline (pandas), notify Telegram chats for hits
- `summary` — daily digest of top reels by views

**Database** (`db/`): SQLite via SQLAlchemy async + aiosqlite. Schema created at startup with `Base.metadata.create_all`. No migrations framework — schema changes require manual DB recreation or ALTER TABLE. Tables: `users`, `instagram_accounts`, `tasks` (user×account×chat subscription), `reels`.

**Key relationship**: `TaskModel` links a Telegram `chat_id` to an `instagram_accounts.username`. The worker deduplicates globally across all chats — it enqueues each unique username once, then `NotifyTrending`/`NotifySummary` fan out to all subscribed `chat_id`s.

**Settings** (`settings.py`): All config via env vars / `.env` file using pydantic-settings. Required vars include `APIFY_TOKEN`, `DATABASE_URL` (e.g. `sqlite+aiosqlite:///data/reels_trends.db`), `TELEGRAM_BOT_TOKEN`. `TELEGRAM_ALLOWED_USERS` is an optional allowlist of Telegram user IDs.

**Trend detection algorithm** (`pipeline/check_trends.py`): A reel is trending if it exceeds `TRENDING_MULTIPLIER` × the `TRENDING_BASELINE_QUANTILE` quantile of the account's last `TRENDING_HISTORY_LIMIT` reels on any of: likes/hour, views/hour, or engagement rate (when views ≥ 1000).

**HTTP client**: Uses `httpx2` (not standard `httpx`) with Apify Bearer token auth. All Apify calls are in scrape steps; the bot handlers also create their own client for profile validation via `/add`.

**Middleware** (`bot/middleware.py`): Enforces `TELEGRAM_ALLOWED_USERS` allowlist before any handler runs.
