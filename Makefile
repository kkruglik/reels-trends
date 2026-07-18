.PHONY: lint format fix check build run up down logs pull-db pull-logs

# --- Ruff ---
lint:
	uvx ruff check .

format:
	uvx ruff format .

fix:
	uvx ruff check --fix .

check:
	uvx ruff check .
	uvx ruff format --check .

# --- Docker ---
build:
	docker compose build

run: up

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f

# --- Data ---
pull-db:
	gcloud compute scp \
		gc_tech@instance-20251215-164416:~/repos/reels-trends/data/reels_trends.db \
		./data/reels_trends.db \
		--zone=europe-north1-a --project=spam-bots-481316

pull-logs:
	gcloud compute scp --recurse \
		gc_tech@instance-20251215-164416:~/repos/reels-trends/logs \
		./ \
		--zone=europe-north1-a --project=spam-bots-481316
