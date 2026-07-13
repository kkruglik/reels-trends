.PHONY: lint format fix check build run up down logs

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
