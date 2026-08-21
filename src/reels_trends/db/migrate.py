from pathlib import Path
from alembic.config import Config
from alembic import command

_REPO_ROOT = Path(__file__).resolve().parents[3]


def run_migrations() -> None:
    cfg = Config(str(_REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_REPO_ROOT / "migrations"))
    command.upgrade(cfg, "head")
