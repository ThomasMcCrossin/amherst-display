"""Current season, read from config/hockeytech.json.

That file is the single season rollover point for the scraper, the display and the
highlight/Drive tooling: change `season_ids` and `season_label` there, add the
matching programs/<team>-<season_label>.json, and re-run setup_highlight_drive.py.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent
HOCKEYTECH_CONFIG = REPO_DIR / "config" / "hockeytech.json"


@lru_cache(maxsize=1)
def _config() -> dict:
    return json.loads(HOCKEYTECH_CONFIG.read_text(encoding="utf-8"))


def season_label() -> str:
    return str(_config()["season_label"]).strip()


def season_ids() -> list[str]:
    return [str(s).strip() for s in _config().get("season_ids", []) if str(s).strip()]


def program_manifest_path(program_slug: str = "mhl-amherst-ramblers") -> Path:
    return REPO_DIR / "programs" / f"{program_slug}-{season_label()}.json"
