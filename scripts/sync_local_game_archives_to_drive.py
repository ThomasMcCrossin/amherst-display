#!/usr/bin/env python3
"""
Upload locally managed game archives into the configured Shared Drive games tree.

This is intended for local source workflows where the original full-game MP4 never
passed through `drive_ingest.py`, but we still want `02_Games/<game>/source/...`
to contain the archived full game and provenance metadata.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402
from highlight_extractor.amherst_integration import AmherstBoxScoreProvider  # noqa: E402
from highlight_extractor.file_manager import FileManager  # noqa: E402
from local_archive_sync import sync_local_game_archive_to_drive  # noqa: E402


def parse_source(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("Expected source in the form YYYY-MM-DD=/path/to/video.mp4")
    game_date, raw_path = value.split("=", 1)
    video_path = Path(raw_path).expanduser().resolve()
    if not video_path.exists():
        raise argparse.ArgumentTypeError(f"Video not found: {video_path}")
    return game_date.strip(), video_path


def load_games_provider() -> AmherstBoxScoreProvider:
    games_json = REPO_ROOT / "games" / "amherst-ramblers.json"
    return AmherstBoxScoreProvider(str(games_json))


def canonical_game_info_from_match(source_video: Path, game_date: str, game: dict) -> dict:
    opponent = str(((game.get("opponent") or {}).get("team_name") or "Opponent")).strip()
    is_home = bool(game.get("home_game"))
    if is_home:
        home_team = "Amherst Ramblers"
        away_team = opponent
        home_away = "home"
    else:
        home_team = opponent
        away_team = "Amherst Ramblers"
        home_away = "away"
    return {
        "date": game_date,
        "date_formatted": game_date,
        "home_team": home_team,
        "away_team": away_team,
        "league": "MHL",
        "filename": source_video.name,
        "home_away": home_away,
        "time": "unknown",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload local full-game archives into the configured Drive games tree.")
    parser.add_argument(
        "--source",
        action="append",
        required=True,
        metavar="DATE=VIDEO",
        help="Source in the form YYYY-MM-DD=/absolute/or/relative/path/to/video.mp4. Repeat as needed.",
    )
    args = parser.parse_args()

    provider = load_games_provider()
    file_manager = FileManager(config)

    for game_date, video_path in [parse_source(item) for item in args.source]:
        game = provider.find_game(game_date=game_date)
        if not game:
            raise RuntimeError(f"No Amherst game found for {game_date}")
        canonical_game_info = canonical_game_info_from_match(video_path, game_date, game)
        folders = file_manager.create_game_folder_from_teams(
            date=canonical_game_info["date"],
            home_team=canonical_game_info["home_team"],
            away_team=canonical_game_info["away_team"],
            league=canonical_game_info["league"],
            filename=canonical_game_info["filename"],
            home_away=canonical_game_info["home_away"],
            time_str=canonical_game_info["time"],
        )
        game_dir = Path(folders["game_dir"])
        url = sync_local_game_archive_to_drive(
            game_dir=game_dir,
            source_video=video_path,
            canonical_game_info=canonical_game_info,
            game=game,
        )
        archive_status_path = game_dir / "output" / "archive_sync.json"
        goal_review_url = ""
        if archive_status_path.exists():
            try:
                payload = json.loads(archive_status_path.read_text(encoding="utf-8"))
                goal_review_url = str(payload.get("goal_review_folder_url") or "").strip()
            except Exception:
                goal_review_url = ""
        if goal_review_url:
            print(f"{game_date}: {url} | goal_review={goal_review_url}")
        else:
            print(f"{game_date}: {url}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
