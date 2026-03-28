#!/usr/bin/env python3
"""
Build a stitched reel from one or more source games using metadata filters.

This supports both:
- multi-game team montages, e.g. every Amherst goal in a series
- player-driven reels, e.g. every goal where Gaudet recorded an assist
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402
from drive_api import ensure_folder, get_drive_service, resolve_folder_path, upsert_file  # noqa: E402
from drive_config import build_program_drive_layout, resolve_drive_config  # noqa: E402
from highlight_extractor import HighlightPipeline  # noqa: E402
from highlight_extractor.amherst_integration import AmherstBoxScoreProvider  # noqa: E402
from highlight_extractor.file_manager import FileManager  # noqa: E402
from highlight_extractor.time_utils import time_string_to_seconds  # noqa: E402
from local_archive_sync import sync_local_game_archive_to_drive  # noqa: E402
try:  # noqa: E402
    from highlight_extractor.video_processor import VideoProcessor  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - depends on local video deps
    VideoProcessor = None  # type: ignore[assignment]


LOGGER = logging.getLogger("build_filtered_reel")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )


def parse_source(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("Expected source in the form YYYY-MM-DD=/path/to/video.mp4")

    game_date, raw_path = value.split("=", 1)
    game_date = game_date.strip()
    video_path = Path(raw_path).expanduser().resolve()

    if not game_date:
        raise argparse.ArgumentTypeError("Game date cannot be empty")
    if not video_path.exists():
        raise argparse.ArgumentTypeError(f"Video not found: {video_path}")

    return game_date, video_path


def load_games_provider() -> AmherstBoxScoreProvider:
    games_json = REPO_ROOT / "games" / "amherst-ramblers.json"
    return AmherstBoxScoreProvider(str(games_json))


def normalize_name(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("-", " ").split())


def is_amherst_team(value: Any) -> bool:
    team = normalize_name(value)
    return team in {"amherst ramblers", "ramblers", "amherst", "amh", "amherst ramblers fc"} or ("rambler" in team)


def canonical_game_info_from_match(source_video: Path, game_date: str, game: dict[str, Any]) -> dict[str, Any]:
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
        "playoff": bool(game.get("playoff")),
        "game_number": game.get("game_number"),
        "schedule_notes": str(game.get("schedule_notes") or "").strip(),
        "overtime": bool(((game.get("result") or {}).get("overtime"))),
        "shootout": bool(((game.get("result") or {}).get("shootout"))),
    }


def resolve_clip_path(game_dir: Path, entry: dict[str, Any]) -> Path | None:
    relpath = str(entry.get("path") or "").strip()
    clip_filename = str(entry.get("clip_filename") or "").strip()
    clip_index = entry.get("index")

    candidates: list[Path] = []
    if relpath:
        candidates.append(game_dir / relpath)
    if clip_filename:
        candidates.append(game_dir / "clips" / clip_filename)
    if clip_index not in (None, ""):
        try:
            index_prefix = f"{int(clip_index):02d} - "
        except (TypeError, ValueError):
            index_prefix = ""
        if index_prefix:
            candidates.extend(sorted((game_dir / "clips" / "goal_review").glob(f"{index_prefix}*.mp4")))

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _text_matches_any(value: Any, needles: list[str]) -> bool:
    if not needles:
        return True
    normalized_value = normalize_name(value)
    return any(needle in normalized_value for needle in needles)


def _event_special_tokens(entry: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    raw = str(entry.get("special") or "").strip().upper()
    if raw:
        tokens.add(raw)
    if entry.get("power_play"):
        tokens.add("PP")
    if entry.get("short_handed"):
        tokens.add("SH")
    if entry.get("empty_net"):
        tokens.add("EN")
    return tokens


def clip_matches_filters(entry: dict[str, Any], filters: dict[str, list[str]]) -> bool:
    event_type = normalize_name(entry.get("type"))
    if filters["event_types"] and event_type not in filters["event_types"]:
        return False

    if filters["teams"]:
        team_value = normalize_name(entry.get("team"))
        if "ramblers" in filters["teams"]:
            if not (is_amherst_team(team_value) or any(t in team_value for t in filters["teams"] if t != "ramblers")):
                return False
        elif not any(team_filter in team_value for team_filter in filters["teams"]):
            return False

    scorer = normalize_name(entry.get("scorer"))
    assist1 = normalize_name(entry.get("assist1"))
    assist2 = normalize_name(entry.get("assist2"))
    penalty_player = normalize_name((entry.get("player") or {}).get("name"))
    assist_values = [assist1, assist2]
    player_values = [v for v in [scorer, assist1, assist2, penalty_player] if v]

    if filters["scorers"] and not any(filter_value in scorer for filter_value in filters["scorers"]):
        return False
    if filters["assists"] and not any(
        any(filter_value in assist for assist in assist_values if assist)
        for filter_value in filters["assists"]
    ):
        return False
    if filters["player_any"] and not any(
        any(filter_value in player for player in player_values)
        for filter_value in filters["player_any"]
    ):
        return False

    if filters["specials"]:
        specials = _event_special_tokens(entry)
        if not specials.intersection({token.upper() for token in filters["specials"]}):
            return False

    return True


def load_clip_manifest(game_dir: Path) -> list[dict[str, Any]]:
    manifest_path = game_dir / "data" / "clips_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing clip manifest: {manifest_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    clips = manifest.get("clips", [])
    return [entry for entry in clips if isinstance(entry, dict)]


def load_archive_sync_status(game_dir: Path) -> dict[str, Any]:
    status_path = game_dir / "output" / "archive_sync.json"
    if not status_path.exists():
        return {}
    try:
        payload = json.loads(status_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _is_amherst_scoring_play(play: dict[str, Any]) -> bool:
    return is_amherst_team(play.get("team"))


def _canonical_play_team(play: dict[str, Any], canonical_game_info: dict[str, Any]) -> str:
    return canonical_game_info["home_team"] if _is_amherst_scoring_play(play) and is_amherst_team(canonical_game_info["home_team"]) else (
        canonical_game_info["away_team"] if _is_amherst_scoring_play(play) else (
            canonical_game_info["home_team"] if not is_amherst_team(canonical_game_info["home_team"]) else canonical_game_info["away_team"]
        )
    )


def _normalized_assists(play: dict[str, Any]) -> tuple[str, str]:
    assists = play.get("assists") if isinstance(play.get("assists"), list) else []
    names = [str((assist or {}).get("name") or "").strip() for assist in assists[:2] if isinstance(assist, dict)]
    while len(names) < 2:
        names.append("")
    return names[0], names[1]


def build_goal_score_lookup(game: dict[str, Any], canonical_game_info: dict[str, Any]) -> dict[tuple[str, str, str, str], list[dict[str, Any]]]:
    lookup: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    scoring = sorted(
        [play for play in (game.get("scoring") or []) if isinstance(play, dict)],
        key=lambda play: (int(play.get("period") or 0), time_string_to_seconds(str(play.get("time") or "0:00"))),
    )

    home_score = 0
    away_score = 0
    for play in scoring:
        team_name = _canonical_play_team(play, canonical_game_info)
        scorer = str(((play.get("scorer") or {}).get("name") or "")).strip()
        if team_name == canonical_game_info["home_team"]:
            home_score += 1
        elif team_name == canonical_game_info["away_team"]:
            away_score += 1

        key = (
            str(play.get("period") or "").strip(),
            str(play.get("time") or "").strip(),
            normalize_name(team_name),
            normalize_name(scorer),
        )
        assist1, assist2 = _normalized_assists(play)
        lookup.setdefault(key, []).append(
            {
                "home_score": home_score,
                "away_score": away_score,
                "assist1": assist1,
                "assist2": assist2,
                "power_play": bool(play.get("power_play")),
                "short_handed": bool(play.get("short_handed")),
                "empty_net": bool(play.get("empty_net")),
            }
        )
    return lookup


def score_lookup_key(entry: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(entry.get("period") or "").strip(),
        str(entry.get("time") or "").strip(),
        normalize_name(entry.get("team")),
        normalize_name(entry.get("scorer")),
    )


def game_label_for_source(index: int, game_date: str, mode: str) -> str:
    normalized = str(mode or "").strip().lower()
    if normalized == "series_game":
        return f"Game {index}"
    if normalized == "date":
        return game_date
    return ""


def format_series_status(*, amherst_wins: int, opponent_wins: int, opponent_name: str) -> str:
    if amherst_wins == opponent_wins:
        return f"Series tied {amherst_wins}-{opponent_wins}"
    if amherst_wins > opponent_wins:
        return f"Amherst leads {amherst_wins}-{opponent_wins}"
    return f"{str(opponent_name or 'Opponent').strip()} leads {opponent_wins}-{amherst_wins}"


def _coerce_int(value: Any) -> int | None:
    if value in ("", None):
        return None
    try:
        return int(value)
    except Exception:
        return None


def build_momentum_headline(
    *,
    amherst_wins_after: int,
    opponent_wins_after: int,
    opponent_name: str,
) -> str:
    if amherst_wins_after == opponent_wins_after:
        if amherst_wins_after >= 3:
            return "WINNER TAKE ALL"
        return f"Series tied {amherst_wins_after}-{opponent_wins_after}".upper()
    if amherst_wins_after > opponent_wins_after:
        if amherst_wins_after == 3 and opponent_wins_after < 3:
            return "Amherst One Win Away"
        return f"Amherst leads {amherst_wins_after}-{opponent_wins_after}".upper()
    if opponent_wins_after == 3 and amherst_wins_after < 3:
        return f"{opponent_name} One Win Away"
    return f"{opponent_name} leads {opponent_wins_after}-{amherst_wins_after}".upper()


def format_display_date(game_date: str) -> str:
    try:
        parsed = datetime.strptime(str(game_date or "").strip(), "%Y-%m-%d")
    except Exception:
        return str(game_date or "").strip()
    return parsed.strftime("%B %d, %Y").replace(" 0", " ")


def build_series_context(
    *,
    source_index: int,
    game_date: str,
    game: dict[str, Any],
    canonical_game_info: dict[str, Any],
    series_title: str,
    game_label: str,
    amherst_wins_before: int,
    opponent_wins_before: int,
) -> dict[str, Any]:
    opponent_name = str(((game.get("opponent") or {}).get("team_name") or "Opponent")).strip() or "Opponent"
    venue = str(game.get("venue") or "").strip()
    attendance = game.get("attendance")
    if attendance in ("", None):
        attendance = None
    else:
        try:
            attendance = int(attendance)
        except Exception:
            attendance = None

    result = game.get("result") if isinstance(game.get("result"), dict) else {}
    schedule_notes = str(game.get("schedule_notes") or "").strip()
    ramblers_score = _coerce_int(result.get("ramblers_score")) if result else None
    opponent_score = _coerce_int(result.get("opponent_score")) if result else None
    amherst_wins_after = int(amherst_wins_before) + (1 if bool(result.get("won")) else 0)
    opponent_wins_after = int(opponent_wins_before) + (0 if bool(result.get("won")) else (1 if result else 0))
    home_team = str(canonical_game_info.get("home_team") or "").strip()
    away_team = str(canonical_game_info.get("away_team") or "").strip()
    if is_amherst_team(home_team):
        final_home_score = ramblers_score
        final_away_score = opponent_score
    else:
        final_home_score = opponent_score
        final_away_score = ramblers_score

    return {
        "source_index": int(source_index),
        "game_date": str(game_date or "").strip(),
        "game_date_display": format_display_date(game_date),
        "game_label": str(game_label or "").strip(),
        "series_title": str(series_title or "").strip(),
        "series_record_before": f"{int(amherst_wins_before)}-{int(opponent_wins_before)}",
        "series_status": format_series_status(
            amherst_wins=int(amherst_wins_before),
            opponent_wins=int(opponent_wins_before),
            opponent_name=opponent_name,
        ),
        "series_record_after": f"{amherst_wins_after}-{opponent_wins_after}",
        "series_status_after": format_series_status(
            amherst_wins=amherst_wins_after,
            opponent_wins=opponent_wins_after,
            opponent_name=opponent_name,
        ),
        "venue": venue,
        "attendance": attendance,
        "opponent_name": opponent_name,
        "schedule_notes": schedule_notes,
        "home_team": home_team,
        "away_team": away_team,
        "ramblers_score": ramblers_score,
        "opponent_score": opponent_score,
        "final_score_display": str(result.get("final_score") or "").strip(),
        "final_home_score": final_home_score,
        "final_away_score": final_away_score,
        "won": bool(result.get("won")) if result else None,
        "overtime": bool(result.get("overtime")) if result else False,
        "shootout": bool(result.get("shootout")) if result else False,
        "momentum_headline": build_momentum_headline(
            amherst_wins_after=amherst_wins_after,
            opponent_wins_after=opponent_wins_after,
            opponent_name=opponent_name,
        ),
        "result": dict(result) if result else {},
    }


def annotate_clip_entry(
    *,
    entry: dict[str, Any],
    score_lookup: dict[tuple[str, str, str, str], list[dict[str, Any]]],
    series_title: str,
    game_label: str,
) -> dict[str, Any]:
    annotated = dict(entry)
    key = score_lookup_key(annotated)
    match_queue = score_lookup.get(key) or []
    if match_queue:
        match = match_queue.pop(0)
        annotated["home_score"] = int(match["home_score"])
        annotated["away_score"] = int(match["away_score"])
        if not str(annotated.get("assist1") or "").strip():
            annotated["assist1"] = str(match.get("assist1") or "").strip()
        if not str(annotated.get("assist2") or "").strip():
            annotated["assist2"] = str(match.get("assist2") or "").strip()
        annotated["power_play"] = bool(annotated.get("power_play") or match.get("power_play"))
        annotated["short_handed"] = bool(annotated.get("short_handed") or match.get("short_handed"))
        annotated["empty_net"] = bool(annotated.get("empty_net") or match.get("empty_net"))
    annotated["overlay_series_title"] = str(series_title or "").strip()
    annotated["overlay_game_label"] = str(game_label or "").strip()
    return annotated


def resolve_drive_section_path(section: str) -> str:
    drive_cfg = resolve_drive_config()
    league = str(os.environ.get("HIGHLIGHTS_PROGRAM_LEAGUE") or "MHL").strip() or "MHL"
    team = str(os.environ.get("HIGHLIGHTS_PROGRAM_TEAM") or "Amherst Ramblers").strip() or "Amherst Ramblers"
    season = str(os.environ.get("HIGHLIGHTS_PROGRAM_SEASON") or "2025-26").strip() or "2025-26"
    root_path = str(os.environ.get("HIGHLIGHTS_PROGRAM_ROOT_PATH") or "").strip()
    root_folder = root_path.split("/", 1)[0] if root_path else "Programs"
    layout = build_program_drive_layout(league=league, team=team, season=season, root_folder=root_folder)
    normalized = str(section or "").strip().lower().replace("-", "_")
    mapping = {
        "games": layout.reels_games_path,
        "series": layout.reels_series_path,
        "players": layout.reels_players_path,
        "special_projects": layout.reels_special_projects_path,
    }
    if normalized not in mapping:
        raise ValueError(f"Unsupported drive section '{section}'")
    return mapping[normalized]


def upload_filtered_reel(
    *,
    output_path: Path,
    manifest_path: Path,
    section: str,
    subfolder: str,
) -> str:
    drive_cfg = resolve_drive_config()
    if not drive_cfg.drive_id:
        raise RuntimeError("HIGHLIGHTS_DRIVE_ID is not configured")
    credentials_path = str(drive_cfg.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if not credentials_path:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS is not configured")

    service = get_drive_service(credentials_path)
    section_path = resolve_drive_section_path(section)
    parent_id = resolve_folder_path(service, drive_id=drive_cfg.drive_id, folder_path=section_path)
    target_id = ensure_folder(service, parent_id=parent_id, name=subfolder, drive_id=drive_cfg.drive_id)
    upsert_file(service, local_path=output_path, parent_id=target_id, drive_id=drive_cfg.drive_id)
    upsert_file(service, local_path=manifest_path, parent_id=target_id, drive_id=drive_cfg.drive_id)
    return f"https://drive.google.com/drive/folders/{target_id}"


def maybe_run_pipeline(
    *,
    provider: AmherstBoxScoreProvider,
    file_manager: FileManager,
    video_path: Path,
    game_date: str,
    game: dict[str, Any],
    execution_profile_name: str,
    reel_mode: str,
    execution_overrides: dict[str, Any],
    dry_run: bool,
    force_reprocess: bool,
    disable_source_overlays: bool,
) -> tuple[Path, dict[str, Any]]:
    canonical_game_info = canonical_game_info_from_match(video_path, game_date, game)
    game_folders = file_manager.create_game_folder_from_teams(
        date=canonical_game_info["date"],
        home_team=canonical_game_info["home_team"],
        away_team=canonical_game_info["away_team"],
        league=canonical_game_info["league"],
        filename=canonical_game_info["filename"],
        home_away=canonical_game_info["home_away"],
        time_str=canonical_game_info["time"],
    )
    game_dir = Path(game_folders["game_dir"])

    summary = {
        "game_date": game_date,
        "game_id": str(game.get("game_id") or ""),
        "video_path": str(video_path),
        "game_dir": str(game_dir),
        "reused_existing_outputs": False,
    }

    manifest_path = game_dir / "data" / "clips_manifest.json"
    if manifest_path.exists() and not force_reprocess:
        LOGGER.info("Reusing existing clips for %s from %s", game_date, game_dir)
        summary["reused_existing_outputs"] = True
        return game_dir, summary

    if dry_run:
        return game_dir, summary

    LOGGER.info(
        "Running highlight pipeline for %s (%s vs %s)",
        game_date,
        canonical_game_info["home_team"],
        canonical_game_info["away_team"],
    )

    fetcher = provider.create_fetcher(game)
    pipeline = HighlightPipeline(
        config=config,
        video_path=video_path,
        box_score_fetcher=fetcher,
        game_info_override=canonical_game_info,
        game_folders_override=game_folders,
        source_game_info_override=dict(canonical_game_info),
    )

    profile_selection = config.resolve_highlight_execution_selection(
        execution_profile_name,
        game_info=canonical_game_info,
        source_game_info=dict(canonical_game_info),
        reel_mode=reel_mode,
        **execution_overrides,
    )
    profile = dict(profile_selection["execution_profile"])
    profile["build_reel"] = False
    profile["build_description"] = False

    original_overlay_enabled = getattr(config, "OVERLAY_ENABLED", True)
    if disable_source_overlays:
        config.OVERLAY_ENABLED = False
    try:
        result = pipeline.execute(**profile)
    finally:
        if disable_source_overlays:
            config.OVERLAY_ENABLED = original_overlay_enabled
    summary.update(
        {
            "execution_profile_name": profile_selection["execution_profile_name"],
            "scorebug_profile": profile_selection["scorebug_profile"],
            "pipeline_success": bool(result.success),
            "events_found": result.events_found,
            "events_matched": result.events_matched,
            "clips_created": result.clips_created,
            "paused_for_review": bool(getattr(result, "paused_for_review", False)),
        }
    )
    if not result.success and result.clips_created <= 0:
        raise RuntimeError(
            f"Highlight pipeline failed for {game_date}: {result.failed_reason or result.errors or 'unknown failure'}"
        )

    return game_dir, summary


def write_filtered_manifest(
    *,
    output_path: Path,
    execution_profile: str,
    reel_mode: str,
    render_style: str,
    title: str,
    game_label_mode: str,
    filters: dict[str, list[str]],
    sources: list[dict[str, Any]],
    selected_clips: list[dict[str, Any]],
) -> Path:
    manifest_path = output_path.with_suffix(".json")
    payload = {
        "kind": "filtered-highlight-reel",
        "output_path": str(output_path),
        "execution_profile": execution_profile,
        "reel_mode": reel_mode,
        "render_style": render_style,
        "title": title,
        "game_label_mode": game_label_mode,
        "filters": filters,
        "sources": sources,
        "clip_count": len(selected_clips),
        "clips": selected_clips,
    }
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a stitched reel from one or more games using clip metadata filters.")
    parser.add_argument(
        "--source",
        action="append",
        required=True,
        metavar="DATE=VIDEO",
        help="Source in the form YYYY-MM-DD=/absolute/or/relative/path/to/video.mp4. Repeat in game order.",
    )
    parser.add_argument("--output", type=Path, required=True, help="Output MP4 for the filtered reel")
    parser.add_argument("--profile", default=getattr(config, "DEFAULT_HIGHLIGHT_EXECUTION_PROFILE", "flohockey_recording"), help="Highlight execution profile to use when processing sources")
    parser.add_argument("--reel-mode", default=getattr(config, "DEFAULT_REEL_MODE", "goals_only"), help="Reel mode to use while generating source clips")
    parser.add_argument("--sample-interval", type=int, default=None, help="Override OCR sampling interval in seconds for source processing")
    parser.add_argument("--tolerance-seconds", type=int, default=None, help="Override event-match tolerance in seconds for source processing")
    parser.add_argument("--before-seconds", type=float, default=None, help="Override clip pre-roll in seconds for source processing")
    parser.add_argument("--after-seconds", type=float, default=None, help="Override clip post-roll in seconds for source processing")
    parser.add_argument("--disable-auto-detect-start", action="store_true", help="Skip game-start auto detection and start OCR at the beginning of the file")
    parser.add_argument("--disable-goal-clock-refinement", action="store_true", help="Skip the post-match goal clock-stop refinement pass")
    parser.add_argument("--disable-local-ocr-refinement", action="store_true", help="Skip the generic post-match local OCR refinement pass")
    parser.add_argument(
        "--goal-legacy-timing-fallback",
        action="store_true",
        help="Allow legacy approximate goal timing fallbacks for broken/unreadable scorebugs. Normal runs should leave this off.",
    )
    parser.add_argument("--render-style", choices=("plain", "production"), default="plain", help="Final reel render style")
    parser.add_argument("--title", default="", help="Optional series/title label for production overlays and output manifest")
    parser.add_argument("--game-label-mode", choices=("none", "date", "series_game"), default="none", help="How to label each source game in production overlays")
    parser.add_argument("--game-intro-cards", action="store_true", help="Insert the series-open card and between-game recap cards in production renders")
    parser.add_argument("--game-intro-card-seconds", type=float, default=3.5, help="Duration of each inserted series-open / between-game card in seconds")
    parser.add_argument("--series-outro-card", action="store_true", help="Append a full-screen series outro card after the final clip in production renders")
    parser.add_argument("--series-outro-card-seconds", type=float, default=4.5, help="Duration of the inserted series outro card in seconds")
    parser.add_argument("--series-outro-status", default="", help="Primary status line for the series outro card")
    parser.add_argument("--series-outro-game-label", default="", help="Upcoming game label for the series outro card")
    parser.add_argument("--series-outro-datetime-label", default="", help="Date/time label for the series outro card")
    parser.add_argument("--series-outro-venue", default="", help="Venue/stadium name for the series outro card")
    parser.add_argument("--series-outro-location", default="", help="Venue location text for the series outro card")
    parser.add_argument("--series-outro-home-team", default="", help="Home team name to use on the series outro card")
    parser.add_argument("--series-outro-away-team", default="", help="Away team name to use on the series outro card")
    parser.add_argument("--event-type", action="append", default=[], help="Event type filter (goal, penalty, ...). Repeat as needed.")
    parser.add_argument("--team", action="append", default=[], help="Team filter. Use 'ramblers' for Amherst clips.")
    parser.add_argument("--scorer", action="append", default=[], help="Scorer name contains filter. Repeat as needed.")
    parser.add_argument("--assist", action="append", default=[], help="Assist name contains filter. Repeat as needed.")
    parser.add_argument("--player-any", action="append", default=[], help="Matches scorer, assist, or penalty player.")
    parser.add_argument("--special", action="append", default=[], help="Special tag filter (PP, SH, EN). Repeat as needed.")
    parser.add_argument("--upload-drive", action="store_true", help="Upload the finished reel and manifest to Google Drive")
    parser.add_argument("--upload-drive-section", choices=("games", "series", "players", "special-projects"), default="series", help="Drive reel branch to publish into")
    parser.add_argument("--upload-drive-subfolder", default="", help="Drive subfolder name to create/use under the selected reel branch")
    parser.set_defaults(upload_game_archives_drive=None)
    parser.add_argument("--upload-game-archives-drive", dest="upload_game_archives_drive", action="store_true", help="Upload/update each source game's archive tree under the Drive games branch")
    parser.add_argument("--no-upload-game-archives-drive", dest="upload_game_archives_drive", action="store_false", help="Skip uploading each source game's archive tree to the Drive games branch")
    parser.add_argument("--dry-run", action="store_true", help="Resolve sources and filters without video processing")
    parser.add_argument("--force-reprocess", action="store_true", help="Re-run the highlight pipeline even if the game folder already has clip outputs")
    args = parser.parse_args()

    configure_logging()

    ordered_sources = [parse_source(item) for item in args.source]
    upload_game_archives = (
        bool(getattr(config, "AUTO_UPLOAD_GAME_ARCHIVES_TO_DRIVE", False))
        if args.upload_game_archives_drive is None
        else bool(args.upload_game_archives_drive)
    )
    provider = load_games_provider()
    file_manager = FileManager(config)

    filters = {
        "event_types": [normalize_name(value) for value in args.event_type],
        "teams": [normalize_name(value) for value in args.team],
        "scorers": [normalize_name(value) for value in args.scorer],
        "assists": [normalize_name(value) for value in args.assist],
        "player_any": [normalize_name(value) for value in args.player_any],
        "specials": [str(value or "").strip().upper() for value in args.special if str(value or "").strip()],
    }

    selected_clip_paths: list[Path] = []
    selected_clip_entries: list[dict[str, Any]] = []
    source_summary: list[dict[str, Any]] = []
    amherst_wins_before = 0
    opponent_wins_before = 0
    execution_overrides = {
        "sample_interval": args.sample_interval,
        "tolerance_seconds": args.tolerance_seconds,
        "before_seconds": args.before_seconds,
        "after_seconds": args.after_seconds,
        "auto_detect_start": False if args.disable_auto_detect_start else None,
        "refine_goal_clock": False if args.disable_goal_clock_refinement else None,
        "refine_local_ocr": False if args.disable_local_ocr_refinement else None,
        "goal_legacy_timing_fallback": True if args.goal_legacy_timing_fallback else None,
    }

    for source_index, (game_date, video_path) in enumerate(ordered_sources, 1):
        game = provider.find_game(game_date=game_date)
        if not game:
            raise RuntimeError(f"No Amherst game found for {game_date}")
        canonical_game_info = canonical_game_info_from_match(video_path, game_date, game)
        score_lookup = build_goal_score_lookup(game, canonical_game_info)
        game_label = game_label_for_source(source_index, game_date, args.game_label_mode)
        series_context = build_series_context(
            source_index=source_index,
            game_date=game_date,
            game=game,
            canonical_game_info=canonical_game_info,
            series_title=str(args.title or "").strip(),
            game_label=game_label,
            amherst_wins_before=amherst_wins_before,
            opponent_wins_before=opponent_wins_before,
        )

        game_dir, summary = maybe_run_pipeline(
            provider=provider,
            file_manager=file_manager,
            video_path=video_path,
            game_date=game_date,
            game=game,
            execution_profile_name=args.profile,
            reel_mode=args.reel_mode,
            execution_overrides=execution_overrides,
            dry_run=args.dry_run,
            force_reprocess=args.force_reprocess,
            disable_source_overlays=(args.render_style == "production"),
        )

        if upload_game_archives and not args.dry_run:
            archive_url = sync_local_game_archive_to_drive(
                game_dir=game_dir,
                source_video=video_path,
                canonical_game_info=canonical_game_info,
                game=game,
            )
            summary["drive_game_archive_url"] = archive_url
            archive_status = load_archive_sync_status(game_dir)
            goal_review_url = str(archive_status.get("goal_review_folder_url") or "").strip()
            if goal_review_url:
                summary["drive_goal_review_url"] = goal_review_url

        clips = load_clip_manifest(game_dir) if (game_dir / "data" / "clips_manifest.json").exists() else []
        matched_count = 0
        for entry in clips:
            if not clip_matches_filters(entry, filters):
                continue
            clip_path = resolve_clip_path(game_dir, entry)
            if clip_path is None:
                continue

            record = {
                "index": len(selected_clip_entries) + 1,
                "game_date": game_date,
                "game_id": str(game.get("game_id") or ""),
                "game_dir": str(game_dir),
                "game_info": canonical_game_info,
                "game_label": game_label,
                "series_title": str(args.title or "").strip(),
                "series_context": series_context,
                "clip_path": str(clip_path),
                "event": annotate_clip_entry(
                    entry=entry,
                    score_lookup=score_lookup,
                    series_title=str(args.title or "").strip(),
                    game_label=game_label,
                ),
            }
            selected_clip_entries.append(record)
            selected_clip_paths.append(clip_path)
            matched_count += 1

        summary["matched_clip_count"] = matched_count
        summary["series_context"] = series_context
        source_summary.append(summary)

        result = game.get("result") if isinstance(game.get("result"), dict) else {}
        if "won" in result:
            if bool(result.get("won")):
                amherst_wins_before += 1
            else:
                opponent_wins_before += 1

    if args.dry_run:
        print(json.dumps({"filters": filters, "sources": source_summary, "clips": selected_clip_entries}, indent=2))
        return 0

    if not selected_clip_paths:
        raise RuntimeError("No clips matched the requested filters")

    output_path = args.output.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = write_filtered_manifest(
        output_path=output_path,
        execution_profile=args.profile,
        reel_mode=args.reel_mode,
        render_style=args.render_style,
        title=str(args.title or "").strip(),
        game_label_mode=args.game_label_mode,
        filters=filters,
        sources=source_summary,
        selected_clips=selected_clip_entries,
    )

    if args.render_style == "plain":
        if VideoProcessor is None:
            raise RuntimeError(
                "VideoProcessor is unavailable because moviepy is not installed. "
                "Use the amherst-display virtualenv or install the local video dependencies."
            )
        LOGGER.info("Building filtered reel with %s clips", len(selected_clip_paths))
        processor = VideoProcessor(selected_clip_paths[0], config)
        if not processor.create_highlights_reel(selected_clip_paths, output_path):
            raise RuntimeError(f"Failed to build filtered reel: {output_path}")
    else:
        cmd = [
            sys.executable,
            str(REPO_ROOT / "scripts" / "build_production_highlight_reel.py"),
            "--reel-manifest",
            str(manifest_path),
            "--output",
            str(output_path),
        ]
        if args.game_intro_cards:
            cmd.extend(["--game-intro-cards", "--game-intro-card-seconds", str(float(args.game_intro_card_seconds))])
        if args.series_outro_card:
            cmd.extend(["--series-outro-card", "--series-outro-card-seconds", str(float(args.series_outro_card_seconds))])
            for flag, value in (
                ("--series-outro-status", args.series_outro_status),
                ("--series-outro-game-label", args.series_outro_game_label),
                ("--series-outro-datetime-label", args.series_outro_datetime_label),
                ("--series-outro-venue", args.series_outro_venue),
                ("--series-outro-location", args.series_outro_location),
                ("--series-outro-home-team", args.series_outro_home_team),
                ("--series-outro-away-team", args.series_outro_away_team),
            ):
                if str(value or "").strip():
                    cmd.extend([flag, str(value)])
        LOGGER.info("Building production filtered reel with %s clips", len(selected_clip_paths))
        subprocess.run(cmd, check=True, cwd=REPO_ROOT)

    drive_url = ""
    if args.upload_drive:
        default_subfolder = args.upload_drive_subfolder.strip() or (str(args.title or "").strip() or output_path.stem)
        drive_url = upload_filtered_reel(
            output_path=output_path,
            manifest_path=manifest_path,
            section=args.upload_drive_section,
            subfolder=default_subfolder,
        )
    LOGGER.info("Filtered reel ready: %s", output_path)
    LOGGER.info("Filtered manifest: %s", manifest_path)
    if drive_url:
        LOGGER.info("Uploaded filtered reel to: %s", drive_url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
