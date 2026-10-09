#!/usr/bin/env python3
"""
Run the highlight engine on one raw recording with every outbound side effect switched off.

Same pipeline call as the watch-rams post-game runner (scorebug profile detection, execution
profile, HighlightPipeline.execute), but for back-catalogue and test corpora:

  - game dirs go under --games-root instead of <repo>/Games
  - no Drive uploads (major-review clips stay local), no review email, no major-review monitor
    flag, no scoreboard-alert email, no game-archive sync
  - the game comes from --games-json (e.g. an older season's games/amherst-ramblers.json taken
    from git history) by --game-id

  .venv/bin/python scripts/run_engine_offline.py --video game.mp4 --game-id 4943 \
      --games-json old-season.json --games-root /data/rerun/Games [--engine-repo ~/amherst-display]

Writes <game-dir>/data/offline_run.json (status, profile, counts) and prints the game dir last.
Remove: delete this file; nothing imports it.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--video", required=True, type=Path)
    ap.add_argument("--game-id", required=True)
    ap.add_argument("--games-json", required=True, type=Path)
    ap.add_argument("--games-root", required=True, type=Path)
    ap.add_argument("--engine-repo", type=Path, default=Path(__file__).resolve().parents[1],
                    help="repo whose highlight_extractor/config to run (default: this checkout)")
    ap.add_argument("--profile", default="auto")
    ap.add_argument("--reel-mode", default="")
    args = ap.parse_args()

    os.environ["AUTO_UPLOAD_GAME_ARCHIVES_TO_DRIVE"] = "0"
    for key in ("RESEND_API_KEY", "NOTIFICATION_EMAIL", "NOTIFICATION_EMAIL_TO"):
        os.environ.pop(key, None)
    repo = args.engine_repo.resolve()
    sys.path.insert(0, str(repo))
    import config  # type: ignore
    from highlight_extractor import HighlightPipeline, event_matcher, major_penalty_handler  # type: ignore
    from highlight_extractor.amherst_integration import AmherstBoxScoreProvider  # type: ignore
    from highlight_extractor.file_manager import FileManager  # type: ignore

    # ---- no outbound side effects ----
    config.AUTO_UPLOAD_GAME_ARCHIVES_TO_DRIVE = False
    config.RESEND_API_KEY = ""
    config.NOTIFICATION_EMAIL_TO = ""
    major_penalty_handler.upload_to_drive = lambda *a, **k: None          # also skips email + monitor flag
    major_penalty_handler.send_review_notification = lambda *a, **k: False
    major_penalty_handler.enable_review_monitor = lambda *a, **k: False
    for cls in vars(event_matcher).values():
        if isinstance(cls, type) and hasattr(cls, "send_email_alert"):
            cls.send_email_alert = lambda self, *a, **k: False
    args.games_root.mkdir(parents=True, exist_ok=True)
    config.GAMES_DIR = args.games_root.resolve()

    provider = AmherstBoxScoreProvider(str(args.games_json))
    game = provider.find_game(game_date="", game_id=str(args.game_id))
    if not game:
        print(f"game {args.game_id} not in {args.games_json}", file=sys.stderr)
        return 2
    opponent = str(((game.get("opponent") or {}).get("team_name") or "Opponent")).strip()
    home = bool(game.get("home_game"))
    info = {"date": str(game.get("date")), "date_formatted": str(game.get("date")),
            "home_team": "Amherst Ramblers" if home else opponent, "away_team": opponent if home else "Amherst Ramblers",
            "league": "MHL", "filename": args.video.name, "home_away": "home" if home else "away", "time": "unknown",
            "overtime": bool((game.get("result") or {}).get("overtime")), "shootout": bool((game.get("result") or {}).get("shootout"))}
    source_info = dict(info, filename=args.video.name)

    folders = FileManager(config).create_game_folder_from_teams(
        date=info["date"], home_team=info["home_team"], away_team=info["away_team"], league="MHL",
        filename=args.video.name, home_away=info["home_away"], time_str="unknown")
    game_dir = Path(folders["game_dir"])

    profile_name, detection = args.profile, None
    if profile_name == "auto":
        try:
            from scorebug_detect import detect_scorebug_profile  # type: ignore
            prof, detection = detect_scorebug_profile(args.video)
            profile_name = prof.execution_profile_name if prof else "auto"
        except Exception as exc:  # noqa: BLE001
            detection = {"method": "error", "error": str(exc)}
    sel = config.resolve_highlight_execution_selection(
        profile_name, game_info=info, source_game_info=source_info,
        reel_mode=args.reel_mode or getattr(config, "DEFAULT_REEL_MODE", "goals_only"))
    t0 = time.time()
    pipeline = HighlightPipeline(config=config, video_path=args.video, box_score_fetcher=provider.create_fetcher(game),
                                 game_info_override=info, game_folders_override=folders, source_game_info_override=source_info)
    result = pipeline.execute(**dict(sel["execution_profile"]))
    status = {"game_id": str(args.game_id), "video": str(args.video), "engine_repo": str(repo), "game_dir": str(game_dir),
              "scorebug_detection": detection, "execution_profile": sel["execution_profile_name"],
              "success": bool(result.success), "paused_for_review": bool(getattr(result, "paused_for_review", False)),
              "events_found": result.events_found, "events_matched": result.events_matched, "clips_created": result.clips_created,
              "warnings": result.warnings, "errors": result.errors, "elapsed_seconds": round(time.time() - t0, 1)}
    (game_dir / "data").mkdir(parents=True, exist_ok=True)
    (game_dir / "data" / "offline_run.json").write_text(json.dumps(status, indent=2, default=str))
    print(game_dir)
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
