#!/usr/bin/env python3
"""
Compatibility wrapper for the generic filtered reel builder.

Builds one stitched reel containing Amherst goal clips across multiple games.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Build one stitched reel containing Amherst goals across multiple games.")
    parser.add_argument(
        "--source",
        action="append",
        required=True,
        metavar="DATE=VIDEO",
        help="Source in the form YYYY-MM-DD=/absolute/or/relative/path/to/video.mp4. Repeat in game order.",
    )
    parser.add_argument("--output", type=Path, required=True, help="Output MP4 for the combined series reel")
    parser.add_argument("--profile", default="auto", help="Highlight execution profile selection")
    parser.add_argument("--reel-mode", default="goals_only", help="Reel mode used while generating per-game clips")
    parser.add_argument("--sample-interval", type=int, default=None, help="Override OCR sampling interval in seconds")
    parser.add_argument("--tolerance-seconds", type=int, default=None, help="Override event-match tolerance in seconds")
    parser.add_argument("--before-seconds", type=float, default=None, help="Override clip pre-roll in seconds")
    parser.add_argument("--after-seconds", type=float, default=None, help="Override clip post-roll in seconds")
    parser.add_argument("--disable-auto-detect-start", action="store_true", help="Skip game-start auto detection")
    parser.add_argument("--disable-goal-clock-refinement", action="store_true", help="Skip the goal clock-stop refinement pass")
    parser.add_argument("--disable-local-ocr-refinement", action="store_true", help="Skip the generic local OCR refinement pass")
    parser.add_argument("--render-style", choices=("plain", "production"), default="production", help="Final series render style")
    parser.add_argument("--title", default="", help="Optional series title label for production overlays")
    parser.add_argument("--game-label-mode", choices=("none", "date", "series_game"), default="series_game", help="How to label each game in production overlays")
    parser.set_defaults(game_intro_cards=True)
    parser.add_argument("--game-intro-cards", dest="game_intro_cards", action="store_true", help="Insert full-screen intro cards before each game block")
    parser.add_argument("--no-game-intro-cards", dest="game_intro_cards", action="store_false", help="Disable inserted game intro cards")
    parser.add_argument("--game-intro-card-seconds", type=float, default=3.5, help="Duration of each inserted game intro card in seconds")
    parser.add_argument("--series-outro-card", action="store_true", help="Append a full-screen series outro card after the final clip")
    parser.add_argument("--series-outro-card-seconds", type=float, default=4.5, help="Duration of the inserted series outro card in seconds")
    parser.add_argument("--series-outro-status", default="", help="Primary status line for the series outro card")
    parser.add_argument("--series-outro-game-label", default="", help="Upcoming game label for the series outro card")
    parser.add_argument("--series-outro-datetime-label", default="", help="Date/time label for the series outro card")
    parser.add_argument("--series-outro-venue", default="", help="Venue name for the series outro card")
    parser.add_argument("--series-outro-location", default="", help="Location text for the series outro card")
    parser.add_argument("--series-outro-home-team", default="", help="Home team name shown on the series outro card")
    parser.add_argument("--series-outro-away-team", default="", help="Away team name shown on the series outro card")
    parser.add_argument("--upload-drive", action="store_true", help="Upload the final reel to Google Drive")
    parser.add_argument("--upload-drive-section", choices=("games", "series", "players", "special-projects"), default="series", help="Drive reel branch to publish into")
    parser.add_argument("--upload-drive-subfolder", default="", help="Drive subfolder name to create/use under the selected reel branch")
    parser.set_defaults(upload_game_archives_drive=None)
    parser.add_argument("--upload-game-archives-drive", dest="upload_game_archives_drive", action="store_true", help="Upload/update each source game's archive tree under the Drive games branch")
    parser.add_argument("--no-upload-game-archives-drive", dest="upload_game_archives_drive", action="store_false", help="Skip uploading each source game's archive tree under the Drive games branch")
    parser.add_argument("--dry-run", action="store_true", help="Resolve sources without processing video")
    parser.add_argument("--force-reprocess", action="store_true", help="Re-run highlight extraction even if clips already exist")
    args = parser.parse_args()

    cmd = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "build_filtered_reel.py"),
        "--profile",
        str(args.profile),
        "--reel-mode",
        str(args.reel_mode),
        *([] if args.sample_interval is None else ["--sample-interval", str(args.sample_interval)]),
        *([] if args.tolerance_seconds is None else ["--tolerance-seconds", str(args.tolerance_seconds)]),
        *([] if args.before_seconds is None else ["--before-seconds", str(args.before_seconds)]),
        *([] if args.after_seconds is None else ["--after-seconds", str(args.after_seconds)]),
        *(["--disable-auto-detect-start"] if args.disable_auto_detect_start else []),
        *(["--disable-goal-clock-refinement"] if args.disable_goal_clock_refinement else []),
        *(["--disable-local-ocr-refinement"] if args.disable_local_ocr_refinement else []),
        "--render-style",
        str(args.render_style),
        "--title",
        str(args.title),
        "--game-label-mode",
        str(args.game_label_mode),
        *(["--game-intro-cards", "--game-intro-card-seconds", str(float(args.game_intro_card_seconds))] if args.game_intro_cards else []),
        *(["--series-outro-card", "--series-outro-card-seconds", str(float(args.series_outro_card_seconds))] if args.series_outro_card else []),
        *(["--series-outro-status", str(args.series_outro_status)] if str(args.series_outro_status or "").strip() else []),
        *(["--series-outro-game-label", str(args.series_outro_game_label)] if str(args.series_outro_game_label or "").strip() else []),
        *(["--series-outro-datetime-label", str(args.series_outro_datetime_label)] if str(args.series_outro_datetime_label or "").strip() else []),
        *(["--series-outro-venue", str(args.series_outro_venue)] if str(args.series_outro_venue or "").strip() else []),
        *(["--series-outro-location", str(args.series_outro_location)] if str(args.series_outro_location or "").strip() else []),
        *(["--series-outro-home-team", str(args.series_outro_home_team)] if str(args.series_outro_home_team or "").strip() else []),
        *(["--series-outro-away-team", str(args.series_outro_away_team)] if str(args.series_outro_away_team or "").strip() else []),
        "--event-type",
        "goal",
        "--team",
        "ramblers",
        "--output",
        str(args.output),
    ]
    if args.upload_drive:
        cmd.extend(["--upload-drive", "--upload-drive-section", str(args.upload_drive_section)])
        if str(args.upload_drive_subfolder or "").strip():
            cmd.extend(["--upload-drive-subfolder", str(args.upload_drive_subfolder)])
    if args.upload_game_archives_drive is True:
        cmd.append("--upload-game-archives-drive")
    elif args.upload_game_archives_drive is False:
        cmd.append("--no-upload-game-archives-drive")
    for source in args.source:
        cmd.extend(["--source", source])
    if args.dry_run:
        cmd.append("--dry-run")
    if args.force_reprocess:
        cmd.append("--force-reprocess")

    result = subprocess.run(cmd)
    return int(result.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
