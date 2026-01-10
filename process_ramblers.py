#!/usr/bin/env python3
"""
Process Ramblers Game - Extract highlights using pre-fetched box score data

This script integrates HockeyHighlightExtractor with the amherst-display project
to extract goal highlights from Amherst Ramblers game videos using pre-fetched
box score data instead of making API calls.

Usage:
    python process_ramblers.py <video_file>
    python process_ramblers.py --list-games
    python process_ramblers.py --game-id 4809 <video_file>
    python process_ramblers.py --date 2026-01-03 <video_file>

Examples:
    # Process a video, auto-detecting the game from filename
    python process_ramblers.py "2026-01-03 Amherst Ramblers vs Pictou County Home 7.00pm.ts"

    # Process with specific game ID
    python process_ramblers.py --game-id 4809 recording.mp4

    # List available games
    python process_ramblers.py --list-games
"""

import argparse
import logging
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

import config
from highlight_extractor import HighlightPipeline
from highlight_extractor.amherst_integration import (
    AmherstBoxScoreProvider,
    find_amherst_display_path
)
from highlight_extractor.file_manager import FileManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def find_games_json() -> Path:
    """Find the amherst-ramblers.json file"""
    # Try to find amherst-display project
    amherst_path = find_amherst_display_path()
    if amherst_path:
        games_file = amherst_path / 'games' / 'amherst-ramblers.json'
        if games_file.exists():
            return games_file

    # Try relative paths
    script_dir = Path(__file__).parent
    candidates = [
        script_dir.parent / 'amherst-display' / 'games' / 'amherst-ramblers.json',
        script_dir / 'amherst-ramblers.json',
        Path.cwd() / 'games' / 'amherst-ramblers.json',
    ]

    for path in candidates:
        if path.exists():
            return path

    raise FileNotFoundError(
        "Could not find amherst-ramblers.json. "
        "Please ensure the amherst-display project is available."
    )


def list_games(provider: AmherstBoxScoreProvider):
    """Display available games"""
    print("\nAvailable Ramblers Games:")
    print("=" * 70)

    for game in provider.list_games():
        home_away = "HOME" if game['home_game'] else "AWAY"
        result = "W" if game['won'] else "L" if game['won'] is False else "?"
        score = game['score'] or "N/A"
        goals = game['goal_count']

        print(
            f"  {game['game_id']:>5} | {game['date']} | {home_away:4} | "
            f"vs {game['opponent']:<25} | {score:>5} {result} | {goals} goals"
        )

    print("=" * 70)


def find_matching_game(
    provider: AmherstBoxScoreProvider,
    video_path: Path,
    game_id: str = None,
    game_date: str = None
) -> dict:
    """Find the game matching the video file"""

    # If game_id provided, use it directly
    if game_id:
        game = provider.find_game(game_date='', game_id=game_id)
        if game:
            logger.info(f"Found game by ID: {game_id}")
            return game
        raise ValueError(f"No game found with ID: {game_id}")

    # Try to parse video filename
    file_manager = FileManager(config)
    try:
        game_info = file_manager.parse_generic_hockey_filename(video_path.name)
        logger.info(f"Parsed from filename: {game_info['date']} - {game_info['home_team']} vs {game_info['away_team']}")

        # Find by teams and date
        game = provider.find_game_by_teams(
            game_info['home_team'],
            game_info['away_team'],
            game_date or game_info['date']
        )
        if game:
            logger.info(f"Found matching game: ID {game.get('game_id')}")
            return game

        # Fallback: find by date only
        game = provider.find_game(game_date=game_info['date'])
        if game:
            logger.info(f"Found game by date: ID {game.get('game_id')}")
            return game

    except Exception as e:
        logger.warning(f"Could not parse filename: {e}")

    # If date provided, search by date
    if game_date:
        game = provider.find_game(game_date=game_date)
        if game:
            return game

    raise ValueError(
        "Could not find matching game. Try:\n"
        "  - Using --game-id to specify the game ID\n"
        "  - Using --date to specify the game date\n"
        "  - Renaming the video file to match the format:\n"
        "    YYYY-MM-DD Team1 vs Team2 Home/Away HH.MMam.ext"
    )


def process_video(
    video_path: Path,
    provider: AmherstBoxScoreProvider,
    game: dict,
    dry_run: bool = False
):
    """Process a video file with pre-fetched box score data"""

    opponent = game.get('opponent', {}).get('team_name', 'Unknown')
    is_home = game.get('home_game', False)

    print("\n" + "=" * 70)
    print("RAMBLERS HIGHLIGHT EXTRACTOR")
    print("Using pre-fetched box score data from amherst-display")
    print("=" * 70)
    print(f"Video: {video_path.name}")
    print(f"Game ID: {game.get('game_id')}")
    print(f"Date: {game.get('date')}")
    print(f"Matchup: {'Amherst Ramblers' if is_home else opponent} vs {opponent if is_home else 'Amherst Ramblers'}")
    print(f"Goals in game: {len(game.get('scoring', []))}")
    print("=" * 70)

    if dry_run:
        print("\n[DRY RUN] Would process video with the following goals:")
        goals = provider.get_goals_for_game(game)
        for i, goal in enumerate(goals, 1):
            print(f"  {i}. P{goal.period} {goal.time} - {goal.scorer} ({goal.team})")
        return

    # Create the pre-loaded fetcher
    fetcher = provider.create_fetcher(game)

    # Create and run the pipeline with the pre-loaded fetcher
    pipeline = HighlightPipeline(
        config=config,
        video_path=video_path,
        box_score_fetcher=fetcher,
    )

    result = pipeline.execute(
        sample_interval=30,
        tolerance_seconds=30,
        before_seconds=8.0,
        after_seconds=6.0,
        parallel_ocr=True,
        ocr_workers=4,
    )

    # Print results
    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)

    if result.success:
        print(f"Status: SUCCESS")
        print(f"Events found: {result.events_found}")
        print(f"Events matched: {result.events_matched} ({result.match_rate():.1f}%)")
        print(f"Clips created: {result.clips_created}")
        if result.highlights_path:
            print(f"Highlights: {result.highlights_path}")
    else:
        print(f"Status: FAILED")
        print(f"Errors: {result.errors}")

    if result.warnings:
        print(f"Warnings: {result.warnings}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Extract highlights from Ramblers game videos using pre-fetched box score data"
    )
    parser.add_argument(
        'video',
        nargs='?',
        help='Path to the video file to process'
    )
    parser.add_argument(
        '--list-games', '-l',
        action='store_true',
        help='List available games from amherst-display'
    )
    parser.add_argument(
        '--game-id', '-g',
        help='Specific game ID to use'
    )
    parser.add_argument(
        '--date', '-d',
        help='Game date (YYYY-MM-DD) to match'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Show what would be done without processing'
    )
    parser.add_argument(
        '--games-json',
        help='Path to amherst-ramblers.json file'
    )

    args = parser.parse_args()

    # Find or use specified games JSON
    try:
        games_json = Path(args.games_json) if args.games_json else find_games_json()
        logger.info(f"Using games data from: {games_json}")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Load provider
    provider = AmherstBoxScoreProvider(str(games_json))

    # Handle list-games command
    if args.list_games:
        list_games(provider)
        return

    # Require video file for processing
    if not args.video:
        parser.print_help()
        print("\nError: video file required for processing")
        sys.exit(1)

    video_path = Path(args.video)
    if not video_path.exists():
        print(f"Error: Video file not found: {video_path}")
        sys.exit(1)

    # Find matching game
    try:
        game = find_matching_game(
            provider,
            video_path,
            game_id=args.game_id,
            game_date=args.date
        )
    except ValueError as e:
        print(f"Error: {e}")
        print("\nUse --list-games to see available games")
        sys.exit(1)

    # Process the video
    process_video(video_path, provider, game, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
