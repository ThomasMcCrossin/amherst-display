#!/usr/bin/env python3
"""
Test script for Hockey Highlight Extractor
Run with: source venv/bin/activate && python test_highlight_extractor.py
"""

import sys
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

import config
from highlight_extractor import (
    HighlightPipeline,
    AmherstBoxScoreProvider,
)

def main():
    # Paths
    games_json = Path(__file__).parent / 'games' / 'amherst-ramblers.json'
    video_path = Path(__file__).parent / 'video_processing' / 'raw' / '2026-01-09 Amherst Ramblers vs Edmundston Blizzard Home 7.00pm.ts'

    # Verify files exist
    if not games_json.exists():
        print(f"ERROR: Games JSON not found: {games_json}")
        return 1

    if not video_path.exists():
        print(f"ERROR: Video file not found: {video_path}")
        return 1

    print("=" * 70)
    print("HIGHLIGHT EXTRACTOR TEST")
    print("=" * 70)
    print(f"Video: {video_path.name}")
    print(f"Games JSON: {games_json}")
    print()

    # Load game data from local JSON
    print("Loading game data from amherst-display...")
    provider = AmherstBoxScoreProvider(str(games_json))

    # Find the Jan 9 Edmundston game
    game = provider.find_game('2026-01-09', 'Edmundston')
    if not game:
        print("ERROR: Could not find Jan 9 Edmundston game in JSON")
        return 1

    print(f"Found game: {game.get('date')} vs {game.get('opponent', {}).get('team_name')}")
    print(f"Score: {game.get('result', {}).get('final_score')}")
    print(f"Goals in scoring data: {len(game.get('scoring', []))}")
    print()

    # Create pre-loaded fetcher that bypasses API calls
    fetcher = provider.create_fetcher(game)

    # Create and run pipeline
    print("Creating pipeline...")
    pipeline = HighlightPipeline(
        config=config,
        video_path=video_path,
        box_score_fetcher=fetcher
    )

    print("Executing pipeline...")
    print("(This may take several minutes for OCR processing)")
    print()

    result = pipeline.execute(
        sample_interval=5,         # Sample every 5 seconds for OCR
        tolerance_seconds=30,      # +/- 30 seconds for event matching
        before_seconds=15.0,       # Include 15 seconds before each goal
        after_seconds=4.0,         # Include 4 seconds after each goal
        parallel_ocr=True,         # Use parallel OCR processing
        ocr_workers=4,             # Number of OCR workers
        broadcast_type='flohockey' # Use FloHockey overlay detection
    )

    print()
    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"Success: {result.success}")
    print(f"Events found: {result.events_found}")
    print(f"Events matched: {result.events_matched}")
    print(f"Clips created: {result.clips_created}")
    print(f"Highlights path: {result.highlights_path}")
    print(f"Total duration: {result.total_duration_seconds:.1f} seconds")

    if result.errors:
        print(f"\nErrors:")
        for err in result.errors:
            print(f"  - {err}")

    if result.warnings:
        print(f"\nWarnings:")
        for warn in result.warnings:
            print(f"  - {warn}")

    return 0 if result.success else 1


if __name__ == '__main__':
    sys.exit(main())
