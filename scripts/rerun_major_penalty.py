#!/usr/bin/env python3
"""
Rerun the major penalty clip creation for a specific game.
Uses existing video_timestamps.json to avoid re-running OCR.
"""

import json
import logging
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from highlight_extractor.video_processor import VideoProcessor
from highlight_extractor.major_penalty_handler import process_major_penalties

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    # Game info
    game_dir = Path("/home/canteenhub/amherst-display-repo/Games/2026-01-10_Amherst Ramblers_vs_Miramichi Timberwolves")
    video_path = Path("/home/canteenhub/amherst-display-repo/temp/drive_ingest/incoming/2026-01-10 Amherst Ramblers vs Miramichi Timberwolves Home 6.00pm.ts")

    # Load existing timestamps
    timestamps_file = game_dir / "data/video_timestamps.json"
    with open(timestamps_file) as f:
        video_timestamps = json.load(f)
    logger.info(f"Loaded {len(video_timestamps)} timestamps from {timestamps_file}")

    # Load game metadata
    metadata_file = game_dir / "data/game_metadata.json"
    with open(metadata_file) as f:
        game_metadata = json.load(f)

    # Penalty data from box score (Austin Walker fighting major at P2 14:45)
    penalties_data = [
        {
            "period": 2,
            "time": "14:45",
            "player_name": "Austin Walker",
            "team": "opponent",
            "minutes": 5,
            "infraction": "Fighting",
            "is_major": True
        },
        {
            "period": 2,
            "time": "14:45",
            "player_name": "Unknown Rambler",
            "team": "ramblers",
            "minutes": 5,
            "infraction": "Fighting",
            "is_major": True
        }
    ]

    # Output directory for major review clips
    output_dir = game_dir / "output/major_review"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Delete old clip files
    for old_file in output_dir.glob("P2_14-45_*"):
        logger.info(f"Removing old file: {old_file}")
        old_file.unlink()

    # Create video processor
    logger.info(f"Loading video: {video_path}")
    video_processor = VideoProcessor(video_path, config)
    if not video_processor.load_video():
        logger.error("Failed to load video!")
        return

    # Process major penalties
    logger.info("Processing major penalties with fixed time calculation...")
    result = process_major_penalties(
        video_processor=video_processor,
        penalties_data=penalties_data,
        game_id="4820",
        game_date="2026-01-10",
        game_info={
            "game_id": "4820",
            "date": "2026-01-10",
            "opponent": {"team_name": "Miramichi Timberwolves"},
            "home_game": True
        },
        output_dir=output_dir,
        config=config,
        video_timestamps=video_timestamps
    )

    # Print results
    print("\n" + "=" * 70)
    print("MAJOR PENALTY PROCESSING RESULTS")
    print("=" * 70)
    print(f"Major penalties found: {result['major_count']}")
    print(f"Clips created: {result['clips_created']}")
    print(f"Drive folder: {result['drive_folder']}")
    print(f"Email sent: {result['email_sent']}")

    # Check the new clip JSON
    for json_file in output_dir.glob("*.json"):
        with open(json_file) as f:
            clip_data = json.load(f)
        print(f"\nClip: {json_file.name}")
        print(f"  clip_video_start: {clip_data.get('clip_video_start')}s")
        print(f"  Expected: ~6467s (6477 - 10s before)")

        expected = 6467  # 6477 - 10 seconds before
        actual = clip_data.get('clip_video_start', 0)
        if abs(actual - expected) < 50:
            print(f"  ✓ Video time is CORRECT (within 50s of expected)")
        else:
            print(f"  ✗ Video time may be wrong (expected ~{expected}, got {actual})")

    if video_processor.video_clip:
        video_processor.video_clip.close()


if __name__ == '__main__':
    main()
