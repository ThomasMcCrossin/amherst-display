#!/usr/bin/env python3
"""
Test script to verify the major penalty time calculation fix.

This script simulates the time matching logic to verify the fix works correctly
for the Miramichi game penalty at P2 14:45.
"""

import json
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from highlight_extractor.time_utils import time_string_to_seconds


def test_time_calculation():
    """Test the time conversion fix for major penalties."""

    # Load video timestamps from the Miramichi game
    game_dir = Path(__file__).parent.parent / "Games/2026-01-10_Amherst Ramblers_vs_Miramichi Timberwolves"
    timestamps_file = game_dir / "data/video_timestamps.json"

    if not timestamps_file.exists():
        print(f"ERROR: Timestamps file not found: {timestamps_file}")
        return False

    with open(timestamps_file) as f:
        video_timestamps = json.load(f)

    # Penalty info: Period 2, time "14:45" (ELAPSED time from box score)
    penalty_period = 2
    penalty_time = "14:45"

    print("=" * 70)
    print("MAJOR PENALTY TIME CALCULATION TEST")
    print("=" * 70)
    print(f"\nPenalty: Period {penalty_period}, Time {penalty_time}")
    print(f"Box score time format: ELAPSED (14:45 into the period)")

    # OLD BEHAVIOR (buggy): Treats penalty time as remaining
    penalty_seconds_old = time_string_to_seconds(penalty_time)  # 885 seconds
    print(f"\n--- OLD (BUGGY) BEHAVIOR ---")
    print(f"penalty_seconds (treated as remaining): {penalty_seconds_old}s ({penalty_seconds_old//60}:{penalty_seconds_old%60:02d})")

    # Find best match with OLD logic
    period_timestamps = [
        ts for ts in video_timestamps
        if ts.get('period') == penalty_period and ts.get('video_time') is not None
    ]

    best_match_old = None
    best_diff_old = float('inf')
    for ts in period_timestamps:
        ts_seconds = ts.get('game_time_seconds', 0)
        diff = abs(ts_seconds - penalty_seconds_old)
        if diff < best_diff_old:
            best_diff_old = diff
            best_match_old = ts

    if best_match_old:
        print(f"Best match (wrong): video_time={best_match_old['video_time']}s, "
              f"game_time={best_match_old['game_time']} ({best_match_old['game_time_seconds']}s remaining)")
        print(f"Match difference: {best_diff_old}s")

        # Calculate video_time with old logic
        video_time_old = best_match_old['video_time']
        time_diff_old = penalty_seconds_old - time_string_to_seconds(best_match_old.get('game_time', '0:00'))
        calculated_video_time_old = max(0, video_time_old - time_diff_old)
        print(f"Calculated video_time (wrong): {calculated_video_time_old}s")

    # NEW BEHAVIOR (fixed): Convert elapsed to remaining
    print(f"\n--- NEW (FIXED) BEHAVIOR ---")
    penalty_elapsed_seconds = time_string_to_seconds(penalty_time)  # 885 seconds elapsed
    period_length = 1200  # 20 minutes
    penalty_remaining_seconds = period_length - penalty_elapsed_seconds  # 315 seconds remaining

    print(f"penalty_elapsed_seconds: {penalty_elapsed_seconds}s ({penalty_time})")
    print(f"period_length: {period_length}s")
    print(f"penalty_remaining_seconds: {penalty_remaining_seconds}s ({penalty_remaining_seconds//60}:{penalty_remaining_seconds%60:02d} on clock)")

    # Find best match with NEW logic
    best_match_new = None
    best_diff_new = float('inf')
    for ts in period_timestamps:
        ts_remaining_seconds = ts.get('game_time_seconds', 0)
        diff = abs(ts_remaining_seconds - penalty_remaining_seconds)
        if diff < best_diff_new:
            best_diff_new = diff
            best_match_new = ts

    if best_match_new:
        print(f"Best match (correct): video_time={best_match_new['video_time']}s, "
              f"game_time={best_match_new['game_time']} ({best_match_new['game_time_seconds']}s remaining)")
        print(f"Match difference: {best_diff_new}s")

        # Calculate video_time with new logic
        video_time_new = best_match_new['video_time']
        match_remaining_seconds = best_match_new.get('game_time_seconds', 0)
        time_diff_new = match_remaining_seconds - penalty_remaining_seconds
        calculated_video_time_new = max(0, video_time_new + time_diff_new)
        print(f"time_diff: {time_diff_new}s (match_remaining - penalty_remaining)")
        print(f"Calculated video_time (correct): {calculated_video_time_new}s")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    if best_match_old and best_match_new:
        print(f"OLD video_time: {calculated_video_time_old:.1f}s (WRONG - matched {best_match_old['game_time']} remaining)")
        print(f"NEW video_time: {calculated_video_time_new:.1f}s (CORRECT - matched {best_match_new['game_time']} remaining)")
        print(f"Difference: {calculated_video_time_new - calculated_video_time_old:.1f}s ({(calculated_video_time_new - calculated_video_time_old)/60:.1f} minutes)")

        # The clip should now start ~780s later in the video
        print(f"\nThe penalty at 14:45 elapsed = 5:15 remaining on the clock")
        print(f"Expected video_time: ~6400-6500s (when clock shows 5:15)")
        print(f"Old (wrong) result: ~{calculated_video_time_old:.0f}s (when clock shows ~{best_match_old['game_time']})")
        print(f"New (correct) result: ~{calculated_video_time_new:.0f}s (when clock shows ~{best_match_new['game_time']})")

        if abs(calculated_video_time_new - 6417) < 100:
            print("\n✓ FIX VERIFIED: New calculation matches expected video time!")
            return True
        else:
            print(f"\n✗ WARNING: Expected ~6417s, got {calculated_video_time_new:.0f}s")
            return False

    return False


if __name__ == '__main__':
    success = test_time_calculation()
    sys.exit(0 if success else 1)
