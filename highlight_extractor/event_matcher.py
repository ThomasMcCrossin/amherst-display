"""
Event Matcher - Syncs box score events with video timestamps using OCR data

This module matches events from box scores (goals, penalties) to their
corresponding timestamps in the video using OCR-extracted time data.
"""

import logging
from typing import List, Dict, Optional, Tuple, Union
import numpy as np

from .goal import Goal
from .time_utils import (
    time_string_to_seconds,
    period_time_to_absolute_seconds,
    seconds_to_time_string,
    PERIOD_LENGTH_MINUTES,
    PERIOD_LENGTH_SECONDS,
    OT_LENGTH_MINUTES,
    OT_LENGTH_SECONDS,
)

logger = logging.getLogger(__name__)


class EventMatcher:
    """Matches box score events to video timestamps"""

    # Hockey period lengths (using centralized constants)
    PERIOD_LENGTH = PERIOD_LENGTH_MINUTES
    OT_LENGTH = OT_LENGTH_MINUTES

    def __init__(self, config=None):
        """
        Initialize EventMatcher

        Args:
            config: Optional configuration object
        """
        self.config = config

    def match_events_to_video(
        self,
        events: List[Dict],
        video_timestamps: List[Dict],
        tolerance_seconds: int = 30
    ) -> List[Dict]:
        """
        Match box score events to video timestamps

        Args:
            events: List of event dicts from box score (with period, time)
            video_timestamps: List of sampled video timestamps (with video_time, period, game_time)
            tolerance_seconds: Maximum time difference for matching (seconds)

        Returns:
            List of events with video_time added
        """
        matched_events = []

        if not video_timestamps:
            logger.warning("No video timestamps available for matching")
            return events

        logger.info(f"Matching {len(events)} events to {len(video_timestamps)} video timestamps")

        for event in events:
            try:
                # Find closest video timestamp for this event with confidence
                match_result = self._find_closest_timestamp_with_confidence(
                    event,
                    video_timestamps,
                    tolerance_seconds
                )

                if match_result is not None:
                    video_time, confidence, time_diff = match_result

                    # Create new event dict with video_time and confidence
                    matched_event = event.copy()
                    matched_event['video_time'] = video_time
                    matched_event['match_confidence'] = confidence
                    matched_event['match_time_diff_seconds'] = time_diff
                    matched_events.append(matched_event)

                    logger.debug(
                        f"Matched {event['type']} at P{event['period']} {event['time']} "
                        f"to video time {video_time:.1f}s (confidence: {confidence:.2f}, diff: {time_diff:.1f}s)"
                    )
                else:
                    logger.warning(
                        f"Could not match {event['type']} at P{event['period']} {event['time']}"
                    )
                    # Still include event but without video_time
                    matched_events.append(event)

            except Exception as e:
                logger.error(f"Error matching event: {e}")
                matched_events.append(event)

        # Count successful matches
        successful = sum(1 for e in matched_events if e.get('video_time') is not None)
        logger.info(f"Successfully matched {successful}/{len(events)} events")

        return matched_events

    def _find_closest_timestamp(
        self,
        event: Dict,
        video_timestamps: List[Dict],
        tolerance_seconds: int
    ) -> Optional[float]:
        """
        Find the closest video timestamp for a box score event

        Args:
            event: Event dictionary with period and time
            video_timestamps: List of video timestamp dictionaries
            tolerance_seconds: Maximum allowed time difference

        Returns:
            Video time in seconds or None if no match found
        """
        event_period = event.get('period')
        event_time = event.get('time', '00:00')

        # Convert event time to seconds
        event_seconds = self._time_to_seconds(event_time)

        # Filter timestamps for matching period
        period_timestamps = [
            ts for ts in video_timestamps
            if ts.get('period') == event_period
        ]

        if not period_timestamps:
            # Try interpolation if we have timestamps before and after this period
            return self._interpolate_timestamp(event, video_timestamps)

        # Find timestamp with closest game time
        best_match = None
        best_diff = float('inf')

        for ts in period_timestamps:
            ts_seconds = ts.get('game_time_seconds', 0)

            # Calculate time difference
            # Note: Hockey clocks count DOWN, so we need to handle this
            time_diff = abs(event_seconds - ts_seconds)

            if time_diff < best_diff:
                best_diff = time_diff
                best_match = ts

        # Check if match is within tolerance
        if best_match and best_diff <= tolerance_seconds:
            return best_match['video_time']

        # If exact period match failed, try interpolation
        return self._interpolate_timestamp(event, video_timestamps)

    def _find_closest_timestamp_with_confidence(
        self,
        event: Dict,
        video_timestamps: List[Dict],
        tolerance_seconds: int
    ) -> Optional[Tuple[float, float, float]]:
        """
        Find the closest video timestamp for a box score event with confidence score

        Args:
            event: Event dictionary with period and time
            video_timestamps: List of video timestamp dictionaries
            tolerance_seconds: Maximum allowed time difference

        Returns:
            Tuple of (video_time, confidence, time_diff) or None if no match found
            confidence: 1.0 for exact match, decreases linearly to 0.0 at tolerance limit
        """
        event_period = event.get('period')
        event_time = event.get('time', '00:00')

        # Convert event time to seconds
        event_seconds = self._time_to_seconds(event_time)

        # Filter timestamps for matching period
        period_timestamps = [
            ts for ts in video_timestamps
            if ts.get('period') == event_period
        ]

        if not period_timestamps:
            # Try interpolation if we have timestamps before and after this period
            video_time = self._interpolate_timestamp(event, video_timestamps)
            if video_time is not None:
                # Lower confidence for interpolated matches
                return (video_time, 0.5, tolerance_seconds / 2)
            return None

        # Find timestamp with closest game time
        best_match = None
        best_diff = float('inf')

        for ts in period_timestamps:
            ts_seconds = ts.get('game_time_seconds', 0)

            # Calculate time difference
            # Note: Hockey clocks count DOWN, so we need to handle this
            time_diff = abs(event_seconds - ts_seconds)

            if time_diff < best_diff:
                best_diff = time_diff
                best_match = ts

        # Check if match is within tolerance
        if best_match and best_diff <= tolerance_seconds:
            video_time = best_match['video_time']

            # Calculate confidence: 1.0 for exact match, 0.0 at tolerance limit
            # Using linear decay for simplicity
            if best_diff == 0:
                confidence = 1.0
            else:
                confidence = max(0.0, 1.0 - (best_diff / tolerance_seconds))

            return (video_time, confidence, best_diff)

        # If exact period match failed, try interpolation
        video_time = self._interpolate_timestamp(event, video_timestamps)
        if video_time is not None:
            # Very low confidence for interpolated matches outside tolerance
            return (video_time, 0.3, tolerance_seconds)

        return None

    def _interpolate_timestamp(
        self,
        event: Dict,
        video_timestamps: List[Dict]
    ) -> Optional[float]:
        """
        Interpolate video timestamp when exact period match not found

        Args:
            event: Event dictionary
            video_timestamps: List of video timestamps

        Returns:
            Interpolated video time or None
        """
        try:
            event_period = event.get('period')
            event_time = event.get('time', '00:00')
            event_seconds = self._time_to_seconds(event_time)

            # Convert event to absolute game time (seconds from game start)
            event_game_seconds = self._event_to_absolute_time(event_period, event_seconds)

            # Find timestamps before and after the event
            before = None
            after = None

            for ts in video_timestamps:
                ts_game_seconds = self._event_to_absolute_time(
                    ts['period'],
                    ts['game_time_seconds']
                )

                if ts_game_seconds <= event_game_seconds:
                    if before is None or ts_game_seconds > before['abs_time']:
                        before = {
                            'video_time': ts['video_time'],
                            'abs_time': ts_game_seconds
                        }

                if ts_game_seconds >= event_game_seconds:
                    if after is None or ts_game_seconds < after['abs_time']:
                        after = {
                            'video_time': ts['video_time'],
                            'abs_time': ts_game_seconds
                        }

            # Interpolate between before and after
            if before and after:
                # Linear interpolation
                total_time_diff = after['abs_time'] - before['abs_time']
                event_offset = event_game_seconds - before['abs_time']

                if total_time_diff > 0:
                    ratio = event_offset / total_time_diff
                    video_time_diff = after['video_time'] - before['video_time']
                    interpolated_time = before['video_time'] + (ratio * video_time_diff)

                    logger.debug(
                        f"Interpolated P{event_period} {event_time} to {interpolated_time:.1f}s"
                    )
                    return interpolated_time

            # If only before or after exists, use that
            if before:
                logger.debug(f"Using nearest timestamp before event: {before['video_time']:.1f}s")
                return before['video_time']

            if after:
                logger.debug(f"Using nearest timestamp after event: {after['video_time']:.1f}s")
                return after['video_time']

            return None

        except Exception as e:
            logger.error(f"Interpolation failed: {e}")
            return None

    def _event_to_absolute_time(self, period: int, time_seconds: int) -> int:
        """
        Convert period + time to absolute game time (seconds from start)

        Args:
            period: Period number (1, 2, 3, 4=OT)
            time_seconds: Time remaining in period (seconds)

        Returns:
            Absolute game time in seconds
        """
        return period_time_to_absolute_seconds(period, time_seconds)

    def _time_to_seconds(self, time_str: str) -> int:
        """
        Convert MM:SS time string to seconds

        Args:
            time_str: Time in MM:SS format

        Returns:
            Time in seconds
        """
        return time_string_to_seconds(time_str)

    def match_goals_to_video(
        self,
        goals: List[Goal],
        video_timestamps: List[Dict],
        tolerance_seconds: int = 30
    ) -> List[Goal]:
        """
        Match Goal objects to video timestamps.

        This is the preferred method for type-safe goal matching.

        Args:
            goals: List of Goal objects
            video_timestamps: List of video timestamp dictionaries
            tolerance_seconds: Maximum time difference for matching

        Returns:
            List of Goal objects with video_time and match_confidence set
        """
        matched_goals = []

        if not video_timestamps:
            logger.warning("No video timestamps available for matching")
            return goals

        logger.info(f"Matching {len(goals)} goals to {len(video_timestamps)} video timestamps")

        for goal in goals:
            try:
                # Create event dict for matching using existing logic
                event_dict = {
                    'type': 'goal',
                    'period': goal.period,
                    'time': goal.time,
                    'team': goal.team,
                }

                # Find closest video timestamp
                match_result = self._find_closest_timestamp_with_confidence(
                    event_dict,
                    video_timestamps,
                    tolerance_seconds
                )

                if match_result is not None:
                    video_time, confidence, time_diff = match_result
                    matched_goal = goal.with_video_time(video_time, confidence)
                    matched_goals.append(matched_goal)

                    logger.debug(
                        f"Matched {goal} to video time {video_time:.1f}s "
                        f"(confidence: {confidence:.2f}, diff: {time_diff:.1f}s)"
                    )
                else:
                    logger.warning(f"Could not match goal: {goal}")
                    matched_goals.append(goal)

            except Exception as e:
                logger.error(f"Error matching goal: {e}")
                matched_goals.append(goal)

        successful = sum(1 for g in matched_goals if g.is_matched)
        logger.info(f"Successfully matched {successful}/{len(goals)} goals")

        return matched_goals

    def filter_events_by_type(
        self,
        events: List[Dict],
        event_types: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Filter events by type

        Args:
            events: List of events
            event_types: Types to include (None for all). E.g., ['goal']

        Returns:
            Filtered event list
        """
        if event_types is None:
            return events

        return [e for e in events if e.get('type') in event_types]

    def sort_events_by_video_time(self, events: List[Dict]) -> List[Dict]:
        """
        Sort events by video timestamp

        Args:
            events: List of events

        Returns:
            Sorted event list
        """
        # Only sort events that have video_time
        with_time = [e for e in events if e.get('video_time') is not None]
        without_time = [e for e in events if e.get('video_time') is None]

        # Sort those with time
        with_time.sort(key=lambda e: e['video_time'])

        # Return sorted + unsorted
        return with_time + without_time

    def estimate_missing_timestamps(
        self,
        video_timestamps: List[Dict],
        video_duration: float
    ) -> List[Dict]:
        """
        Fill in missing timestamps using linear interpolation

        This can help when OCR misses some frames

        Args:
            video_timestamps: Existing timestamps
            video_duration: Total video duration in seconds

        Returns:
            Enhanced timestamp list with interpolated values
        """
        if len(video_timestamps) < 2:
            return video_timestamps

        enhanced = video_timestamps.copy()

        # Sort by video time
        enhanced.sort(key=lambda t: t['video_time'])

        # Find gaps and interpolate
        i = 0
        while i < len(enhanced) - 1:
            current = enhanced[i]
            next_ts = enhanced[i + 1]

            video_gap = next_ts['video_time'] - current['video_time']

            # If gap is large (>60 seconds), interpolate
            if video_gap > 60:
                # Check if this might be a period break
                if current['period'] != next_ts['period']:
                    logger.debug(
                        f"Detected period break: P{current['period']} -> P{next_ts['period']}"
                    )
                    # Don't interpolate across period breaks
                    i += 1
                    continue

                # Interpolate timestamps in the gap
                num_interpolated = int(video_gap / 30)  # Every 30 seconds

                for j in range(1, num_interpolated + 1):
                    ratio = j / (num_interpolated + 1)

                    interp_video_time = current['video_time'] + (ratio * video_gap)

                    # Estimate game time (counting down)
                    game_time_diff = current['game_time_seconds'] - next_ts['game_time_seconds']
                    interp_game_time_sec = current['game_time_seconds'] - int(ratio * game_time_diff)

                    enhanced.append({
                        'video_time': interp_video_time,
                        'period': current['period'],
                        'game_time': f"{interp_game_time_sec//60}:{interp_game_time_sec%60:02d}",
                        'game_time_seconds': interp_game_time_sec,
                        'interpolated': True
                    })

            i += 1

        # Re-sort after adding interpolated timestamps
        enhanced.sort(key=lambda t: t['video_time'])

        logger.info(f"Enhanced timestamps: {len(video_timestamps)} -> {len(enhanced)}")

        return enhanced
