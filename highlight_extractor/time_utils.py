"""
Time Utilities - Shared time conversion functions for hockey highlight extraction

This module provides centralized time handling for:
- Hockey clock time (MM:SS format, counting DOWN)
- Period-based time calculations
- Video timestamp conversions
"""

import re
from typing import Any, Optional, Tuple
from dataclasses import dataclass


# Hockey period constants
PERIOD_LENGTH_MINUTES = 20
PERIOD_LENGTH_SECONDS = PERIOD_LENGTH_MINUTES * 60
OT_LENGTH_MINUTES = 5
OT_LENGTH_SECONDS = OT_LENGTH_MINUTES * 60


@dataclass(frozen=True)
class GameClockRules:
    """Period-length rules for a specific game context."""
    playoff: bool = False
    regular_season_ot_minutes: int = OT_LENGTH_MINUTES
    playoff_first_ot_minutes: int = 10
    playoff_later_ot_minutes: int = 20

    def period_length_seconds(self, period: int) -> int:
        period_num = int(period or 1)
        if period_num <= 3:
            return PERIOD_LENGTH_SECONDS
        if not self.playoff:
            return int(self.regular_season_ot_minutes) * 60
        if period_num == 4:
            return int(self.playoff_first_ot_minutes) * 60
        return int(self.playoff_later_ot_minutes) * 60


DEFAULT_GAME_CLOCK_RULES = GameClockRules()


def _context_get(game_context: Any, key: str, default: Any = None) -> Any:
    if game_context is None:
        return default
    if isinstance(game_context, dict):
        return game_context.get(key, default)
    return getattr(game_context, key, default)


def game_clock_rules_from_context(game_context: Any = None) -> GameClockRules:
    """Build clock rules from a game metadata object or dict."""
    if isinstance(game_context, GameClockRules):
        return game_context

    playoff_raw = _context_get(game_context, "playoff", None)
    schedule_notes = str(_context_get(game_context, "schedule_notes", "") or "").strip().lower()
    game_number = _context_get(game_context, "game_number", None)

    playoff = False
    if isinstance(playoff_raw, bool):
        playoff = playoff_raw
    elif playoff_raw is not None:
        playoff = str(playoff_raw).strip().lower() in {"1", "true", "yes", "y", "on"}

    if not playoff:
        playoff = ("best of" in schedule_notes) or bool(game_number)

    def _coerce_minutes(value: Any, fallback: int) -> int:
        try:
            iv = int(value)
        except Exception:
            return fallback
        return iv if iv > 0 else fallback

    regular_season_ot_minutes = _coerce_minutes(
        _context_get(game_context, "regular_season_ot_minutes", None),
        OT_LENGTH_MINUTES,
    )
    playoff_first_ot_minutes = _coerce_minutes(
        _context_get(game_context, "playoff_first_ot_minutes", None),
        10,
    )
    playoff_later_ot_minutes = _coerce_minutes(
        _context_get(game_context, "playoff_later_ot_minutes", None),
        20,
    )

    return GameClockRules(
        playoff=bool(playoff),
        regular_season_ot_minutes=regular_season_ot_minutes,
        playoff_first_ot_minutes=playoff_first_ot_minutes,
        playoff_later_ot_minutes=playoff_later_ot_minutes,
    )


def period_length_seconds(period: int, clock_rules: Any = None) -> int:
    rules = game_clock_rules_from_context(clock_rules)
    return rules.period_length_seconds(period)


@dataclass(frozen=True)
class GameTime:
    """
    Represents a point in game time.

    Hockey clocks count DOWN from 20:00 to 0:00 each period.
    This class handles the conversion between:
    - Period + time remaining (what you see on the clock)
    - Absolute game time (seconds from start of game)
    """
    period: int
    time_remaining: str  # MM:SS format

    def __post_init__(self):
        """Validate the game time"""
        if int(self.period or 0) < 1:
            raise ValueError(f"Invalid period {self.period}, expected >= 1")

        if not re.match(r'^\d{1,2}:\d{2}$', self.time_remaining):
            raise ValueError(f"Invalid time format '{self.time_remaining}', expected MM:SS")

        minutes, seconds = parse_time_string(self.time_remaining)
        if minutes is None or not (0 <= minutes <= 20) or not (0 <= seconds <= 59):
            raise ValueError(f"Invalid time '{self.time_remaining}'")

    @property
    def time_remaining_seconds(self) -> int:
        """Get time remaining in period as seconds"""
        minutes, seconds = parse_time_string(self.time_remaining)
        return minutes * 60 + seconds

    @property
    def time_elapsed_in_period(self) -> int:
        """Get time elapsed in current period (seconds)"""
        return period_length_seconds(self.period) - self.time_remaining_seconds

    @property
    def absolute_seconds(self) -> int:
        """
        Get absolute game time in seconds from start.

        Example:
        - Period 1, 15:00 remaining = 5 minutes elapsed = 300 seconds
        - Period 2, 10:00 remaining = 20 + 10 minutes elapsed = 1800 seconds
        """
        return period_time_to_absolute_seconds(self.period, self.time_remaining_seconds)

    def __str__(self) -> str:
        return f"P{self.period} {self.time_remaining}"

    @classmethod
    def from_period_and_seconds(cls, period: int, seconds_remaining: int) -> 'GameTime':
        """Create GameTime from period and seconds remaining"""
        time_str = seconds_to_time_string(seconds_remaining)
        return cls(period=period, time_remaining=time_str)


def parse_time_string(time_str: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Parse MM:SS time string to minutes and seconds.

    Args:
        time_str: Time in MM:SS format (e.g., "15:23", "5:45")

    Returns:
        Tuple of (minutes, seconds) or (None, None) if parsing fails
    """
    try:
        parts = time_str.strip().split(':')
        if len(parts) == 2:
            minutes = int(parts[0])
            seconds = int(parts[1])
            return (minutes, seconds)
    except (ValueError, AttributeError):
        pass
    return (None, None)


def time_string_to_seconds(time_str: str) -> int:
    """
    Convert MM:SS time string to total seconds.

    Args:
        time_str: Time in MM:SS format

    Returns:
        Time in seconds, or 0 if parsing fails
    """
    minutes, seconds = parse_time_string(time_str)
    if minutes is not None and seconds is not None:
        return minutes * 60 + seconds
    return 0


def seconds_to_time_string(total_seconds: int) -> str:
    """
    Convert total seconds to MM:SS format.

    Args:
        total_seconds: Time in seconds

    Returns:
        Time string in MM:SS format
    """
    if total_seconds < 0:
        total_seconds = 0
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes}:{seconds:02d}"


def period_time_to_absolute_seconds(period: int, time_remaining_seconds: int, clock_rules: Any = None) -> int:
    """
    Convert period + time remaining to absolute game time.

    Hockey clocks count DOWN, so:
    - Period 1, 15:00 remaining = 5 minutes elapsed = 300 seconds from start
    - Period 2, 10:00 remaining = 20 min (P1) + 10 min elapsed = 30 min = 1800 seconds

    Args:
        period: Period number (1, 2, 3, 4=OT, 5=2OT)
        time_remaining_seconds: Time remaining in period (seconds)

    Returns:
        Absolute game time in seconds from start
    """
    # Calculate time from completed previous periods
    period_num = int(period or 1)
    previous_periods_time = 0
    for p in range(1, period_num):
        previous_periods_time += period_length_seconds(p, clock_rules)

    # Time elapsed in current period = period length - time remaining
    period_length = period_length_seconds(period_num, clock_rules)
    time_elapsed_in_period = period_length - time_remaining_seconds

    return previous_periods_time + time_elapsed_in_period


def absolute_seconds_to_period_time(absolute_seconds: int, clock_rules: Any = None) -> Tuple[int, int]:
    """
    Convert absolute game time to period + time remaining.

    Args:
        absolute_seconds: Absolute game time in seconds from start

    Returns:
        Tuple of (period, time_remaining_seconds)
    """
    if absolute_seconds < 0:
        return (1, PERIOD_LENGTH_SECONDS)

    remaining_absolute = int(absolute_seconds)
    period = 1
    while True:
        current_period_length = period_length_seconds(period, clock_rules)
        if remaining_absolute < current_period_length:
            elapsed_in_period = remaining_absolute
            break
        remaining_absolute -= current_period_length
        period += 1

    # Time remaining = period length - time elapsed
    current_period_length = period_length_seconds(period, clock_rules)
    time_remaining = current_period_length - elapsed_in_period

    return (period, max(0, time_remaining))


def format_period(period: int) -> str:
    """
    Format period number for display.

    Args:
        period: Period number

    Returns:
        Formatted string (e.g., "1st", "2nd", "OT")
    """
    if period == 1:
        return "1st"
    elif period == 2:
        return "2nd"
    elif period == 3:
        return "3rd"
    elif period == 4:
        return "OT"
    elif period == 5:
        return "2OT"
    else:
        return f"{period - 3}OT"


def parse_period_string(period_str: str) -> Optional[int]:
    """
    Parse period string to period number.

    Handles formats like: "1", "1st", "P1", "2nd", "OT", "2OT"

    Args:
        period_str: Period string

    Returns:
        Period number or None if parsing fails
    """
    period_str = period_str.strip().upper()

    # Direct number
    if period_str.isdigit():
        return int(period_str)

    # "1st", "2nd", "3rd" format
    match = re.match(r'^(\d+)(?:ST|ND|RD|TH)?$', period_str)
    if match:
        return int(match.group(1))

    # "P1", "P2" format
    match = re.match(r'^P(\d+)$', period_str)
    if match:
        return int(match.group(1))

    # Overtime formats
    if period_str in ('OT', 'OT1', '1OT'):
        return 4

    match = re.match(r'^(\d+)OT$', period_str)
    if match:
        return 3 + int(match.group(1))

    return None
