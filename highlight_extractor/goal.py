"""
Goal Model - Represents a goal scored in a hockey game

This module provides a dedicated Goal dataclass for box score goal data,
with proper typing, validation, and time conversion utilities.
"""

from dataclasses import dataclass, field
from typing import Optional, List
from enum import Enum
import re

from .time_utils import (
    GameTime,
    time_string_to_seconds,
    period_time_to_absolute_seconds,
    seconds_to_time_string,
    format_period,
)


class GoalType(Enum):
    """Type of goal scored"""
    EVEN_STRENGTH = "ES"  # Even strength (5v5, 4v4, 3v3)
    POWER_PLAY = "PP"     # Power play goal
    SHORT_HANDED = "SH"   # Short-handed goal
    EMPTY_NET = "EN"      # Empty net goal
    PENALTY_SHOT = "PS"   # Penalty shot goal
    OVERTIME = "OT"       # Overtime winner
    SHOOTOUT = "SO"       # Shootout goal

    @classmethod
    def from_string(cls, value: Optional[str]) -> Optional['GoalType']:
        """Parse goal type from string (e.g., 'PP', 'SH', 'EN')"""
        if not value:
            return None

        value = value.strip().upper()

        # Handle common variations
        mappings = {
            'PP': cls.POWER_PLAY,
            'PPG': cls.POWER_PLAY,
            'POWER PLAY': cls.POWER_PLAY,
            'SH': cls.SHORT_HANDED,
            'SHG': cls.SHORT_HANDED,
            'SHORT HANDED': cls.SHORT_HANDED,
            'EN': cls.EMPTY_NET,
            'ENG': cls.EMPTY_NET,
            'EMPTY NET': cls.EMPTY_NET,
            'PS': cls.PENALTY_SHOT,
            'OT': cls.OVERTIME,
            'OTW': cls.OVERTIME,
            'SO': cls.SHOOTOUT,
            'ES': cls.EVEN_STRENGTH,
            'EV': cls.EVEN_STRENGTH,
        }

        return mappings.get(value)


@dataclass
class Goal:
    """
    Represents a goal scored in a hockey game.

    This is the primary data structure for goal extraction from box scores.
    It includes all relevant information about a goal including timing,
    scorer, assists, and goal type (PP, SH, EN, etc.).

    Attributes:
        period: Period number (1-5, where 4=OT, 5=2OT)
        time: Time remaining in period when goal was scored (MM:SS format)
        team: Name of the scoring team
        scorer: Name of the goal scorer
        assist1: First assist (primary), if any
        assist2: Second assist (secondary), if any
        goal_type: Type of goal (PP, SH, EN, etc.)
        video_time: Video timestamp in seconds (set during matching)
        match_confidence: Confidence of video time match (0.0-1.0)
    """

    period: int
    time: str  # MM:SS format (time remaining in period)
    team: str
    scorer: str
    assist1: Optional[str] = None
    assist2: Optional[str] = None
    goal_type: Optional[GoalType] = None
    video_time: Optional[float] = None
    match_confidence: Optional[float] = None

    def __post_init__(self):
        """Validate goal data after initialization"""
        # Validate period
        if not 1 <= self.period <= 5:
            raise ValueError(f"Invalid period {self.period}, expected 1-5")

        # Validate time format
        if not re.match(r'^\d{1,2}:\d{2}$', self.time):
            raise ValueError(f"Invalid time format '{self.time}', expected MM:SS")

        # Validate time values
        parts = self.time.split(':')
        minutes, seconds = int(parts[0]), int(parts[1])

        if not (0 <= minutes <= 20):
            raise ValueError(f"Invalid minutes {minutes}, expected 0-20")

        if not (0 <= seconds <= 59):
            raise ValueError(f"Invalid seconds {seconds}, expected 0-59")

        # Validate team name
        if not self.team or not self.team.strip():
            raise ValueError("Team cannot be empty")

        # Validate scorer
        if not self.scorer or not self.scorer.strip():
            raise ValueError("Scorer cannot be empty")

        # Validate confidence if provided
        if self.match_confidence is not None:
            if not 0.0 <= self.match_confidence <= 1.0:
                raise ValueError(f"Invalid confidence {self.match_confidence}, expected 0.0-1.0")

    @property
    def time_seconds(self) -> int:
        """Get time remaining in period as seconds"""
        return time_string_to_seconds(self.time)

    @property
    def absolute_game_seconds(self) -> int:
        """Get absolute game time in seconds from start of game"""
        return period_time_to_absolute_seconds(self.period, self.time_seconds)

    @property
    def game_time(self) -> GameTime:
        """Get GameTime object for this goal"""
        return GameTime(period=self.period, time_remaining=self.time)

    @property
    def period_formatted(self) -> str:
        """Get formatted period string (e.g., '1st', 'OT')"""
        return format_period(self.period)

    @property
    def has_assists(self) -> bool:
        """Check if goal has any assists"""
        return bool(self.assist1 or self.assist2)

    @property
    def assist_count(self) -> int:
        """Count the number of assists"""
        count = 0
        if self.assist1:
            count += 1
        if self.assist2:
            count += 1
        return count

    @property
    def is_special_teams(self) -> bool:
        """Check if this is a special teams goal (PP or SH)"""
        return self.goal_type in (GoalType.POWER_PLAY, GoalType.SHORT_HANDED)

    @property
    def is_matched(self) -> bool:
        """Check if this goal has been matched to a video timestamp"""
        return self.video_time is not None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        result = {
            'type': 'goal',  # For compatibility with Event model
            'period': self.period,
            'time': self.time,
            'team': self.team,
            'scorer': self.scorer,
        }

        if self.assist1:
            result['assist1'] = self.assist1
        if self.assist2:
            result['assist2'] = self.assist2
        if self.goal_type:
            result['special'] = self.goal_type.value  # For compatibility
            result['goal_type'] = self.goal_type.value
        if self.video_time is not None:
            result['video_time'] = self.video_time
        if self.match_confidence is not None:
            result['match_confidence'] = self.match_confidence

        return result

    @classmethod
    def from_dict(cls, data: dict) -> 'Goal':
        """Create Goal from dictionary"""
        # Handle goal_type / special field
        goal_type = None
        if 'goal_type' in data:
            goal_type = GoalType.from_string(data['goal_type'])
        elif 'special' in data:
            goal_type = GoalType.from_string(data['special'])

        return cls(
            period=data['period'],
            time=data['time'],
            team=data['team'],
            scorer=data['scorer'],
            assist1=data.get('assist1'),
            assist2=data.get('assist2'),
            goal_type=goal_type,
            video_time=data.get('video_time'),
            match_confidence=data.get('match_confidence'),
        )

    def with_video_time(self, video_time: float, confidence: float = 1.0) -> 'Goal':
        """Create a new Goal with video time set"""
        return Goal(
            period=self.period,
            time=self.time,
            team=self.team,
            scorer=self.scorer,
            assist1=self.assist1,
            assist2=self.assist2,
            goal_type=self.goal_type,
            video_time=video_time,
            match_confidence=confidence,
        )

    def __str__(self) -> str:
        """Human-readable string representation"""
        goal_str = f"P{self.period} {self.time} - {self.scorer} ({self.team})"

        if self.goal_type:
            goal_str += f" [{self.goal_type.value}]"

        if self.has_assists:
            assists = []
            if self.assist1:
                assists.append(self.assist1)
            if self.assist2:
                assists.append(self.assist2)
            goal_str += f" from {', '.join(assists)}"

        return goal_str


@dataclass
class GoalSummary:
    """
    Summary of goals in a game.

    Provides aggregate statistics and lists of goals by team.
    """
    home_team: str
    away_team: str
    goals: List[Goal] = field(default_factory=list)

    @property
    def home_goals(self) -> List[Goal]:
        """Get goals scored by home team"""
        return [g for g in self.goals if g.team == self.home_team]

    @property
    def away_goals(self) -> List[Goal]:
        """Get goals scored by away team"""
        return [g for g in self.goals if g.team == self.away_team]

    @property
    def home_score(self) -> int:
        """Get home team score"""
        return len(self.home_goals)

    @property
    def away_score(self) -> int:
        """Get away team score"""
        return len(self.away_goals)

    @property
    def total_goals(self) -> int:
        """Get total goals in game"""
        return len(self.goals)

    @property
    def power_play_goals(self) -> List[Goal]:
        """Get all power play goals"""
        return [g for g in self.goals if g.goal_type == GoalType.POWER_PLAY]

    @property
    def short_handed_goals(self) -> List[Goal]:
        """Get all short-handed goals"""
        return [g for g in self.goals if g.goal_type == GoalType.SHORT_HANDED]

    @property
    def empty_net_goals(self) -> List[Goal]:
        """Get all empty net goals"""
        return [g for g in self.goals if g.goal_type == GoalType.EMPTY_NET]

    def goals_in_period(self, period: int) -> List[Goal]:
        """Get goals scored in a specific period"""
        return [g for g in self.goals if g.period == period]

    def goals_by_scorer(self, scorer: str) -> List[Goal]:
        """Get goals scored by a specific player"""
        return [g for g in self.goals if g.scorer.lower() == scorer.lower()]

    def score_at_time(self, period: int, time_remaining_seconds: int) -> tuple:
        """
        Get the score at a specific point in the game.

        Args:
            period: Period number
            time_remaining_seconds: Time remaining in period

        Returns:
            Tuple of (home_score, away_score) at that point
        """
        target_absolute = period_time_to_absolute_seconds(period, time_remaining_seconds)

        home = 0
        away = 0

        for goal in self.goals:
            goal_absolute = goal.absolute_game_seconds
            # Goal counts if it was scored before or at the target time
            if goal_absolute <= target_absolute:
                if goal.team == self.home_team:
                    home += 1
                elif goal.team == self.away_team:
                    away += 1

        return (home, away)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'home_team': self.home_team,
            'away_team': self.away_team,
            'home_score': self.home_score,
            'away_score': self.away_score,
            'goals': [g.to_dict() for g in self.goals],
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'GoalSummary':
        """Create GoalSummary from dictionary"""
        goals = [Goal.from_dict(g) for g in data.get('goals', [])]
        return cls(
            home_team=data['home_team'],
            away_team=data['away_team'],
            goals=goals,
        )
