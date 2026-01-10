"""
Domain Models - Type-safe data structures with validation

These models provide type safety, validation, and cleaner APIs for the
hockey highlight extraction pipeline.
"""

from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime
import re


@dataclass
class GameInfo:
    """Information about a hockey game parsed from filename or metadata"""

    date: str  # YYYY-MM-DD format
    home_team: str
    away_team: str
    league: str
    filename: str
    home_away: str = 'unknown'  # 'home' | 'away' | 'unknown'
    time: str = 'unknown'
    date_formatted: Optional[str] = None

    def __post_init__(self):
        """Validate game info after initialization"""
        # Validate date format
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', self.date):
            raise ValueError(f"Invalid date format '{self.date}', expected YYYY-MM-DD")

        # Parse date to ensure it's valid
        try:
            datetime.strptime(self.date, '%Y-%m-%d')
        except ValueError as e:
            raise ValueError(f"Invalid date '{self.date}': {e}")

        # Validate league
        if self.league not in ['MHL', 'BSHL', 'Unknown']:
            raise ValueError(f"Invalid league '{self.league}', expected MHL, BSHL, or Unknown")

        # Validate home_away
        if self.home_away not in ['home', 'away', 'unknown']:
            raise ValueError(f"Invalid perspective '{self.home_away}', expected home, away, or unknown")

        # Validate team names are not empty
        if not self.home_team or not self.home_team.strip():
            raise ValueError("Home team cannot be empty")

        if not self.away_team or not self.away_team.strip():
            raise ValueError("Away team cannot be empty")

        # Auto-generate date_formatted if not provided
        if not self.date_formatted:
            try:
                date_obj = datetime.strptime(self.date, '%Y-%m-%d')
                self.date_formatted = date_obj.strftime('%B %d, %Y')
            except Exception:
                self.date_formatted = self.date


@dataclass
class Event:
    """A hockey event (goal, penalty, etc.) from box score"""

    type: str  # 'goal' | 'penalty'
    period: int
    time: str  # MM:SS format
    team: str
    video_time: Optional[float] = None
    match_confidence: Optional[float] = None  # 0.0 to 1.0

    # Goal-specific fields
    scorer: Optional[str] = None
    assist1: Optional[str] = None
    assist2: Optional[str] = None
    special: Optional[str] = None  # PP, SH, EN, etc.

    # Penalty-specific fields
    player: Optional[str] = None
    infraction: Optional[str] = None
    minutes: Optional[int] = None

    def __post_init__(self):
        """Validate event data after initialization"""
        # Validate event type
        if self.type not in ['goal', 'penalty']:
            raise ValueError(f"Invalid event type '{self.type}', expected 'goal' or 'penalty'")

        # Validate period
        if not 1 <= self.period <= 5:  # Regular + OT + 2OT
            raise ValueError(f"Invalid period {self.period}, expected 1-5")

        # Validate time format
        if not re.match(r'^\d{1,2}:\d{2}$', self.time):
            raise ValueError(f"Invalid time format '{self.time}', expected MM:SS")

        # Parse time to validate values
        parts = self.time.split(':')
        minutes, seconds = int(parts[0]), int(parts[1])

        if not (0 <= minutes <= 20):
            raise ValueError(f"Invalid minutes {minutes}, expected 0-20")

        if not (0 <= seconds <= 59):
            raise ValueError(f"Invalid seconds {seconds}, expected 0-59")

        # Validate team name
        if not self.team or not self.team.strip():
            raise ValueError("Team cannot be empty")

        # Validate confidence if provided
        if self.match_confidence is not None:
            if not 0.0 <= self.match_confidence <= 1.0:
                raise ValueError(f"Invalid confidence {self.match_confidence}, expected 0.0-1.0")

        # Type-specific validation
        if self.type == 'goal':
            if not self.scorer:
                raise ValueError("Goal event must have a scorer")

        if self.type == 'penalty':
            if not self.player:
                raise ValueError("Penalty event must have a player")
            if self.minutes is not None and self.minutes < 0:
                raise ValueError(f"Invalid penalty minutes {self.minutes}")

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        result = {
            'type': self.type,
            'period': self.period,
            'time': self.time,
            'team': self.team,
        }

        # Add optional fields if present
        if self.video_time is not None:
            result['video_time'] = self.video_time

        if self.match_confidence is not None:
            result['match_confidence'] = self.match_confidence

        # Goal fields
        if self.scorer:
            result['scorer'] = self.scorer
        if self.assist1:
            result['assist1'] = self.assist1
        if self.assist2:
            result['assist2'] = self.assist2
        if self.special:
            result['special'] = self.special

        # Penalty fields
        if self.player:
            result['player'] = self.player
        if self.infraction:
            result['infraction'] = self.infraction
        if self.minutes is not None:
            result['minutes'] = self.minutes

        return result

    @classmethod
    def from_dict(cls, data: dict) -> 'Event':
        """Create Event from dictionary"""
        return cls(
            type=data['type'],
            period=data['period'],
            time=data['time'],
            team=data['team'],
            video_time=data.get('video_time'),
            match_confidence=data.get('match_confidence'),
            scorer=data.get('scorer'),
            assist1=data.get('assist1'),
            assist2=data.get('assist2'),
            special=data.get('special'),
            player=data.get('player'),
            infraction=data.get('infraction'),
            minutes=data.get('minutes')
        )


@dataclass
class VideoTimestamp:
    """A timestamp extracted from video via OCR"""

    video_time: float  # Seconds from video start
    period: int
    game_time: str  # MM:SS format
    game_time_seconds: int
    interpolated: bool = False
    confidence: Optional[float] = None  # OCR confidence if available

    def __post_init__(self):
        """Validate timestamp data"""
        # Validate video_time
        if self.video_time < 0:
            raise ValueError(f"Invalid video_time {self.video_time}, must be >= 0")

        # Validate period
        if not 1 <= self.period <= 5:
            raise ValueError(f"Invalid period {self.period}, expected 1-5")

        # Validate game_time format
        if not re.match(r'^\d{1,2}:\d{2}$', self.game_time):
            raise ValueError(f"Invalid game_time format '{self.game_time}', expected MM:SS")

        # Validate game_time_seconds matches parsed time
        parts = self.game_time.split(':')
        minutes, seconds = int(parts[0]), int(parts[1])
        expected_seconds = minutes * 60 + seconds

        if self.game_time_seconds != expected_seconds:
            raise ValueError(
                f"Inconsistent game_time_seconds: got {self.game_time_seconds}, "
                f"expected {expected_seconds} from '{self.game_time}'"
            )

        # Validate confidence if provided
        if self.confidence is not None:
            if not 0.0 <= self.confidence <= 1.0:
                raise ValueError(f"Invalid confidence {self.confidence}, expected 0.0-1.0")

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        result = {
            'video_time': self.video_time,
            'period': self.period,
            'game_time': self.game_time,
            'game_time_seconds': self.game_time_seconds,
        }

        if self.interpolated:
            result['interpolated'] = True

        if self.confidence is not None:
            result['confidence'] = self.confidence

        return result

    @classmethod
    def from_dict(cls, data: dict) -> 'VideoTimestamp':
        """Create VideoTimestamp from dictionary"""
        return cls(
            video_time=data['video_time'],
            period=data['period'],
            game_time=data['game_time'],
            game_time_seconds=data['game_time_seconds'],
            interpolated=data.get('interpolated', False),
            confidence=data.get('confidence')
        )


@dataclass
class PipelineResult:
    """Result from running the highlight extraction pipeline"""

    success: bool
    game_info: GameInfo
    events_found: int
    events_matched: int
    clips_created: int
    highlights_path: Optional[str] = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    # Performance metrics
    ocr_duration_seconds: Optional[float] = None
    matching_duration_seconds: Optional[float] = None
    rendering_duration_seconds: Optional[float] = None
    total_duration_seconds: Optional[float] = None

    def __post_init__(self):
        """Validate result data"""
        if self.events_found < 0:
            raise ValueError(f"Invalid events_found {self.events_found}, must be >= 0")

        if self.events_matched < 0:
            raise ValueError(f"Invalid events_matched {self.events_matched}, must be >= 0")

        if self.events_matched > self.events_found:
            raise ValueError(
                f"events_matched ({self.events_matched}) cannot exceed "
                f"events_found ({self.events_found})"
            )

        if self.clips_created < 0:
            raise ValueError(f"Invalid clips_created {self.clips_created}, must be >= 0")

    def match_rate(self) -> float:
        """Calculate the percentage of events successfully matched"""
        if self.events_found == 0:
            return 0.0
        return (self.events_matched / self.events_found) * 100

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'success': self.success,
            'game_info': self.game_info.__dict__,
            'events_found': self.events_found,
            'events_matched': self.events_matched,
            'clips_created': self.clips_created,
            'match_rate_percent': round(self.match_rate(), 1),
            'highlights_path': self.highlights_path,
            'errors': self.errors,
            'warnings': self.warnings,
            'performance': {
                'ocr_duration_seconds': self.ocr_duration_seconds,
                'matching_duration_seconds': self.matching_duration_seconds,
                'rendering_duration_seconds': self.rendering_duration_seconds,
                'total_duration_seconds': self.total_duration_seconds,
            }
        }
