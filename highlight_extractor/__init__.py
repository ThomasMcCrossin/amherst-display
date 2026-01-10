"""
Hockey Highlight Extractor - Box Score Based Detection

A modular system for extracting hockey highlights using box score data
and OCR-based time matching.
"""

__version__ = "2.1.0"
__author__ = "Thomas McCrossin"

# Core processors
from .video_processor import VideoProcessor
from .box_score import BoxScoreFetcher
from .box_score_parser import BoxScoreParser
from .ocr_engine import OCREngine
from .event_matcher import EventMatcher
from .file_manager import FileManager

# Domain models
from .models import GameInfo, Event, VideoTimestamp, PipelineResult
from .goal import Goal, GoalType, GoalSummary

# Time utilities
from .time_utils import (
    GameTime,
    time_string_to_seconds,
    seconds_to_time_string,
    period_time_to_absolute_seconds,
    absolute_seconds_to_period_time,
    format_period,
    parse_period_string,
    PERIOD_LENGTH_MINUTES,
    PERIOD_LENGTH_SECONDS,
)

# Pipeline
from .pipeline import HighlightPipeline

# Amherst Display Integration
from .amherst_integration import (
    AmherstBoxScoreProvider,
    PreloadedBoxScoreFetcher,
    find_amherst_display_path,
)

__all__ = [
    # Core processors
    'VideoProcessor',
    'BoxScoreFetcher',
    'BoxScoreParser',
    'OCREngine',
    'EventMatcher',
    'FileManager',
    'HighlightPipeline',
    # Domain models
    'GameInfo',
    'Event',
    'VideoTimestamp',
    'PipelineResult',
    'Goal',
    'GoalType',
    'GoalSummary',
    # Time utilities
    'GameTime',
    'time_string_to_seconds',
    'seconds_to_time_string',
    'period_time_to_absolute_seconds',
    'absolute_seconds_to_period_time',
    'format_period',
    'parse_period_string',
    'PERIOD_LENGTH_MINUTES',
    'PERIOD_LENGTH_SECONDS',
    # Amherst Display Integration
    'AmherstBoxScoreProvider',
    'PreloadedBoxScoreFetcher',
    'find_amherst_display_path',
]
