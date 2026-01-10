"""
Pipeline Orchestrator - Coordinates the 7-step highlight extraction pipeline

This module encapsulates the complete processing pipeline, making it
testable, reusable, and easier to maintain.
"""

import logging
import time
import json
from pathlib import Path
from typing import Optional, Dict, List

from .models import GameInfo, Event, VideoTimestamp, PipelineResult
from .goal import Goal, GoalSummary
from .file_manager import FileManager
from .box_score import BoxScoreFetcher
from .video_processor import VideoProcessor
from .ocr_engine import OCREngine
from .event_matcher import EventMatcher

logger = logging.getLogger(__name__)


class HighlightPipeline:
    """
    Orchestrates the complete highlight extraction pipeline

    Usage:
        pipeline = HighlightPipeline(config, video_path)
        result = pipeline.execute()

        if result.success:
            print(f"Created {result.clips_created} clips")
    """

    def __init__(
        self,
        config,
        video_path: Path,
        file_manager: Optional[FileManager] = None,
        box_score_fetcher: Optional[BoxScoreFetcher] = None,
        video_processor: Optional[VideoProcessor] = None,
        ocr_engine: Optional[OCREngine] = None,
        event_matcher: Optional[EventMatcher] = None
    ):
        """
        Initialize the pipeline

        Args:
            config: Configuration module
            video_path: Path to video file to process
            file_manager: Optional FileManager instance (for testing)
            box_score_fetcher: Optional BoxScoreFetcher instance (for testing)
            video_processor: Optional VideoProcessor instance (for testing)
            ocr_engine: Optional OCREngine instance (for testing)
            event_matcher: Optional EventMatcher instance (for testing)
        """
        self.config = config
        self.video_path = Path(video_path)

        # Dependency injection (allows testing with mocks)
        self.file_manager = file_manager or FileManager(config)
        self.box_score_fetcher = box_score_fetcher or BoxScoreFetcher()
        self.video_processor = video_processor or VideoProcessor(self.video_path, config)
        self.ocr_engine = ocr_engine or OCREngine(config)
        self.event_matcher = event_matcher or EventMatcher(config)

        # State
        self.game_info: Optional[GameInfo] = None
        self.game_folders: Optional[Dict[str, Path]] = None
        self.box_score: Optional[Dict] = None
        self.events: List[Dict] = []
        self.video_timestamps: List[Dict] = []
        self.matched_events: List[Dict] = []
        self.created_clips: List = []

        # Typed goal data (new in v2.1)
        self._goals: List[Goal] = []
        self._matched_goals: List[Goal] = []
        self._goal_summary: Optional[GoalSummary] = None

        # Performance tracking
        self._step_timings: Dict[str, float] = {}
        self._pipeline_start_time: Optional[float] = None

    @property
    def goals(self) -> List[Goal]:
        """Get typed Goal objects from box score"""
        return self._goals

    @property
    def matched_goals(self) -> List[Goal]:
        """Get matched Goal objects with video timestamps"""
        return self._matched_goals

    @property
    def goal_summary(self) -> Optional[GoalSummary]:
        """Get GoalSummary with team context"""
        return self._goal_summary

    def execute(
        self,
        sample_interval: int = 30,
        tolerance_seconds: int = 30,
        before_seconds: float = 8.0,
        after_seconds: float = 6.0,
        max_clips: Optional[int] = None,
        parallel_ocr: bool = True,
        ocr_workers: int = 4
    ) -> PipelineResult:
        """
        Execute the complete 7-step pipeline

        Args:
            sample_interval: OCR sampling interval in seconds
            tolerance_seconds: Max time difference for event matching
            before_seconds: Seconds to include before each event
            after_seconds: Seconds to include after each event
            max_clips: Maximum clips in highlights reel (None for all)
            parallel_ocr: Use parallel OCR processing
            ocr_workers: Number of worker threads for parallel OCR

        Returns:
            PipelineResult with success status and metrics
        """
        self._pipeline_start_time = time.time()
        errors = []
        warnings = []

        try:
            logger.info("=" * 70)
            logger.info("HOCKEY HIGHLIGHT EXTRACTOR v2.0")
            logger.info("Box-Score-Based Detection")
            logger.info("=" * 70)
            logger.info(f"Processing: {self.video_path.name}")

            # STEP 1: Parse filename and create folders
            try:
                self._step1_parse_and_setup()
            except Exception as e:
                error_msg = f"Step 1 failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                return self._create_result(False, errors, warnings)

            # STEP 2: Fetch box score
            try:
                self._step2_fetch_box_score()
            except Exception as e:
                error_msg = f"Step 2 failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                return self._create_result(False, errors, warnings)

            # STEP 3: Load video
            try:
                self._step3_load_video()
            except Exception as e:
                error_msg = f"Step 3 failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                return self._create_result(False, errors, warnings)

            # STEP 4: Extract timestamps via OCR
            try:
                self._step4_extract_timestamps(
                    sample_interval=sample_interval,
                    parallel=parallel_ocr,
                    workers=ocr_workers
                )
            except Exception as e:
                error_msg = f"Step 4 failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                return self._create_result(False, errors, warnings)

            # STEP 5: Match events to video
            try:
                self._step5_match_events(tolerance_seconds=tolerance_seconds)
            except Exception as e:
                error_msg = f"Step 5 failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                return self._create_result(False, errors, warnings)

            # STEP 6: Create individual clips
            try:
                self._step6_create_clips(
                    before_seconds=before_seconds,
                    after_seconds=after_seconds
                )
            except Exception as e:
                error_msg = f"Step 6 failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                # Non-fatal - we can still try to create highlights reel
                warnings.append(error_msg)

            # STEP 7: Create highlights reel
            highlights_path = None
            try:
                highlights_path = self._step7_create_highlights_reel(max_clips=max_clips)
            except Exception as e:
                error_msg = f"Step 7 failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                warnings.append(error_msg)

            # Generate summary
            self._log_summary(highlights_path)

            # Success if we got through all steps (even with warnings)
            success = len(errors) == 0 or (len(self.created_clips) > 0)

            return self._create_result(success, errors, warnings, highlights_path)

        except Exception as e:
            error_msg = f"Pipeline failed: {e}"
            logger.error(error_msg)
            errors.append(error_msg)
            return self._create_result(False, errors, warnings)

        finally:
            # Always cleanup
            self._cleanup()

    def _step1_parse_and_setup(self):
        """Step 1: Parse filename and create folder structure"""
        start_time = time.time()

        logger.info("\n" + "=" * 70)
        logger.info("STEP 1: PARSING GAME INFORMATION")
        logger.info("=" * 70)

        # Parse filename
        game_info_dict = self.file_manager.parse_mhl_filename(self.video_path.name)
        if not game_info_dict:
            logger.warning("MHL format not detected, using generic parser")
            game_info_dict = self.file_manager.parse_generic_hockey_filename(self.video_path.name)

        # Create validated GameInfo model
        self.game_info = GameInfo(**game_info_dict)

        logger.info(f"📅 Date: {self.game_info.date}")
        logger.info(f"🏒 League: {self.game_info.league}")
        logger.info(f"🏠 Home: {self.game_info.home_team}")
        logger.info(f"✈️  Away: {self.game_info.away_team}")
        logger.info(f"🎯 Perspective: {self.game_info.home_away.title()}")

        # Create folder structure
        self.game_folders = self.file_manager.create_game_folder(game_info_dict)
        logger.info(f"\n📁 Output folder: {self.game_folders['game_dir']}")

        self._step_timings['parse_and_setup'] = time.time() - start_time

    def _step2_fetch_box_score(self):
        """Step 2: Fetch box score from API"""
        start_time = time.time()

        logger.info("\n" + "=" * 70)
        logger.info("STEP 2: FETCHING BOX SCORE")
        logger.info("=" * 70)

        # Initialize fetcher with cache dir (only if not already provided for testing)
        if not hasattr(self.box_score_fetcher, 'find_game'):
            self.box_score_fetcher = BoxScoreFetcher(cache_dir=self.game_folders['data_dir'])

        # Find game ID
        game_id = self.box_score_fetcher.find_game(
            self.game_info.league,
            self.game_info.home_team,
            self.game_info.away_team,
            self.game_info.date
        )

        if not game_id:
            raise ValueError(
                "Could not find game in league database. "
                "This could mean: (1) Game hasn't been played yet, "
                "(2) Team names don't match league records, "
                "(3) League API is unavailable"
            )

        # Fetch box score
        self.box_score = self.box_score_fetcher.fetch_box_score(
            self.game_info.league,
            game_id
        )

        if not self.box_score:
            raise ValueError("Failed to fetch box score from API")

        # Extract events (dictionary format for backward compatibility)
        self.events = self.box_score_fetcher.extract_events(self.box_score)

        # Also extract typed Goal objects (new in v2.1)
        self._goals = self.box_score_fetcher.get_goals(self.box_score)
        self._goal_summary = self.box_score_fetcher.get_goal_summary(
            self.box_score,
            self.game_info.home_team,
            self.game_info.away_team
        )

        if not self.events:
            logger.warning("⚠️  No events found in box score")
            logger.info("   This might be a scoreless game or data issue")

        logger.info(f"✅ Found {len(self.events)} events ({len(self._goals)} goals)")
        for event in self.events[:5]:  # Show first 5
            logger.info(f"   - P{event['period']} {event['time']}: {event['type'].upper()}")
        if len(self.events) > 5:
            logger.info(f"   ... and {len(self.events) - 5} more")

        # Save game metadata
        self.file_manager.save_game_metadata(
            self.game_folders,
            self.game_info.__dict__,
            self.box_score
        )

        self._step_timings['fetch_box_score'] = time.time() - start_time

    def _step3_load_video(self):
        """Step 3: Load video file"""
        start_time = time.time()

        logger.info("\n" + "=" * 70)
        logger.info("STEP 3: LOADING VIDEO")
        logger.info("=" * 70)

        if not self.video_processor.load_video():
            raise ValueError("Failed to load video file")

        logger.info(
            f"✅ Video loaded: {self.video_processor.duration:.1f}s "
            f"@ {self.video_processor.fps:.1f} FPS"
        )

        self._step_timings['load_video'] = time.time() - start_time

    def _step4_extract_timestamps(
        self,
        sample_interval: int = 30,
        parallel: bool = True,
        workers: int = 4
    ):
        """Step 4: Extract timestamps from video via OCR"""
        start_time = time.time()

        logger.info("\n" + "=" * 70)
        logger.info("STEP 4: EXTRACTING TIME FROM VIDEO (OCR)")
        logger.info("=" * 70)

        logger.info("Sampling video frames for time extraction...")
        logger.info("This may take a few minutes depending on video length...")

        # Sample video with OCR
        self.video_timestamps = self.ocr_engine.sample_video_times(
            self.video_processor,
            sample_interval=sample_interval,
            max_samples=None,
            debug_dir=self.game_folders['data_dir'],
            parallel=parallel,
            workers=workers
        )

        if not self.video_timestamps:
            raise ValueError(
                "No timestamps extracted from video. "
                "OCR may have failed to detect scoreboard. "
                "Tips: (1) Ensure scoreboard is visible, "
                "(2) Check tesseract-ocr is installed, "
                "(3) Try adjusting ROI settings"
            )

        logger.info(f"✅ Extracted {len(self.video_timestamps)} timestamps from video")

        # Save debug info
        debug_file = self.game_folders['data_dir'] / 'video_timestamps.json'
        with open(debug_file, 'w') as f:
            json.dump(self.video_timestamps, f, indent=2)

        self._step_timings['extract_timestamps'] = time.time() - start_time

    def _step5_match_events(self, tolerance_seconds: int = 30):
        """Step 5: Match box score events to video timestamps"""
        start_time = time.time()

        logger.info("\n" + "=" * 70)
        logger.info("STEP 5: MATCHING EVENTS TO VIDEO")
        logger.info("=" * 70)

        # Enhance timestamps with interpolation
        self.video_timestamps = self.event_matcher.estimate_missing_timestamps(
            self.video_timestamps,
            self.video_processor.duration
        )

        # Match events (dictionary format for backward compatibility)
        self.matched_events = self.event_matcher.match_events_to_video(
            self.events,
            self.video_timestamps,
            tolerance_seconds=tolerance_seconds
        )

        # Also match typed Goal objects (new in v2.1)
        if self._goals:
            self._matched_goals = self.event_matcher.match_goals_to_video(
                self._goals,
                self.video_timestamps,
                tolerance_seconds=tolerance_seconds
            )

        # Filter to only events with successful matches
        valid_events = [e for e in self.matched_events if e.get('video_time') is not None]

        if not valid_events:
            raise ValueError(
                "No events could be matched to video timestamps. "
                "This could mean: (1) OCR failed to extract accurate times, "
                "(2) Video doesn't cover the entire game, "
                "(3) Scoreboard format is incompatible"
            )

        logger.info(f"✅ Matched {len(valid_events)}/{len(self.events)} events to video")

        # Sort by video time
        self.matched_events = self.event_matcher.sort_events_by_video_time(self.matched_events)

        # Save matched events
        self.file_manager.save_events(self.game_folders, self.matched_events)

        self._step_timings['match_events'] = time.time() - start_time

    def _step6_create_clips(
        self,
        before_seconds: float = 8.0,
        after_seconds: float = 6.0
    ):
        """Step 6: Create individual highlight clips"""
        start_time = time.time()

        logger.info("\n" + "=" * 70)
        logger.info("STEP 6: CREATING HIGHLIGHT CLIPS")
        logger.info("=" * 70)

        # Filter to goals only (can include penalties later)
        goal_events = self.event_matcher.filter_events_by_type(
            [e for e in self.matched_events if e.get('video_time') is not None],
            ['goal']
        )

        if not goal_events:
            logger.warning("⚠️  No goals found in matched events")
            logger.info("   Creating clips for all events instead...")
            goal_events = [e for e in self.matched_events if e.get('video_time') is not None]

        logger.info(f"Creating {len(goal_events)} highlight clips...")

        self.created_clips = self.video_processor.create_highlight_clips(
            goal_events,
            self.game_folders['clips_dir'],
            before_seconds=before_seconds,
            after_seconds=after_seconds
        )

        if not self.created_clips:
            raise ValueError("No clips could be created")

        logger.info(f"✅ Created {len(self.created_clips)} clips")

        self._step_timings['create_clips'] = time.time() - start_time

    def _step7_create_highlights_reel(
        self,
        max_clips: Optional[int] = None
    ) -> Optional[Path]:
        """Step 7: Create final highlights reel"""
        start_time = time.time()

        logger.info("\n" + "=" * 70)
        logger.info("STEP 7: CREATING HIGHLIGHTS REEL")
        logger.info("=" * 70)

        clip_paths = [clip_path for _, clip_path in self.created_clips]
        highlights_path = self.game_folders['output_dir'] / 'highlights.mp4'

        # Use max_clips from config if not specified
        if max_clips is None:
            max_clips = getattr(self.config, 'MAX_HIGHLIGHT_CLIPS', None)

        success = self.video_processor.create_highlights_reel(
            clip_paths,
            highlights_path,
            max_clips=max_clips
        )

        self._step_timings['create_highlights_reel'] = time.time() - start_time

        return highlights_path if success else None

    def _log_summary(self, highlights_path: Optional[Path]):
        """Log processing summary"""
        logger.info("\n" + "=" * 70)
        logger.info("PROCESSING COMPLETE!")
        logger.info("=" * 70)

        valid_events = [e for e in self.matched_events if e.get('video_time') is not None]

        logger.info(f"\n📊 Summary:")
        logger.info(f"   Events in box score: {len(self.events)}")
        logger.info(f"   Events matched to video: {len(valid_events)}")
        logger.info(f"   Highlight clips created: {len(self.created_clips)}")

        logger.info(f"\n📁 Output:")
        if highlights_path:
            logger.info(f"   Highlights reel: {highlights_path}")
        logger.info(f"   Individual clips: {self.game_folders['clips_dir']}")
        logger.info(f"   Game data: {self.game_folders['data_dir']}")
        logger.info(f"   Logs: {self.game_folders['logs_dir'] / 'processing.log'}")

        # Performance summary
        total_time = time.time() - self._pipeline_start_time
        logger.info(f"\n⏱️  Performance:")
        logger.info(f"   Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
        for step_name, duration in self._step_timings.items():
            logger.info(f"   {step_name}: {duration:.1f}s")

    def _create_result(
        self,
        success: bool,
        errors: List[str],
        warnings: List[str],
        highlights_path: Optional[Path] = None
    ) -> PipelineResult:
        """Create PipelineResult from pipeline state"""
        valid_events = [e for e in self.matched_events if e.get('video_time') is not None]

        total_time = time.time() - self._pipeline_start_time if self._pipeline_start_time is not None else 0.0

        return PipelineResult(
            success=success,
            game_info=self.game_info or GameInfo(
                date='unknown',
                home_team='unknown',
                away_team='unknown',
                league='Unknown',
                filename=self.video_path.name
            ),
            events_found=len(self.events),
            events_matched=len(valid_events),
            clips_created=len(self.created_clips),
            highlights_path=str(highlights_path) if highlights_path else None,
            errors=errors,
            warnings=warnings,
            ocr_duration_seconds=self._step_timings.get('extract_timestamps'),
            matching_duration_seconds=self._step_timings.get('match_events'),
            rendering_duration_seconds=self._step_timings.get('create_clips', 0) +
                                      self._step_timings.get('create_highlights_reel', 0),
            total_duration_seconds=total_time
        )

    def _cleanup(self):
        """Clean up resources"""
        try:
            self.video_processor.cleanup()
            logger.debug("Pipeline cleanup completed")
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self._cleanup()
