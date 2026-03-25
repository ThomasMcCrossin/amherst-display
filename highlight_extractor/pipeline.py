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
from .time_utils import (
    time_string_to_seconds,
    PERIOD_LENGTH_SECONDS,
    OT_LENGTH_SECONDS,
    period_time_to_absolute_seconds,
)
from .penalty_analyzer import analyze_game_penalties, PenaltyInfo
from .description_generator import generate_and_save_description
from .major_penalty_handler import process_major_penalties, detect_major_penalties
from .version import __version__ as HIGHLIGHT_EXTRACTOR_VERSION

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
        event_matcher: Optional[EventMatcher] = None,
        *,
        game_info_override: Optional[Dict] = None,
        game_folders_override: Optional[Dict[str, Path]] = None,
        source_game_info_override: Optional[Dict] = None,
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
        # If the game was matched/derived externally (e.g., Drive ingest), keep the raw filename-parsed
        # info alongside the canonical info used for folder naming and overlays.
        self.source_game_info: Optional[GameInfo] = None
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

        # Major review pause/resume state
        self.paused_for_review: bool = False
        self.resume_state_path: Optional[Path] = None
        self.major_review_folder_url: Optional[str] = None

        # Failure diagnostics (best-effort)
        self.failed_step: Optional[int] = None
        self.failed_reason: Optional[str] = None
        self.exception_type: Optional[str] = None

        # Optional overrides to bypass filename parsing / folder naming.
        if game_info_override:
            try:
                self.game_info = GameInfo(**game_info_override)
            except Exception:
                # Leave unset; step1 will raise and capture the error in a structured result.
                self.game_info = None
        if source_game_info_override:
            try:
                self.source_game_info = GameInfo(**source_game_info_override)
            except Exception:
                self.source_game_info = None
        if game_folders_override:
            self.game_folders = game_folders_override

        self._log_handler = None

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

    def _record_failure(self, step: int, reason: str, exc: Optional[BaseException] = None) -> None:
        # Keep the first failure cause; later warnings shouldn't clobber it.
        if self.failed_step is None:
            self.failed_step = int(step)
            self.failed_reason = str(reason or "").strip() or None
            if exc is not None:
                self.exception_type = type(exc).__name__

    def _configure_pipeline_logging(self) -> None:
        """
        Attach a per-game log file handler for highlight_extractor.* loggers.

        This avoids losing context when running under drive_ingest or other long-running loops.
        """
        if self._log_handler is not None:
            return
        if not self.game_folders:
            return
        logs_dir = self.game_folders.get("logs_dir")
        if not isinstance(logs_dir, Path):
            return
        try:
            logs_dir.mkdir(parents=True, exist_ok=True)
            log_path = logs_dir / "pipeline.log"
            handler = logging.FileHandler(log_path, encoding="utf-8")
            handler.setLevel(logging.INFO)
            handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))

            pkg_logger = logging.getLogger("highlight_extractor")
            pkg_logger.addHandler(handler)
            pkg_logger.setLevel(logging.INFO)
            pkg_logger.propagate = True

            self._log_handler = handler
        except Exception:
            # Logging must never break processing.
            self._log_handler = None

    def execute(
        self,
        sample_interval: int = 5,
        tolerance_seconds: int = 30,
        before_seconds: float = 15.0,
        after_seconds: float = 4.0,
        max_clips: Optional[int] = None,
        parallel_ocr: bool = True,
        ocr_workers: int = 4,
        broadcast_type: str = 'auto',
        auto_detect_start: bool = True
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
            broadcast_type: Type of broadcast ('auto', 'flohockey', 'yarmouth', 'standard')
            auto_detect_start: Auto-detect game start to skip pre-game content (default True)

        Returns:
            PipelineResult with success status and metrics
        """
        # Configure OCR for broadcast type
        if broadcast_type != 'auto':
            self.ocr_engine.set_broadcast_type(broadcast_type)
            logger.info(f"Using broadcast type: {broadcast_type}")
        self._pipeline_start_time = time.time()
        errors = []
        warnings = []

        try:
            # If the caller pre-created folders (e.g., Drive ingest), attach the per-game logger now.
            self._configure_pipeline_logging()

            logger.info("=" * 70)
            logger.info(f"HOCKEY HIGHLIGHT EXTRACTOR v{HIGHLIGHT_EXTRACTOR_VERSION}")
            logger.info("Box-Score-Based Detection")
            logger.info("=" * 70)
            logger.info(f"Processing: {self.video_path.name}")

            # STEP 1: Parse filename and create folders
            try:
                self._step1_parse_and_setup()
            except Exception as e:
                self._record_failure(1, "parse_failed", e)
                error_msg = f"Step 1 failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                return self._create_result(False, errors, warnings)

            # STEP 2: Fetch box score
            try:
                self._step2_fetch_box_score()
            except Exception as e:
                self._record_failure(2, "box_score_failed", e)
                error_msg = f"Step 2 failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                return self._create_result(False, errors, warnings)

            # STEP 3: Load video
            try:
                self._step3_load_video()
            except Exception as e:
                self._record_failure(3, "video_load_failed", e)
                error_msg = f"Step 3 failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                return self._create_result(False, errors, warnings)

            # STEP 3.5: Auto-detect game start (optional)
            game_start_time = 0.0
            if auto_detect_start:
                try:
                    detected_start = self._detect_game_start()
                    if detected_start is not None:
                        game_start_time = detected_start
                        logger.info(f"Game start detected at {game_start_time/60:.1f} minutes")
                    else:
                        logger.warning("Could not auto-detect game start, starting from beginning")
                except Exception as e:
                    logger.warning(f"Game start detection failed: {e}, starting from beginning")

            # STEP 4: Extract timestamps via OCR
            try:
                self._step4_extract_timestamps(
                    sample_interval=sample_interval,
                    parallel=parallel_ocr,
                    workers=ocr_workers,
                    start_time=game_start_time
                )
            except Exception as e:
                self._record_failure(4, "ocr_failed", e)
                error_msg = f"Step 4 failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                return self._create_result(False, errors, warnings)

            # STEP 5: Match events to video
            try:
                self._step5_match_events(tolerance_seconds=tolerance_seconds)
            except Exception as e:
                self._record_failure(5, "event_match_failed", e)
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

            # STEP 6.5: Handle major penalties (async review workflow)
            try:
                self._step6_5_process_major_penalties()
            except Exception as e:
                warning_msg = f"Step 6.5 (major penalties) failed: {e}"
                logger.warning(warning_msg)
                warnings.append(warning_msg)
                # Non-fatal - major penalties are handled asynchronously

            # If major penalties require review, pause before any reel-building steps.
            if self.paused_for_review:
                logger.info("Major penalty review required; pausing pipeline before reel creation")
                warnings.append("Paused for major penalty review; resume after approvals to build final reel")
                if not str(self.major_review_folder_url or "").strip():
                    try:
                        local_review = None
                        if self.game_folders and self.game_folders.get("output_dir"):
                            local_review = Path(self.game_folders["output_dir"]) / "major_review"
                        if local_review and local_review.exists():
                            warnings.append(f"Major review clips were not uploaded to Drive; review locally at: {local_review}")
                        else:
                            warnings.append("Major review clips were not uploaded to Drive; check Drive credentials/config.")
                    except Exception:
                        warnings.append("Major review clips were not uploaded to Drive; check Drive credentials/config.")
                self._log_summary(None)
                success = len(errors) == 0 or (len(self.created_clips) > 0)
                return self._create_result(success, errors, warnings, highlights_path=None)

            # STEP 7: Create highlights reel
            highlights_path = None
            try:
                highlights_path = self._step7_create_highlights_reel(max_clips=max_clips)
            except Exception as e:
                error_msg = f"Step 7 failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                warnings.append(error_msg)

            # STEP 8: Generate YouTube description
            try:
                self._step8_generate_description()
            except Exception as e:
                warning_msg = f"Step 8 (YouTube description) failed: {e}"
                logger.warning(warning_msg)
                warnings.append(warning_msg)

            # Generate summary
            self._log_summary(highlights_path)

            # Success if we got through all steps (even with warnings)
            success = len(errors) == 0 or (len(self.created_clips) > 0)

            return self._create_result(success, errors, warnings, highlights_path)

        except Exception as e:
            self._record_failure(999, "pipeline_failed", e)
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

        # If the caller provided canonical info and folders (e.g., Drive ingest), trust it.
        if self.game_info is not None and self.game_folders is not None:
            try:
                # Ensure folders exist (callers may have created them already, but be tolerant).
                for k, p in self.game_folders.items():
                    if k == "folder_name":
                        continue
                    if isinstance(p, Path):
                        p.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass

            logger.info(f"📅 Date: {self.game_info.date}")
            logger.info(f"🏒 League: {self.game_info.league}")
            logger.info(f"🏠 Home: {self.game_info.home_team}")
            logger.info(f"✈️  Away: {self.game_info.away_team}")
            logger.info(f"🎯 Perspective: {self.game_info.home_away.title()}")
            if self.game_folders.get("game_dir"):
                logger.info(f"\n📁 Output folder: {self.game_folders['game_dir']}")

            self._configure_pipeline_logging()
            self._step_timings['parse_and_setup'] = time.time() - start_time
            return

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

        # Create folder structure (if not already provided)
        self.game_folders = self.file_manager.create_game_folder(game_info_dict)
        logger.info(f"\n📁 Output folder: {self.game_folders['game_dir']}")

        self._configure_pipeline_logging()
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
            self.box_score,
            source_game_info=self.source_game_info.__dict__ if self.source_game_info else None,
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

    def _detect_game_start(self) -> Optional[float]:
        """Detect when the actual game starts (puck drop) to skip pre-game content"""
        start_time = time.time()

        logger.info("\n" + "=" * 70)
        logger.info("STEP 3.5: AUTO-DETECTING GAME START")
        logger.info("=" * 70)

        game_start = self.ocr_engine.find_game_start(self.video_processor)

        if game_start is not None:
            logger.info(f"✅ Game starts at {game_start/60:.1f} minutes ({game_start:.0f}s)")
        else:
            logger.warning("Could not detect game start")

        self._step_timings['detect_game_start'] = time.time() - start_time
        return game_start

    def _step4_extract_timestamps(
        self,
        sample_interval: int = 5,
        parallel: bool = True,
        workers: int = 4,
        start_time: float = 0.0
    ):
        """Step 4: Extract timestamps from video via OCR"""
        step_start = time.time()
        video_start_time = start_time  # Rename to avoid conflict

        logger.info("\n" + "=" * 70)
        logger.info("STEP 4: EXTRACTING TIME FROM VIDEO (OCR)")
        logger.info("=" * 70)

        if video_start_time > 0:
            logger.info(f"Starting OCR from {video_start_time/60:.1f} minutes (skipping pre-game)")
        logger.info("Sampling video frames for time extraction...")
        logger.info("This may take a few minutes depending on video length...")

        # Determine game_id for logging
        ocr_game_id = "unknown"
        if self.game_folders.get('game_dir'):
            game_dir = self.game_folders['game_dir']
            ocr_game_id = game_dir.name if hasattr(game_dir, 'name') else str(game_dir).split('/')[-1]

        # Sample video with OCR
        self.video_timestamps = self.ocr_engine.sample_video_times(
            self.video_processor,
            sample_interval=sample_interval,
            max_samples=None,
            debug_dir=self.game_folders['data_dir'],
            parallel=parallel,
            workers=workers,
            start_time=video_start_time,
            output_dir=self.game_folders.get('data_dir'),
            game_id=ocr_game_id,
        )

        # Hybrid policy: if OCR quality is poor, run a probe pass to lock onto the most stable
        # scoreboard settings and rerun sampling before failing the pipeline.
        stats = self.ocr_engine.get_last_sampling_stats()
        try:
            min_success = float(getattr(self.config, "OCR_MIN_SUCCESS_RATE", 0.05))
            min_period = float(getattr(self.config, "OCR_MIN_PERIOD_RATE", 0.20))
            min_conf = float(getattr(self.config, "OCR_MIN_AVG_CONFIDENCE", 55.0))
        except Exception:
            min_success, min_period, min_conf = 0.05, 0.20, 55.0

        if stats:
            sr = float(stats.get("success_rate", 0.0) or 0.0)
            pr = float(stats.get("period_rate", 0.0) or 0.0)
            ac = float(stats.get("avg_confidence", 0.0) or 0.0)
            if (sr < min_success) or (pr < min_period) or (ac < min_conf):
                logger.warning(
                    "OCR health is poor (success_rate=%.3f, period_rate=%.3f, avg_conf=%.1f). "
                    "Probing scoreboard settings and retrying OCR sampling...",
                    sr,
                    pr,
                    ac,
                )
                try:
                    probe_report = self.ocr_engine.probe_video_scoreboard(
                        self.video_processor,
                        start_time=video_start_time,
                        samples=60,
                    )
                    probe_path = self.game_folders['data_dir'] / "ocr_probe_report.json"
                    probe_path.write_text(json.dumps(probe_report, indent=2), encoding="utf-8")
                    logger.info(f"Wrote OCR probe report: {probe_path}")
                except Exception as e:
                    logger.warning(f"OCR probe pass failed: {e}")

                # Retry full sampling using the newly cached ROI/broadcast/backend/preprocess.
                self.video_timestamps = self.ocr_engine.sample_video_times(
                    self.video_processor,
                    sample_interval=sample_interval,
                    max_samples=None,
                    debug_dir=self.game_folders['data_dir'],
                    parallel=parallel,
                    workers=workers,
                    start_time=video_start_time,
                    output_dir=self.game_folders.get('data_dir'),
                    game_id=ocr_game_id,
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

        self._step_timings['extract_timestamps'] = time.time() - step_start

    def _step5_match_events(self, tolerance_seconds: int = 30):
        """Step 5: Match box score events to video timestamps"""
        start_time = time.time()

        logger.info("\n" + "=" * 70)
        logger.info("STEP 5: MATCHING EVENTS TO VIDEO")
        logger.info("=" * 70)

        # Identify this run for normalization + health reports.
        game_id = "unknown"
        output_dir = None
        if self.game_folders:
            output_dir = self.game_folders.get('data_dir')
            game_dir = self.game_folders.get('game_dir')
            if game_dir:
                game_id = game_dir.name if hasattr(game_dir, 'name') else str(game_dir).split('/')[-1]

        # Normalize raw OCR timestamps first (period inference + glitch filtering),
        # then interpolate within inferred periods to fill long gaps. This ordering
        # avoids interpolating using unknown periods (period=0) which can create
        # unrealistic clock jumps and hurt matching.
        self.video_timestamps = self.event_matcher._normalize_video_timestamps(
            self.video_timestamps,
            output_dir=output_dir,
            game_id=game_id,
        )
        self.video_timestamps = self.event_matcher.estimate_missing_timestamps(
            self.video_timestamps,
            self.video_processor.duration
        )
        # Re-normalize after interpolation (keeps clock monotonic + periods stable).
        self.video_timestamps = self.event_matcher._normalize_video_timestamps(
            self.video_timestamps,
            output_dir=output_dir,
            game_id=game_id,
        )

        # Match events (dictionary format for backward compatibility)
        # Pass game identifier and output directory for scoreboard health alerting
        self.matched_events = self.event_matcher.match_events_to_video(
            self.events,
            self.video_timestamps,
            tolerance_seconds=tolerance_seconds,
            game_id=game_id,
            output_dir=output_dir,
        )

        # Also match typed Goal objects (new in v2.1)
        if self._goals:
            self._matched_goals = self.event_matcher.match_goals_to_video(
                self._goals,
                self.video_timestamps,
                tolerance_seconds=tolerance_seconds
            )

        # Refine any low-confidence goal matches by locating the clock-stop moment
        # (the scoreboard freezes at the goal time during the stoppage).
        try:
            refined = self._refine_goal_events_by_clock_stop(self.matched_events)
            if refined:
                logger.info(f"Refined {refined} goal timestamps via clock-stop OCR")
        except Exception as e:
            logger.warning(f"Goal timestamp refinement failed: {e}")

        # Generic fallback: for low-confidence matches, do a small local OCR scan around the
        # approximate match timestamp and snap to the closest persistent clock reading.
        try:
            refined_any = self._refine_low_confidence_events_by_local_ocr(self.matched_events)
            if refined_any:
                logger.info(f"Refined {refined_any} event timestamps via local OCR")
        except Exception as e:
            logger.warning(f"Local OCR refinement failed: {e}")

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

    def _refine_goal_events_by_clock_stop(
        self,
        matched_events: List[Dict],
        *,
        step_seconds: float = 0.5,
        lookback_seconds: float = 90.0,
        lookforward_seconds: float = 20.0,
        persistence_window_seconds: float = 20.0,
        min_target_hits: int = 10,
    ) -> int:
        """
        Refine goal video timestamps by searching for the stoppage where the clock
        freezes at the goal time (box score time).

        We prefer the *start* of the freeze (clock reaches goal time and stays),
        not just any later frame during the stoppage.
        """
        if not matched_events:
            return 0

        refined_count = 0

        duration = getattr(self.video_processor, 'duration', 0.0) or 0.0
        if duration <= 0:
            return 0

        # Use the existing OCR sample map to narrow the search range.
        video_timestamps = self.video_timestamps or []

        for event in matched_events:
            if event.get('type') != 'goal':
                continue

            video_time = event.get('video_time')
            if video_time is None:
                continue

            period = event.get('period')
            time_str = event.get('time', '0:00')
            target_seconds = self.event_matcher.event_time_to_remaining_seconds(period, time_str)
            period_length = OT_LENGTH_SECONDS if (period or 1) >= 4 else PERIOD_LENGTH_SECONDS

            period_ts = [
                ts for ts in video_timestamps
                if ts.get('period') == period and ts.get('video_time') is not None
            ]
            period_ts.sort(key=lambda t: t.get('video_time', 0))

            # Start scanning from a timestamp where the clock is still running (> target),
            # so the first stable target run is the start of the stoppage.
            anchor_before = None
            if period_ts:
                nearest_idx = min(
                    range(len(period_ts)),
                    key=lambda i: abs(float(period_ts[i]['video_time']) - float(video_time))
                )
                for i in range(nearest_idx, -1, -1):
                    ts = period_ts[i]
                    ts_seconds = ts.get('game_time_seconds')
                    if ts_seconds is None:
                        ts_seconds = time_string_to_seconds(ts.get('game_time', '0:00'))
                    if ts_seconds < 0 or ts_seconds > period_length:
                        continue
                    if ts_seconds > target_seconds:
                        anchor_before = ts
                        break

            search_start = max(0.0, float(video_time) - lookback_seconds)
            if anchor_before is not None:
                search_start = max(0.0, float(anchor_before['video_time']) - 5.0)

            search_end = min(duration, float(video_time) + lookforward_seconds)
            if search_end <= search_start:
                continue

            # Scan window and store OCR results so we can apply a robust "freeze-start"
            # heuristic even with intermittent OCR dropouts/misreads.
            samples: List[Dict] = []
            t = search_start
            while t <= search_end:
                frame = self.video_processor.get_frame_at_time(t)
                ocr_seconds = None
                if frame is not None:
                    ocr_result = self.ocr_engine.extract_time_from_frame(frame)
                    if ocr_result:
                        _, ocr_time_str = ocr_result
                        parsed = time_string_to_seconds(ocr_time_str)
                        if 0 <= parsed <= period_length:
                            ocr_seconds = parsed
                samples.append({'t': t, 'sec': ocr_seconds})
                t += step_seconds

            refined_time = None

            # Precompute whether we've ever seen the clock running (> target) up to each sample.
            running_prefix = []
            seen_running = anchor_before is not None
            for s in samples:
                sec = s['sec']
                if sec is not None and sec > target_seconds:
                    seen_running = True
                running_prefix.append(seen_running)

            # Find earliest timestamp where:
            # - OCR reads target_seconds (at least once)
            # - Clock was running (>target) before it
            # - In the subsequent persistence window, the mode is target_seconds and
            #   we have enough hits of target_seconds (tolerates OCR noise).
            from collections import Counter

            for i, s in enumerate(samples):
                if s['sec'] != target_seconds:
                    continue
                if not running_prefix[i]:
                    continue

                window_end = s['t'] + persistence_window_seconds
                window_vals = []
                for j in range(i, len(samples)):
                    if samples[j]['t'] > window_end:
                        break
                    sec = samples[j]['sec']
                    if sec is not None:
                        window_vals.append(sec)

                if len(window_vals) < 3:
                    continue

                counts = Counter(window_vals)
                mode_val, mode_count = counts.most_common(1)[0]
                if mode_val != target_seconds:
                    continue
                if mode_count < min_target_hits:
                    continue

                refined_time = s['t']
                break

            if refined_time is None:
                # Fallback: if the scoreboard clock never freezes at the goal time (operator mistake),
                # pick the closest observed clock reading to the target within the scan window.
                best = None  # (diff, t)
                for i, s in enumerate(samples):
                    sec = s['sec']
                    if sec is None:
                        continue
                    if not running_prefix[i]:
                        continue
                    diff = abs(sec - target_seconds)
                    if best is None or diff < best[0] or (diff == best[0] and s['t'] < best[1]):
                        best = (diff, s['t'])

                if best is None:
                    continue

                refined_time = float(best[1])
                # Mark as a best-effort refinement (not a true clock-stop freeze).
                if 'video_time_original' not in event:
                    event['video_time_original'] = video_time
                event['video_time'] = refined_time
                event['refined_by'] = 'closest_clock'
                refined_count += 1
                logger.info(
                    f"Best-effort refine goal P{period} {time_str}: {video_time:.1f}s → {refined_time:.1f}s (closest_clock)"
                )
                continue

            # Update event with refined timestamp (preserve the original for debugging).
            if 'video_time_original' not in event:
                event['video_time_original'] = video_time
            event['video_time'] = float(refined_time)
            event['refined_by'] = 'clock_stop'
            refined_count += 1

            logger.info(
                f"Refined goal P{period} {time_str}: {video_time:.1f}s → {refined_time:.1f}s"
            )

        return refined_count

    def _refine_low_confidence_events_by_local_ocr(self, matched_events: List[Dict]) -> int:
        """
        For low-confidence matches, run a small local OCR scan around the approximate
        video timestamp and snap to the best persistent clock reading.

        This is a generic fallback (goals + penalties) and is intentionally conservative
        to avoid slowing down healthy runs.
        """
        if not matched_events:
            return 0

        refined = 0
        for event in matched_events:
            video_time = event.get("video_time")
            if video_time is None:
                continue

            # Skip events already refined by a stronger mechanism.
            if str(event.get("refined_by") or "") in {"clock_stop"}:
                continue

            try:
                conf = float(event.get("match_confidence") or 0.0)
            except Exception:
                conf = 0.0
            try:
                diff = abs(float(event.get("match_time_diff_seconds") or 0.0))
            except Exception:
                diff = 0.0

            # Only refine when the match looks questionable.
            if not (conf < 0.90 or diff > 10.0 or bool(event.get("match_unreliable"))):
                continue

            try:
                approx = float(video_time)
            except Exception:
                continue

            refined_time = self._refine_event_video_time_by_local_ocr(event, approx_video_time=approx)
            if refined_time is None:
                continue

            if "video_time_original" not in event:
                event["video_time_original"] = approx
            event["video_time"] = float(refined_time)
            event["refined_by"] = "local_ocr"
            refined += 1

        return refined

    def _refine_event_video_time_by_local_ocr(
        self,
        event: Dict,
        *,
        approx_video_time: float,
    ) -> Optional[float]:
        """
        Search around an approximate timestamp for frames where the scoreboard clock
        matches the box-score event time (converted to broadcast "remaining" time).

        Uses a persistence window so we prefer stable reads and avoid one-off OCR glitches.
        """
        get_frame = getattr(self.video_processor, "get_frame_at_time", None)
        if not callable(get_frame):
            return None

        duration = float(getattr(self.video_processor, "duration", 0.0) or 0.0)
        if duration <= 0:
            return None

        try:
            period = int(event.get("period") or 1)
        except Exception:
            period = 1
        time_str = str(event.get("time") or "0:00")
        try:
            target_remaining = int(self.event_matcher.event_time_to_remaining_seconds(period, time_str))
        except Exception:
            return None

        period_length = OT_LENGTH_SECONDS if period >= 4 else PERIOD_LENGTH_SECONDS
        if not (0 <= target_remaining <= period_length):
            return None

        window_seconds = float(getattr(self.config, "EVENT_LOCAL_OCR_WINDOW_SECONDS", 60.0))
        step_seconds = float(getattr(self.config, "EVENT_LOCAL_OCR_STEP_SECONDS", 0.5))
        persistence_window_seconds = float(getattr(self.config, "EVENT_LOCAL_OCR_PERSISTENCE_WINDOW_SECONDS", 6.0))
        min_target_hits = int(getattr(self.config, "EVENT_LOCAL_OCR_MIN_HITS", 3))
        max_diff_seconds = float(getattr(self.config, "EVENT_LOCAL_OCR_MAX_DIFF_SECONDS", 6.0))

        if window_seconds <= 0:
            return None
        if step_seconds <= 0:
            step_seconds = 0.5
        if persistence_window_seconds <= 0:
            persistence_window_seconds = 0.0
        if min_target_hits <= 0:
            min_target_hits = 1
        if max_diff_seconds <= 0:
            max_diff_seconds = 0.0

        start = max(0.0, float(approx_video_time) - window_seconds)
        end = min(duration, float(approx_video_time) + window_seconds)
        if end <= start:
            return None

        hits: List[Dict] = []
        best = None  # (diff, confidence, t)

        t = start
        while t <= end:
            frame = get_frame(t)
            if frame is None:
                t += step_seconds
                continue

            ocr = None
            try:
                ocr = self.ocr_engine.extract_time_from_frame_detailed(frame, broadcast_type="auto")
            except Exception:
                ocr = None

            if ocr is not None:
                # If OCR can read a period here, require it to match.
                if int(getattr(ocr, "period", 0) or 0) not in {0, period}:
                    t += step_seconds
                    continue

                sec = int(getattr(ocr, "time_seconds", -1) or -1)
                if 0 <= sec <= period_length:
                    diff = abs(int(sec) - int(target_remaining))
                    conf = float(getattr(ocr, "confidence", 0.0) or 0.0)

                    # Track best single-frame hit (used as a fallback if no stable cluster).
                    if best is None or (diff, conf) < (best[0], best[1]):
                        best = (diff, conf, float(t))

                    if diff <= max_diff_seconds:
                        hits.append({"t": float(t), "diff": int(diff), "conf": conf})

            t += step_seconds

        if not hits:
            # Fallback to best single-frame candidate if it's close enough.
            if best is not None and float(best[0]) <= max_diff_seconds:
                return float(best[2])
            return None

        # Find the earliest cluster with >= min_target_hits within persistence_window_seconds.
        hits.sort(key=lambda h: h["t"])
        if persistence_window_seconds <= 0:
            return float(hits[0]["t"])

        j = 0
        for i in range(len(hits)):
            while hits[i]["t"] - hits[j]["t"] > persistence_window_seconds:
                j += 1
                if j > i:
                    break
            if i - j + 1 >= min_target_hits:
                return float(hits[j]["t"])

        # No cluster met the threshold; use the closest hit (diff, then higher confidence).
        hits.sort(key=lambda h: (h["diff"], -h["conf"], h["t"]))
        return float(hits[0]["t"])

    def _step6_create_clips(
        self,
        before_seconds: float = 15.0,
        after_seconds: float = 4.0
    ):
        """Step 6: Create individual highlight clips (including penalty clips for PP goals)"""
        start_time = time.time()

        logger.info("\n" + "=" * 70)
        logger.info("STEP 6: CREATING HIGHLIGHT CLIPS")
        logger.info("=" * 70)

        # Filter to goals only
        goal_events = self.event_matcher.filter_events_by_type(
            [e for e in self.matched_events if e.get('video_time') is not None],
            ['goal']
        )

        if not goal_events:
            logger.warning("⚠️  No goals found in matched events")
            logger.info("   Creating clips for all events instead...")
            goal_events = [e for e in self.matched_events if e.get('video_time') is not None]

        # Ensure we don't mix clips from previous runs (important when team labels change).
        try:
            clips_dir = self.game_folders['clips_dir']
            for p in clips_dir.glob("*.mp4"):
                p.unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"Could not clear existing clips: {e}")

        # Analyze penalties and link to PP goals
        # Get penalties from box_score - nested under SiteKit.Gamesummary.penalties
        pp_penalty_map = {}
        penalties_data = []
        if self.box_score:
            penalties_data = (self.box_score.get('SiteKit', {})
                              .get('Gamesummary', {})
                              .get('penalties', []))
        if penalties_data:
            logger.info(f"Analyzing {len(penalties_data)} penalties for PP goal linking...")
            time_is_elapsed = bool(getattr(self.config, 'BOX_SCORE_TIME_IS_ELAPSED', True))
            penalty_analysis = analyze_game_penalties(
                goal_events,
                penalties_data,
                our_team='ramblers',
                time_is_elapsed=time_is_elapsed,
            )
            pp_penalty_map = penalty_analysis.get('pp_penalty_map', {})
            logger.info(f"Found {len(pp_penalty_map)} penalties linked to PP goals")

        # Match penalty video times using the same timestamp data
        for goal_idx, penalty_info in pp_penalty_map.items():
            if penalty_info.video_time is None:
                # Find video time for this penalty
                penalty_video_time = self._find_penalty_video_time(penalty_info)
                if penalty_video_time is not None:
                    penalty_info.video_time = penalty_video_time
                    logger.debug(f"Penalty P{penalty_info.period} {penalty_info.time} matched to video time {penalty_video_time:.1f}s")
                else:
                    # Fallback: estimate penalty clip position relative to the matched PP goal,
                    # then optionally refine via a local OCR scan around that estimate.
                    # This helps when OCR sampling starts late or period inference fails,
                    # leaving no usable timestamps for the penalty's period.
                    try:
                        goal_event = goal_events[int(goal_idx)]
                        goal_video_time = goal_event.get("video_time")
                        if goal_video_time is not None:
                            # Convert both times to absolute elapsed seconds in game.
                            goal_remaining = self.event_matcher.event_time_to_remaining_seconds(
                                goal_event.get("period"), str(goal_event.get("time", "0:00"))
                            )
                            goal_abs = period_time_to_absolute_seconds(int(goal_event.get("period") or 1), int(goal_remaining))
                            pen_abs = period_time_to_absolute_seconds(int(penalty_info.period or 1), int(penalty_info.time_seconds))
                            delta = goal_abs - pen_abs

                            max_delta = int(getattr(self.config, "PENALTY_VIDEO_TIME_FALLBACK_MAX_DELTA_SECONDS", 15 * 60))
                            if 0 < delta <= max_delta:
                                approx = max(0.0, float(goal_video_time) - float(delta))
                                refined = None
                                if bool(getattr(self.config, "PENALTY_VIDEO_TIME_LOCAL_OCR_REFINEMENT", True)):
                                    refined = self._refine_penalty_video_time_by_local_ocr(
                                        penalty_info,
                                        approx_video_time=approx,
                                    )

                                if refined is not None:
                                    penalty_info.video_time = refined
                                    logger.info(
                                        f"Refined penalty video time via local OCR: "
                                        f"P{penalty_info.period} {penalty_info.time} → {refined:.1f}s "
                                        f"(approx {approx:.1f}s)"
                                    )
                                elif bool(getattr(self.config, "PENALTY_VIDEO_TIME_ALLOW_ESTIMATE_FALLBACK", True)):
                                    penalty_info.video_time = approx
                                    logger.info(
                                        f"Estimated penalty video time via PP-goal delta: "
                                        f"P{penalty_info.period} {penalty_info.time} ≈ {approx:.1f}s "
                                        f"(Δ{delta}s before goal)"
                                    )
                    except Exception:
                        pass

        # Build final events list with penalty clips inserted before PP goals
        final_events = []
        penalty_before = getattr(self.config, 'PENALTY_PP_BEFORE_SECONDS', 3.0)
        penalty_after = getattr(self.config, 'PENALTY_PP_AFTER_SECONDS', 3.0)

        for i, goal in enumerate(goal_events):
            # Check if this goal has a linked penalty
            if i in pp_penalty_map:
                penalty_info = pp_penalty_map[i]
                if penalty_info.video_time is not None:
                    # Create penalty event dict for clip creation
                    penalty_event = {
                        'type': 'penalty',
                        'period': penalty_info.period,
                        'time': penalty_info.time,
                        'team': penalty_info.team,
                        'video_time': penalty_info.video_time,
                        'before_seconds': penalty_before,
                        'after_seconds': penalty_after,
                        'player': {
                            'name': penalty_info.player_name,
                            'number': penalty_info.player_number
                        },
                        'infraction': penalty_info.infraction,
                        'minutes': penalty_info.minutes,
                        'linked_to_goal': i,  # Track which goal this penalty leads to
                    }
                    final_events.append(penalty_event)
                    logger.info(f"Adding penalty clip: {penalty_info.player_name} - {penalty_info.infraction} ({penalty_info.minutes} min)")
                else:
                    logger.warning(f"Could not find video time for penalty P{penalty_info.period} {penalty_info.time}")

            # If the match is low-confidence or not refined by a clock-stop, expand the pre-roll
            conf = goal.get('match_confidence')
            diff = goal.get('match_time_diff_seconds')
            refined_by = str(goal.get('refined_by') or '')
            try:
                conf_f = float(conf) if conf is not None else 0.0
            except Exception:
                conf_f = 0.0
            try:
                diff_f = abs(float(diff)) if diff is not None else 0.0
            except Exception:
                diff_f = 0.0

            if refined_by != 'clock_stop' or conf_f < 0.95:
                # Add buffer: diff + 5 seconds, capped to avoid huge clips.
                extra = min(20.0, max(0.0, diff_f + 5.0))
                goal['before_seconds'] = float(before_seconds) + extra
                goal['after_seconds'] = float(after_seconds)

            final_events.append(goal)

        logger.info(f"Creating {len(final_events)} highlight clips ({len(pp_penalty_map)} penalty clips + {len(goal_events)} goal clips)...")

        self.created_clips = self.video_processor.create_highlight_clips(
            final_events,
            self.game_folders['clips_dir'],
            before_seconds=before_seconds,
            after_seconds=after_seconds
        )

        if not self.created_clips:
            raise ValueError("No clips could be created")

        # Save a manifest so downstream production tools can render correct overlays (goals + penalties)
        try:
            clips_dir = self.game_folders['clips_dir']
            game_dir = self.game_folders['game_dir']
            manifest: Dict[str, List[Dict]] = {"clips": []}
            for idx, (event, clip_path) in enumerate(self.created_clips, 1):
                relpath = None
                try:
                    relpath = str(Path(clip_path).relative_to(game_dir))
                except Exception:
                    relpath = str(clip_path)

                entry: Dict = dict(event) if isinstance(event, dict) else {"type": "unknown"}
                entry["index"] = idx
                entry["clip_filename"] = Path(clip_path).name
                entry["path"] = relpath
                manifest["clips"].append(entry)

            manifest_path = self.game_folders['data_dir'] / "clips_manifest.json"
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            logger.info(f"Saved clip manifest: {manifest_path}")
        except Exception as e:
            logger.warning(f"Could not write clips manifest: {e}")

        logger.info(f"✅ Created {len(self.created_clips)} clips")

        self._step_timings['create_clips'] = time.time() - start_time

    def _find_penalty_video_time(self, penalty_info: PenaltyInfo) -> Optional[float]:
        """
        Find the video timestamp for a penalty using OCR timestamp data.

        Args:
            penalty_info: PenaltyInfo object with period and time

        Returns:
            Video time in seconds, or None if not found
        """
        if not self.video_timestamps:
            return None

        penalty_period = int(penalty_info.period or 1)

        # Box scores typically provide time ELAPSED; OCR provides time REMAINING.
        elapsed_seconds = time_string_to_seconds(penalty_info.time)
        time_is_elapsed = bool(getattr(self.config, 'BOX_SCORE_TIME_IS_ELAPSED', True))
        period_length = OT_LENGTH_SECONDS if penalty_period >= 4 else PERIOD_LENGTH_SECONDS
        penalty_remaining = (period_length - elapsed_seconds) if time_is_elapsed else elapsed_seconds

        # Find timestamps in the same period
        period_timestamps = [
            ts for ts in self.video_timestamps
            if ts.get('period') == penalty_period and ts.get('video_time') is not None
        ]

        if not period_timestamps:
            return None

        # Find the closest timestamp to the penalty time
        best_match = None
        best_diff = float('inf')

        for ts in period_timestamps:
            ts_remaining = ts.get('game_time_seconds')
            if ts_remaining is None:
                ts_remaining = time_string_to_seconds(ts.get('game_time', '0:00'))

            diff = abs(ts_remaining - penalty_remaining)
            if diff < best_diff:
                best_diff = diff
                best_match = ts

        max_diff = float(getattr(self.config, 'PENALTY_VIDEO_TIME_MAX_DIFF_SECONDS', 600))
        if best_match and best_diff <= max_diff:
            video_time = float(best_match.get('video_time') or 0.0)
            best_remaining = best_match.get('game_time_seconds')
            if best_remaining is None:
                best_remaining = time_string_to_seconds(best_match.get('game_time', '0:00'))
            # Clock counts down: later video_time == smaller remaining time.
            adjusted_time = video_time + float(best_remaining - penalty_remaining)
            return max(0, adjusted_time)

        return None

    def _refine_penalty_video_time_by_local_ocr(
        self,
        penalty_info: PenaltyInfo,
        *,
        approx_video_time: float,
    ) -> Optional[float]:
        """
        Use a local OCR scan around an approximate video timestamp to find a better
        match for a penalty time on the scoreboard clock.

        This is used when the coarse OCR sampling does not yield any usable
        timestamps for the penalty's period.
        """
        # This refinement requires a real video processor (tests may inject a stub).
        get_frame = getattr(self.video_processor, "get_frame_at_time", None)
        if not callable(get_frame):
            return None

        duration = float(getattr(self.video_processor, "duration", 0.0) or 0.0)
        if duration <= 0:
            return None

        try:
            penalty_period = int(penalty_info.period or 1)
        except Exception:
            penalty_period = 1

        try:
            target_remaining = int(penalty_info.time_seconds)
        except Exception:
            return None

        period_length = OT_LENGTH_SECONDS if penalty_period >= 4 else PERIOD_LENGTH_SECONDS
        if not (0 <= target_remaining <= period_length):
            return None

        window_seconds = float(getattr(self.config, "PENALTY_LOCAL_OCR_WINDOW_SECONDS", 90.0))
        step_seconds = float(getattr(self.config, "PENALTY_LOCAL_OCR_STEP_SECONDS", 1.0))
        persistence_window_seconds = float(getattr(self.config, "PENALTY_LOCAL_OCR_PERSISTENCE_WINDOW_SECONDS", 6.0))
        min_target_hits = int(getattr(self.config, "PENALTY_LOCAL_OCR_MIN_HITS", 2))
        max_diff_seconds = float(getattr(self.config, "PENALTY_LOCAL_OCR_MAX_DIFF_SECONDS", 12.0))

        if window_seconds <= 0:
            return None
        if step_seconds <= 0:
            step_seconds = 1.0
        if persistence_window_seconds <= 0:
            persistence_window_seconds = 0.0
        if min_target_hits <= 0:
            min_target_hits = 1
        if max_diff_seconds <= 0:
            max_diff_seconds = 0.0

        start = max(0.0, float(approx_video_time) - window_seconds)
        end = min(duration, float(approx_video_time) + window_seconds)
        if end <= start:
            return None

        samples: List[Dict] = []
        t = start
        while t <= end:
            frame = get_frame(t)
            sec = None
            period = None
            if frame is not None:
                result = self.ocr_engine.extract_time_from_frame(frame)
                if result:
                    p, time_str = result
                    try:
                        p_int = int(p)
                        if 1 <= p_int <= 5:
                            period = p_int
                    except Exception:
                        period = None

                    parsed = time_string_to_seconds(str(time_str))
                    if 0 <= parsed <= period_length:
                        sec = int(parsed)

            samples.append({"t": float(t), "period": period, "sec": sec})
            t += step_seconds

        if not samples:
            return None

        # Track whether we've seen the clock still running (> target) up to each sample.
        running_prefix = []
        seen_running = False
        for s in samples:
            sec = s["sec"]
            if sec is not None and sec > target_remaining:
                seen_running = True
            running_prefix.append(seen_running)

        # Prefer a stable run of exact target hits (penalty calls often stop the clock).
        for i, s in enumerate(samples):
            if s["sec"] != target_remaining:
                continue
            if not running_prefix[i]:
                continue

            # If OCR yields a period here, require it to match the penalty period.
            if s["period"] is not None and s["period"] != penalty_period:
                continue

            window_end = s["t"] + persistence_window_seconds
            hits = 0
            for j in range(i, len(samples)):
                if samples[j]["t"] > window_end:
                    break
                if samples[j]["sec"] == target_remaining:
                    if samples[j]["period"] is None or samples[j]["period"] == penalty_period:
                        hits += 1
            if hits >= min_target_hits:
                return float(s["t"])

        # Otherwise, pick the closest observed clock reading and adjust by the delta.
        best = None  # (diff, period_match, t, sec)
        for s in samples:
            sec = s["sec"]
            if sec is None:
                continue
            diff = abs(int(sec) - int(target_remaining))
            period_match = 1 if (s["period"] == penalty_period) else 0
            if best is None or diff < best[0] or (diff == best[0] and period_match > best[1]) or (
                diff == best[0] and period_match == best[1] and s["t"] < best[2]
            ):
                best = (diff, period_match, float(s["t"]), int(sec))

        if best is None or float(best[0]) > max_diff_seconds:
            return None

        adjusted = float(best[2]) + float(best[3] - target_remaining)
        return max(0.0, min(duration, adjusted))

    def _step6_5_process_major_penalties(self):
        """Step 6.5: Process 5-minute major penalties for async review"""
        start_time = time.time()

        logger.info("\n" + "=" * 70)
        logger.info("STEP 6.5: CHECKING FOR MAJOR PENALTIES")
        logger.info("=" * 70)

        # Get penalties from box_score - nested under SiteKit.Gamesummary.penalties
        penalties_data = []
        if self.box_score:
            penalties_data = (self.box_score.get('SiteKit', {})
                              .get('Gamesummary', {})
                              .get('penalties', []))
        if not penalties_data:
            logger.info("No penalties in box score, skipping major penalty check")
            return

        # Quick check for majors before full processing
        major_groups = detect_major_penalties(
            penalties_data,
            time_is_elapsed=bool(getattr(self.config, "BOX_SCORE_TIME_IS_ELAPSED", True)),
        )
        if not major_groups:
            logger.info("No 5-minute major penalties detected")
            self._step_timings['major_penalties'] = time.time() - start_time
            return

        logger.info(f"Found {sum(len(g) for g in major_groups)} major penalties in {len(major_groups)} groups")

        # Assign video times to major penalties
        from .penalty_analyzer import parse_penalties
        all_penalties = parse_penalties(penalties_data)
        for penalty in all_penalties:
            if penalty.is_major and penalty.video_time is None:
                penalty.video_time = self._find_penalty_video_time(penalty)

        # Get game ID from box score fetcher cache or generate one
        game_id = 'unknown'
        if hasattr(self.box_score_fetcher, '_last_game_id'):
            game_id = str(self.box_score_fetcher._last_game_id)
        elif self.game_info:
            game_id = f"{self.game_info.date}_{self.game_info.home_team}_{self.game_info.away_team}"

        # Build game info for notification
        opponent_name = 'Unknown'
        home_team = ''
        away_team = ''
        if self.game_info:
            home_team = str(self.game_info.home_team or '')
            away_team = str(self.game_info.away_team or '')
            opponent_name = (
                self.game_info.away_team
                if self.game_info.home_away == 'home'
                else self.game_info.home_team
            ) or 'Unknown'

        game_info = {
            'date': self.game_info.date if self.game_info else '',
            'home_team': home_team,
            'away_team': away_team,
            'opponent': {
                'team_name': opponent_name
            }
        }

        # Create temp directory for major clips
        major_output_dir = self.game_folders['output_dir'] / 'major_review'
        major_output_dir.mkdir(parents=True, exist_ok=True)

        # Avoid stale review artifacts when re-processing the same game folder.
        # (Old JSONs/clips can confuse the reviewer and the monitor.)
        try:
            for child in major_output_dir.iterdir():
                if child.is_file():
                    child.unlink(missing_ok=True)
        except Exception:
            pass

        resume_state_path = None
        if self.game_folders:
            resume_state_path = self.game_folders['data_dir'] / "major_review_state.json"

        # Process major penalties (create clips, upload to Drive, send notification, enable monitor)
        result = process_major_penalties(
            self.video_processor,
            penalties_data,
            game_id,
            self.game_info.date if self.game_info else '',
            game_info,
            major_output_dir,
            self.config,
            video_timestamps=self.video_timestamps,
            resume_state_path=resume_state_path,
            ocr_engine=self.ocr_engine,
        )

        if result['major_count'] > 0:
            logger.info(f"Major penalties processed:")
            logger.info(f"  - Majors found: {result['major_count']}")
            logger.info(f"  - Clips created: {result['clips_created']}")
            if result['drive_folder']:
                logger.info(f"  - Drive folder: {result['drive_folder']}")
            if result['email_sent']:
                logger.info(f"  - Email notification: sent")

        # Pause pipeline whenever majors exist (review workflow is required).
        if result.get('major_count', 0) > 0 and result.get('clips_created', 0) > 0:
            self.paused_for_review = True
            self.major_review_folder_url = str(result.get('drive_folder') or '')

            # Persist resume metadata inside the game folder so cron can resume later.
            try:
                if self.game_folders:
                    state_path = self.game_folders['data_dir'] / "major_review_state.json"
                    state = {
                        "status": "paused_for_major_review",
                        "game_id": game_id,
                        "game_date": self.game_info.date if self.game_info else '',
                        "game_dir": str(self.game_folders.get('game_dir')),
                        "major_review": {
                            "drive_folder_url": self.major_review_folder_url,
                            "local_dir": str(major_output_dir),
                        },
                        "created_at_unix": time.time(),
                    }
                    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
                    self.resume_state_path = state_path
                    logger.info(f"Saved major review state: {state_path}")
            except Exception as e:
                logger.warning(f"Could not write major review state: {e}")

        self._step_timings['major_penalties'] = time.time() - start_time

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

    def _step8_generate_description(self):
        """Step 8: Generate YouTube description file"""
        start_time = time.time()

        logger.info("\n" + "=" * 70)
        logger.info("STEP 8: GENERATING YOUTUBE DESCRIPTION")
        logger.info("=" * 70)

        # Build game data dict for description generator
        game_data = {
            'date': self.game_info.date,
            'home_game': self.game_info.home_away == 'home',
            'opponent': {
                'team_name': self.game_info.away_team if self.game_info.home_away == 'home' else self.game_info.home_team
            },
            'venue': getattr(self.game_info, 'venue', ''),
            'result': {},
            'box_score': self.box_score if self.box_score else {}
        }

        # Try to get more data from box_score
        if self.box_score:
            game_data['attendance'] = self.box_score.get('attendance')
            game_data['result'] = self.box_score.get('result', {})
            if 'game_info' in self.box_score:
                gi = self.box_score['game_info']
                game_data['venue'] = gi.get('arena', game_data['venue'])
                game_data['attendance'] = gi.get('attendance', game_data['attendance'])

        # Use matched events with video times for clickable timestamps
        matched_goals = [e for e in self.matched_events
                        if e.get('type') == 'goal' and e.get('video_time') is not None]

        # Generate and save description
        desc_path = generate_and_save_description(
            game_data,
            matched_goals,
            self.game_folders['output_dir']
        )

        logger.info(f"✅ YouTube description saved: {desc_path}")

        self._step_timings['generate_description'] = time.time() - start_time

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
        logger.info(f"   Logs: {self.game_folders['logs_dir'] / 'pipeline.log'}")

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
            game_info=self.game_info,
            events_found=len(self.events),
            events_matched=len(valid_events),
            clips_created=len(self.created_clips),
            highlights_path=str(highlights_path) if highlights_path else None,
            paused_for_review=bool(self.paused_for_review),
            resume_state_path=str(self.resume_state_path) if self.resume_state_path else None,
            major_review_folder_url=self.major_review_folder_url,
            failed_step=self.failed_step,
            failed_reason=self.failed_reason,
            exception_type=self.exception_type,
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

        # Detach per-game log handler to avoid duplicate logs in long-running processes.
        try:
            if self._log_handler is not None:
                logging.getLogger("highlight_extractor").removeHandler(self._log_handler)
                try:
                    self._log_handler.close()
                except Exception:
                    pass
                self._log_handler = None
        except Exception:
            pass

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self._cleanup()
