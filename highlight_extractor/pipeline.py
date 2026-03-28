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
try:
    from .video_processor import VideoProcessor
except ModuleNotFoundError:
    VideoProcessor = None  # type: ignore[assignment]
from .ocr_engine import OCREngine
from .event_matcher import EventMatcher
from .time_utils import (
    game_clock_rules_from_context,
    period_length_seconds,
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
        if video_processor is not None:
            self.video_processor = video_processor
        else:
            if VideoProcessor is None:
                raise RuntimeError(
                    "VideoProcessor is unavailable because moviepy is not installed. "
                    "Inject a stub video_processor for tests or install highlight video dependencies."
                )
            self.video_processor = VideoProcessor(self.video_path, config)
        self.ocr_engine = ocr_engine
        self._ocr_engine_init_error: Optional[BaseException] = None
        self._pending_broadcast_type = 'auto'
        if self.ocr_engine is None:
            try:
                self.ocr_engine = OCREngine(config)
            except Exception as exc:
                # Defer OCR dependency failures until a code path actually needs OCR.
                # This keeps non-OCR tests and stubbed flows working when OCR deps are absent.
                self._ocr_engine_init_error = exc
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
        self.reel_mode = str(getattr(self.config, "DEFAULT_REEL_MODE", "goals_only"))
        self._refine_goal_clock = True
        self._refine_local_ocr = True
        self._detected_game_start_time: Optional[float] = None
        self._game_context: Dict = {}
        if game_info_override:
            self._game_context.update(dict(game_info_override))
        if source_game_info_override:
            for key, value in dict(source_game_info_override).items():
                self._game_context.setdefault(key, value)
        self._clock_rules = game_clock_rules_from_context(self._game_context)

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

    def _refresh_game_context(self) -> None:
        context: Dict = {}
        if self.game_info is not None:
            context.update(getattr(self.game_info, "__dict__", {}) or {})
        if self.source_game_info is not None:
            for key, value in (getattr(self.source_game_info, "__dict__", {}) or {}).items():
                context.setdefault(key, value)

        if isinstance(self.box_score, dict):
            amherst_payload = self.box_score.get("_amherst_display")
            if isinstance(amherst_payload, dict):
                for key in ("playoff", "schedule_notes", "result", "date", "game_number"):
                    if amherst_payload.get(key) not in (None, ""):
                        context[key] = amherst_payload.get(key)
                game_meta = amherst_payload.get("game_info")
                if isinstance(game_meta, dict):
                    for key in ("playoff", "schedule_notes", "result", "game_number"):
                        if game_meta.get(key) not in (None, ""):
                            context[key] = game_meta.get(key)

        if self._game_context:
            merged = dict(self._game_context)
            merged.update({k: v for k, v in context.items() if v not in (None, "")})
            context = merged

        self._game_context = context
        self._clock_rules = game_clock_rules_from_context(context)
        if hasattr(self.event_matcher, "set_game_context"):
            try:
                self.event_matcher.set_game_context(context)
            except Exception:
                pass

    def _period_length_seconds(self, period: int) -> int:
        return period_length_seconds(period, self._clock_rules)

    def _period_time_to_absolute_seconds(self, period: int, time_remaining_seconds: int) -> int:
        return period_time_to_absolute_seconds(period, time_remaining_seconds, self._clock_rules)

    def _normalize_reel_mode(self, reel_mode: Optional[str]) -> str:
        mode = str(reel_mode or getattr(self.config, "DEFAULT_REEL_MODE", "goals_only")).strip().lower()
        supported = tuple(getattr(self.config, "SUPPORTED_REEL_MODES", ("goals_only",)))
        if mode not in supported:
            raise ValueError(
                f"Unsupported reel mode '{mode}'. Expected one of: {', '.join(supported)}"
            )
        return mode

    def _include_pp_penalty_clips(self) -> bool:
        return self.reel_mode in {"goals_with_pp_penalties", "full_production"}

    def _requires_major_review_workflow(self) -> bool:
        return self.reel_mode in {"goals_with_approved_majors", "full_production"}

    def _ensure_ocr_engine(self) -> OCREngine:
        if self.ocr_engine is None:
            try:
                self.ocr_engine = OCREngine(self.config)
                self._ocr_engine_init_error = None
            except Exception as exc:
                self._ocr_engine_init_error = exc

        if self.ocr_engine is None:
            detail = str(self._ocr_engine_init_error or "").strip() or "OCR backend unavailable"
            raise RuntimeError(detail) from self._ocr_engine_init_error

        if self._pending_broadcast_type != 'auto':
            self.ocr_engine.set_broadcast_type(self._pending_broadcast_type)
        return self.ocr_engine

    def _minimum_plausible_video_time_for_event(self, event: Dict) -> Optional[float]:
        helper = getattr(self.event_matcher, "minimum_video_time_for_event", None)
        if not callable(helper):
            return None
        try:
            return helper(
                event,
                recording_game_start_time=self._detected_game_start_time,
            )
        except Exception:
            return None

    def _refinement_broadcast_type(self) -> str:
        """
        Use the resolved execution-profile broadcast type during local refinement too.

        Falling back to auto here causes the refinement passes to re-probe generic ROIs,
        which is both slower and less accurate for seeded home-broadcast layouts.
        """
        return str(self._pending_broadcast_type or "auto")

    @staticmethod
    def _goal_match_key(entry: Dict) -> tuple[int, str, str, str]:
        return (
            int(entry.get("period") or 0),
            str(entry.get("time") or "").strip(),
            str(entry.get("team") or "").strip().lower(),
            str(entry.get("scorer") or "").strip().lower(),
        )

    def _hydrate_goal_events_from_typed_matches(self) -> int:
        """
        Backfill legacy event dicts from typed Goal matches.

        The pipeline still creates clips from self.matched_events, but the typed
        goal matcher is often more resilient when OCR coverage is sparse. Copy any
        successful typed-goal matches back into the dict event list so clip creation
        and manifests don't silently drop goals.
        """
        if not self.matched_events or not self._matched_goals:
            return 0

        typed_lookup: Dict[tuple[int, str, str, str], List[Goal]] = {}
        for goal in self._matched_goals:
            key = (
                int(goal.period or 0),
                str(goal.time or "").strip(),
                str(goal.team or "").strip().lower(),
                str(goal.scorer or "").strip().lower(),
            )
            typed_lookup.setdefault(key, []).append(goal)

        hydrated = 0
        for event in self.matched_events:
            if str(event.get("type") or "").strip().lower() != "goal":
                continue

            key = self._goal_match_key(event)
            matches = typed_lookup.get(key) or []
            if not matches:
                continue

            goal = matches.pop(0)
            if event.get("video_time") is None and goal.video_time is not None:
                event["video_time"] = float(goal.video_time)
                hydrated += 1
            if event.get("match_confidence") is None and goal.match_confidence is not None:
                event["match_confidence"] = float(goal.match_confidence)
            if not str(event.get("assist1") or "").strip() and goal.assist1:
                event["assist1"] = goal.assist1
            if not str(event.get("assist2") or "").strip() and goal.assist2:
                event["assist2"] = goal.assist2
            if not str(event.get("special") or "").strip() and getattr(goal, "goal_type", None):
                event["special"] = str(goal.goal_type.value)

        return hydrated

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
        auto_detect_start: bool = True,
        refine_goal_clock: bool = True,
        refine_local_ocr: bool = True,
        reel_mode: Optional[str] = None,
        build_reel: bool = True,
        build_description: bool = True,
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
            refine_goal_clock: Use the goal clock-stop refinement pass after matching
            refine_local_ocr: Use the local OCR fallback refinement pass after matching
            reel_mode: Reel composition mode (goals_only, goals_with_pp_penalties,
                goals_with_approved_majors, full_production)
            build_reel: Build the per-game stitched highlights reel after creating clips
            build_description: Generate the YouTube description sidecar after processing

        Returns:
            PipelineResult with success status and metrics
        """
        self.reel_mode = self._normalize_reel_mode(reel_mode)
        self._refine_goal_clock = bool(refine_goal_clock)
        self._refine_local_ocr = bool(refine_local_ocr)

        self._pending_broadcast_type = str(broadcast_type or 'auto')
        if self._pending_broadcast_type != 'auto':
            logger.info(f"Using broadcast type: {self._pending_broadcast_type}")
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
            logger.info(f"Reel mode: {self.reel_mode}")

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
            self._detected_game_start_time = None
            if auto_detect_start:
                try:
                    detected_start = self._detect_game_start()
                    if detected_start is not None:
                        game_start_time = detected_start
                        self._detected_game_start_time = float(detected_start)
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
                self._step5_match_events(
                    tolerance_seconds=tolerance_seconds,
                    recording_game_start_time=self._detected_game_start_time,
                )
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

            highlights_path = None
            if build_reel:
                try:
                    highlights_path = self._step7_create_highlights_reel(max_clips=max_clips)
                except Exception as e:
                    error_msg = f"Step 7 failed: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)
                    warnings.append(error_msg)
            else:
                logger.info("Skipping per-game highlights reel build")

            if build_description:
                try:
                    self._step8_generate_description()
                except Exception as e:
                    warning_msg = f"Step 8 (YouTube description) failed: {e}"
                    logger.warning(warning_msg)
                    warnings.append(warning_msg)
            else:
                logger.info("Skipping YouTube description generation")

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
            self._refresh_game_context()
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
        self._refresh_game_context()
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

        self._refresh_game_context()
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

        game_start = self._ensure_ocr_engine().find_game_start(self.video_processor)

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
        ocr_engine = self._ensure_ocr_engine()

        self.video_timestamps = ocr_engine.sample_video_times(
            self.video_processor,
            sample_interval=sample_interval,
            max_samples=None,
            debug_dir=self.game_folders['data_dir'],
            parallel=parallel,
            workers=workers,
            start_time=video_start_time,
            output_dir=self.game_folders.get('data_dir'),
            game_id=ocr_game_id,
            broadcast_type=str(self._pending_broadcast_type or "auto"),
        )

        # Hybrid policy: if OCR quality is poor, run a probe pass to lock onto the most stable
        # scoreboard settings and rerun sampling before failing the pipeline.
        stats = ocr_engine.get_last_sampling_stats()
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
                    probe_report = ocr_engine.probe_video_scoreboard(
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
                self.video_timestamps = ocr_engine.sample_video_times(
                    self.video_processor,
                    sample_interval=sample_interval,
                    max_samples=None,
                    debug_dir=self.game_folders['data_dir'],
                    parallel=parallel,
                    workers=workers,
                    start_time=video_start_time,
                    output_dir=self.game_folders.get('data_dir'),
                    game_id=ocr_game_id,
                    broadcast_type=str(self._pending_broadcast_type or "auto"),
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

    def _step5_match_events(
        self,
        tolerance_seconds: int = 30,
        recording_game_start_time: Optional[float] = None,
    ):
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
            recording_game_start_time=recording_game_start_time,
        )

        # Also match typed Goal objects (new in v2.1)
        if self._goals:
            self._matched_goals = self.event_matcher.match_goals_to_video(
                self._goals,
                self.video_timestamps,
                tolerance_seconds=tolerance_seconds,
                recording_game_start_time=recording_game_start_time,
            )
            hydrated = self._hydrate_goal_events_from_typed_matches()
            if hydrated:
                logger.info(f"Hydrated {hydrated} goal events from typed goal matches")

        # Refine any low-confidence goal matches by locating the clock-stop moment
        # (the scoreboard freezes at the goal time during the stoppage).
        if self._refine_goal_clock:
            try:
                refined = self._refine_goal_events_by_clock_stop(self.matched_events)
                if refined:
                    logger.info(f"Refined {refined} goal timestamps via clock-stop OCR")
            except Exception as e:
                logger.warning(f"Goal timestamp refinement failed: {e}")
        else:
            logger.info("Skipping goal clock-stop refinement")

        # Generic fallback: for low-confidence matches, do a small local OCR scan around the
        # approximate match timestamp and snap to the closest persistent clock reading.
        if self._refine_local_ocr:
            try:
                refined_any = self._refine_low_confidence_events_by_local_ocr(self.matched_events)
                if refined_any:
                    logger.info(f"Refined {refined_any} event timestamps via local OCR")
            except Exception as e:
                logger.warning(f"Local OCR refinement failed: {e}")
        else:
            logger.info("Skipping local OCR refinement")

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

    def _ocr_clock_sample(self, t: float, *, expected_period: int, period_length: int) -> Dict:
        frame = self.video_processor.get_frame_at_time(float(t))
        if frame is None:
            return {"t": float(t), "sec": None, "period": None, "confidence": 0.0}

        ocr = None
        try:
            ocr = self._ensure_ocr_engine().extract_time_from_frame_detailed(
                frame,
                broadcast_type=self._refinement_broadcast_type(),
            )
        except Exception:
            ocr = None

        if ocr is None:
            return {"t": float(t), "sec": None, "period": None, "confidence": 0.0}

        observed_period = int(getattr(ocr, "period", 0) or 0)
        if observed_period not in {0, int(expected_period or 1)}:
            return {"t": float(t), "sec": None, "period": observed_period, "confidence": float(getattr(ocr, "confidence", 0.0) or 0.0)}

        sec = int(getattr(ocr, "time_seconds", -1) or -1)
        if not (0 <= sec <= int(period_length)):
            sec = None

        return {
            "t": float(t),
            "sec": sec,
            "period": observed_period,
            "confidence": float(getattr(ocr, "confidence", 0.0) or 0.0),
        }

    def _sample_clock_window(
        self,
        *,
        search_start: float,
        search_end: float,
        step_seconds: float,
        expected_period: int,
        period_length: int,
    ) -> List[Dict]:
        samples: List[Dict] = []
        t = float(search_start)
        while t <= float(search_end) + 1e-6:
            samples.append(
                self._ocr_clock_sample(
                    t,
                    expected_period=int(expected_period or 1),
                    period_length=int(period_length),
                )
            )
            t += float(step_seconds)
        return samples

    def _find_clock_stop_from_samples(
        self,
        samples: List[Dict],
        *,
        target_seconds: int,
        persistence_window_seconds: float,
        min_target_hits: int,
        allow_close_seconds: int = 0,
    ) -> Optional[float]:
        if not samples:
            return None

        from collections import Counter

        running_prefix: List[bool] = []
        seen_running = False
        for sample in samples:
            sec = sample.get("sec")
            if sec is not None and int(sec) > int(target_seconds):
                seen_running = True
            running_prefix.append(seen_running)

        for idx, sample in enumerate(samples):
            sec = sample.get("sec")
            if sec is None:
                continue
            if abs(int(sec) - int(target_seconds)) > int(allow_close_seconds):
                continue
            if not running_prefix[idx]:
                continue

            window_end = float(sample["t"]) + float(persistence_window_seconds)
            window_vals: List[int] = []
            target_hits = 0
            for follow in samples[idx:]:
                if float(follow["t"]) > window_end:
                    break
                follow_sec = follow.get("sec")
                if follow_sec is None:
                    continue
                follow_sec_int = int(follow_sec)
                window_vals.append(follow_sec_int)
                if abs(follow_sec_int - int(target_seconds)) <= int(allow_close_seconds):
                    target_hits += 1

            if len(window_vals) < max(3, int(min_target_hits)):
                continue

            mode_val, _mode_count = Counter(window_vals).most_common(1)[0]
            if abs(int(mode_val) - int(target_seconds)) > int(allow_close_seconds):
                continue
            if target_hits < int(min_target_hits):
                continue
            return float(sample["t"])

        return None

    def _candidate_goal_search_ranges(
        self,
        coarse_samples: List[Dict],
        *,
        target_seconds: int,
        default_start: float,
        default_end: float,
    ) -> List[tuple[float, float]]:
        candidates: List[tuple[float, float]] = []
        best = None  # (diff, t)
        seen_running = False
        prev = None

        for sample in coarse_samples:
            sec = sample.get("sec")
            if sec is None:
                continue
            sec_int = int(sec)
            if sec_int > int(target_seconds):
                seen_running = True

            diff = abs(sec_int - int(target_seconds))
            if seen_running and (best is None or diff < best[0] or (diff == best[0] and float(sample["t"]) < best[1])):
                best = (diff, float(sample["t"]))

            if prev is not None and prev.get("sec") is not None:
                prev_sec = int(prev["sec"])
                curr_sec = sec_int
                crossed_target = prev_sec > int(target_seconds) and curr_sec <= int(target_seconds)
                exact_target = curr_sec == int(target_seconds) and seen_running
                if crossed_target or exact_target:
                    left = max(float(default_start), float(prev["t"]) - 2.0)
                    right = min(float(default_end), float(sample["t"]) + 4.0)
                    candidates.append((left, right))
            prev = sample

        if not candidates and best is not None:
            candidates.append(
                (
                    max(float(default_start), float(best[1]) - 4.0),
                    min(float(default_end), float(best[1]) + 4.0),
                )
            )

        if not candidates:
            candidates.append((float(default_start), float(default_end)))

        merged: List[tuple[float, float]] = []
        for start, end in sorted(candidates, key=lambda pair: (pair[0], pair[1])):
            if not merged or start > merged[-1][1] + 0.5:
                merged.append((start, end))
            else:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        return merged[:3]

    def _refine_goal_events_by_clock_stop(
        self,
        matched_events: List[Dict],
        *,
        coarse_step_seconds: float = 2.0,
        fine_step_seconds: float = 0.25,
        lookback_seconds: float = 90.0,
        lookforward_seconds: float = 20.0,
        persistence_window_seconds: float = 8.0,
        min_target_hits: int = 4,
    ) -> int:
        """
        Refine goal video timestamps by locating the first stable clock-stop at the
        box-score goal time. Coarse OCR markers narrow the search range before a
        finer stop-time scan runs inside likely intervals.
        """
        if not matched_events:
            return 0

        refined_count = 0
        duration = float(getattr(self.video_processor, "duration", 0.0) or 0.0)
        if duration <= 0:
            return 0

        video_timestamps = self.video_timestamps or []
        allow_close_seconds = max(
            0,
            int(getattr(self.config, "GOAL_CLOCK_STOP_ALLOW_CLOSE_SECONDS", 0) or 0),
        )
        allow_projected_fallback = bool(
            getattr(self.config, "GOAL_ENABLE_PROJECTED_CLOCK_FALLBACK", False)
        )
        projected_requires_unreliable = bool(
            getattr(self.config, "GOAL_PROJECTED_CLOCK_FALLBACK_REQUIRES_UNRELIABLE", True)
        )

        for event in matched_events:
            if str(event.get("type") or "").strip().lower() != "goal":
                continue

            video_time = event.get("video_time")
            if video_time is None:
                continue

            try:
                period = int(event.get("period") or 1)
            except Exception:
                period = 1
            time_str = str(event.get("time") or "0:00").strip()

            try:
                target_seconds = int(self.event_matcher.event_time_to_remaining_seconds(period, time_str))
            except Exception:
                continue

            period_length = self._period_length_seconds(period)
            minimum_video_time = self._minimum_plausible_video_time_for_event(event)

            period_ts = [
                ts for ts in video_timestamps
                if int(ts.get("period") or 0) == period
                and ts.get("video_time") is not None
                and (
                    minimum_video_time is None
                    or float(ts.get("video_time") or 0.0) >= minimum_video_time
                )
            ]
            period_ts.sort(key=lambda t: float(t.get("video_time") or 0.0))

            anchor_before = None
            if period_ts:
                nearest_idx = min(
                    range(len(period_ts)),
                    key=lambda i: abs(float(period_ts[i]["video_time"]) - float(video_time)),
                )
                for idx in range(nearest_idx, -1, -1):
                    sample = period_ts[idx]
                    sample_sec = sample.get("game_time_seconds")
                    if sample_sec is None:
                        sample_sec = time_string_to_seconds(str(sample.get("game_time") or "0:00"))
                    try:
                        sample_sec_int = int(sample_sec)
                    except Exception:
                        continue
                    if sample_sec_int > target_seconds:
                        anchor_before = sample
                        break

            anchor_video_time = float(video_time)
            if minimum_video_time is not None:
                anchor_video_time = max(anchor_video_time, float(minimum_video_time))

            search_start = max(0.0, anchor_video_time - float(lookback_seconds))
            if anchor_before is not None:
                search_start = max(0.0, float(anchor_before["video_time"]) - 5.0)
            if minimum_video_time is not None:
                search_start = max(search_start, float(minimum_video_time))
            search_end = min(duration, anchor_video_time + float(lookforward_seconds))
            if search_end <= search_start:
                continue

            coarse_samples = self._sample_clock_window(
                search_start=search_start,
                search_end=search_end,
                step_seconds=float(coarse_step_seconds),
                expected_period=period,
                period_length=period_length,
            )
            candidate_ranges = self._candidate_goal_search_ranges(
                coarse_samples,
                target_seconds=target_seconds,
                default_start=search_start,
                default_end=search_end,
            )

            refined_time = None
            refined_method = None
            for candidate_start, candidate_end in candidate_ranges:
                fine_samples = self._sample_clock_window(
                    search_start=max(search_start, candidate_start),
                    search_end=min(search_end, candidate_end),
                    step_seconds=float(fine_step_seconds),
                    expected_period=period,
                    period_length=period_length,
                )

                refined_time = self._find_clock_stop_from_samples(
                    fine_samples,
                    target_seconds=target_seconds,
                    persistence_window_seconds=float(persistence_window_seconds),
                    min_target_hits=int(min_target_hits),
                )
                if refined_time is not None:
                    refined_method = "clock_stop"
                    break

                if allow_close_seconds > 0:
                    refined_time = self._find_clock_stop_from_samples(
                        fine_samples,
                        target_seconds=target_seconds,
                        persistence_window_seconds=max(4.0, float(persistence_window_seconds) / 2.0),
                        min_target_hits=max(3, int(min_target_hits) - 1),
                        allow_close_seconds=int(allow_close_seconds),
                    )
                    if refined_time is not None:
                        refined_method = "clock_stop"
                        break

                fallback_allowed = allow_projected_fallback and (
                    not projected_requires_unreliable or bool(event.get("match_unreliable"))
                )
                if not fallback_allowed:
                    continue

                best = None  # (diff, -confidence, t, sec)
                seen_running = False
                for sample in fine_samples:
                    sec = sample.get("sec")
                    if sec is None:
                        continue
                    sec_int = int(sec)
                    if sec_int > target_seconds:
                        seen_running = True
                    if not seen_running:
                        continue
                    diff = abs(sec_int - target_seconds)
                    candidate = (diff, -float(sample.get("confidence") or 0.0), float(sample["t"]), sec_int)
                    if best is None or candidate < best:
                        best = candidate
                if best is not None:
                    observed_t = float(best[2])
                    observed_sec = int(best[3])
                    projected_time = observed_t + float(observed_sec - int(target_seconds))
                    projected_time = min(max(projected_time, 0.0), duration)
                    if minimum_video_time is not None:
                        projected_time = max(projected_time, float(minimum_video_time))
                    refined_time = float(projected_time)
                    refined_method = "closest_clock_projected"
                    break

            if refined_time is None or refined_method is None:
                continue

            if "video_time_original" not in event:
                event["video_time_original"] = float(video_time)
            event["video_time"] = float(refined_time)
            event["refined_by"] = refined_method
            refined_count += 1

            logger.info(
                "%s goal P%s %s: %.1fs -> %.1fs",
                "Refined" if refined_method == "clock_stop" else "Best-effort refined",
                period,
                time_str,
                float(video_time),
                float(refined_time),
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

            # Goals-only reels do not benefit from rescanning penalties or other
            # non-goal events during the generic local-OCR fallback.
            if self.reel_mode == "goals_only" and str(event.get("type") or "").strip().lower() != "goal":
                continue

            # Skip events already refined by the stronger clock-stop mechanism.
            # "closest_clock" is only a best-effort fallback and still benefits
            # from the generic local OCR pass.
            if str(event.get("refined_by") or "") in {"clock_stop", "manual_source_review"}:
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

        period_length = self._period_length_seconds(period)
        if not (0 <= target_remaining <= period_length):
            return None

        event_type = str(event.get("type") or "").strip().lower()
        is_goal = event_type == "goal"
        window_seconds = float(getattr(self.config, "EVENT_LOCAL_OCR_WINDOW_SECONDS", 60.0))
        step_seconds = float(getattr(self.config, "EVENT_LOCAL_OCR_STEP_SECONDS", 0.5))
        persistence_window_seconds = float(getattr(self.config, "EVENT_LOCAL_OCR_PERSISTENCE_WINDOW_SECONDS", 6.0))
        min_target_hits = int(getattr(self.config, "EVENT_LOCAL_OCR_MIN_HITS", 3))
        max_diff_seconds = float(getattr(self.config, "EVENT_LOCAL_OCR_MAX_DIFF_SECONDS", 6.0))
        goal_allow_close_seconds = max(
            0,
            int(getattr(self.config, "GOAL_LOCAL_OCR_ALLOW_CLOSE_SECONDS", 0) or 0),
        )
        goal_allow_closest_fallback = bool(
            getattr(self.config, "GOAL_ENABLE_LOCAL_OCR_CLOSEST_FALLBACK", False)
        )
        goal_closest_requires_unreliable = bool(
            getattr(self.config, "GOAL_LOCAL_OCR_CLOSEST_FALLBACK_REQUIRES_UNRELIABLE", True)
        )
        goal_closest_active = (
            is_goal
            and goal_allow_closest_fallback
            and (
                not goal_closest_requires_unreliable
                or bool(event.get("match_unreliable"))
            )
        )

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

        minimum_video_time = self._minimum_plausible_video_time_for_event(event)
        center_time = float(approx_video_time)
        if minimum_video_time is not None and center_time < minimum_video_time:
            center_time = minimum_video_time

        start = max(0.0, center_time - window_seconds)
        if minimum_video_time is not None:
            start = max(start, minimum_video_time)
        end = min(duration, center_time + window_seconds)
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
                ocr = self._ensure_ocr_engine().extract_time_from_frame_detailed(
                    frame,
                    broadcast_type=self._refinement_broadcast_type(),
                )
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

                    if is_goal:
                        if diff <= float(goal_allow_close_seconds):
                            hits.append({"t": float(t), "diff": int(diff), "conf": conf})
                    elif diff <= max_diff_seconds:
                        hits.append({"t": float(t), "diff": int(diff), "conf": conf})

            t += step_seconds

        if not hits:
            # Fallback to best single-frame candidate if it's close enough.
            if is_goal and not goal_closest_active:
                return None
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
        if is_goal and not goal_closest_active:
            return None
        hits.sort(key=lambda h: (h["diff"], -h["conf"], h["t"]))
        return float(hits[0]["t"])

    def _goal_clip_window(
        self,
        goal: Dict,
        *,
        before_seconds: float,
        after_seconds: float,
    ) -> tuple[float, float]:
        try:
            conf_f = float(goal.get("match_confidence") or 0.0)
        except Exception:
            conf_f = 0.0
        try:
            diff_f = abs(float(goal.get("match_time_diff_seconds") or 0.0))
        except Exception:
            diff_f = 0.0
        try:
            period = int(goal.get("period") or 1)
        except Exception:
            period = 1
        try:
            original_video_time = float(goal.get("video_time_original"))
        except Exception:
            original_video_time = None
        try:
            current_video_time = float(goal.get("video_time"))
        except Exception:
            current_video_time = None

        refined_by = str(goal.get("refined_by") or "").strip().lower()
        special = str(goal.get("special") or "").strip().upper()
        is_power_play = bool(goal.get("power_play")) or special == "PP"
        is_ot = period >= 4

        clip_before = float(before_seconds)
        clip_after = float(after_seconds)

        if refined_by in {"clock_stop", "manual_source_review"}:
            clip_before = max(
                clip_before,
                float(getattr(self.config, "GOAL_CLOCK_STOP_BEFORE_SECONDS", 32.0) or 32.0),
            )
            clip_after = min(
                clip_after,
                float(getattr(self.config, "GOAL_CLOCK_STOP_AFTER_SECONDS", 3.0) or 3.0),
            )
        else:
            extra = min(20.0, max(0.0, diff_f + 5.0))
            clip_before = max(
                float(before_seconds) + extra,
                float(getattr(self.config, "GOAL_FALLBACK_BEFORE_SECONDS", 20.0) or 20.0),
            )
            clip_after = min(
                clip_after,
                float(getattr(self.config, "GOAL_FALLBACK_AFTER_SECONDS", 4.0) or 4.0),
            )

        if conf_f < 0.95 or bool(goal.get("match_unreliable")):
            clip_before = max(clip_before, float(getattr(self.config, "GOAL_FALLBACK_BEFORE_SECONDS", 20.0) or 20.0))

        if is_ot:
            clip_before = max(
                clip_before,
                float(getattr(self.config, "GOAL_OT_BEFORE_SECONDS", 60.0) or 60.0),
            )
            clip_after = min(
                clip_after,
                float(getattr(self.config, "GOAL_OT_AFTER_SECONDS", 4.0) or 4.0),
            )
            if is_power_play:
                clip_before = max(
                    clip_before,
                    float(getattr(self.config, "GOAL_OT_POWER_PLAY_BEFORE_SECONDS", 120.0) or 120.0),
                )

        if (
            refined_by == "closest_clock_projected"
            and original_video_time is not None
            and current_video_time is not None
        ):
            projection_delta = float(current_video_time) - float(original_video_time)
            if projection_delta > 0:
                # Keep the earlier setup when we had to project the true stop time later.
                clip_before += float(projection_delta)
            elif projection_delta < 0:
                clip_after += abs(float(projection_delta))

        return (float(clip_before), float(clip_after))

    def _step6_create_clips(
        self,
        before_seconds: float = 15.0,
        after_seconds: float = 4.0
    ):
        """Step 6: Create individual highlight clips for the selected reel mode."""
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

        pp_penalty_map = {}
        if self._include_pp_penalty_clips():
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
                    penalty_video_time = self._find_penalty_video_time(penalty_info)
                    if penalty_video_time is not None:
                        penalty_info.video_time = penalty_video_time
                        logger.debug(f"Penalty P{penalty_info.period} {penalty_info.time} matched to video time {penalty_video_time:.1f}s")
                    else:
                        try:
                            goal_event = goal_events[int(goal_idx)]
                            goal_video_time = goal_event.get("video_time")
                            if goal_video_time is not None:
                                goal_remaining = self.event_matcher.event_time_to_remaining_seconds(
                                    goal_event.get("period"), str(goal_event.get("time", "0:00"))
                                )
                                goal_abs = self._period_time_to_absolute_seconds(int(goal_event.get("period") or 1), int(goal_remaining))
                                pen_abs = self._period_time_to_absolute_seconds(int(penalty_info.period or 1), int(penalty_info.time_seconds))
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
        else:
            logger.info("Skipping PP penalty clip insertion for reel mode '%s'", self.reel_mode)

        # Build final events list with penalty clips inserted before PP goals
        final_events = []
        penalty_before = getattr(self.config, 'PENALTY_PP_BEFORE_SECONDS', 3.0)
        penalty_after = getattr(self.config, 'PENALTY_PP_AFTER_SECONDS', 3.0)
        inserted_penalty_clips = 0

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
                    inserted_penalty_clips += 1
                    logger.info(f"Adding penalty clip: {penalty_info.player_name} - {penalty_info.infraction} ({penalty_info.minutes} min)")
                else:
                    logger.warning(f"Could not find video time for penalty P{penalty_info.period} {penalty_info.time}")

            goal_before, goal_after = self._goal_clip_window(
                goal,
                before_seconds=float(before_seconds),
                after_seconds=float(after_seconds),
            )
            goal['before_seconds'] = goal_before
            goal['after_seconds'] = goal_after

            final_events.append(goal)

        logger.info(
            "Creating %s highlight clips (%s inserted penalty clips + %s goal clips)...",
            len(final_events),
            inserted_penalty_clips,
            len(goal_events),
        )

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
        period_length = self._period_length_seconds(penalty_period)
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

        period_length = self._period_length_seconds(penalty_period)
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
                result = self._ensure_ocr_engine().extract_time_from_frame(frame)
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

        if not self._requires_major_review_workflow():
            logger.info("Skipping major penalty workflow for reel mode '%s'", self.reel_mode)
            self._step_timings['major_penalties'] = time.time() - start_time
            return

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
            ocr_engine=self._ensure_ocr_engine(),
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
