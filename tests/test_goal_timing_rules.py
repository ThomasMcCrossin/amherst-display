from types import SimpleNamespace

from highlight_extractor.pipeline import HighlightPipeline
from highlight_extractor.time_utils import period_length_seconds


class DummyVideoProcessor:
    duration = 120.0

    def get_frame_at_time(self, _timestamp):
        return object()


class SequenceOcrEngine:
    def __init__(self, samples):
        self._samples = iter(samples)

    def extract_time_from_frame_detailed(self, *args, **kwargs):
        return next(self._samples, None)


class StubEventMatcher:
    def event_time_to_remaining_seconds(self, _period, _time_str):
        return 600


def _make_clock_sample(t: float, sec: int | None, confidence: float = 90.0):
    return {"t": float(t), "sec": sec, "period": 1, "confidence": float(confidence)}


def test_playoff_overtime_rules_use_ten_then_twenty_minutes():
    playoff_context = {"playoff": True}

    assert period_length_seconds(4, playoff_context) == 10 * 60
    assert period_length_seconds(5, playoff_context) == 20 * 60
    assert period_length_seconds(6, playoff_context) == 20 * 60


def test_goal_local_ocr_requires_exact_goal_time_by_default(tmp_path):
    config = SimpleNamespace(
        DEFAULT_REEL_MODE="goals_only",
        EVENT_LOCAL_OCR_WINDOW_SECONDS=1.0,
        EVENT_LOCAL_OCR_STEP_SECONDS=1.0,
        EVENT_LOCAL_OCR_PERSISTENCE_WINDOW_SECONDS=0.0,
        EVENT_LOCAL_OCR_MIN_HITS=1,
        EVENT_LOCAL_OCR_MAX_DIFF_SECONDS=6.0,
        GOAL_LOCAL_OCR_ALLOW_CLOSE_SECONDS=0,
        GOAL_ENABLE_LOCAL_OCR_CLOSEST_FALLBACK=False,
        GOAL_LOCAL_OCR_CLOSEST_FALLBACK_REQUIRES_UNRELIABLE=True,
    )
    ocr_samples = [
        SimpleNamespace(period=1, time_seconds=601, confidence=90.0),
        SimpleNamespace(period=1, time_seconds=599, confidence=90.0),
        SimpleNamespace(period=1, time_seconds=598, confidence=90.0),
    ]
    pipeline = HighlightPipeline(
        config=config,
        video_path=tmp_path / "dummy.mp4",
        video_processor=DummyVideoProcessor(),
        ocr_engine=SequenceOcrEngine(ocr_samples),
        event_matcher=StubEventMatcher(),
    )

    refined = pipeline._refine_event_video_time_by_local_ocr(
        {"type": "goal", "period": 1, "time": "10:00", "match_unreliable": True},
        approx_video_time=10.0,
    )

    assert refined is None


def test_goal_clock_stop_uses_first_stable_exact_match(tmp_path):
    config = SimpleNamespace(
        DEFAULT_REEL_MODE="goals_only",
        GOAL_CLOCK_STOP_ALLOW_CLOSE_SECONDS=0,
        GOAL_ENABLE_PROJECTED_CLOCK_FALLBACK=False,
    )
    pipeline = HighlightPipeline(
        config=config,
        video_path=tmp_path / "dummy.mp4",
        video_processor=DummyVideoProcessor(),
        event_matcher=StubEventMatcher(),
    )
    pipeline.video_timestamps = [{"period": 1, "video_time": 50.0, "game_time_seconds": 605}]

    samples = iter([
        [_make_clock_sample(50.0, 605), _make_clock_sample(52.0, 600)],
        [
            _make_clock_sample(50.0, 605),
            _make_clock_sample(50.5, 604),
            _make_clock_sample(51.0, 600),
            _make_clock_sample(51.25, 600),
            _make_clock_sample(51.5, 600),
            _make_clock_sample(51.75, 600),
        ],
    ])
    pipeline._sample_clock_window = lambda **kwargs: next(samples)  # type: ignore[method-assign]
    pipeline._candidate_goal_search_ranges = lambda *args, **kwargs: [(49.0, 54.0)]  # type: ignore[method-assign]

    matched_events = [{"type": "goal", "period": 1, "time": "10:00", "video_time": 50.0}]
    refined = pipeline._refine_goal_events_by_clock_stop(matched_events)

    assert refined == 1
    assert matched_events[0]["video_time"] == 51.0
    assert matched_events[0]["refined_by"] == "clock_stop"


def test_goal_clock_stop_does_not_project_by_default(tmp_path):
    config = SimpleNamespace(
        DEFAULT_REEL_MODE="goals_only",
        GOAL_CLOCK_STOP_ALLOW_CLOSE_SECONDS=0,
        GOAL_ENABLE_PROJECTED_CLOCK_FALLBACK=False,
    )
    pipeline = HighlightPipeline(
        config=config,
        video_path=tmp_path / "dummy.mp4",
        video_processor=DummyVideoProcessor(),
        event_matcher=StubEventMatcher(),
    )
    pipeline.video_timestamps = [{"period": 1, "video_time": 50.0, "game_time_seconds": 605}]

    samples = iter([
        [_make_clock_sample(50.0, 605), _make_clock_sample(52.0, 599)],
        [
            _make_clock_sample(50.0, 605),
            _make_clock_sample(50.5, 604),
            _make_clock_sample(51.0, 599),
            _make_clock_sample(51.25, 599),
            _make_clock_sample(51.5, 599),
            _make_clock_sample(51.75, 599),
        ],
    ])
    pipeline._sample_clock_window = lambda **kwargs: next(samples)  # type: ignore[method-assign]
    pipeline._candidate_goal_search_ranges = lambda *args, **kwargs: [(49.0, 54.0)]  # type: ignore[method-assign]

    matched_events = [{"type": "goal", "period": 1, "time": "10:00", "video_time": 50.0, "match_unreliable": True}]
    refined = pipeline._refine_goal_events_by_clock_stop(matched_events)
    pipeline._finalize_goal_timing_verification(matched_events)

    assert refined == 0
    assert matched_events[0]["goal_clock_verified"] is False
    assert matched_events[0]["goal_timing_source"] == "unverified_match_approximation"
    assert matched_events[0]["match_unreliable"] is True
    assert "Exact goal clock-stop verification not found" in matched_events[0]["match_unreliable_reason"]


def test_goal_clock_stop_legacy_projection_is_opt_in(tmp_path):
    config = SimpleNamespace(
        DEFAULT_REEL_MODE="goals_only",
        GOAL_ENABLE_LEGACY_TIMING_FALLBACK=True,
        GOAL_CLOCK_STOP_ALLOW_CLOSE_SECONDS=0,
        GOAL_ENABLE_PROJECTED_CLOCK_FALLBACK=False,
        GOAL_PROJECTED_CLOCK_FALLBACK_REQUIRES_UNRELIABLE=True,
    )
    pipeline = HighlightPipeline(
        config=config,
        video_path=tmp_path / "dummy.mp4",
        video_processor=DummyVideoProcessor(),
        event_matcher=StubEventMatcher(),
    )
    pipeline.video_timestamps = [{"period": 1, "video_time": 50.0, "game_time_seconds": 605}]

    samples = iter([
        [_make_clock_sample(50.0, 605), _make_clock_sample(52.0, 599)],
        [
            _make_clock_sample(50.0, 605),
            _make_clock_sample(50.5, 604),
            _make_clock_sample(51.0, 599),
            _make_clock_sample(51.25, 599),
            _make_clock_sample(51.5, 599),
            _make_clock_sample(51.75, 599),
        ],
    ])
    pipeline._sample_clock_window = lambda **kwargs: next(samples)  # type: ignore[method-assign]
    pipeline._candidate_goal_search_ranges = lambda *args, **kwargs: [(49.0, 54.0)]  # type: ignore[method-assign]

    matched_events = [{"type": "goal", "period": 1, "time": "10:00", "video_time": 50.0, "match_unreliable": True}]
    refined = pipeline._refine_goal_events_by_clock_stop(matched_events)
    pipeline._finalize_goal_timing_verification(matched_events)

    assert refined == 1
    assert matched_events[0]["refined_by"] == "closest_clock_projected"
    assert matched_events[0]["goal_clock_verified"] is False
    assert matched_events[0]["goal_timing_source"] == "legacy_projected_clock_fallback"
