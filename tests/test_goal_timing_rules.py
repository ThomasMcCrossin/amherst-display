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
