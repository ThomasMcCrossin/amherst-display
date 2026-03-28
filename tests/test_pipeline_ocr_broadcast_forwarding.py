from pathlib import Path
from types import SimpleNamespace

from highlight_extractor.goal import Goal
from highlight_extractor.pipeline import HighlightPipeline


class DummyVideoProcessor:
    duration = 100.0


class RecordingOcrEngine:
    def __init__(self):
        self.calls = []
        self.broadcast_type = "auto"

    def set_broadcast_type(self, value):
        self.broadcast_type = value

    def sample_video_times(self, *args, **kwargs):
        self.calls.append(kwargs)
        return [{"video_time": 10.0, "period": 1, "game_time": "10:00", "game_time_seconds": 600}]

    def get_last_sampling_stats(self):
        return {"success_rate": 1.0, "period_rate": 1.0, "avg_confidence": 90.0}

    def extract_time_from_frame(self, *args, **kwargs):
        self.calls.append(kwargs)
        return (1, "10:00")

    def extract_time_from_frame_detailed(self, *args, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(period=1, time_seconds=600, confidence=90.0)


def test_pipeline_forwards_broadcast_type_to_ocr_sampling(tmp_path: Path):
    config = SimpleNamespace(
        DEFAULT_REEL_MODE="goals_only",
        OCR_MIN_SUCCESS_RATE=0.05,
        OCR_MIN_PERIOD_RATE=0.20,
        OCR_MIN_AVG_CONFIDENCE=55.0,
    )
    ocr_engine = RecordingOcrEngine()
    pipeline = HighlightPipeline(
        config=config,
        video_path=tmp_path / "dummy.mp4",
        video_processor=DummyVideoProcessor(),
        ocr_engine=ocr_engine,
    )
    pipeline.game_folders = {"data_dir": tmp_path, "game_dir": tmp_path}
    pipeline._pending_broadcast_type = "flohockey"

    pipeline._step4_extract_timestamps(sample_interval=15, parallel=False, workers=1, start_time=0.0)

    assert len(ocr_engine.calls) == 1
    assert ocr_engine.calls[0]["broadcast_type"] == "flohockey"


class RefinementVideoProcessor:
    duration = 120.0

    def get_frame_at_time(self, _timestamp):
        return object()


class StubEventMatcher:
    def event_time_to_remaining_seconds(self, _period, _time_str):
        return 600


def test_pipeline_forwards_broadcast_type_to_local_refinement(tmp_path: Path):
    config = SimpleNamespace(
        DEFAULT_REEL_MODE="goals_only",
        EVENT_LOCAL_OCR_WINDOW_SECONDS=1.0,
        EVENT_LOCAL_OCR_STEP_SECONDS=1.0,
        EVENT_LOCAL_OCR_PERSISTENCE_WINDOW_SECONDS=0.0,
        EVENT_LOCAL_OCR_MIN_HITS=1,
        EVENT_LOCAL_OCR_MAX_DIFF_SECONDS=1.0,
    )
    ocr_engine = RecordingOcrEngine()
    pipeline = HighlightPipeline(
        config=config,
        video_path=tmp_path / "dummy.mp4",
        video_processor=RefinementVideoProcessor(),
        ocr_engine=ocr_engine,
        event_matcher=StubEventMatcher(),
    )
    pipeline._pending_broadcast_type = "mhl_summerside"

    refined = pipeline._refine_event_video_time_by_local_ocr(
        {"period": 1, "time": "10:00"},
        approx_video_time=10.0,
    )

    assert refined is not None
    assert any(call.get("broadcast_type") == "mhl_summerside" for call in ocr_engine.calls)


def test_goals_only_local_refinement_skips_goals_without_legacy_fallback(tmp_path: Path):
    config = SimpleNamespace(DEFAULT_REEL_MODE="goals_only")
    pipeline = HighlightPipeline(
        config=config,
        video_path=tmp_path / "dummy.mp4",
        video_processor=RefinementVideoProcessor(),
        ocr_engine=RecordingOcrEngine(),
        event_matcher=StubEventMatcher(),
    )
    pipeline.reel_mode = "goals_only"

    scanned = []

    def fake_refine(event, *, approx_video_time):
        scanned.append((event.get("type"), event.get("time"), approx_video_time))
        return approx_video_time - 1.0

    pipeline._refine_event_video_time_by_local_ocr = fake_refine  # type: ignore[method-assign]

    matched_events = [
        {"type": "penalty", "time": "10:00", "video_time": 10.0, "match_confidence": 0.1, "match_time_diff_seconds": 20.0},
        {"type": "goal", "time": "9:00", "video_time": 20.0, "match_confidence": 0.1, "match_time_diff_seconds": 20.0, "refined_by": "clock_stop"},
        {"type": "goal", "time": "8:00", "video_time": 30.0, "match_confidence": 0.1, "match_time_diff_seconds": 20.0, "refined_by": "closest_clock"},
        {"type": "goal", "time": "7:00", "video_time": 40.0, "match_confidence": 0.1, "match_time_diff_seconds": 20.0},
    ]

    refined = pipeline._refine_low_confidence_events_by_local_ocr(matched_events)

    assert refined == 0
    assert scanned == []


def test_goal_local_refinement_requires_explicit_legacy_fallback(tmp_path: Path):
    config = SimpleNamespace(
        DEFAULT_REEL_MODE="goals_only",
        GOAL_ENABLE_LEGACY_TIMING_FALLBACK=True,
    )
    pipeline = HighlightPipeline(
        config=config,
        video_path=tmp_path / "dummy.mp4",
        video_processor=RefinementVideoProcessor(),
        ocr_engine=RecordingOcrEngine(),
        event_matcher=StubEventMatcher(),
    )
    pipeline.reel_mode = "goals_only"

    scanned = []

    def fake_refine(event, *, approx_video_time):
        scanned.append((event.get("type"), event.get("time"), approx_video_time))
        return approx_video_time - 1.0

    pipeline._refine_event_video_time_by_local_ocr = fake_refine  # type: ignore[method-assign]

    matched_events = [
        {"type": "goal", "time": "8:00", "video_time": 30.0, "match_confidence": 0.1, "match_time_diff_seconds": 20.0, "match_unreliable": True, "refined_by": "closest_clock"},
        {"type": "goal", "time": "7:00", "video_time": 40.0, "match_confidence": 0.1, "match_time_diff_seconds": 20.0, "match_unreliable": True},
    ]

    refined = pipeline._refine_low_confidence_events_by_local_ocr(matched_events)

    assert refined == 2
    assert scanned == [("goal", "8:00", 30.0), ("goal", "7:00", 40.0)]
    assert matched_events[0]["refined_by"] == "local_ocr"
    assert matched_events[1]["refined_by"] == "local_ocr"


def test_pipeline_hydrates_goal_events_from_typed_matches(tmp_path: Path):
    pipeline = HighlightPipeline(
        config=SimpleNamespace(DEFAULT_REEL_MODE="goals_only"),
        video_path=tmp_path / "dummy.mp4",
        video_processor=DummyVideoProcessor(),
        ocr_engine=RecordingOcrEngine(),
    )
    pipeline.matched_events = [
        {
            "type": "goal",
            "period": 1,
            "time": "10:00",
            "team": "Amherst Ramblers",
            "scorer": "Jane Doe",
            "video_time": None,
        }
    ]
    pipeline._matched_goals = [
        Goal(
            period=1,
            time="10:00",
            team="Amherst Ramblers",
            scorer="Jane Doe",
            assist1="Player One",
            assist2="Player Two",
            video_time=123.0,
            match_confidence=0.9,
        )
    ]

    hydrated = pipeline._hydrate_goal_events_from_typed_matches()

    assert hydrated == 1
    assert pipeline.matched_events[0]["video_time"] == 123.0
    assert pipeline.matched_events[0]["match_confidence"] == 0.9
    assert pipeline.matched_events[0]["assist1"] == "Player One"
    assert pipeline.matched_events[0]["assist2"] == "Player Two"
