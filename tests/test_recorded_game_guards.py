from types import SimpleNamespace

from highlight_extractor.event_matcher import EventMatcher
from highlight_extractor.ocr_engine import OCREngine


def test_event_matcher_rejects_warmup_timestamp_before_plausible_game_time():
    config = SimpleNamespace(
        BOX_SCORE_TIME_IS_ELAPSED=True,
        EVENT_ENFORCE_MIN_VIDEO_TIME_FROM_GAME_START=True,
        EVENT_MIN_VIDEO_TIME_BUFFER_SECONDS=240.0,
    )
    matcher = EventMatcher(config)
    event = {"type": "goal", "period": 1, "time": "15:21", "team": "Amherst Ramblers"}
    timestamps = [
        {"video_time": 660.0, "period": 1, "game_time": "4:39", "game_time_seconds": 279},
        {"video_time": 1620.0, "period": 1, "game_time": "4:39", "game_time_seconds": 279},
    ]

    matched = matcher._find_closest_timestamp_with_confidence(
        event,
        timestamps,
        tolerance_seconds=5,
        recording_game_start_time=0.0,
    )

    assert matched is not None
    assert matched[0] == 1620.0


def test_find_game_start_ignores_lone_late_period_fallback():
    observed = []

    class StubVideoProcessor:
        duration = 10824.0

        @staticmethod
        def get_frame_at_time(timestamp):
            return timestamp

    engine = object.__new__(OCREngine)

    def fake_extract(frame, *args, **kwargs):
        observed.append(float(frame))
        if float(frame) >= 1920.0:
            return (1, "0:21")
        return None

    engine.extract_time_from_frame = fake_extract  # type: ignore[method-assign]

    start = engine.find_game_start(StubVideoProcessor())

    assert start is None
    assert observed
    assert observed[0] == 900.0


def test_find_game_start_detects_warmup_to_game_clock_reset():
    class StubVideoProcessor:
        duration = 10824.0

        @staticmethod
        def get_frame_at_time(timestamp):
            return timestamp

    engine = object.__new__(OCREngine)

    def fake_extract(frame, *args, **kwargs):
        timestamp = float(frame)
        if abs(timestamp - 900.0) < 1e-6:
            return (1, "1:05")
        if abs(timestamp - 2100.0) < 1e-6:
            return (1, "19:49")
        return None

    engine.extract_time_from_frame = fake_extract  # type: ignore[method-assign]

    start = engine.find_game_start(StubVideoProcessor())

    assert start is not None
    assert start == 2086.0
