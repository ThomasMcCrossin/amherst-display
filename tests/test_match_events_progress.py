"""match_events speed and intra-step progress (amherst-display#48).

The clock-stop pass reads hundreds of scorebug frames; it now reads them with MATCH_OCR_WORKERS
in flight and reports n/total on the shared progress record. These tests call the sampler and the
vision locator as step 5 does and assert the reads, the ROI, and the progress line.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import numpy as np
import pytest

import goal_locator
import pipeline_progress
import vision_api
from highlight_extractor.pipeline import HighlightPipeline, _StepProgress


class FrameVideoProcessor:
    """Stands in for the MoviePy reader: hands back the frame time, records every read."""

    duration = 120.0

    def __init__(self) -> None:
        self.reads: list[float] = []

    def get_frame_at_time(self, t):
        self.reads.append(float(t))
        return SimpleNamespace(t=float(t), shape=(720, 1280, 3))


class StoppedClockOcr:
    """A scorebug clock that runs down to 10:00 and then freezes."""

    def __init__(self, stop_at: float = 60.0) -> None:
        self.stop_at = stop_at
        self.reads: list[dict] = []

    def set_broadcast_type(self, value: str) -> None:
        self.broadcast_type = value

    def detect_scoreboard_roi(self, frame, method: str = "auto"):
        return (346, 68, 70, 49)

    def extract_time_from_frame_detailed(self, frame, roi=None, broadcast_type="auto"):
        self.reads.append({"t": frame.t, "roi": roi, "broadcast_type": broadcast_type})
        return SimpleNamespace(period=1, time_seconds=620 if frame.t < self.stop_at else 600, confidence=90.0)


class StubEventMatcher:
    def event_time_to_remaining_seconds(self, _period, _time_str):
        return 600


def _pipeline(tmp_path, ocr_engine, video=None):
    config = SimpleNamespace(DEFAULT_REEL_MODE="goals_only", GOAL_VISION_LOCATOR=False)
    pipeline = HighlightPipeline(
        config=config,
        video_path=tmp_path / "dummy.mp4",
        video_processor=video or FrameVideoProcessor(),
        ocr_engine=ocr_engine,
        event_matcher=StubEventMatcher(),
    )
    pipeline._pending_broadcast_type = "flo_stacked_topleft"
    return pipeline


def test_clock_samples_are_read_in_order_with_the_pinned_roi(tmp_path, monkeypatch):
    monkeypatch.setenv("MATCH_OCR_CAPTURE", "moviepy")
    monkeypatch.setenv("MATCH_OCR_WORKERS", "4")
    ocr = StoppedClockOcr(stop_at=12.0)
    pipeline = _pipeline(tmp_path, ocr)

    samples = pipeline._ocr_clock_samples(
        [10.0, 10.5, 11.0, 11.5, 12.0, 12.5, 13.0],
        expected_period=1,
        period_length=1200,
    )

    assert [sample["t"] for sample in samples] == [10.0, 10.5, 11.0, 11.5, 12.0, 12.5, 13.0]
    assert [sample["sec"] for sample in samples] == [620, 620, 620, 620, 600, 600, 600]
    # The pinned scorebug box is detected once and passed in, not re-detected per frame.
    assert {read["roi"] for read in ocr.reads} == {(346, 68, 70, 49)}
    assert {read["broadcast_type"] for read in ocr.reads} == {"flo_stacked_topleft"}
    # One frame at 0 s detects the pinned box; the samples follow, in time order.
    assert pipeline.video_processor.reads == [0.0, 10.0, 10.5, 11.0, 11.5, 12.0, 12.5, 13.0]


def test_clock_stop_refinement_progress_counts_every_sample(tmp_path, monkeypatch):
    monkeypatch.setenv("MATCH_OCR_CAPTURE", "moviepy")
    monkeypatch.setenv("MATCH_OCR_WORKERS", "4")
    status = tmp_path / "highlights-status.json"
    monkeypatch.setattr(pipeline_progress.progress, "path", status)
    monkeypatch.setattr(pipeline_progress.progress, "_last_write", 0.0)
    pipeline = _pipeline(tmp_path, StoppedClockOcr())
    pipeline.video_timestamps = [{"period": 1, "video_time": 40.0, "game_time_seconds": 640}]

    pipeline_progress.progress.stage("match_events")
    step_progress = _StepProgress()
    refined = pipeline._refine_goal_events_by_clock_stop(
        [{"type": "goal", "period": 1, "time": "10:00", "video_time": 50.0}],
        step_progress=step_progress,
    )

    progress = json.loads(status.read_text())["progress"]
    assert refined == 1
    assert progress["stage"] == "match_events"
    assert progress["done"] == step_progress.total > 0
    assert "P1 10:00" in progress["line"]
    pipeline_progress.progress.finish("test")


def test_goal_locator_labels_every_batch_of_every_bracket(tmp_path, monkeypatch):
    """One bracket of 100 s is five requests: all of them go out and all their labels come back."""
    seen: list[list[float]] = []

    class FakeCapture:
        def __init__(self, _path):
            self.t = 0.0

        def set(self, _prop, value):
            self.t = float(value) / 1000.0

        def read(self):
            return True, np.zeros((72, 128, 3), np.uint8)

        def release(self):
            pass

    def fake_chat(base_url, api_key, body, timeout):
        times = [float(part["text"].split("=")[1]) for part in body["messages"][0]["content"]
                 if part["type"] == "text" and part["text"].startswith("t=")]
        seen.append(times)
        frames = [{"t": t, "label": "celebration" if t < 140 else "play"} for t in times]
        return {"choices": [{"message": {"content": json.dumps({"frames": frames})}}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5}}

    monkeypatch.setattr(goal_locator.cv2, "VideoCapture", FakeCapture)
    monkeypatch.setattr(goal_locator, "_vision_settings", lambda: ("key", "https://example.test/v1", "model"))
    monkeypatch.setattr(vision_api, "chat", fake_chat)
    monkeypatch.setenv("GOAL_LOCATOR_WORKERS", "6")
    planned: list[tuple[int, str]] = []
    ticks: list[str] = []
    progress = SimpleNamespace(
        plan=lambda n, detail="": planned.append((n, detail)),
        tick=lambda n=1, detail=None: ticks.append(detail or ""),
        note=lambda detail: None,
    )

    report = goal_locator.locate_goals(
        "unused.mp4",
        [{"period": 1, "time": "10:00"}],
        [{"period": 1, "video_time": 100.0, "game_time_seconds": 640},
         {"period": 1, "video_time": 130.0, "game_time_seconds": 610},
         {"period": 1, "video_time": 280.0, "game_time_seconds": 590}],
        lambda goal: 600,
        progress=progress,
    )

    assert len(seen) == 2  # 26 frames at 6 s over a 150 s bracket: two requests
    assert sorted(t for batch in seen for t in batch) == [float(t) for t in range(130, 280, 6)] + [280.0]
    assert report["usage"] == {"prompt_tokens": 20, "completion_tokens": 10}
    assert report["brackets"][0]["bracket"] == [130.0, 280.0]
    assert report["brackets"][0]["candidates"] == [130.0]
    assert planned and planned[0][0] == 2
    assert ticks == ["vision locator: 1/2 frame batches read", "vision locator: 2/2 frame batches read"]
