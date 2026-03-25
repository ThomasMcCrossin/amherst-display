import json
from pathlib import Path

from highlight_extractor.major_penalty_handler import create_major_review_clip
from highlight_extractor.penalty_analyzer import PenaltyInfo


class _DummyClip:
    def close(self) -> None:
        return None


class _DummyVideoProcessor:
    def __init__(self, duration: float = 1000.0):
        self.duration = duration
        self.calls = []

    def create_clip(self, start_time: float, end_time: float, output_path: Path):
        self.calls.append((float(start_time), float(end_time), str(output_path)))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"dummy")
        return _DummyClip()


class _Config:
    MAJOR_PENALTY_BEFORE_SECONDS = 10.0
    MAJOR_PENALTY_AFTER_SECONDS = 110.0


def test_create_major_review_clip_applies_pre_buffer(tmp_path: Path) -> None:
    vp = _DummyVideoProcessor(duration=500.0)
    penalty = PenaltyInfo(
        period=1,
        time="10:00",
        time_seconds=600,
        team="ramblers",
        player_name="Test Player",
        player_number=None,
        infraction="Fighting",
        minutes=5,
        is_major=True,
        video_time=100.0,
    )

    clip_path, json_path = create_major_review_clip(vp, [penalty], tmp_path, _Config)
    assert clip_path.exists()
    assert json_path.exists()
    assert vp.calls

    start, end, _ = vp.calls[-1]
    assert start == 90.0
    assert end == 210.0

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["clip_video_start"] == 90.0


def test_create_major_review_clip_clamps_at_zero(tmp_path: Path) -> None:
    vp = _DummyVideoProcessor(duration=500.0)
    penalty = PenaltyInfo(
        period=1,
        time="10:00",
        time_seconds=600,
        team="ramblers",
        player_name="Test Player",
        player_number=None,
        infraction="Fighting",
        minutes=5,
        is_major=True,
        video_time=5.0,
    )

    clip_path, json_path = create_major_review_clip(vp, [penalty], tmp_path, _Config)
    assert clip_path.exists()
    assert json_path.exists()
    start, _, _ = vp.calls[-1]
    assert start == 0.0
