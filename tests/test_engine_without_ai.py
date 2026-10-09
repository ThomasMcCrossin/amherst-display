"""The engine's default tier needs no API key: vision steps skip cleanly and windows are code-only."""
import numpy as np

import goal_locator
import scorebug_detect


def _no_keys(monkeypatch):
    for name in ("SCOREBUG_VISION_API_KEY", "DEEPSEEK_API_KEY"):
        monkeypatch.delenv(name, raising=False)


def test_goal_locator_skips_without_a_key_and_leaves_goals_alone(monkeypatch):
    _no_keys(monkeypatch)
    goal = {"type": "goal", "period": 1, "time": "5:00", "video_time": 100.0}
    report = goal_locator.locate_goals("/nonexistent.mp4", [goal], [], lambda g: 900)
    assert report["skipped"] == "no vision API key"
    assert goal == {"type": "goal", "period": 1, "time": "5:00", "video_time": 100.0}


def test_scorebug_vision_choice_skips_without_a_key(monkeypatch):
    _no_keys(monkeypatch)
    choice, info = scorebug_detect.vision_choose([np.zeros((10, 10, 3), dtype=np.uint8)], [])
    assert choice is None and "no vision API key" in info["skipped"]


def test_goal_and_penalty_windows_come_from_config_alone(monkeypatch):
    import config
    from types import SimpleNamespace
    from highlight_extractor.pipeline import HighlightPipeline

    _no_keys(monkeypatch)
    goal = {"refined_by": "clock_stop", "match_confidence": 1.0, "period": 2}
    before, after = HighlightPipeline._goal_clip_window(
        SimpleNamespace(config=config), goal, before_seconds=15.0, after_seconds=4.0)
    assert (before, after) == (config.GOAL_CLOCK_STOP_BEFORE_SECONDS, config.GOAL_CLOCK_STOP_AFTER_SECONDS)
