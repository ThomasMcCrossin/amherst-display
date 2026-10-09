import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import eval_clip_windows as ev  # noqa: E402

LABEL = {"label": "firm", "moment": -2.0, "play_start": -25.0, "end": 14.0, "windows": {"x": {"in": -25, "out": 14, "score": 9.0}}}


def test_window_that_cuts_the_celebration_fails_ending_only():
    m = ev.score_window(LABEL, -32.0, 3.0)
    assert m["build_up_ok"] and m["moment_ok"] and not m["ending_ok"] and not m["wrong"]
    assert m["end_gap_s"] == 11.0


def test_window_on_the_label_is_all_ok_and_dead_air_is_counted():
    assert ev.score_window(LABEL, -25.0, 14.0)["all_ok"]
    assert ev.score_window(LABEL, -60.0, 40.0)["excess_s"] > 0


def test_window_missing_the_moment_is_wrong_incident():
    assert ev.score_window(LABEL, 10.0, 30.0)["wrong"]


def test_committed_labels_cover_the_judged_set_and_replay_beats_recorded_engine():
    labels = json.loads(ev.DEFAULT_LABELS.read_text())
    assert len(labels) == 39
    engine = ev.aggregate([r for r in ev.evaluate(labels, "engine").values() if r["label"] in ("firm", "weak")])
    replay = ev.aggregate([r for r in ev.evaluate(labels, "replay").values() if r["label"] in ("firm", "weak")])
    assert replay["all_ok"] > engine["all_ok"]
