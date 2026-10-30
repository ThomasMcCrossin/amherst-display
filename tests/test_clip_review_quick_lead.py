"""Goal lead-in floor by play kind (Tom, 2026-10-30): quick goals 6 s, built-up plays 15 s."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "skills/hockey-clip-review/scripts"))

from check_verdict import clamp  # noqa: E402
from clip_review.incidents import current_bounds  # noqa: E402


def _goal(in_kind, in_t):
    inc = current_bounds({"class": "goal", "bounds": {"lead_min": 15, "lead_max": 45, "tail_min": 8, "tail_max": 25,
                                                      "len_min": 8, "len_max": 60,
                                                      "video_from": -120, "video_to": 120}})
    v = {"role": "reviewer", "decision": "adjust", "event_t": 0.0, "in_t": in_t, "out_t": 10.0, "in_kind": in_kind}
    return clamp(v, inc)[0]["in_t"]


def test_quick_goal_keeps_short_lead():
    assert _goal("faceoff", -7.0) == -7.0
    assert _goal("possession_change", -4.0) == -6.0


def test_built_up_goal_keeps_15s_floor():
    assert _goal("rush", -7.0) == -15.0
    assert _goal(None, -7.0) == -15.0
