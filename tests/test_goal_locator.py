"""Goal placement when the scorebug froze (09-12 Grand Falls, P2 8:28)."""

import goal_locator as gl


def _r(t, period, remaining):
    return {"video_time": float(t), "period": period, "game_time_seconds": remaining}


def test_bracket_spans_a_frozen_clock():
    # Clock runs to 15:24 (924 s), freezes there for ~15 min of video, resumes at 5:17.
    readings = [_r(6360, 2, 956), _r(6375, 2, 948), _r(6480, 2, 924)]
    readings += [_r(t, 2, 924) for t in range(6495, 7300, 30)]
    readings += [_r(7395, 2, 302), _r(7410, 2, 290)]
    assert gl.bracket(2, 692, readings) == (6480.0, 7395.0)


def test_bracket_needs_readings_on_both_sides():
    readings = [_r(t, 3, 820 - (t - 10)) for t in range(10, 500, 10)]
    assert gl.bracket(2, 692, readings) is None  # other period
    assert gl.bracket(3, 900, readings) is None  # before the recording
    assert gl.bracket(3, 600, readings) is not None


def test_only_runs_with_a_celebration_are_goals():
    labels = [
        (6576.0, "closeup"),  # stoppage close-up, no celebration
        (6666.0, "celebration"), (6672.0, "closeup"), (6690.0, "closeup"),
        (6696.0, "other"),
        (6750.0, "closeup"),
        (7212.0, "faceoff_center"), (7218.0, "closeup"), (7224.0, "closeup"),
    ]
    assert gl.candidates(labels) == [6666.0]


def test_two_celebrations_far_apart_are_two_candidates():
    labels = [(100.0, "celebration"), (106.0, "closeup"), (400.0, "closeup"), (406.0, "celebration")]
    assert gl.candidates(labels) == [100.0, 400.0]
