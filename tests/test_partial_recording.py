"""Recordings that join mid-game or end early (late start, restart after a crash)."""

import config
from highlight_extractor.event_matcher import EventMatcher


def _ts(video_time, period, remaining):
    return {
        "video_time": float(video_time),
        "period": period,
        "game_time": f"{remaining // 60}:{remaining % 60:02d}",
        "game_time_seconds": remaining,
        "ocr_confidence": 95.0,
    }


def test_recording_that_joins_in_the_third_keeps_the_scorebug_period():
    em = EventMatcher(config)
    # Starts at 13:40 left in the 3rd; one early read misses the period.
    ts = [_ts(0, 0, 820)] + [_ts(5 * i, 3, 820 - 5 * i) for i in range(1, 20)]
    out = em._normalize_video_timestamps(ts)
    assert out and {t["period"] for t in out} == {3}


def test_initial_period_needs_a_clear_majority():
    assert EventMatcher._initial_period([_ts(i, 2, 1000 - i) for i in range(2)]) == 1  # too few votes
    assert EventMatcher._initial_period([_ts(0, 1, 1200), _ts(5, 3, 1195), _ts(10, 1, 1190), _ts(15, 3, 1185)]) == 1
    assert EventMatcher._initial_period([_ts(5 * i, 2, 1100 - 5 * i) for i in range(6)]) == 2


def test_event_outside_the_recording_is_not_clamped_to_its_edge():
    em = EventMatcher(config)
    covered = [_ts(10 + 5 * i, 3, 820 - 5 * i) for i in range(100)]  # 13:40 -> 5:25 left in the 3rd
    goal_in_second = {"type": "goal", "period": 2, "time": "7:16"}
    assert em._interpolate_timestamp(goal_in_second, covered) is None
    goal_after_end = {"type": "goal", "period": 3, "time": "18:00"}  # 2:00 left, after the recording ends
    assert em._interpolate_timestamp(goal_after_end, covered) is None
    goal_inside = {"type": "goal", "period": 3, "time": "10:00"}  # 10:00 left, inside the covered range
    assert em._interpolate_timestamp(goal_inside, covered) is not None


def test_minimum_video_time_counts_from_the_game_time_the_recording_joined_at():
    em = EventMatcher(config)
    # 09-16 Valley: readings begin at 20:00 of the 2nd, 982 s into the video.
    em._normalize_video_timestamps([_ts(1022 + 5 * i, 2, 1200 - 5 * i) for i in range(20)])
    minimum = em.minimum_video_time_for_event({"type": "goal", "period": 2, "time": "6:29"},
                                               recording_game_start_time=982.0)
    assert minimum < 1922.0  # the real goal stoppage (clock held at 13:31)
