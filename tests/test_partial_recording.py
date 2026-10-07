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


def test_clock_that_jumps_ahead_after_a_freeze_still_matches():
    em = EventMatcher(config)
    # 09-24 West Kent: bug held 20:00 until ~2640 s, then the operator jumped it ahead.
    readings = [_ts(2640 + 5 * i, 1, 1200) for i in range(4)]
    readings += [_ts(2700 + 5 * i, 1, 800 - 5 * i) for i in range(30)]  # 12:46 at ~2735 s
    readings += [_ts(1500, 1, 766)]  # a warm-up countdown that happens to show 12:46
    event = {"type": "penalty", "period": 1, "time": "7:14"}
    match = em._find_closest_timestamp_with_confidence(event, readings, 30, recording_game_start_time=2640.0)
    assert match is not None and 2700 <= match[0] <= 2860


def test_confirmed_clock_catch_up_is_kept_and_a_lone_misread_is_not():
    em = EventMatcher(config)
    # 09-12 Grand Falls: 15:24 frozen, then the operator jumps it to 5:32 and it runs on.
    frozen = [_ts(6400 + 5 * i, 2, 924) for i in range(20)]
    caught_up = [_ts(7370 + 5 * i, 2, 332 - 5 * i) for i in range(10)]
    out = em._normalize_video_timestamps(frozen + caught_up)
    assert any(t["game_time_seconds"] < 340 for t in out)

    em = EventMatcher(config)
    lone = frozen[:10] + [_ts(6452, 2, 332)] + [_ts(6455 + 5 * i, 2, 924) for i in range(5)]
    out = em._normalize_video_timestamps(lone)
    assert all(t["game_time_seconds"] == 924 for t in out)
