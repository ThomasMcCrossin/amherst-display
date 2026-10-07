from types import SimpleNamespace

from highlight_extractor.pipeline import HighlightPipeline


def _pipeline(shootout, samples):
    p = HighlightPipeline.__new__(HighlightPipeline)
    p.game_info = SimpleNamespace(shootout=shootout)
    p.video_timestamps = samples
    return p


def _s(t, period, secs):
    return {"video_time": t, "period": period, "game_time_seconds": secs}


def test_shootout_clip_spans_end_of_ot_to_last_ot_zero_sample():
    # 2026-09-24 West Kent: OT hits 0:00 at 2:55:40, scorebug holds OT 0:00 through ~3:08.
    samples = [_s(10530, 4, 16), _s(10540, 4, 0), _s(10800, 4, 0), _s(11280, 4, 0)]
    event = _pipeline(True, samples)._shootout_event()
    assert event["type"] == "shootout"
    assert event["video_time"] == 10540
    assert event["before_seconds"] == HighlightPipeline.SHOOTOUT_LEAD_SECONDS
    assert event["after_seconds"] == HighlightPipeline.SHOOTOUT_MAX_SECONDS  # 760s capped to 720s


def test_short_shootout_gets_minimum_window():
    event = _pipeline(True, [_s(5000, 4, 0), _s(5030, 4, 0)])._shootout_event()
    assert event["after_seconds"] == HighlightPipeline.SHOOTOUT_MIN_SECONDS


def test_no_shootout_or_no_ot_samples_means_no_clip():
    assert _pipeline(False, [_s(5000, 4, 0)])._shootout_event() is None
    assert _pipeline(True, [_s(5000, 3, 0)])._shootout_event() is None
