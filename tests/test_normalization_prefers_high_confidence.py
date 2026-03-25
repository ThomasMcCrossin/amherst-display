import config


def test_normalization_drops_low_confidence_baseline_on_unexpected_increase():
    from highlight_extractor.event_matcher import EventMatcher

    em = EventMatcher(config)
    ts = [
        {
            "video_time": 0.0,
            "period": 1,
            "game_time": "18:00",
            "game_time_seconds": 18 * 60,
            "ocr_confidence": 10.0,
        },
        {
            "video_time": 5.0,
            "period": 1,
            "game_time": "19:50",
            "game_time_seconds": 19 * 60 + 50,
            "ocr_confidence": 90.0,
        },
    ]
    out = em._normalize_video_timestamps(ts)
    assert len(out) == 1
    assert out[0]["game_time"] == "19:50"

