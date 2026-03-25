import config


def _engine():
    from highlight_extractor.ocr_engine import OCREngine

    return OCREngine(config)


def test_parse_flohockey_period_and_time():
    eng = _engine()
    assert eng._parse_time_text("1st 19:56") == (1, "19:56")
    assert eng._parse_time_text("IST1956") == (1, "19:56")
    assert eng._parse_time_text("1ST 19 56") == (1, "19:56")


def test_parse_ignores_pregame_clock():
    eng = _engine()
    assert eng._parse_time_text("PRE 7:19") is None


def test_parse_time_only_marks_period_unknown():
    eng = _engine()
    assert eng._parse_time_text("19:44") == (0, "19:44")


def test_parse_rejects_invalid_period_but_can_parse_time_only():
    eng = _engine()
    # Should not accept period 8; time-only fallback may still parse 20:00.
    out = eng._parse_time_text("P8 20:00")
    assert out in {None, (0, "20:00")}

