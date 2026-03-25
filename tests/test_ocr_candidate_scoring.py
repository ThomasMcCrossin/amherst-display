import config


def test_score_candidate_prefers_period_present():
    from highlight_extractor.ocr_engine import OCREngine

    eng = OCREngine(config)
    with_period = eng._score_candidate((1, "19:56"), 80.0)
    no_period = eng._score_candidate((0, "19:56"), 80.0)
    assert with_period > no_period

