from highlight_extractor.ocr_engine import OCREngine


def test_parse_time_text_accepts_period_separator_after_token():
    ocr = OCREngine.__new__(OCREngine)

    assert ocr._parse_time_text("1st| 9:05 univers") == (1, "9:05")


def test_parse_time_text_accepts_i_as_one_for_period_token():
    ocr = OCREngine.__new__(OCREngine)

    assert ocr._parse_time_text("Ist 9:04") == (1, "9:04")
