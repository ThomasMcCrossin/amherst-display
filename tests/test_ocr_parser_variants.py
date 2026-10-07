from highlight_extractor.ocr_engine import OCREngine


def test_parse_time_text_accepts_period_separator_after_token():
    ocr = OCREngine.__new__(OCREngine)

    assert ocr._parse_time_text("1st| 9:05 univers") == (1, "9:05")


def test_parse_time_text_accepts_i_as_one_for_period_token():
    ocr = OCREngine.__new__(OCREngine)

    assert ocr._parse_time_text("Ist 9:04") == (1, "9:04")


def test_parse_time_text_accepts_small_caps_period_misreads():
    ocr = OCREngine.__new__(OCREngine)

    assert ocr._parse_time_text("end 9:13") == (2, "9:13")
    assert ocr._parse_time_text("8rd 9:32 | 1g 4") == (3, "9:32")


def test_parse_time_text_reads_stitched_overtime_label():
    ocr = OCREngine.__new__(OCREngine)

    assert ocr._parse_time_text("1st OT 0:00") == (4, "0:00")
