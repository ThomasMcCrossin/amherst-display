"""Real scorebug crops must read correctly with their own layout and not with the others.

Each fixture is a crop of a real broadcast frame; the test pastes it back at its original
position in a blank frame of the original size so layout fractions resolve as in production.
Add a crop + manifest entry whenever a new broadcast layout shows up.
"""

import json
import shutil
from pathlib import Path

import numpy as np
import pytest

cv2 = pytest.importorskip("cv2")
if not shutil.which("tesseract"):
    pytest.skip("tesseract not installed", allow_module_level=True)

from highlight_extractor.ocr_engine import OCREngine, SCOREBUG_BOX_LAYOUTS  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures" / "scorebugs"
MANIFEST = json.loads((FIXTURES / "manifest.json").read_text())
LAYOUTS = ["flo_strip", *SCOREBUG_BOX_LAYOUTS]


def _frame(name: str) -> np.ndarray:
    entry = MANIFEST[name]
    width, height = entry["frame_size"]
    x, y = entry["offset"]
    crop = cv2.imread(str(FIXTURES / name))
    frame = np.zeros((height, width, 3), dtype=np.uint8)
    frame[y:y + crop.shape[0], x:x + crop.shape[1]] = crop
    return frame


@pytest.fixture(scope="module")
def engine():
    return OCREngine()


@pytest.mark.parametrize("name", sorted(MANIFEST))
def test_fixture_reads_with_its_layout(engine, name):
    entry = MANIFEST[name]
    result = engine.extract_time_from_frame(_frame(name), broadcast_type=entry["layout"])
    assert result == tuple(entry["expect"])


@pytest.mark.parametrize("name", sorted(MANIFEST))
def test_fixture_does_not_read_with_other_layouts(engine, name):
    entry = MANIFEST[name]
    for layout in LAYOUTS:
        if layout == entry["layout"]:
            continue
        assert engine.extract_time_from_frame(_frame(name), broadcast_type=layout) is None, layout
