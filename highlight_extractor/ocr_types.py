"""
Shared OCR types.

We keep the legacy API (`extract_time_from_frame -> (period, time_str)`) for
backwards compatibility, but newer code should use OcrResult for better
diagnostics and confidence-aware downstream logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class OcrResult:
    """
    Parsed scoreboard clock reading.

    - `period` may be 0 when OCR cannot reliably read a period token.
    - `confidence` is backend-specific but normalized to 0..100.
    """

    period: int
    time_str: str
    time_seconds: int
    confidence: float
    raw_text: str
    backend: str
    broadcast_type: str
    roi: Optional[Tuple[int, int, int, int]] = None
    preprocess_style: str = "standard"

