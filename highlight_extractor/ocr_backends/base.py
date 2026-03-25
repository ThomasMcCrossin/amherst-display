"""
OCR backend interface.

Backends should return:
  - a raw text string that can be parsed for (period, clock)
  - a confidence score normalized to 0..100
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Protocol


@dataclass(frozen=True)
class OcrBackendResult:
    text: str
    confidence: float  # 0..100


class BaseOcrBackend(Protocol):
    name: str

    def is_available(self) -> bool:
        ...

    def read_text(self, image: Any, *, config: Optional[str] = None) -> OcrBackendResult:
        ...

