from __future__ import annotations

import logging
import re
from typing import Optional

from .base import OcrBackendResult

logger = logging.getLogger(__name__)


class EasyOcrBackend:
    name = "easyocr"

    def __init__(self, *, langs=None, gpu: bool = False):
        self._available = False
        self._reader = None
        self._langs = list(langs or ["en"])
        self._gpu = bool(gpu)

        try:
            import easyocr  # noqa: F401

            self._easyocr = easyocr
            self._available = True
        except Exception:
            self._easyocr = None
            self._available = False

    def is_available(self) -> bool:
        return bool(self._available and self._easyocr is not None)

    def _ensure_reader(self):
        if self._reader is not None:
            return
        if not self.is_available():
            return
        # Lazy init; EasyOCR import/init can be expensive.
        self._reader = self._easyocr.Reader(self._langs, gpu=self._gpu)

    def read_text(self, image, *, config: Optional[str] = None) -> OcrBackendResult:
        if not self.is_available():
            return OcrBackendResult(text="", confidence=0.0)

        try:
            self._ensure_reader()
            if self._reader is None:
                return OcrBackendResult(text="", confidence=0.0)

            # EasyOCR returns [(bbox, text, prob), ...]
            results = self._reader.readtext(image)
            if not results:
                return OcrBackendResult(text="", confidence=0.0)

            parts = []
            probs = []
            rel_probs = []
            for _bbox, txt, prob in results:
                s = str(txt or "").strip()
                if not s:
                    continue
                parts.append(s)
                try:
                    p = float(prob) * 100.0
                except Exception:
                    p = 0.0
                probs.append(p)
                if re.search(r"[0-9:]", s) or s.upper() in {"OT", "SO", "1ST", "2ND", "3RD"}:
                    rel_probs.append(p)

            raw = " ".join(parts)
            confs = rel_probs or probs
            conf = sum(confs) / float(len(confs)) if confs else 0.0
            conf = max(0.0, min(100.0, conf))
            return OcrBackendResult(text=raw, confidence=conf)
        except Exception as e:
            logger.debug(f"EasyOCR backend failed: {e}")
            return OcrBackendResult(text="", confidence=0.0)

