from __future__ import annotations

import logging
import re
from typing import Optional

from .base import OcrBackendResult

logger = logging.getLogger(__name__)


class TesseractBackend:
    name = "tesseract"

    def __init__(self):
        try:
            import pytesseract  # noqa: F401

            self._pytesseract = pytesseract
            self._available = True
        except Exception:
            self._pytesseract = None
            self._available = False

    def is_available(self) -> bool:
        return bool(self._available and self._pytesseract is not None)

    def _normalize_conf(self, values) -> float:
        confs = []
        for v in values or []:
            try:
                fv = float(v)
            except Exception:
                continue
            if fv >= 0:
                confs.append(fv)
        if not confs:
            return 0.0
        # pytesseract confs are usually 0..100 (sometimes -1).
        return max(0.0, min(100.0, sum(confs) / float(len(confs))))

    def read_text(self, image, *, config: Optional[str] = None) -> OcrBackendResult:
        if not self.is_available():
            return OcrBackendResult(text="", confidence=0.0)

        cfg = str(config or "")
        try:
            # Prefer image_to_data so we can extract confidence.
            data = self._pytesseract.image_to_data(image, config=cfg, output_type=self._pytesseract.Output.DICT)
            texts = data.get("text", []) if isinstance(data, dict) else []
            confs = data.get("conf", []) if isinstance(data, dict) else []

            # Build a compact raw text line for downstream parsing.
            raw = " ".join([str(t).strip() for t in texts if str(t).strip()])
            if not raw:
                raw = self._pytesseract.image_to_string(image, config=cfg) or ""

            # Confidence: focus on tokens that look relevant (digits, colon, OT/SO).
            relevant_confs = []
            for t, c in zip(texts, confs):
                ts = str(t or "").strip()
                if not ts:
                    continue
                if re.search(r"[0-9:]", ts) or ts.upper() in {"OT", "SO", "1ST", "2ND", "3RD", "PERIOD", "P1", "P2", "P3"}:
                    relevant_confs.append(c)

            conf = self._normalize_conf(relevant_confs or confs)
            return OcrBackendResult(text=str(raw), confidence=float(conf))
        except Exception as e:
            logger.debug(f"Tesseract backend failed: {e}")
            try:
                raw = self._pytesseract.image_to_string(image, config=cfg) or ""
                return OcrBackendResult(text=str(raw), confidence=0.0)
            except Exception:
                return OcrBackendResult(text="", confidence=0.0)

