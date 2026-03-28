"""
OCR Engine - Extracts game time from video scoreboards.

Legacy API compatibility:
  - extract_time_from_frame(...) -> Optional[(period, "MM:SS")]
New API for diagnostics + confidence-aware logic:
  - extract_time_from_frame_detailed(...) -> Optional[OcrResult]
"""

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple, List, Dict
from pathlib import Path
import numpy as np
import cv2
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import pytesseract
    TESSERACT_AVAILABLE = True
except ImportError:
    TESSERACT_AVAILABLE = False
    logging.warning("pytesseract not installed - OCR functionality disabled")

from .ocr_types import OcrResult
from .ocr_backends import TesseractBackend, EasyOcrBackend

logger = logging.getLogger(__name__)

ROI_PINNED_BROADCAST_TYPES = {
    "flohockey",
    "yarmouth",
    "mhl_summerside",
    "mhl_amherst",
}
FLO_LIKE_BROADCAST_TYPES = {
    "flohockey",
    "mhl_summerside",
    "mhl_amherst",
}
OCR_STYLE_BROADCAST_TYPES = FLO_LIKE_BROADCAST_TYPES | {"yarmouth"}


@dataclass
class OCRSampleLog:
    """Log entry for a single OCR sample."""
    video_time: float
    raw_text: str
    parsed_period: Optional[int]
    parsed_time: Optional[str]
    parsed_time_seconds: Optional[int]
    success: bool
    confidence: Optional[float] = None
    backend: Optional[str] = None
    failure_reason: Optional[str] = None
    roi_used: Optional[Tuple[int, int, int, int]] = None
    broadcast_type: str = "unknown"
    preprocess_style: str = "standard"
    sharpness_score: Optional[float] = None
    crop_debug_path: Optional[str] = None

    def to_dict(self) -> Dict:
        return {
            "video_time": self.video_time,
            "video_time_formatted": self._format_time(self.video_time),
            "raw_ocr_text": self.raw_text,
            "parsed": {
                "period": self.parsed_period,
                "time": self.parsed_time,
                "time_seconds": self.parsed_time_seconds,
                "confidence": self.confidence,
                "backend": self.backend,
            },
            "success": self.success,
            "failure_reason": self.failure_reason,
            "roi": self.roi_used,
            "broadcast_type": self.broadcast_type,
            "preprocess_style": self.preprocess_style,
            "sharpness_score": self.sharpness_score,
            "crop_debug_path": self.crop_debug_path,
        }

    @staticmethod
    def _format_time(seconds: float) -> str:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        if hours > 0:
            return f"{hours}:{mins:02d}:{secs:02d}"
        return f"{mins}:{secs:02d}"


class OCRLogger:
    """
    Manages detailed logging for OCR sampling.

    Creates a log file with every OCR sample attempt, useful for diagnosing
    scoreboard detection issues, OCR failures, and period detection problems.
    """

    def __init__(self, output_dir: Optional[Path] = None, game_id: str = "unknown"):
        self.output_dir = output_dir
        self.game_id = game_id
        self.samples: List[OCRSampleLog] = []
        self.start_time = datetime.now()

    def add_sample(self, sample: OCRSampleLog):
        """Add an OCR sample log entry."""
        self.samples.append(sample)

    def write_logs(self):
        """Write OCR logs to files."""
        if not self.output_dir:
            logger.debug("No output_dir specified - skipping OCR log file write")
            return

        output_path = Path(self.output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Calculate stats
        total = len(self.samples)
        successful = sum(1 for s in self.samples if s.success)
        with_period = sum(1 for s in self.samples if s.success and s.parsed_period and s.parsed_period > 0)
        failed = total - successful

        # Write JSON log
        json_path = output_path / "ocr_sampling_log.json"
        try:
            json_data = {
                "game_id": self.game_id,
                "timestamp": self.start_time.isoformat(),
                "summary": {
                    "total_samples": total,
                    "successful": successful,
                    "failed": failed,
                    "with_period_detected": with_period,
                    "success_rate": f"{100*successful/total:.1f}%" if total > 0 else "N/A",
                    "period_detection_rate": f"{100*with_period/successful:.1f}%" if successful > 0 else "N/A",
                },
                "samples": [s.to_dict() for s in self.samples],
            }
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=2)
            logger.info(f"Wrote OCR sampling log to: {json_path}")
        except Exception as e:
            logger.error(f"Failed to write OCR JSON log: {e}")

        # Write human-readable log
        txt_path = output_path / "ocr_sampling_log.txt"
        try:
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(f"OCR SAMPLING LOG - {self.game_id}\n")
                f.write(f"Generated: {self.start_time.isoformat()}\n")
                f.write("=" * 70 + "\n\n")

                # Summary
                f.write("SUMMARY:\n")
                f.write(f"  Total samples: {total}\n")
                f.write(f"  Successful OCR reads: {successful} ({100*successful/total:.1f}%)\n" if total > 0 else "  Successful: N/A\n")
                f.write(f"  Period detected: {with_period} ({100*with_period/successful:.1f}% of successful)\n" if successful > 0 else "  Period detected: N/A\n")
                f.write(f"  Failed samples: {failed}\n")
                f.write("\n")

                # Period distribution
                period_counts = {}
                for s in self.samples:
                    if s.success:
                        p = s.parsed_period if s.parsed_period else 0
                        period_counts[p] = period_counts.get(p, 0) + 1
                f.write("PERIOD DISTRIBUTION:\n")
                for p in sorted(period_counts.keys()):
                    label = f"P{p}" if p > 0 else "Unknown"
                    f.write(f"  {label}: {period_counts[p]} samples\n")
                f.write("\n")

                # Detailed samples
                f.write("DETAILED SAMPLE LOG:\n")
                f.write("-" * 70 + "\n")
                for s in self.samples:
                    vt = s.video_time
                    vt_fmt = OCRSampleLog._format_time(vt)
                    if s.success:
                        period_str = f"P{s.parsed_period}" if s.parsed_period else "P?"
                        f.write(f"[{vt_fmt}] {period_str} {s.parsed_time} | raw: \"{s.raw_text.strip()[:50]}\"\n")
                    else:
                        f.write(f"[{vt_fmt}] FAILED: {s.failure_reason} | raw: \"{s.raw_text.strip()[:50]}\"\n")

            logger.info(f"Wrote OCR sampling text log to: {txt_path}")
        except Exception as e:
            logger.error(f"Failed to write OCR text log: {e}")


class OCREngine:
    """Extracts time information from video scoreboards"""

    def __init__(self, config=None):
        """
        Initialize OCR Engine

        Args:
            config: Optional configuration object
        """
        self.config = config
        self.scoreboard_roi: Optional[Tuple[int, int, int, int]] = None  # (x, y, w, h)
        self._last_sampling_stats: Dict[str, float] = {}
        self._consecutive_bad_samples: int = 0

        # Backends (tesseract required for historical workflows, EasyOCR optional).
        self._tesseract_backend = TesseractBackend()
        enable_easy = bool(getattr(self.config, "OCR_ENABLE_EASYOCR_FALLBACK", True))
        self._easyocr_backend = EasyOcrBackend(
            langs=getattr(self.config, "OCR_EASYOCR_LANGS", ["en"]),
            gpu=bool(getattr(self.config, "OCR_EASYOCR_GPU", False)),
        ) if enable_easy else None

        backend_order = list(getattr(self.config, "OCR_BACKENDS", ["tesseract", "easyocr"]))
        self._backends = []
        for name in backend_order:
            n = str(name or "").strip().lower()
            if n == "tesseract" and self._tesseract_backend.is_available():
                self._backends.append(self._tesseract_backend)
            elif n == "easyocr" and self._easyocr_backend is not None and self._easyocr_backend.is_available():
                self._backends.append(self._easyocr_backend)

        if not self._backends:
            raise RuntimeError(
                "No OCR backend available. Install pytesseract (and tesseract-ocr) "
                "and/or easyocr to enable OCR functionality."
            )

        # Validate tesseract-ocr system package if we will use it.
        if any(getattr(b, "name", "") == "tesseract" for b in self._backends):
            if not TESSERACT_AVAILABLE:
                raise RuntimeError("pytesseract not installed. Install with: pip install pytesseract")
            try:
                pytesseract.get_tesseract_version()
                logger.debug(f"Tesseract version: {pytesseract.get_tesseract_version()}")
            except Exception as e:
                raise RuntimeError(
                    "tesseract-ocr system package not found. "
                    "Install it:\n"
                    "  macOS: brew install tesseract\n"
                    "  Ubuntu/Debian: sudo apt-get install tesseract-ocr\n"
                    "  Windows: Download from https://github.com/UB-Mannheim/tesseract/wiki\n"
                    f"Error: {e}"
                )

    def detect_scoreboard_roi(
        self,
        frame: np.ndarray,
        method: str = 'auto'
    ) -> Optional[Tuple[int, int, int, int]]:
        """
        Detect scoreboard region in frame

        Args:
            frame: Video frame (RGB or BGR)
            method: Detection method ('auto', 'top', 'bottom', 'flohockey', 'yarmouth',
                'mhl_summerside', 'mhl_amherst')

        Returns:
            ROI as (x, y, width, height) or None
        """
        try:
            height, width = frame.shape[:2]

            if method == 'top':
                # Assume scoreboard is in top portion of frame
                return (0, 0, width, int(height * 0.15))

            elif method == 'bottom':
                # Assume scoreboard is in bottom portion
                y_start = int(height * 0.85)
                return (0, y_start, width, height - y_start)

            elif method == 'flohockey':
                # FloHockey overlay: banner in upper portion of frame
                # Discovered position: near top of frame for 720p video
                # Layout: FLOHOCKEY | Away | Score | Home | Score | Period | Time
                # "1st 20:00" portion is at far right (~x=750 onwards)
                # Use a taller ROI starting at the top edge to avoid clipping
                # the period token ("1st"/"2nd"/"3rd") and digit tops.
                y_start = 0
                roi_height = max(40, int(height * 0.09))  # ~65px for 720p
                # Capture the period token + clock reliably. 0.52 was too far right and
                # could clip the period label (leading to noisy, period-less OCR reads).
                x_start = int(width * 0.50)
                roi_width = width - x_start - 5           # Leave small margin at right
                roi = (x_start, y_start, roi_width, roi_height)
                logger.info(f"FloHockey ROI: {roi}")
                return roi

            elif method == 'mhl_summerside':
                # Summerside home broadcasts use a wide black banner centered at the
                # top of the frame. The period/clock block lives on the right side of
                # that banner, so crop narrowly around it instead of the whole banner.
                y_start = 0
                roi_height = max(60, int(height * 0.10))
                x_start = int(width * 0.57)
                roi_width = max(280, int(width * 0.15))
                roi = (x_start, y_start, roi_width, roi_height)
                logger.info(f"MHL Summerside ROI: {roi}")
                return roi

            elif method == 'mhl_amherst':
                # Amherst home broadcasts use a lighter full-width strip. The right
                # clock block is slightly wider than the Summerside layout.
                y_start = 0
                roi_height = max(60, int(height * 0.09))
                x_start = int(width * 0.58)
                roi_width = max(340, int(width * 0.19))
                roi = (x_start, y_start, roi_width, roi_height)
                logger.info(f"MHL Amherst ROI: {roi}")
                return roi

            elif method == 'yarmouth':
                # Yarmouth home broadcast: custom scorebug typically lives in the top-left.
                # Use a generous ROI so auto-probing can lock onto it.
                y_start = 0
                roi_height = max(50, int(height * 0.14))  # ~100px for 720p
                x_start = 0
                roi_width = max(200, int(width * 0.70))
                roi = (x_start, y_start, roi_width, roi_height)
                logger.info(f"Yarmouth ROI: {roi}")
                return roi

            else:  # 'auto'
                # Default to top 15% of frame (most common for hockey)
                roi = (0, 0, width, int(height * 0.15))
                logger.info(f"Auto-detected scoreboard ROI: {roi}")
                return roi

        except Exception as e:
            logger.error(f"Failed to detect scoreboard ROI: {e}")
            return None

    def set_scoreboard_roi(self, x: int, y: int, width: int, height: int):
        """
        Manually set scoreboard ROI

        Args:
            x: X coordinate
            y: Y coordinate
            width: Width
            height: Height
        """
        self.scoreboard_roi = (x, y, width, height)
        logger.info(f"Scoreboard ROI set to: {self.scoreboard_roi}")

    def extract_time_from_frame(
        self,
        frame: np.ndarray,
        roi: Optional[Tuple[int, int, int, int]] = None,
        broadcast_type: str = 'auto'
    ) -> Optional[Tuple[int, str]]:
        """
        Extract game time from video frame (legacy API).

        Args:
            frame: Video frame (RGB or BGR)
            roi: Optional region of interest (x, y, w, h). Uses stored ROI if None.
            broadcast_type: Type of broadcast ('auto', 'flohockey', 'yarmouth', 'standard')

        Returns:
            Tuple of (period, time_string) or None if extraction failed
            Example: (1, "15:23") for Period 1, 15:23 remaining
        """
        detailed = self.extract_time_from_frame_detailed(frame, roi=roi, broadcast_type=broadcast_type)
        if detailed is None:
            return None
        return (int(detailed.period), str(detailed.time_str))

    def extract_time_from_frame_detailed(
        self,
        frame: np.ndarray,
        roi: Optional[Tuple[int, int, int, int]] = None,
        broadcast_type: str = "auto",
    ) -> Optional[OcrResult]:
        """
        Extract game time from video frame with diagnostics + confidence.
        """
        parsed, raw_text, conf, backend_name, used_broadcast, used_roi, preprocess_style = self._extract_time_from_frame_with_meta(
            frame,
            roi=roi,
            broadcast_type=broadcast_type,
        )
        if parsed is None:
            return None
        period, time_str = parsed
        try:
            seconds = int(self._time_to_seconds(str(time_str)))
        except Exception:
            return None
        return OcrResult(
            period=int(period),
            time_str=str(time_str),
            time_seconds=int(seconds),
            confidence=float(conf),
            raw_text=str(raw_text or ""),
            backend=str(backend_name or "unknown"),
            broadcast_type=str(used_broadcast or "unknown"),
            roi=used_roi,
            preprocess_style=str(preprocess_style or "standard"),
        )

    def _score_candidate(self, parsed: Optional[Tuple[int, str]], confidence: float) -> float:
        score = 0.0
        if parsed is not None:
            score += 50.0
            try:
                if int(parsed[0]) != 0:
                    score += 25.0
            except Exception:
                pass
        score += max(0.0, min(100.0, float(confidence))) * 0.5
        return score

    def _tesseract_configs_for_broadcast(self, broadcast_type: str) -> List[str]:
        bt = str(broadcast_type or "standard").lower()
        if bt in OCR_STYLE_BROADCAST_TYPES:
            return ["--psm 7 --oem 3"]
        # "standard": keep a whitelist variant and a looser variant.
        return [
            "--psm 7 --oem 3 -c tessedit_char_whitelist=0123456789:OTSO.",
            "--psm 6 --oem 3",
        ]

    def _candidate_rois(self, frame: np.ndarray) -> List[Tuple[str, Tuple[int, int, int, int]]]:
        height, width = frame.shape[:2]
        rois: List[Tuple[str, Tuple[int, int, int, int]]] = []

        # Known broadcast layouts first.
        fh = self.detect_scoreboard_roi(frame, method="flohockey")
        if fh is not None:
            rois.append(("flohockey", fh))
        su = self.detect_scoreboard_roi(frame, method="mhl_summerside")
        if su is not None:
            rois.append(("mhl_summerside", su))
        am = self.detect_scoreboard_roi(frame, method="mhl_amherst")
        if am is not None:
            rois.append(("mhl_amherst", am))
        ya = self.detect_scoreboard_roi(frame, method="yarmouth")
        if ya is not None:
            rois.append(("yarmouth", ya))

        # Generic candidates (corners + top band).
        roi_h = max(20, int(height * 0.22))
        roi_w = max(50, int(width * 0.45))
        rois.extend(
            [
                ("standard", (0, 0, roi_w, roi_h)),
                ("standard", (width - roi_w, 0, roi_w, roi_h)),
                ("standard", (0, height - roi_h, roi_w, roi_h)),
                ("standard", (width - roi_w, height - roi_h, roi_w, roi_h)),
            ]
        )
        auto_roi = self.detect_scoreboard_roi(frame, method="auto")
        if auto_roi is not None:
            rois.append(("standard", auto_roi))

        return rois

    def _select_best_settings(self, frame: np.ndarray) -> Tuple[str, Tuple[int, int, int, int], str, str]:
        """
        Pick the best (broadcast_type, roi, preprocess_style, backend_name) for this video.
        """
        best = None  # (score, parsed, raw, conf, bt, roi, style, backend)

        for bt, candidate_roi in self._candidate_rois(frame):
            x0, y0, w0, h0 = candidate_roi
            probe = frame[y0:y0 + h0, x0:x0 + w0]

            preprocess_variants = self._preprocess_variants(
                probe,
                base_style=bt if bt in OCR_STYLE_BROADCAST_TYPES else "standard",
            )
            for style_name, processed in preprocess_variants:
                for backend in self._backends:
                    if getattr(backend, "name", "") == "tesseract":
                        cfgs = self._tesseract_configs_for_broadcast(bt)
                    else:
                        cfgs = [None]
                    for cfg in cfgs:
                        bres = backend.read_text(processed, config=cfg)
                        raw_text = str(bres.text or "")
                        parsed = self._parse_time_text(raw_text)
                        score = self._score_candidate(parsed, float(bres.confidence or 0.0))
                        if best is None or score > best[0]:
                            best = (score, parsed, raw_text, float(bres.confidence or 0.0), bt, candidate_roi, style_name, str(getattr(backend, "name", "unknown")))

        if best is None:
            # Last-resort defaults.
            roi = self.detect_scoreboard_roi(frame, method="auto") or (0, 0, frame.shape[1], int(frame.shape[0] * 0.15))
            return ("standard", roi, "standard", "tesseract")

        _score, _parsed, _raw, _conf, bt, roi, style_name, backend_name = best
        return (bt if bt in OCR_STYLE_BROADCAST_TYPES else "standard", roi, style_name, backend_name)

    def set_broadcast_type(self, broadcast_type: str):
        """
        Set the broadcast type for ROI detection and preprocessing

        Args:
            broadcast_type: 'flohockey', 'yarmouth', 'standard', or 'auto'
        """
        self._broadcast_type = broadcast_type
        # Reset cached settings; caller is explicitly overriding.
        self.scoreboard_roi = None
        if hasattr(self, "_preprocess_style"):
            try:
                delattr(self, "_preprocess_style")
            except Exception:
                pass
        if hasattr(self, "_backend_name"):
            try:
                delattr(self, "_backend_name")
            except Exception:
                pass
        logger.info(f"Broadcast type set to: {broadcast_type}")

    def _extract_time_from_frame_with_meta(
        self,
        frame: np.ndarray,
        roi: Optional[Tuple[int, int, int, int]] = None,
        broadcast_type: str = 'auto'
    ) -> Tuple[Optional[Tuple[int, str]], str, float, str, str, Optional[Tuple[int, int, int, int]], str]:
        """
        Extract game time from video frame, also returning raw OCR metadata for logging.

        Args:
            frame: Video frame (RGB or BGR)
            roi: Optional region of interest (x, y, w, h)
            broadcast_type: Type of broadcast

        Returns:
            (result, raw_text, confidence, backend_name, used_broadcast_type, used_roi, preprocess_style)
        """
        try:
            used_broadcast = str(broadcast_type or "auto").lower()
            # One-time probe + cache for auto mode.
            if used_broadcast == "auto":
                detected = getattr(self, "_broadcast_type", None)
                if not detected or self.scoreboard_roi is None or not hasattr(self, "_preprocess_style") or not hasattr(self, "_backend_name"):
                    bt, roi_sel, style_sel, backend_sel = self._select_best_settings(frame)
                    self._broadcast_type = bt
                    self.scoreboard_roi = roi_sel
                    self._preprocess_style = style_sel
                    self._backend_name = backend_sel
                used_broadcast = str(getattr(self, "_broadcast_type", "standard"))

            # Choose ROI
            used_roi = roi or self.scoreboard_roi
            if used_roi is None:
                method = used_broadcast if used_broadcast in ROI_PINNED_BROADCAST_TYPES else "auto"
                used_roi = self.detect_scoreboard_roi(frame, method=method)

            if used_roi is None:
                return None, "", 0.0, "unknown", used_broadcast, None, "standard"

            x, y, w, h = used_roi
            scoreboard = frame[y:y + h, x:x + w]

            # Choose preprocess style (cached for auto; otherwise default for broadcast).
            preprocess_style = getattr(self, "_preprocess_style", None)
            if str(broadcast_type or "").lower() != "auto":
                preprocess_style = used_broadcast if used_broadcast in OCR_STYLE_BROADCAST_TYPES else "standard"
            preprocess_style = str(
                preprocess_style or (used_broadcast if used_broadcast in OCR_STYLE_BROADCAST_TYPES else "standard")
            )

            processed = self._preprocess_for_ocr(scoreboard, style=preprocess_style)

            # Choose backend
            backend_name = str(getattr(self, "_backend_name", "tesseract"))
            if str(broadcast_type or "").lower() != "auto":
                # If user pinned broadcast type, prefer tesseract first, but allow fallback list.
                backend_name = "tesseract"

            backend = next((b for b in self._backends if str(getattr(b, "name", "")) == backend_name), None) or self._backends[0]
            backend_name = str(getattr(backend, "name", "unknown"))

            def _attempt_with(backend_obj):
                name = str(getattr(backend_obj, "name", "unknown"))
                cfgs = self._tesseract_configs_for_broadcast(used_broadcast) if name == "tesseract" else [None]
                best_local = None  # (score, parsed, raw, conf, backend_name)
                for cfg in cfgs:
                    bres = backend_obj.read_text(processed, config=cfg)
                    raw = str(bres.text or "")
                    conf_f = float(bres.confidence or 0.0)
                    parsed = self._parse_time_text(raw)
                    score = self._score_candidate(parsed, conf_f)
                    if best_local is None or score > best_local[0]:
                        best_local = (score, parsed, raw, conf_f, name)
                return best_local

            best_attempt = _attempt_with(backend)

            # Fast fallback: if parse failed, try other backends (e.g., EasyOCR) on the same processed ROI.
            if best_attempt is None or best_attempt[1] is None:
                for b in self._backends:
                    if b is backend:
                        continue
                    alt = _attempt_with(b)
                    if alt is None:
                        continue
                    if best_attempt is None or alt[0] > best_attempt[0]:
                        best_attempt = alt

            if best_attempt is None:
                return None, "", 0.0, "unknown", used_broadcast, used_roi, preprocess_style

            _score, parsed, raw_text, conf, backend_name = best_attempt
            if parsed is not None and str(broadcast_type or "").lower() == "auto":
                # Cache the winning backend for subsequent frames.
                self._backend_name = backend_name

            return parsed, str(raw_text or ""), float(conf or 0.0), str(backend_name or "unknown"), used_broadcast, used_roi, preprocess_style

        except Exception as e:
            logger.error(f"Failed to extract time from frame: {e}")
            return None, "", 0.0, "unknown", str(broadcast_type or "unknown"), roi, "standard"

    # Backwards-compatible alias for older callers.
    def _extract_time_from_frame_with_raw(
        self,
        frame: np.ndarray,
        roi: Optional[Tuple[int, int, int, int]] = None,
        broadcast_type: str = "auto",
    ) -> Tuple[Optional[Tuple[int, str]], str]:
        parsed, raw_text, *_ = self._extract_time_from_frame_with_meta(frame, roi=roi, broadcast_type=broadcast_type)
        return parsed, raw_text

    def get_last_sampling_stats(self) -> Dict[str, float]:
        """Stats from the most recent `sample_video_times` call."""
        return dict(self._last_sampling_stats or {})

    def probe_video_scoreboard(
        self,
        video_processor,
        *,
        start_time: float = 0.0,
        samples: int = 60,
    ) -> Dict:
        """
        Probe across the video to determine the most stable scoreboard settings.

        Returns a JSON-serializable report and also installs the winning settings
        into the engine cache (broadcast_type/roi/preprocess/backend).
        """
        report: Dict = {
            "samples": [],
            "summary": {},
            "selected": {},
        }

        duration = float(getattr(video_processor, "duration", 0.0) or 0.0)
        if duration <= 0:
            report["summary"] = {"error": "video duration unknown"}
            return report

        # Save/restore current cache.
        prev = {
            "broadcast_type": getattr(self, "_broadcast_type", None),
            "roi": self.scoreboard_roi,
            "preprocess": getattr(self, "_preprocess_style", None),
            "backend": getattr(self, "_backend_name", None),
        }

        try:
            n = int(samples) if int(samples) > 0 else 1
            n = max(1, min(200, n))
            start = max(0.0, float(start_time or 0.0))
            end = max(start, duration - 1.0)
            if n == 1:
                times = [start]
            else:
                step = (end - start) / float(n - 1)
                times = [start + i * step for i in range(n)]

            by_setting: Dict[str, Dict[str, float]] = {}
            for t in times:
                frame = video_processor.get_frame_at_time(float(t))
                if frame is None:
                    continue

                bt, roi, style, backend = self._select_best_settings(frame)
                # Apply and evaluate on this frame (auto path uses cached values).
                self._broadcast_type = bt
                self.scoreboard_roi = roi
                self._preprocess_style = style
                self._backend_name = backend

                parsed, raw_text, conf, backend_name, used_broadcast, used_roi, preprocess_style = self._extract_time_from_frame_with_meta(
                    frame, broadcast_type="auto"
                )
                score = self._score_candidate(parsed, float(conf or 0.0))
                key = f"{used_broadcast}|{preprocess_style}|{backend_name}|{used_roi}"

                agg = by_setting.setdefault(key, {"count": 0.0, "success": 0.0, "score_sum": 0.0})
                agg["count"] += 1.0
                agg["score_sum"] += float(score)
                if parsed is not None:
                    agg["success"] += 1.0

                report["samples"].append(
                    {
                        "t": float(t),
                        "settings": {
                            "broadcast_type": used_broadcast,
                            "roi": used_roi,
                            "preprocess_style": preprocess_style,
                            "backend": backend_name,
                        },
                        "raw_text": raw_text,
                        "confidence": float(conf or 0.0),
                        "parsed": {"period": parsed[0], "time": parsed[1]} if parsed else None,
                        "score": float(score),
                    }
                )

            # Select the most reliable setting: highest average score, tie-break by success rate.
            best_key = None
            best_tuple = None  # (avg_score, success_rate, key)
            for key, agg in by_setting.items():
                count = float(agg.get("count") or 0.0)
                if count <= 0:
                    continue
                avg_score = float(agg.get("score_sum") or 0.0) / count
                success_rate = float(agg.get("success") or 0.0) / count
                tup = (avg_score, success_rate, key)
                if best_tuple is None or tup > best_tuple:
                    best_tuple = tup
                    best_key = key

            report["summary"] = {
                "distinct_settings": len(by_setting),
                "evaluated_samples": len(report["samples"]),
            }

            if best_key is None:
                report["selected"] = {}
                return report

            # Parse back the selected settings key.
            # key format: broadcast|preprocess|backend|roi_tuple
            parts = best_key.split("|", 3)
            sel_broadcast = parts[0] if len(parts) > 0 else "standard"
            sel_preprocess = parts[1] if len(parts) > 1 else "standard"
            sel_backend = parts[2] if len(parts) > 2 else "tesseract"
            sel_roi = None
            if len(parts) > 3:
                roi_s = str(parts[3] or "")
                nums = re.findall(r"-?\\d+", roi_s)
                if len(nums) == 4:
                    try:
                        sel_roi = (int(nums[0]), int(nums[1]), int(nums[2]), int(nums[3]))
                    except Exception:
                        sel_roi = None

            if isinstance(sel_roi, tuple) and len(sel_roi) == 4:
                self.scoreboard_roi = sel_roi
            else:
                self.scoreboard_roi = None
            self._broadcast_type = sel_broadcast
            self._preprocess_style = sel_preprocess
            self._backend_name = sel_backend
            report["selected"] = {
                "broadcast_type": sel_broadcast,
                "roi": self.scoreboard_roi,
                "preprocess_style": sel_preprocess,
                "backend": sel_backend,
                "avg_score": float(best_tuple[0]) if best_tuple else None,
                "success_rate": float(best_tuple[1]) if best_tuple else None,
            }
            return report
        finally:
            # If selection did not succeed, restore previous cached values.
            if not report.get("selected"):
                if prev.get("broadcast_type") is not None:
                    self._broadcast_type = prev["broadcast_type"]
                else:
                    if hasattr(self, "_broadcast_type"):
                        try:
                            delattr(self, "_broadcast_type")
                        except Exception:
                            pass
                self.scoreboard_roi = prev.get("roi")
                if prev.get("preprocess") is not None:
                    self._preprocess_style = prev.get("preprocess")
                else:
                    if hasattr(self, "_preprocess_style"):
                        try:
                            delattr(self, "_preprocess_style")
                        except Exception:
                            pass
                if prev.get("backend") is not None:
                    self._backend_name = prev.get("backend")
                else:
                    if hasattr(self, "_backend_name"):
                        try:
                            delattr(self, "_backend_name")
                        except Exception:
                            pass

    def _preprocess_variants(self, image: np.ndarray, *, base_style: str) -> List[Tuple[str, np.ndarray]]:
        """
        Return a small set of preprocessing variants to try during probing.

        We keep this intentionally small: probing runs on a single frame but still
        needs to be fast enough for unattended ingest.
        """
        style = str(base_style or "standard").lower()
        variants = []
        if style in FLO_LIKE_BROADCAST_TYPES:
            variants = ["flohockey", "flohockey_sharp"]
        elif style in {"yarmouth"}:
            variants = ["yarmouth", "yarmouth_invert"]
        else:
            variants = ["standard", "standard_otsu"]

        out: List[Tuple[str, np.ndarray]] = []
        for v in variants:
            try:
                out.append((v, self._preprocess_for_ocr(image, style=v)))
            except Exception:
                continue
        return out or [(style, self._preprocess_for_ocr(image, style=style))]

    def find_game_start(
        self,
        video_processor,
        search_start_minutes: int = 15,
        max_search_minutes: int = 45,
        scan_interval_seconds: int = 60
    ) -> Optional[float]:
        """
        Auto-detect when the actual game starts (puck drop).

        Algorithm:
        1. Linear scan from search_start_minutes with scan_interval_seconds steps
        2. Look for pattern: 20:00 readings, then a reading < 20:00
        3. Once transition found, refine with smaller steps
        4. Return video timestamp ~3 seconds before clock starts counting down

        This is more robust than binary search because OCR can fail intermittently.

        Args:
            video_processor: VideoProcessor instance with loaded video
            search_start_minutes: Start scanning from this point (default 15 min)
            max_search_minutes: Stop scanning at this point (default 45 min)
            scan_interval_seconds: Seconds between scan samples (default 60s)

        Returns:
            Video timestamp in seconds where puck drops, or None if not found
        """
        logger.info("=" * 60)
        logger.info("AUTO-DETECTING GAME START")
        logger.info("=" * 60)

        duration = video_processor.duration
        scan_start = min(search_start_minutes * 60, duration * 0.1)
        scan_end = min(max_search_minutes * 60, duration * 0.5)

        def check_time_at(timestamp: float) -> Optional[tuple]:
            """Check OCR at timestamp, return (period, time_str, time_seconds) or None"""
            frame = video_processor.get_frame_at_time(timestamp)
            if frame is None:
                return None
            result = self.extract_time_from_frame(frame)
            if result:
                period, time_str = result
                time_seconds = self._time_to_seconds(time_str)
                return (period, time_str, time_seconds)
            return None

        logger.info(f"Scanning from {scan_start/60:.1f} to {scan_end/60:.1f} minutes...")

        # Phase 1: Find the 20:00 → <20:00 transition region
        last_20_00_timestamp = None
        first_running_timestamp = None
        first_running_time = None
        scanned_results = []  # (video_time, period, time_str, time_seconds)

        current_time = scan_start
        while current_time <= scan_end:
            result = check_time_at(current_time)

            if result:
                period, time_str, time_seconds = result
                scanned_results.append((float(current_time), int(period), str(time_str), int(time_seconds)))
                logger.debug(f"  {current_time/60:.1f}m: P{period} {time_str}")

                if time_seconds >= 20 * 60:
                    # Clock shows 20:00 - period hasn't started yet
                    last_20_00_timestamp = current_time
                    logger.info(f"  {current_time/60:.1f}m: Found 20:00 (pre-game)")
                elif time_seconds < 20 * 60:
                    # Clock is running (< 20:00) - game in progress
                    if first_running_timestamp is None:
                        first_running_timestamp = current_time
                        first_running_time = time_str
                        logger.info(f"  {current_time/60:.1f}m: Clock running at {time_str}")

                        # If we found both, we have the transition region
                        if last_20_00_timestamp is not None:
                            break

            current_time += scan_interval_seconds

        # Phase 2: Refine the transition point
        if last_20_00_timestamp is not None and first_running_timestamp is not None:
            logger.info(f"Transition region: {last_20_00_timestamp/60:.1f}m - {first_running_timestamp/60:.1f}m")

            # Fine scan within the transition region
            game_start = self._refine_game_start(
                video_processor,
                last_20_00_timestamp,
                first_running_timestamp,
                check_time_at
            )

            if game_start is not None:
                logger.info(f"Game starts at {game_start/60:.2f} minutes ({game_start:.0f}s)")
                return game_start

        # Detect the common recorded-stream pattern where warmup counts down in P1,
        # then the real game later resets back near 20:00. This shows up as a large
        # upward clock jump within the same displayed period.
        reset_candidate = None
        for prev, curr in zip(scanned_results, scanned_results[1:]):
            prev_t, prev_period, prev_time_str, prev_seconds = prev
            curr_t, curr_period, curr_time_str, curr_seconds = curr
            if prev_period != 1 or curr_period != 1:
                continue
            if prev_seconds > 2 * 60:
                continue
            if curr_seconds < 19 * 60:
                continue
            if (curr_seconds - prev_seconds) < 10 * 60:
                continue
            elapsed_game_time = 20 * 60 - curr_seconds
            reset_candidate = max(0.0, curr_t - elapsed_game_time - 3.0)
            logger.info(
                "Detected warmup-to-game clock reset: P1 %s at %.1fm -> P1 %s at %.1fm",
                prev_time_str,
                prev_t / 60.0,
                curr_time_str,
                curr_t / 60.0,
            )
            break

        if reset_candidate is not None:
            logger.info(
                "Estimated game start from clock reset: %.2f minutes",
                reset_candidate / 60.0,
            )
            return reset_candidate

        elif first_running_timestamp is not None:
            # Found running clock but not 20:00 - estimate from game time
            # This is only safe if the clock is still near the start of P1.
            # Estimating from a lone late-period reading (for example 0:21) can
            # jump us deep into warmup or intermission content.
            result = check_time_at(first_running_timestamp)
            if result:
                period, time_str, time_seconds = result
                if period != 1 or time_seconds < 15 * 60:
                    logger.warning(
                        "Ignoring lone running-clock fallback at %.1fm (P%s %s); "
                        "not near the start of the first period",
                        first_running_timestamp / 60.0,
                        period,
                        time_str,
                    )
                    return None
                elapsed_game_time = 20 * 60 - time_seconds  # How much of period has elapsed
                estimated_start = first_running_timestamp - elapsed_game_time - 3
                estimated_start = max(0, estimated_start)
                logger.info(f"Estimated game start: {estimated_start/60:.2f} minutes (from clock {time_str})")
                return estimated_start

        logger.warning(f"Could not find game start within {max_search_minutes} minutes")
        return None

    def _refine_game_start(
        self,
        video_processor,
        pre_start: float,
        post_start: float,
        check_func
    ) -> Optional[float]:
        """
        Refine game start within a known transition region using finer sampling.

        Args:
            video_processor: Video processor instance
            pre_start: Timestamp known to show 20:00
            post_start: Timestamp known to show < 20:00
            check_func: Function to check time at a timestamp

        Returns:
            Refined game start timestamp
        """
        # Sample every 10 seconds within the region
        step = 10
        last_20_00 = pre_start

        current = pre_start + step
        while current < post_start:
            result = check_func(current)
            if result:
                period, time_str, time_seconds = result
                if time_seconds >= 20 * 60:
                    last_20_00 = current
                elif time_seconds < 20 * 60:
                    # Found transition - game started between last_20_00 and current
                    # Puck drop is about 3 seconds before clock starts
                    game_start = last_20_00 + (current - last_20_00) / 2
                    game_start = max(0, game_start - 3)
                    return game_start
            current += step

        # If we get here, return midpoint minus puck drop offset
        game_start = (pre_start + post_start) / 2 - 3
        return max(0, game_start)

    def _binary_search_clock_start(
        self,
        video_processor,
        low: float,
        high: float,
        precision: int,
        check_func
    ) -> Optional[float]:
        """
        Binary search to find where game starts (transition from no-time to time < 20:00)

        Args:
            video_processor: Video processor instance
            low: Lower bound timestamp
            high: Upper bound timestamp (known to have game content)
            precision: Stop when range is within this many seconds
            check_func: Function to check time at timestamp

        Returns:
            Timestamp where game starts
        """
        # First, verify high point has valid time
        high_result = check_func(high)
        if not high_result:
            logger.warning("High point has no valid time - cannot binary search")
            return high

        iterations = 0
        max_iterations = 20  # Safety limit

        while (high - low) > precision and iterations < max_iterations:
            mid = (low + high) / 2
            iterations += 1

            result = check_func(mid)

            if result:
                period, time_str, time_seconds = result
                logger.debug(f"  Binary search: {mid/60:.1f}m -> P{period} {time_str}")

                # If we find valid game time, game has started before this point
                # But we need to check if it's actually game time (< 20:00) or just 20:00
                if time_seconds < 20 * 60:
                    # Game in progress here, search earlier
                    high = mid
                else:
                    # Could be period start, check if game actually in progress
                    low = mid
            else:
                # No time detected - game hasn't started yet, search later
                logger.debug(f"  Binary search: {mid/60:.1f}m -> no time")
                low = mid

        # Return the point where we're confident game has started
        # Add a small buffer to ensure we're past the very start
        game_start = low

        # Verify we can get valid time at our found start point
        # If not, move forward slightly
        for offset in [0, 30, 60, 90, 120]:
            test_point = game_start + offset
            if test_point <= high:
                result = check_func(test_point)
                if result and result[2] < 20 * 60:
                    return test_point

        return game_start

    def _preprocess_for_ocr(self, image: np.ndarray, style: str = 'standard') -> np.ndarray:
        """
        Preprocess image for better OCR accuracy

        Args:
            image: Input image (RGB or BGR)
            style: Preprocessing style ('standard', 'flohockey', 'yarmouth',
                'mhl_summerside', 'mhl_amherst')

        Returns:
            Preprocessed grayscale image
        """
        try:
            # Convert to grayscale
            if len(image.shape) == 3:
                # MoviePy frames are RGB; prefer RGB conversion but be tolerant.
                try:
                    gray = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)
                except Exception:
                    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            else:
                gray = image

            # Resize for better OCR (if too small)
            height = gray.shape[0]
            min_height = 50
            if style in OCR_STYLE_BROADCAST_TYPES:
                min_height = 80
            if height < min_height:
                scale = min_height / height
                gray = cv2.resize(gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)

            style = str(style or "standard").lower()

            if style in {'flohockey', 'mhl_summerside', 'mhl_amherst'}:
                # FloHockey: dark text on a light/gray banner. Hard thresholding
                # can drop punctuation (:) or thin glyphs, producing noisy reads
                # like "1244" instead of "12:44". Tesseract tends to do better on
                # a resized grayscale image with mild edge-preserving denoise.
                return cv2.bilateralFilter(gray, 5, 50, 50)

            if style == 'flohockey_sharp':
                base = cv2.bilateralFilter(gray, 5, 50, 50)
                blur = cv2.GaussianBlur(base, (0, 0), sigmaX=1.0)
                sharp = cv2.addWeighted(base, 1.6, blur, -0.6, 0)
                return sharp

            if style == 'yarmouth':
                # Yarmouth scorebug varies; use a conservative contrast boost + binarization.
                clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
                enhanced = clahe.apply(gray)
                _, binary = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
                return binary

            if style == 'yarmouth_invert':
                inv = cv2.bitwise_not(gray)
                clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
                enhanced = clahe.apply(inv)
                _, binary = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
                return binary

            # Standard preprocessing for other scoreboard types
            # Apply bilateral filter to reduce noise while keeping edges sharp
            denoised = cv2.bilateralFilter(gray, 5, 50, 50)

            # Increase contrast using CLAHE (Contrast Limited Adaptive Histogram Equalization)
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
            enhanced = clahe.apply(denoised)

            if style == "standard_otsu":
                _, binary = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
                return binary

            # Apply thresholding (default)
            return cv2.adaptiveThreshold(
                enhanced,
                255,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY,
                11,
                2
            )

        except Exception as e:
            logger.warning(f"Preprocessing failed, using original: {e}")
            return image

    def _parse_time_text(self, text: str) -> Optional[Tuple[int, str]]:
        """
        Parse period and time from OCR text

        Args:
            text: Raw OCR text

        Returns:
            Tuple of (period, time_string) or None
        """
        text = str(text or "").strip().upper()

        # Pre-game clock (e.g., "PRE 7:19") is not the in-game clock; ignore so game-start
        # detection and timestamp sampling don't lock onto the wrong timer.
        if "PRE" in text:
            return None

        # Common OCR mistakes
        text = re.sub(r"\b0T\b", "OT", text)  # 0T -> OT
        text = re.sub(r"\s+", " ", text).strip()

        def _clean_time(mm: str, ss: str) -> str:
            m = str(mm or "").upper().replace("U", "0").replace("O", "0")
            s = str(ss or "").upper().replace("U", "0").replace("O", "0")
            return f"{m}:{s}"

        def _try_return(period: int, mm: str, ss: str) -> Optional[Tuple[int, str]]:
            try:
                p = int(period)
            except Exception:
                return None
            if not (0 <= p <= 5):
                return None
            time_str = _clean_time(mm, ss)
            if self._validate_time_format(time_str):
                return (p, time_str)
            return None

        # FloHockey-style period tokens. These patterns intentionally allow the colon to be missing:
        #   "1ST 19:56", "1ST1956", "1ST 19 56"
        fh_patterns = [
            (
                1,
                r"\b[01IJLI][ST]{2}[\]\|\)}\-_]*\s*([0-9UO]{1,2})\s*[:\.]?\s*([0-9UO]{2})\b",
            ),
            (
                2,
                r"\b[2Z@][ND]{2}[\]\|\)}\-_]*\s*([0-9UO]{1,2})\s*[:\.]?\s*([0-9UO]{2})\b",
            ),
            (
                3,
                r"\b3[RD]{2}[\]\|\)}\-_]*\s*([0-9UO]{1,2})\s*[:\.]?\s*([0-9UO]{2})\b",
            ),
        ]
        for p, pat in fh_patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                mm, ss = m.group(1), m.group(2)
                out = _try_return(p, mm, ss)
                if out:
                    return out

        # OT / SO
        m = re.search(r"\b(OT|SO)\s*([0-9UO]{1,2})\s*[:\.]?\s*([0-9UO]{2})\b", text, re.IGNORECASE)
        if m:
            period = 4 if m.group(1).upper() == "OT" else 5
            out = _try_return(period, m.group(2), m.group(3))
            if out:
                return out

        # Traditional patterns: "P2 12:00", "PERIOD 3 5:45", or "1 15:23"
        m = re.search(r"\bPERIOD\s*(\d)\s*([0-9UO]{1,2})\s*[:\.]?\s*([0-9UO]{2})\b", text, re.IGNORECASE)
        if m:
            out = _try_return(int(m.group(1)), m.group(2), m.group(3))
            if out:
                return out

        # Some broadcasts show "1 15:23" (digit + whitespace + clock). Require whitespace so we don't
        # mis-parse "19:44" as "P1 9:44".
        m = re.search(r"\b(\d)\s+([0-9UO]{1,2})\s*[:\.]?\s*([0-9UO]{2})\b", text, re.IGNORECASE)
        if m:
            out = _try_return(int(m.group(1)), m.group(2), m.group(3))
            if out:
                return out

        m = re.search(r"\bP(?:ERIOD)?\s*(\d)\s*([0-9UO]{1,2})\s*[:\.]?\s*([0-9UO]{2})\b", text, re.IGNORECASE)
        if m:
            out = _try_return(int(m.group(1)), m.group(2), m.group(3))
            if out:
                return out

        # Time-only fallback. Period token is frequently missed; return period=0 and infer later.
        m = re.search(r"\b([0-9UO]{1,2})\s*[:\.]?\s*([0-9UO]{2})\b", text, re.IGNORECASE)
        if m:
            out = _try_return(0, m.group(1), m.group(2))
            if out:
                logger.debug(f"Found time {out[1]} but no period - marking period unknown (P0)")
                return out

        logger.debug(f"Could not parse time from: {text}")
        return None

    def _validate_time_format(self, time_str: str) -> bool:
        """
        Validate time string format (MM:SS) and sanity check values

        Args:
            time_str: Time string to validate

        Returns:
            True if valid format and sane values
        """
        try:
            parts = time_str.split(':')
            if len(parts) != 2:
                logger.debug(f"Invalid time format (not MM:SS): {time_str}")
                return False

            minutes = int(parts[0])
            seconds = int(parts[1])

            # Validate seconds range
            if not (0 <= seconds <= 59):
                logger.warning(
                    f"Invalid time '{time_str}' - seconds must be 0-59 (got {seconds})"
                )
                return False

            # Hockey periods are 20 minutes max
            if not (0 <= minutes <= 20):
                logger.warning(
                    f"Invalid time '{time_str}' - hockey periods are 20 minutes max (got {minutes})"
                )
                return False

            # The broadcast clock never shows 20:xx (only 20:00 at a faceoff/start).
            if minutes == 20 and seconds != 0:
                logger.warning(
                    f"Invalid time '{time_str}' - 20:xx only valid as 20:00"
                )
                return False

            return True

        except (ValueError, AttributeError) as e:
            logger.debug(f"Failed to parse time '{time_str}': {e}")
            return False

    def _extract_scorebug_crop(
        self,
        frame: np.ndarray,
        roi: Optional[Tuple[int, int, int, int]],
    ) -> Optional[np.ndarray]:
        """Crop the scorebug directly from the native frame without resizing the full image."""
        if roi is None:
            return None
        try:
            x, y, w, h = roi
            if w <= 0 or h <= 0:
                return None
            return frame[y:y + h, x:x + w].copy()
        except Exception:
            return None

    def _measure_sharpness(self, image: Optional[np.ndarray]) -> Optional[float]:
        """
        Estimate blur/sharpness using variance of the Laplacian.

        Higher values generally indicate a sharper crop.
        """
        if image is None:
            return None
        try:
            if len(image.shape) == 3:
                try:
                    gray = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)
                except Exception:
                    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            else:
                gray = image
            return float(cv2.Laplacian(gray, cv2.CV_64F).var())
        except Exception:
            return None

    def _save_scorebug_crop_debug(
        self,
        crop: Optional[np.ndarray],
        *,
        output_dir: Optional[Path],
        sample_idx: int,
        current_time: float,
        confidence: float,
        success: bool,
        raw_text: str,
        failure_counter: int,
        low_conf_counter: int,
    ) -> Optional[str]:
        """
        Save scorebug-only crops for failed and low-confidence OCR samples.
        """
        if crop is None or output_dir is None:
            return None
        if not bool(getattr(self.config, "OCR_DEBUG_SAVE_SCOREBUG_CROPS", True)):
            return None

        threshold = float(getattr(self.config, "OCR_DEBUG_LOW_CONFIDENCE_THRESHOLD", 65.0) or 65.0)
        failure_limit = int(getattr(self.config, "OCR_DEBUG_FAILURE_CROP_LIMIT", 40) or 40)
        low_conf_limit = int(getattr(self.config, "OCR_DEBUG_LOW_CONFIDENCE_CROP_LIMIT", 25) or 25)

        label = None
        ordinal = None
        if not success:
            if failure_counter > failure_limit:
                return None
            label = "failed"
            ordinal = failure_counter
        elif float(confidence or 0.0) < threshold:
            if low_conf_counter > low_conf_limit:
                return None
            label = "lowconf"
            ordinal = low_conf_counter
        else:
            return None

        crop_dir = output_dir / str(getattr(self.config, "OCR_DEBUG_SCOREBUG_CROP_DIRNAME", "ocr_scorebug_crops") or "ocr_scorebug_crops")
        crop_dir.mkdir(parents=True, exist_ok=True)

        safe_text = re.sub(r"[^A-Za-z0-9]+", "-", str(raw_text or "").strip())[:40].strip("-") or "blank"
        crop_path = crop_dir / (
            f"{label}_{ordinal:03d}_sample{sample_idx:04d}_{current_time:08.1f}s_"
            f"conf{int(float(confidence or 0.0)):03d}_{safe_text}.png"
        )
        try:
            image = crop
            if len(image.shape) == 3:
                image = cv2.cvtColor(image, cv2.COLOR_RGB2BGR)
            cv2.imwrite(str(crop_path), image)
            return str(crop_path)
        except Exception:
            return None

    def sample_video_times(
        self,
        video_processor,
        sample_interval: int = 5,
        max_samples: Optional[int] = None,
        debug_dir: Optional[Path] = None,
        parallel: bool = True,
        workers: int = 4,
        start_time: float = 0.0,
        output_dir: Optional[Path] = None,
        game_id: str = "unknown",
        broadcast_type: str = "auto",
    ) -> List[Dict]:
        """
        Sample time from video at regular intervals

        Args:
            video_processor: VideoProcessor instance with loaded video
            sample_interval: Seconds between samples
            max_samples: Maximum number of samples (None for all)
            debug_dir: Optional directory to save debug frames (auto-saves first/middle/last)
            parallel: Whether to use parallel processing (default True)
            workers: Number of worker threads for parallel processing (default 4)
            start_time: Video timestamp to start sampling from (default 0.0)
            output_dir: Optional directory to write OCR logs (for diagnostics)
            game_id: Game identifier for logging

        Returns:
            List of dictionaries with {video_time, period, game_time}
        """
        if parallel and workers > 1:
            try:
                return self._sample_video_times_parallel(
                    video_processor,
                    sample_interval=sample_interval,
                    max_samples=max_samples,
                    debug_dir=debug_dir,
                    workers=workers,
                    start_time=start_time,
                    output_dir=output_dir,
                    game_id=game_id,
                    broadcast_type=broadcast_type,
                )
            except Exception as exc:
                logger.warning(
                    "Parallel OCR sampling failed (%s); falling back to sequential sampling",
                    exc,
                )

        return self._sample_video_times_sequential(
            video_processor,
            sample_interval,
            max_samples,
            debug_dir,
            start_time,
            output_dir=output_dir,
            game_id=game_id,
            broadcast_type=broadcast_type,
        )

    def _sample_video_times_sequential(
        self,
        video_processor,
        sample_interval: int = 5,
        max_samples: Optional[int] = None,
        debug_dir: Optional[Path] = None,
        start_time: float = 0.0,
        output_dir: Optional[Path] = None,
        game_id: str = "unknown",
        broadcast_type: str = "auto",
    ) -> List[Dict]:
        """
        Sample time from video sequentially (original implementation)

        Args:
            video_processor: VideoProcessor instance with loaded video
            sample_interval: Seconds between samples
            max_samples: Maximum number of samples (None for all)
            debug_dir: Optional directory to save debug frames (auto-saves first/middle/last)
            start_time: Video timestamp to start sampling from (default 0.0)
            output_dir: Optional directory to write OCR logs
            game_id: Game identifier for logging

        Returns:
            List of dictionaries with {video_time, period, game_time}
        """
        timestamps = []
        failure_crop_count = 0
        low_conf_crop_count = 0

        # Initialize OCR logger for detailed diagnostics
        ocr_logger = OCRLogger(
            output_dir=Path(output_dir) if output_dir else None,
            game_id=game_id
        )

        try:
            duration = video_processor.duration

            # Calculate total number of samples for progress bar
            sample_duration = duration - start_time
            total_samples = int(sample_duration / sample_interval) + 1
            if max_samples:
                total_samples = min(total_samples, max_samples)

            # Determine which samples to save as debug frames
            debug_sample_indices = set()
            if total_samples > 0:
                debug_sample_indices = {
                    0,                          # First sample
                    total_samples // 2,         # Middle sample
                    total_samples - 1           # Last sample
                }

            # Create progress bar
            progress_bar = tqdm(
                total=total_samples,
                desc="OCR Sampling",
                unit="frame",
                ncols=100
            )

            current_time = start_time
            sample_count = 0

            logger.info(f"Starting OCR sampling from {start_time/60:.1f} minutes")

            while current_time < duration:
                # Check max samples limit
                if max_samples and sample_count >= max_samples:
                    break

                # Get frame at current time
                frame = video_processor.get_frame_at_time(current_time)

                if frame is not None:
                    # Save debug frame for first, middle, and last samples
                    if debug_dir and sample_count in debug_sample_indices:
                        method = str(broadcast_type or "auto").lower()
                        if method not in ROI_PINNED_BROADCAST_TYPES:
                            method = "auto"
                        roi = self.scoreboard_roi or self.detect_scoreboard_roi(frame, method=method)
                        debug_path = debug_dir / f"debug_ocr_frame_{sample_count:04d}_{current_time:.1f}s.jpg"
                        self.save_debug_frame(frame, debug_path, roi)
                        logger.debug(f"Saved debug frame: {debug_path}")

                    # Extract time from frame with metadata for logging
                    result, raw_text, conf, backend_name, used_broadcast, used_roi, preprocess_style = self._extract_time_from_frame_with_meta(
                        frame,
                        broadcast_type=broadcast_type,
                    )
                    scorebug_crop = self._extract_scorebug_crop(frame, used_roi or self.scoreboard_roi)
                    sharpness_score = self._measure_sharpness(scorebug_crop)

                    if result:
                        period, game_time = result
                        time_seconds = self._time_to_seconds(game_time)
                        if float(conf or 0.0) < float(getattr(self.config, "OCR_DEBUG_LOW_CONFIDENCE_THRESHOLD", 65.0) or 65.0):
                            low_conf_crop_count += 1
                        crop_debug_path = self._save_scorebug_crop_debug(
                            scorebug_crop,
                            output_dir=Path(output_dir) if output_dir else None,
                            sample_idx=sample_count,
                            current_time=current_time,
                            confidence=float(conf or 0.0),
                            success=True,
                            raw_text=raw_text,
                            failure_counter=failure_crop_count,
                            low_conf_counter=low_conf_crop_count,
                        )
                        timestamps.append({
                            'video_time': current_time,
                            'period': period,
                            'game_time': game_time,
                            'game_time_seconds': time_seconds,
                            'ocr_confidence': float(conf or 0.0),
                            'ocr_backend': str(backend_name or "unknown"),
                            'ocr_broadcast_type': str(used_broadcast or "unknown"),
                            'ocr_preprocess': str(preprocess_style or "standard"),
                            'ocr_sharpness_score': sharpness_score,
                            'ocr_crop_debug_path': crop_debug_path,
                        })
                        logger.debug(f"Sample at {current_time:.1f}s: P{period} {game_time}")
                        # Update progress bar description with latest result
                        progress_bar.set_postfix({'latest': f"P{period} {game_time}", 'conf': f"{float(conf or 0.0):.0f}"})

                        # Log successful sample
                        ocr_logger.add_sample(OCRSampleLog(
                            video_time=current_time,
                            raw_text=raw_text,
                            parsed_period=period,
                            parsed_time=game_time,
                            parsed_time_seconds=time_seconds,
                            confidence=float(conf or 0.0),
                            backend=str(backend_name or "unknown"),
                            success=True,
                            roi_used=used_roi or self.scoreboard_roi,
                            broadcast_type=str(used_broadcast or getattr(self, '_broadcast_type', 'unknown')),
                            preprocess_style=str(preprocess_style or "standard"),
                            sharpness_score=sharpness_score,
                            crop_debug_path=crop_debug_path,
                        ))
                        self._consecutive_bad_samples = 0
                    else:
                        failure_crop_count += 1
                        crop_debug_path = self._save_scorebug_crop_debug(
                            scorebug_crop,
                            output_dir=Path(output_dir) if output_dir else None,
                            sample_idx=sample_count,
                            current_time=current_time,
                            confidence=float(conf or 0.0),
                            success=False,
                            raw_text=raw_text,
                            failure_counter=failure_crop_count,
                            low_conf_counter=low_conf_crop_count,
                        )
                        # Log failed sample
                        ocr_logger.add_sample(OCRSampleLog(
                            video_time=current_time,
                            raw_text=raw_text or "",
                            parsed_period=None,
                            parsed_time=None,
                            parsed_time_seconds=None,
                            confidence=float(conf or 0.0),
                            backend=str(backend_name or "unknown"),
                            success=False,
                            failure_reason="Could not parse time from OCR text",
                            roi_used=used_roi or self.scoreboard_roi,
                            broadcast_type=str(used_broadcast or getattr(self, '_broadcast_type', 'unknown')),
                            preprocess_style=str(preprocess_style or "standard"),
                            sharpness_score=sharpness_score,
                            crop_debug_path=crop_debug_path,
                        ))
                        self._consecutive_bad_samples += 1

                        # If OCR quality collapses, drop cached ROI/broadcast and re-probe.
                        reset_n = int(getattr(self.config, "OCR_HEALTH_BAD_CONSECUTIVE_SAMPLES_RESET", 10) or 10)
                        if self._consecutive_bad_samples >= max(3, reset_n):
                            logger.info("OCR health collapsed; resetting cached ROI/broadcast and re-probing")
                            self.scoreboard_roi = None
                            if hasattr(self, "_broadcast_type"):
                                try:
                                    delattr(self, "_broadcast_type")
                                except Exception:
                                    pass
                            if hasattr(self, "_preprocess_style"):
                                try:
                                    delattr(self, "_preprocess_style")
                                except Exception:
                                    pass
                            if hasattr(self, "_backend_name"):
                                try:
                                    delattr(self, "_backend_name")
                                except Exception:
                                    pass
                            self._consecutive_bad_samples = 0

                # Update progress bar
                progress_bar.update(1)

                # Move to next sample
                current_time += sample_interval
                sample_count += 1

            # Close progress bar
            progress_bar.close()

            # Write OCR logs
            ocr_logger.write_logs()

            # Persist sampling stats for pipeline-level health decisions.
            total = float(total_samples or 0)
            successful = float(len([s for s in ocr_logger.samples if s.success]))
            with_period = float(len([s for s in ocr_logger.samples if s.success and (s.parsed_period or 0) > 0]))
            confs = [float(s.confidence) for s in ocr_logger.samples if s.success and s.confidence is not None]
            avg_conf = float(sum(confs) / len(confs)) if confs else 0.0
            self._last_sampling_stats = {
                "total_samples": total,
                "successful": successful,
                "with_period": with_period,
                "success_rate": (successful / total) if total > 0 else 0.0,
                "period_rate": (with_period / successful) if successful > 0 else 0.0,
                "avg_confidence": avg_conf,
            }

            logger.info(f"Sampled {len(timestamps)} timestamps from video")
            if debug_dir and debug_sample_indices:
                logger.info(f"Debug frames saved to: {debug_dir}")

            return timestamps

        except Exception as e:
            logger.error(f"Failed to sample video times: {e}")
            # Still write logs on failure
            ocr_logger.write_logs()
            return []

    def _sample_video_times_parallel(
        self,
        video_processor,
        sample_interval: int = 5,
        max_samples: Optional[int] = None,
        debug_dir: Optional[Path] = None,
        workers: int = 4,
        start_time: float = 0.0,
        output_dir: Optional[Path] = None,
        game_id: str = "unknown",
        broadcast_type: str = "auto",
    ) -> List[Dict]:
        """
        Capture frames sequentially, then OCR the scorebug crops in parallel.

        Args:
            video_processor: VideoProcessor instance with loaded video
            sample_interval: Seconds between samples
            max_samples: Maximum number of samples (None for all)
            debug_dir: Optional directory to save debug frames (auto-saves first/middle/last)
            workers: Number of worker threads (default 4)
            start_time: Video timestamp to start sampling from (default 0.0)
            output_dir: Optional directory to write OCR logs
            game_id: Game identifier for logging
            broadcast_type: Pinned or auto-detected broadcast type

        Returns:
            List of dictionaries with {video_time, period, game_time}
        """
        timestamps: List[Dict] = []
        failure_crop_count = 0
        low_conf_crop_count = 0
        ocr_logger = OCRLogger(
            output_dir=Path(output_dir) if output_dir else None,
            game_id=game_id,
        )

        try:
            duration = float(video_processor.duration or 0.0)
            if duration <= 0:
                return []

            logger.info(
                "Starting parallel OCR sampling from %.1f minutes with %s workers",
                float(start_time) / 60.0,
                int(max(1, workers)),
            )

            sample_times: List[float] = []
            current_time = float(start_time)
            while current_time < duration:
                sample_times.append(current_time)
                current_time += float(sample_interval)
                if max_samples and len(sample_times) >= max_samples:
                    break

            total_samples = len(sample_times)
            if total_samples == 0:
                return []

            debug_sample_indices = set()
            if total_samples > 0:
                debug_sample_indices = {
                    0,
                    total_samples // 2,
                    total_samples - 1,
                }

            requested_broadcast = str(broadcast_type or "auto").lower()
            pinned_broadcast = requested_broadcast
            pinned_roi = self.scoreboard_roi

            sample_payloads: List[Dict] = []
            capture_bar = tqdm(total=total_samples, desc="Capture Frames", unit="frame", ncols=100)

            for idx, sample_time in enumerate(sample_times):
                frame = video_processor.get_frame_at_time(float(sample_time))
                if frame is None:
                    sample_payloads.append({"idx": idx, "sample_time": float(sample_time), "crop": None})
                    capture_bar.update(1)
                    continue

                if pinned_roi is None or requested_broadcast == "auto":
                    if requested_broadcast == "auto":
                        bt, roi_sel, style_sel, backend_sel = self._select_best_settings(frame)
                        self._broadcast_type = bt
                        self.scoreboard_roi = roi_sel
                        self._preprocess_style = style_sel
                        self._backend_name = backend_sel
                        pinned_broadcast = bt
                        pinned_roi = roi_sel
                    else:
                        method = requested_broadcast if requested_broadcast in ROI_PINNED_BROADCAST_TYPES else "auto"
                        pinned_roi = self.detect_scoreboard_roi(frame, method=method)
                        self.scoreboard_roi = pinned_roi

                if debug_dir and idx in debug_sample_indices and pinned_roi is not None:
                    debug_path = debug_dir / f"debug_ocr_frame_{idx:04d}_{sample_time:.1f}s.jpg"
                    self.save_debug_frame(frame, debug_path, pinned_roi)

                crop = self._extract_scorebug_crop(frame, pinned_roi)
                sample_payloads.append(
                    {
                        "idx": idx,
                        "sample_time": float(sample_time),
                        "crop": crop,
                        "broadcast_type": str(pinned_broadcast or requested_broadcast or "standard"),
                        "roi": pinned_roi,
                    }
                )
                capture_bar.update(1)

            capture_bar.close()

            def _ocr_payload(payload: Dict) -> Dict:
                crop = payload.get("crop")
                sample_time = float(payload.get("sample_time") or 0.0)
                if crop is None:
                    return {
                        "video_time": sample_time,
                        "result": None,
                        "raw_text": "",
                        "confidence": 0.0,
                        "backend_name": "unknown",
                        "used_broadcast": str(payload.get("broadcast_type") or "unknown"),
                        "used_roi": payload.get("roi"),
                        "preprocess_style": "standard",
                        "crop": None,
                        "sharpness": None,
                    }
                h, w = crop.shape[:2]
                full_roi = (0, 0, int(w), int(h))
                result, raw_text, conf, backend_name, used_broadcast, _used_roi, preprocess_style = self._extract_time_from_frame_with_meta(
                    crop,
                    roi=full_roi,
                    broadcast_type=str(payload.get("broadcast_type") or "standard"),
                )
                return {
                    "video_time": sample_time,
                    "result": result,
                    "raw_text": raw_text,
                    "confidence": float(conf or 0.0),
                    "backend_name": str(backend_name or "unknown"),
                    "used_broadcast": str(used_broadcast or payload.get("broadcast_type") or "unknown"),
                    "used_roi": payload.get("roi"),
                    "preprocess_style": str(preprocess_style or "standard"),
                    "crop": crop,
                    "sharpness": self._measure_sharpness(crop),
                }

            ocr_results: List[Dict] = []
            ocr_bar = tqdm(total=total_samples, desc=f"OCR ({max(1, workers)} workers)", unit="frame", ncols=100)
            with ThreadPoolExecutor(max_workers=max(1, int(workers or 1))) as executor:
                future_map = {executor.submit(_ocr_payload, payload): payload for payload in sample_payloads}
                for future in as_completed(future_map):
                    payload = future_map[future]
                    try:
                        ocr_results.append(future.result())
                    except Exception as exc:
                        logger.warning("OCR sample at %.1fs failed: %s", float(payload.get("sample_time") or 0.0), exc)
                        ocr_results.append(
                            {
                                "video_time": float(payload.get("sample_time") or 0.0),
                                "result": None,
                                "raw_text": "",
                                "confidence": 0.0,
                                "backend_name": "unknown",
                                "used_broadcast": str(payload.get("broadcast_type") or "unknown"),
                                "used_roi": payload.get("roi"),
                                "preprocess_style": "standard",
                                "crop": payload.get("crop"),
                                "sharpness": None,
                            }
                        )
                    ocr_bar.update(1)
            ocr_bar.close()

            ocr_results.sort(key=lambda item: float(item.get("video_time") or 0.0))

            for sample in ocr_results:
                sample_time = float(sample.get("video_time") or 0.0)
                crop = sample.get("crop")
                result = sample.get("result")
                raw_text = str(sample.get("raw_text") or "")
                conf = float(sample.get("confidence") or 0.0)
                backend_name = str(sample.get("backend_name") or "unknown")
                used_broadcast = str(sample.get("used_broadcast") or "unknown")
                used_roi = sample.get("used_roi")
                preprocess_style = str(sample.get("preprocess_style") or "standard")
                sharpness_score = sample.get("sharpness")

                if result:
                    period, game_time = result
                    time_seconds = self._time_to_seconds(game_time)
                    if conf < float(getattr(self.config, "OCR_DEBUG_LOW_CONFIDENCE_THRESHOLD", 65.0) or 65.0):
                        low_conf_crop_count += 1
                    crop_debug_path = self._save_scorebug_crop_debug(
                        crop,
                        output_dir=Path(output_dir) if output_dir else None,
                        sample_idx=int(round(sample_time)),
                        current_time=sample_time,
                        confidence=conf,
                        success=True,
                        raw_text=raw_text,
                        failure_counter=failure_crop_count,
                        low_conf_counter=low_conf_crop_count,
                    )
                    timestamps.append(
                        {
                            "video_time": sample_time,
                            "period": period,
                            "game_time": game_time,
                            "game_time_seconds": time_seconds,
                            "ocr_confidence": conf,
                            "ocr_backend": backend_name,
                            "ocr_broadcast_type": used_broadcast,
                            "ocr_preprocess": preprocess_style,
                            "ocr_sharpness_score": sharpness_score,
                            "ocr_crop_debug_path": crop_debug_path,
                        }
                    )
                    ocr_logger.add_sample(
                        OCRSampleLog(
                            video_time=sample_time,
                            raw_text=raw_text,
                            parsed_period=period,
                            parsed_time=game_time,
                            parsed_time_seconds=time_seconds,
                            confidence=conf,
                            backend=backend_name,
                            success=True,
                            roi_used=used_roi,
                            broadcast_type=used_broadcast,
                            preprocess_style=preprocess_style,
                            sharpness_score=sharpness_score,
                            crop_debug_path=crop_debug_path,
                        )
                    )
                else:
                    failure_crop_count += 1
                    crop_debug_path = self._save_scorebug_crop_debug(
                        crop,
                        output_dir=Path(output_dir) if output_dir else None,
                        sample_idx=int(round(sample_time)),
                        current_time=sample_time,
                        confidence=conf,
                        success=False,
                        raw_text=raw_text,
                        failure_counter=failure_crop_count,
                        low_conf_counter=low_conf_crop_count,
                    )
                    ocr_logger.add_sample(
                        OCRSampleLog(
                            video_time=sample_time,
                            raw_text=raw_text,
                            parsed_period=None,
                            parsed_time=None,
                            parsed_time_seconds=None,
                            confidence=conf,
                            backend=backend_name,
                            success=False,
                            failure_reason="Could not parse time from OCR text",
                            roi_used=used_roi,
                            broadcast_type=used_broadcast,
                            preprocess_style=preprocess_style,
                            sharpness_score=sharpness_score,
                            crop_debug_path=crop_debug_path,
                        )
                    )

            ocr_logger.write_logs()

            total = float(total_samples or 0)
            successful = float(len([s for s in ocr_logger.samples if s.success]))
            with_period = float(len([s for s in ocr_logger.samples if s.success and (s.parsed_period or 0) > 0]))
            confs = [float(s.confidence) for s in ocr_logger.samples if s.success and s.confidence is not None]
            avg_conf = float(sum(confs) / len(confs)) if confs else 0.0
            self._last_sampling_stats = {
                "total_samples": total,
                "successful": successful,
                "with_period": with_period,
                "success_rate": (successful / total) if total > 0 else 0.0,
                "period_rate": (with_period / successful) if successful > 0 else 0.0,
                "avg_confidence": avg_conf,
            }

            logger.info("Sampled %s timestamps from video (parallel OCR)", len(timestamps))
            if debug_dir and debug_sample_indices:
                logger.info("Debug frames saved to: %s", debug_dir)
            return timestamps

        except Exception as e:
            logger.error(f"Failed to sample video times (parallel): {e}")
            ocr_logger.write_logs()
            return []

    def _extract_time_at_sample(
        self,
        video_processor,
        sample_time: float,
        sample_idx: int,
        save_debug: bool,
        debug_dir: Optional[Path]
    ) -> Optional[Dict]:
        """
        Helper method to extract time at a specific sample position (thread-safe)

        Args:
            video_processor: VideoProcessor instance
            sample_time: Time in video to sample
            sample_idx: Index of this sample
            save_debug: Whether to save debug frame
            debug_dir: Directory for debug frames

        Returns:
            Dictionary with timestamp data or None if extraction failed
        """
        try:
            # Get frame at current time
            frame = video_processor.get_frame_at_time(sample_time)

            if frame is None:
                return None

            # Save debug frame if requested
            if save_debug and debug_dir:
                roi = self.scoreboard_roi or self.detect_scoreboard_roi(frame)
                debug_path = debug_dir / f"debug_ocr_frame_{sample_idx:04d}_{sample_time:.1f}s.jpg"
                self.save_debug_frame(frame, debug_path, roi)
                logger.debug(f"Saved debug frame: {debug_path}")

            # Extract time from frame
            result = self.extract_time_from_frame(frame)

            if result:
                period, game_time = result
                return {
                    'video_time': sample_time,
                    'period': period,
                    'game_time': game_time,
                    'game_time_seconds': self._time_to_seconds(game_time)
                }

            return None

        except Exception as e:
            logger.debug(f"Failed to extract time at {sample_time:.1f}s: {e}")
            return None

    def _time_to_seconds(self, time_str: str) -> int:
        """
        Convert MM:SS time string to seconds

        Args:
            time_str: Time string in MM:SS format

        Returns:
            Time in seconds
        """
        try:
            parts = time_str.split(':')
            if len(parts) == 2:
                minutes = int(parts[0])
                seconds = int(parts[1])
                return minutes * 60 + seconds
        except (ValueError, AttributeError):
            pass

        return 0

    def save_debug_frame(self, frame: np.ndarray, output_path: Path, roi: Optional[Tuple] = None):
        """
        Save frame with ROI highlighted for debugging

        Args:
            frame: Video frame
            output_path: Where to save image
            roi: Optional ROI to highlight
        """
        try:
            # Make a copy
            debug_frame = frame.copy()

            # Draw ROI if provided
            if roi:
                x, y, w, h = roi
                cv2.rectangle(debug_frame, (x, y), (x+w, y+h), (0, 255, 0), 2)

            # Save
            cv2.imwrite(str(output_path), debug_frame)
            logger.info(f"Saved debug frame to {output_path}")

        except Exception as e:
            logger.error(f"Failed to save debug frame: {e}")
