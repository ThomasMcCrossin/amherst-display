"""
Pick the scorebug layout for a recording before the full OCR pass.

1. OCR vote (local, free): every known layout reads a handful of frames. Each layout
   only parses its own scorebug (see tests/test_scorebug_layouts.py), so the layout
   with the most valid reads wins.
2. Vision fallback (optional): when no layout wins clearly and a vision API key is
   configured, an OpenAI-compatible vision model (DeepSeek by default) is shown a few
   frames and asked which known option they show, or "none".

Returns a ScorebugProfile, or None to keep the catalog/auto-probe behaviour.

Vision env: SCOREBUG_VISION_API_KEY (or DEEPSEEK_API_KEY), SCOREBUG_VISION_BASE_URL
(default https://api.deepseek.com), SCOREBUG_VISION_MODEL (default deepseek-flash).
"""

from __future__ import annotations

import base64
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import cv2
import numpy as np

from scorebug_profiles import SCOREBUG_PROFILES, ScorebugProfile

logger = logging.getLogger(__name__)

DEFAULT_VISION_BASE_URL = "https://api.deepseek.com"
DEFAULT_VISION_MODEL = "deepseek-flash"
# Optional reference crop per profile, shown to the vision model next to its option.
REFERENCE_DIR = Path(__file__).resolve().parent / "assets" / "scorebugs"


def sample_frames(video_path: Path, count: int = 8, start_frac: float = 0.2, end_frac: float = 0.8) -> List[np.ndarray]:
    """Evenly spaced frames from the middle of the recording (skips pregame and postgame)."""
    cap = cv2.VideoCapture(str(video_path))
    try:
        fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
        total = cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0.0
        duration = total / fps if fps else 0.0
        if duration <= 0:
            return []
        frames = []
        for i in range(count):
            t = duration * (start_frac + (end_frac - start_frac) * i / max(1, count - 1))
            cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000.0)
            ok, frame = cap.read()
            if ok and frame is not None:
                frames.append(frame)
        return frames
    finally:
        cap.release()


def candidate_profiles(profiles=SCOREBUG_PROFILES) -> List[ScorebugProfile]:
    """One known profile per distinct layout, in catalog order."""
    seen, out = set(), []
    for profile in profiles:
        if profile.known_layout and profile.broadcast_type not in seen:
            seen.add(profile.broadcast_type)
            out.append(profile)
    return out


def ocr_vote(frames: List[np.ndarray], profiles: List[ScorebugProfile], engine=None) -> Dict[str, int]:
    if engine is None:
        from highlight_extractor.ocr_engine import OCREngine
        engine = OCREngine()
    hits: Dict[str, int] = {}
    for profile in profiles:
        count = 0
        for frame in frames:
            engine.scoreboard_roi = None
            parsed = engine.extract_time_from_frame(frame, broadcast_type=profile.broadcast_type)
            if parsed is not None and int(parsed[0]) != 0:
                count += 1
        hits[profile.profile_id] = count
    return hits


def _jpeg_data_url(frame: np.ndarray, width: int = 640) -> str:
    h, w = frame.shape[:2]
    small = cv2.resize(frame, (width, int(h * width / w))) if w > width else frame
    ok, buf = cv2.imencode(".jpg", small, [cv2.IMWRITE_JPEG_QUALITY, 80])
    return "data:image/jpeg;base64," + base64.b64encode(buf.tobytes()).decode()


def vision_choose(frames: List[np.ndarray], profiles: List[ScorebugProfile]) -> Tuple[Optional[str], Dict[str, Any]]:
    api_key = os.environ.get("SCOREBUG_VISION_API_KEY") or os.environ.get("DEEPSEEK_API_KEY")
    if not api_key or not frames:
        return None, {"skipped": "no vision API key" if not api_key else "no frames"}
    import requests

    base_url = (os.environ.get("SCOREBUG_VISION_BASE_URL") or DEFAULT_VISION_BASE_URL).rstrip("/")
    model = os.environ.get("SCOREBUG_VISION_MODEL") or DEFAULT_VISION_MODEL
    content: List[Dict[str, Any]] = [{"type": "text", "text": (
        "Below are reference scorebug layouts, then frames from one hockey broadcast. "
        "Which reference layout do the broadcast frames show? Compare where the scorebug sits "
        "and how its rows, period and clock are arranged, not just the team names."
    )}]
    for p in profiles:
        ref = REFERENCE_DIR / f"{p.profile_id}.png"
        content.append({"type": "text", "text": f'Option "{p.profile_id}": {p.description}'})
        if ref.exists():
            content.append({"type": "image_url", "image_url": {"url": _jpeg_data_url(cv2.imread(str(ref)))}})
    content.append({"type": "text", "text": "Top band of each broadcast frame:"})
    for frame in frames[:4]:
        # Scorebugs sit in the top band; send it at full width so row structure stays legible.
        band = frame[: max(1, int(frame.shape[0] * 0.22))]
        content.append({"type": "image_url", "image_url": {"url": _jpeg_data_url(band, width=1280)}})
    content.append({"type": "text", "text": (
        'Reply with JSON only: {"choice": "<option id>" or "none", "confidence": 0-1, '
        '"description": "where the scorebug is and how period, clock and scores are laid out"}'
    )})
    body: Dict[str, Any] = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [{"role": "user", "content": content}],
    }
    if "deepseek" in base_url:
        body["thinking"] = {"type": "disabled"}
    resp = requests.post(f"{base_url}/chat/completions", json=body, timeout=120,
                         headers={"Authorization": f"Bearer {api_key}"})
    resp.raise_for_status()
    payload = resp.json()
    answer = json.loads(payload["choices"][0]["message"]["content"])
    report = {"answer": answer, "usage": payload.get("usage", {}), "model": model}
    choice = str(answer.get("choice") or "").strip()
    known = {p.profile_id for p in profiles}
    return (choice if choice in known else None), report


def detect_scorebug_profile(
    video_path: Path,
    *,
    profiles=SCOREBUG_PROFILES,
    engine=None,
    frame_count: int = 8,
    min_hits: int = 3,
) -> Tuple[Optional[ScorebugProfile], Dict[str, Any]]:
    candidates = candidate_profiles(profiles)
    by_id = {p.profile_id: p for p in candidates}
    frames = sample_frames(Path(video_path), count=frame_count)
    report: Dict[str, Any] = {"frames": len(frames), "method": None}
    if not frames:
        report["method"] = "no_frames"
        return None, report

    hits = ocr_vote(frames, candidates, engine=engine)
    report["ocr_hits"] = hits
    ranked = sorted(hits.items(), key=lambda kv: kv[1], reverse=True)
    best_id, best = ranked[0]
    runner_up = ranked[1][1] if len(ranked) > 1 else 0
    if best >= min_hits and best > runner_up:
        report["method"] = "ocr_vote"
        return by_id[best_id], report

    try:
        choice, vision_report = vision_choose(frames, candidates)
    except Exception as exc:  # vision is best-effort; never block highlights on it
        choice, vision_report = None, {"error": str(exc)}
    report["vision"] = vision_report
    if choice:
        report["method"] = "vision"
        return by_id[choice], report
    report["method"] = "undecided"
    return None, report
