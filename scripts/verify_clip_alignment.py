#!/usr/bin/env python3
"""
Verify that per-clip video content matches box score event times.

For each clip in `Games/<game>/data/clips_manifest.json`, this script:
- Converts the box-score time (ELAPSED) → expected broadcast clock (REMAINING)
- Samples frames near the event moment inside the clip (≈ before_seconds)
- Runs OCR on the scoreboard to read (period, clock)
- Reports pass/fail based on tolerance

This helps catch gross mismatches like: "box score says 13:00 elapsed" but the clip
shows "10:00 remaining" on the broadcast clock.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402
from highlight_extractor.ocr_engine import OCREngine  # noqa: E402
from highlight_extractor.time_utils import (  # noqa: E402
    PERIOD_LENGTH_SECONDS,
    OT_LENGTH_SECONDS,
    seconds_to_time_string,
    time_string_to_seconds,
)

try:
    from moviepy import VideoFileClip
except Exception:  # pragma: no cover
    from moviepy.editor import VideoFileClip  # type: ignore


def _period_length_seconds(period: int) -> int:
    return OT_LENGTH_SECONDS if int(period or 0) >= 4 else PERIOD_LENGTH_SECONDS


def _expected_remaining_seconds(*, period: int, box_time: str) -> int:
    """
    Convert a box-score time string to the expected broadcast clock time in seconds.

    MHL box scores report time ELAPSED in the period; broadcasts show time REMAINING.
    """
    value = time_string_to_seconds(str(box_time or "0:00"))
    if not bool(getattr(config, "BOX_SCORE_TIME_IS_ELAPSED", True)):
        return int(value)
    period_len = _period_length_seconds(period)
    return max(0, min(period_len, period_len - int(value)))


@dataclass(frozen=True)
class CheckResult:
    status: str  # pass | fail | no_ocr | missing
    clip: str
    event: Dict[str, Any]
    expected_period: int
    expected_clock: str
    observed_period: Optional[int] = None
    observed_clock: Optional[str] = None
    observed_t: Optional[float] = None
    diff_seconds: Optional[int] = None
    saved_frame: Optional[str] = None


def _load_manifest(game_dir: Path) -> List[Dict[str, Any]]:
    manifest_path = game_dir / "data" / "clips_manifest.json"
    if not manifest_path.exists():
        # Backwards-compat fallback for older runs: reconstruct a best-effort manifest
        # by pairing `data/matched_events.json` (sorted by video_time) with the sorted
        # clip filenames in `clips/`.
        clips_dir = game_dir / "clips"
        clip_files = sorted([p for p in clips_dir.glob("*.mp4")]) if clips_dir.exists() else []

        matched_events_path = game_dir / "data" / "matched_events.json"
        events: List[Dict[str, Any]] = []
        if matched_events_path.exists():
            try:
                payload = json.loads(matched_events_path.read_text(encoding="utf-8"))
                if isinstance(payload, list):
                    events = [e for e in payload if isinstance(e, dict) and e.get("video_time") is not None]
                    events.sort(key=lambda e: float(e.get("video_time") or 0.0))
            except Exception:
                events = []

        if clip_files:
            out: List[Dict[str, Any]] = []
            for idx, clip_path in enumerate(clip_files, 1):
                event = events[idx - 1] if idx - 1 < len(events) else {}
                entry: Dict[str, Any] = dict(event) if isinstance(event, dict) else {}
                entry["index"] = idx
                entry["clip_filename"] = clip_path.name
                try:
                    entry["path"] = str(clip_path.relative_to(game_dir))
                except Exception:
                    entry["path"] = str(clip_path)
                out.append(entry)
            return out

        raise FileNotFoundError(f"Missing clips manifest: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    # New format: {"clips": [...]}
    if isinstance(payload, dict) and isinstance(payload.get("clips"), list):
        return [c for c in payload["clips"] if isinstance(c, dict)]

    # Legacy format: [...]
    if isinstance(payload, list):
        return [c for c in payload if isinstance(c, dict)]

    raise ValueError(f"Invalid clips manifest structure: {manifest_path}")


def _try_ocr_at_times(
    *,
    clip: VideoFileClip,
    ocr: OCREngine,
    times: List[float],
    expected_period: int,
    expected_remaining: int,
    broadcast_type: str,
) -> Optional[Tuple[int, int, str, float]]:
    """
    Try OCR at multiple timestamps; returns (period, seconds_remaining, time_str, t).
    Prefers matches where OCR period matches expected.
    """
    best = None  # (period_match, diff, period, seconds, time_str, t)
    for t in times:
        if t < 0 or t > float(clip.duration or 0):
            continue
        try:
            frame = clip.get_frame(t)
        except Exception:
            continue
        result = ocr.extract_time_from_frame(frame, broadcast_type=broadcast_type)
        if not result:
            continue
        p_raw, time_str = result
        try:
            p = int(p_raw)
        except Exception:
            p = 0
        try:
            sec = int(time_string_to_seconds(str(time_str)))
        except Exception:
            continue
        period_match = 1 if (p == expected_period) else 0
        diff = abs(int(sec) - int(expected_remaining))
        key = (period_match, -diff)
        if best is None or key > best[0]:
            best = (key, p, sec, str(time_str), float(t))
    if best is None:
        return None
    _, p, sec, time_str, t = best
    return (p, sec, time_str, t)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify per-clip alignment via OCR.")
    parser.add_argument("--game-dir", type=Path, required=True, help="Game folder under Games/...")
    parser.add_argument("--tolerance-seconds", type=int, default=15, help="Allowed clock difference (seconds)")
    parser.add_argument("--max-clips", type=int, default=0, help="Limit number of clips checked (0=all)")
    parser.add_argument("--scan-window-seconds", type=float, default=12.0, help="Extra scan window around event time (default: 12s)")
    parser.add_argument("--scan-step-seconds", type=float, default=1.5, help="Scan step when initial probe fails (default: 1.5s)")
    parser.add_argument(
        "--broadcast-type",
        default="auto",
        help="OCR broadcast type: auto, flohockey, yarmouth, standard (default: auto)",
    )
    parser.add_argument("--save-frames", action="store_true", help="Save a debug frame for each checked clip")
    args = parser.parse_args()

    game_dir: Path = args.game_dir
    manifest = _load_manifest(game_dir)
    if int(args.max_clips) > 0:
        manifest = manifest[: int(args.max_clips)]

    ocr = OCREngine(config)
    out_dir = game_dir / "data" / "verify"
    if bool(args.save_frames):
        out_dir.mkdir(parents=True, exist_ok=True)

    results: List[CheckResult] = []
    for entry in manifest:
        # Normalize legacy/new manifest shapes:
        # - New: event fields at top-level + clip_filename
        # - Old: {"clip_filename": "...", "event": {...}}
        event = entry.get("event") if isinstance(entry.get("event"), dict) else entry

        clip_name = str(entry.get("clip_filename") or entry.get("path") or entry.get("clip_relpath") or "")
        clip_filename = str(entry.get("clip_filename") or "").strip()
        clip_relpath = str(entry.get("path") or entry.get("clip_relpath") or "").strip()
        clip_path: Optional[Path] = None
        if clip_relpath:
            clip_path = game_dir / clip_relpath
        elif clip_filename:
            clip_path = game_dir / "clips" / clip_filename

        if clip_path is None or not clip_path.exists():
            results.append(
                CheckResult(
                    status="missing",
                    clip=clip_name,
                    event=event,
                    expected_period=int(event.get("period") or 0),
                    expected_clock="",
                )
            )
            continue

        period = int(event.get("period") or 0)
        time_str = str(event.get("time") or "").strip()
        expected_remaining = _expected_remaining_seconds(period=period or 1, box_time=time_str)
        expected_clock = seconds_to_time_string(expected_remaining)

        # Default: event happens ~before_seconds into the clip.
        before_seconds = event.get("before_seconds") if isinstance(event, dict) else None
        try:
            before_f = float(before_seconds) if before_seconds is not None else 0.0
        except Exception:
            before_f = 0.0
        if before_f <= 0:
            clip_type = str(event.get("type") or "").strip().lower()
            if clip_type == "penalty":
                before_f = float(getattr(config, "PENALTY_PP_BEFORE_SECONDS", 2.0))
            else:
                before_f = float(getattr(config, "DEFAULT_CLIP_BEFORE_TIME", 15.0))

        try:
            with VideoFileClip(str(clip_path)) as clip:
                # Reset cached detection per clip to avoid carrying over a bad ROI/broadcast lock.
                try:
                    ocr.scoreboard_roi = None
                except Exception:
                    pass
                if hasattr(ocr, "_broadcast_type"):
                    try:
                        delattr(ocr, "_broadcast_type")
                    except Exception:
                        pass

                t0 = max(0.0, min(float(clip.duration or 0.0), before_f))
                sample_times = [t0 + dt for dt in (-1.0, -0.5, 0.0, 0.5, 1.0, 2.0)]
                ocr_hit = _try_ocr_at_times(
                    clip=clip,
                    ocr=ocr,
                    times=sample_times,
                    expected_period=period,
                    expected_remaining=expected_remaining,
                    broadcast_type=str(args.broadcast_type),
                )
                if ocr_hit is None:
                    window = max(0.0, float(args.scan_window_seconds))
                    step = max(0.25, float(args.scan_step_seconds))
                    scan_start = max(0.0, t0 - window)
                    scan_end = min(float(clip.duration or 0.0), t0 + window)
                    scan_times: List[float] = []
                    t = scan_start
                    while t <= scan_end:
                        scan_times.append(float(t))
                        t += step
                    ocr_hit = _try_ocr_at_times(
                        clip=clip,
                        ocr=ocr,
                        times=scan_times,
                        expected_period=period,
                        expected_remaining=expected_remaining,
                        broadcast_type=str(args.broadcast_type),
                    )

                saved_frame = None
                if bool(args.save_frames):
                    frame_t = t0
                    frame_path = out_dir / f"{entry.get('index', 0):03d}_{clip_path.stem}.jpg"
                    try:
                        clip.save_frame(str(frame_path), t=frame_t)
                        saved_frame = str(frame_path)
                    except Exception:
                        saved_frame = None

                if not ocr_hit:
                    results.append(
                        CheckResult(
                            status="no_ocr",
                            clip=clip_path.name,
                            event=event,
                            expected_period=period,
                            expected_clock=expected_clock,
                            saved_frame=saved_frame,
                        )
                    )
                    continue

                observed_period, observed_sec, observed_clock, observed_t = ocr_hit
                diff = abs(int(observed_sec) - int(expected_remaining))
                status = "pass" if diff <= int(args.tolerance_seconds) else "fail"
                results.append(
                    CheckResult(
                        status=status,
                        clip=clip_path.name,
                        event=event,
                        expected_period=period,
                        expected_clock=expected_clock,
                        observed_period=int(observed_period) if observed_period is not None else None,
                        observed_clock=str(observed_clock),
                        observed_t=float(observed_t),
                        diff_seconds=int(diff),
                        saved_frame=saved_frame,
                    )
                )
        except Exception:
            results.append(
                CheckResult(
                    status="no_ocr",
                    clip=clip_path.name,
                    event=event,
                    expected_period=period,
                    expected_clock=expected_clock,
                )
            )

    # Print summary
    counts = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1

    print("Verification summary:")
    for k in sorted(counts):
        print(f"  {k}: {counts[k]}")

    # Write machine-readable report
    report_path = game_dir / "data" / "verify_report.json"
    report_payload = {
        "game_dir": str(game_dir),
        "tolerance_seconds": int(args.tolerance_seconds),
        "results": [r.__dict__ for r in results],
    }
    report_path.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")
    print(f"Wrote: {report_path}")

    # Non-zero exit when we have hard failures.
    return 1 if counts.get("fail", 0) else 0


if __name__ == "__main__":
    raise SystemExit(main())
