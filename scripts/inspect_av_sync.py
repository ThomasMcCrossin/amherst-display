#!/usr/bin/env python3
"""
Inspect A/V stream metadata (start_time, default streams) and estimate a fixed
audio delay that would align audio to video when timestamps are badly offset.

This does not attempt content-based sync; it only uses ffprobe metadata and/or
 user-provided observed markers.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def _ffprobe_json(path: Path) -> Dict[str, Any]:
    proc = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "stream=index,codec_type,codec_name,start_time,bit_rate,channels,disposition:stream_tags=language,title:format=start_time,duration,filename",
            "-of",
            "json",
            str(path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(proc.stdout or "{}")


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _pick_default_stream(streams: List[Dict[str, Any]], codec_type: str) -> Optional[Dict[str, Any]]:
    cands = [s for s in streams if str(s.get("codec_type") or "") == codec_type]
    if not cands:
        return None
    for s in cands:
        disp = s.get("disposition")
        if isinstance(disp, dict) and int(disp.get("default") or 0) == 1:
            return s
    return cands[0]


def main() -> int:
    ap = argparse.ArgumentParser(description="Inspect ffprobe start_time and estimate audio delay.")
    ap.add_argument("video", type=Path, help="Input media file (ts/mp4/mkv/...)")
    ap.add_argument(
        "--threshold-seconds",
        type=float,
        default=60.0,
        help="If abs(video_start-audio_start) >= threshold, print a suggested --audio-delay-seconds (default: 60)",
    )
    ap.add_argument(
        "--marker-audio",
        type=float,
        default=None,
        help="Observed time (seconds) of a known event in the audio track (e.g. puck drop) within the same file",
    )
    ap.add_argument(
        "--marker-video",
        type=float,
        default=None,
        help="Observed time (seconds) of the same event in the video track within the same file",
    )
    args = ap.parse_args()

    path = args.video
    if not path.exists():
        print(f"File not found: {path}", file=sys.stderr)
        return 2

    payload = _ffprobe_json(path)
    streams = payload.get("streams")
    fmt = payload.get("format") or {}
    if not isinstance(streams, list):
        streams = []

    print(f"file: {path}")
    print(f"container_start_time: {fmt.get('start_time')}")
    print(f"duration: {fmt.get('duration')}")
    print("")
    print("streams:")
    for s in streams:
        if not isinstance(s, dict):
            continue
        idx = s.get("index")
        ctype = s.get("codec_type")
        cname = s.get("codec_name")
        start = s.get("start_time")
        br = s.get("bit_rate")
        ch = s.get("channels")
        disp = s.get("disposition") if isinstance(s.get("disposition"), dict) else {}
        tags = s.get("tags") if isinstance(s.get("tags"), dict) else {}
        lang = tags.get("language")
        title = tags.get("title")
        is_default = int(disp.get("default") or 0) == 1
        extra = []
        if lang:
            extra.append(f"lang={lang}")
        if title:
            extra.append(f"title={title}")
        if is_default:
            extra.append("default=1")
        extra_s = (" " + " ".join(extra)) if extra else ""
        print(f"  - index={idx} type={ctype} codec={cname} start_time={start} bit_rate={br} channels={ch}{extra_s}")

    a = _pick_default_stream([s for s in streams if isinstance(s, dict)], "audio")
    v = _pick_default_stream([s for s in streams if isinstance(s, dict)], "video")
    a_start = _to_float(a.get("start_time")) if a else None
    v_start = _to_float(v.get("start_time")) if v else None

    print("")
    print("default_streams:")
    print(f"  video_index: {v.get('index') if v else None}")
    print(f"  audio_index: {a.get('index') if a else None}")
    print(f"  video_start_time: {v_start}")
    print(f"  audio_start_time: {a_start}")

    if a_start is not None and v_start is not None:
        delay = float(v_start) - float(a_start)
        print("")
        print(f"estimated_delay_from_metadata_seconds (video_start - audio_start): {delay}")
        if abs(delay) >= float(args.threshold_seconds):
            print(f"suggest: --audio-delay-seconds {delay}")

    if args.marker_audio is not None and args.marker_video is not None:
        # Positive means audio needs to be delayed to match video.
        marker_delay = float(args.marker_video) - float(args.marker_audio)
        print("")
        print(f"estimated_delay_from_markers_seconds (video_marker - audio_marker): {marker_delay}")
        print(f"suggest: --audio-delay-seconds {marker_delay}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

