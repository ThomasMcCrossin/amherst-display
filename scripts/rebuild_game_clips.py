#!/usr/bin/env python3
"""
Rebuild per-event clips for an already-ingested game using stored OCR timestamps.

Use this when matching logic changes after an ingest run, and you need to:
- recompute `data/matched_events.json` from `data/video_timestamps.json`
- regenerate `clips/*.mp4` accordingly

Optionally downloads the original source video from Google Drive (by file id stored
in `output/ingest_status.json`) so clip regeneration can run even if the local source
video was deleted to save disk.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402
from highlight_extractor.event_matcher import EventMatcher  # noqa: E402
from highlight_extractor.pipeline import HighlightPipeline  # noqa: E402


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _merge_game_context(metadata: Dict[str, Any]) -> Dict[str, Any]:
    context: Dict[str, Any] = {}

    game_info = metadata.get("game_info")
    if isinstance(game_info, dict):
        context.update(game_info)

    box_score = metadata.get("box_score")
    if isinstance(box_score, dict):
        amherst_meta = box_score.get("_amherst_display")
        if isinstance(amherst_meta, dict):
            for key in ("playoff", "schedule_notes", "result", "date", "game_number"):
                value = amherst_meta.get(key)
                if value not in (None, ""):
                    context[key] = value

    return context


def _get_drive_service():
    from googleapiclient.discovery import build
    from google.oauth2 import service_account

    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not creds_path or not Path(creds_path).exists():
        raise FileNotFoundError(f"GOOGLE_APPLICATION_CREDENTIALS not found: {creds_path}")

    credentials = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=credentials)


def _drive_file_meta(service, file_id: str) -> Tuple[str, int]:
    meta = (
        service.files()
        .get(
            fileId=file_id,
            fields="name,size",
            supportsAllDrives=True,
        )
        .execute()
    )
    name = str(meta.get("name") or file_id)
    try:
        size = int(meta.get("size") or 0)
    except Exception:
        size = 0
    return name, size


def _download_drive_file(service, file_id: str, dst: Path) -> None:
    from googleapiclient.http import MediaIoBaseDownload

    dst.parent.mkdir(parents=True, exist_ok=True)
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(str(dst), "wb")
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024 * 16)  # 16MB
    done = False
    while not done:
        _status, done = downloader.next_chunk()
    fh.close()


def _resolve_drive_file_id(game_dir: Path, override: str) -> str:
    if override:
        return override
    status_path = game_dir / "output" / "ingest_status.json"
    if not status_path.exists():
        raise FileNotFoundError(f"Missing ingest status: {status_path}")
    status = _read_json(status_path)
    drive = status.get("drive") if isinstance(status, dict) else None
    ingest = status.get("ingest") if isinstance(status, dict) else None

    drive_file_id = ""
    if isinstance(drive, dict):
        # Prefer the archived working MP4 when present (more reliable than raw TS).
        drive_file_id = str(drive.get("working_file_id") or "").strip() or str(drive.get("source_file_id") or "").strip()
    if not drive_file_id and isinstance(ingest, dict):
        drive_file_id = str(ingest.get("drive_file_id") or "").strip()
    if not drive_file_id:
        raise ValueError(f"drive_file_id missing in: {status_path}")
    return drive_file_id


def _probe_av_start_delay_seconds(src: Path) -> Optional[float]:
    """
    Return (video_start_time - audio_start_time) in seconds when probe-able.
    """
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "stream=index,codec_type,start_time,disposition",
                "-of",
                "json",
                str(src),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        payload = json.loads(proc.stdout or "{}")
        streams = payload.get("streams") if isinstance(payload, dict) else None
        if not isinstance(streams, list):
            return None

        def _pick(codec_type: str) -> Optional[Dict[str, Any]]:
            cands = [s for s in streams if isinstance(s, dict) and str(s.get("codec_type") or "") == codec_type]
            if not cands:
                return None
            for s in cands:
                disp = s.get("disposition")
                if isinstance(disp, dict) and int(disp.get("default") or 0) == 1:
                    return s
            return cands[0]

        a = _pick("audio")
        v = _pick("video")
        if not a or not v:
            return None

        a_start = float(str(a.get("start_time") or "").strip() or "nan")
        v_start = float(str(v.get("start_time") or "").strip() or "nan")
        if a_start != a_start or v_start != v_start:  # NaN check
            return None
        return float(v_start) - float(a_start)
    except Exception:
        return None


def _remux_to_working_mp4(
    src: Path,
    *,
    dst: Path,
    audio_delay_seconds: float,
    auto_threshold_seconds: float,
    audio_stream_index: int | None = None,
    video_stream_index: int | None = None,
) -> Path:
    """
    Create a stable "working" MP4 for downstream clip extraction.
    """
    if dst.exists() and dst.stat().st_size > 0:
        return dst

    delay = float(audio_delay_seconds or 0.0)
    if abs(delay) < 1e-6:
        estimated = _probe_av_start_delay_seconds(src)
        if estimated is not None and abs(float(estimated)) >= float(auto_threshold_seconds):
            delay = float(estimated)

    cmd = [
        "ffmpeg",
        "-y",
        "-fflags",
        "+genpts",
        "-analyzeduration",
        "2000M",
        "-probesize",
        "2000M",
    ]
    if abs(delay) > 1e-6:
        cmd += ["-i", str(src), "-itsoffset", str(delay), "-i", str(src)]
        v_map = f"0:{int(video_stream_index)}" if video_stream_index is not None else "0:v:0"
        a_map = f"1:{int(audio_stream_index)}?" if audio_stream_index is not None else "1:a:0?"
        cmd += ["-map", v_map, "-map", a_map]
    else:
        cmd += ["-i", str(src)]
        v_map = f"0:{int(video_stream_index)}" if video_stream_index is not None else "0:v:0"
        a_map = f"0:{int(audio_stream_index)}?" if audio_stream_index is not None else "0:a:0?"
        cmd += ["-map", v_map, "-map", a_map]
    cmd += [
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-af",
        "aresample=async=1:first_pts=0",
        "-movflags",
        "+faststart",
        str(dst),
    ]
    subprocess.run(cmd, check=True)
    return dst


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild clips + matched events for an ingested game.")
    parser.add_argument("--game-dir", type=Path, required=True, help="Game folder under Games/...")
    parser.add_argument("--drive-file-id", default="", help="Optional Drive file id to download source video")
    parser.add_argument(
        "--video-path",
        type=Path,
        default=None,
        help="Use an existing local source video path instead of downloading from Drive",
    )
    parser.add_argument(
        "--download-dir",
        type=Path,
        default=REPO_ROOT / "temp" / "drive_ingest" / "incoming",
        help="Where to download the source video when using Drive",
    )
    parser.add_argument("--keep-video", action="store_true", help="Keep the downloaded source video on disk")
    parser.add_argument("--tolerance-seconds", type=int, default=30, help="Event match tolerance in seconds")
    parser.add_argument("--before-seconds", type=float, default=15.0, help="Seconds before goals to include")
    parser.add_argument("--after-seconds", type=float, default=4.0, help="Seconds after goals to include")
    parser.add_argument(
        "--goal-legacy-timing-fallback",
        action="store_true",
        help="Allow legacy approximate goal timing fallbacks for broken/unreadable scorebugs.",
    )
    parser.add_argument(
        "--audio-delay-seconds",
        type=float,
        default=0.0,
        help="Delay audio relative to video by this many seconds when remuxing TS sources (default: 0)",
    )
    parser.add_argument(
        "--auto-av-sync-threshold-seconds",
        type=float,
        default=60.0,
        help="Auto-apply audio delay when ffprobe start_time suggests a large A/V offset >= this many seconds (default: 60)",
    )
    parser.add_argument(
        "--audio-stream-index",
        type=int,
        default=None,
        help="Force which input audio stream index to use when remuxing TS sources (default: use file's default audio stream)",
    )
    parser.add_argument(
        "--video-stream-index",
        type=int,
        default=None,
        help="Force which input video stream index to use when remuxing TS sources (default: use file's default video stream)",
    )
    args = parser.parse_args()

    game_dir: Path = args.game_dir
    if not game_dir.exists():
        raise FileNotFoundError(f"Game dir not found: {game_dir}")

    timestamps_path = game_dir / "data" / "video_timestamps.json"
    events_path = game_dir / "data" / "matched_events.json"
    metadata_path = game_dir / "data" / "game_metadata.json"
    if not timestamps_path.exists():
        raise FileNotFoundError(f"Missing timestamps: {timestamps_path}")
    if not events_path.exists():
        raise FileNotFoundError(f"Missing events: {events_path}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"Missing metadata: {metadata_path}")

    video_path: Optional[Path] = args.video_path
    if video_path is not None and not video_path.exists():
        raise FileNotFoundError(f"Video not found: {video_path}")

    downloaded_path: Optional[Path] = None
    if video_path is None:
        drive_file_id = _resolve_drive_file_id(game_dir, str(args.drive_file_id))
        service = _get_drive_service()
        remote_name, remote_size = _drive_file_meta(service, drive_file_id)
        dst = args.download_dir / remote_name

        reuse = dst.exists()
        if reuse and remote_size > 0:
            try:
                reuse = int(dst.stat().st_size) == int(remote_size)
            except Exception:
                reuse = False

        if not reuse:
            print(f"[rebuild] Downloading source video ({remote_size} bytes): {remote_name}")
            _download_drive_file(service, drive_file_id, dst)
        else:
            print(f"[rebuild] Reusing existing download: {dst.name}")

        video_path = dst
        downloaded_path = dst

    assert video_path is not None

    # TS sources are often unstable for seeking and can have massive A/V start offsets.
    # Remuxing to a working MP4 makes clip regeneration deterministic.
    working_path: Optional[Path] = None
    if video_path.suffix.lower() == ".ts":
        working_dst = video_path.with_suffix(".working.mp4")
        try:
            video_path = _remux_to_working_mp4(
                video_path,
                dst=working_dst,
                audio_delay_seconds=float(args.audio_delay_seconds),
                auto_threshold_seconds=float(args.auto_av_sync_threshold_seconds),
                audio_stream_index=args.audio_stream_index,
                video_stream_index=args.video_stream_index,
            )
            working_path = video_path
        except Exception as e:
            raise RuntimeError(f"Failed to remux TS to working MP4: {e}") from e

    raw_timestamps = _read_json(timestamps_path)
    raw_events = _read_json(events_path)
    metadata = _read_json(metadata_path)
    game_context = _merge_game_context(metadata) if isinstance(metadata, dict) else {}

    pipeline_kwargs: Dict[str, Any] = {
        "config": config,
        "video_path": video_path,
    }
    if game_context:
        pipeline_kwargs["game_info_override"] = dict(game_context)
        pipeline_kwargs["source_game_info_override"] = dict(game_context)

    pipeline = HighlightPipeline(**pipeline_kwargs)
    pipeline._goal_legacy_timing_fallback_override = bool(args.goal_legacy_timing_fallback)
    if not pipeline.video_processor.load_video():
        raise RuntimeError(f"Failed to load video: {video_path}")

    matcher = EventMatcher(config)
    if game_context:
        matcher.set_game_context(game_context)
    ts_norm = matcher._normalize_video_timestamps(raw_timestamps)
    ts_norm = matcher.estimate_missing_timestamps(ts_norm, pipeline.video_processor.duration)
    ts_norm = matcher._normalize_video_timestamps(ts_norm)

    # Re-match events using the normalized timestamps.
    matched = matcher.match_events_to_video(
        list(raw_events),
        ts_norm,
        tolerance_seconds=int(args.tolerance_seconds),
    )

    # Optional: refine low-confidence goals by clock-stop detection (uses the source video).
    pipeline.video_timestamps = ts_norm
    pipeline.matched_events = matched
    try:
        pipeline._refine_goal_events_by_clock_stop(pipeline.matched_events)
    except Exception:
        pass
    pipeline._finalize_goal_timing_verification(pipeline.matched_events)

    _write_json(events_path, pipeline.matched_events)
    print(f"[rebuild] Wrote: {events_path}")

    # Rebuild clips using the pipeline's Step 6 logic (penalty insertions, manifests, etc).
    game_folders = {
        "game_dir": game_dir,
        "clips_dir": game_dir / "clips",
        "data_dir": game_dir / "data",
        "output_dir": game_dir / "output",
        "logs_dir": game_dir / "logs",
        "source_dir": game_dir / "source",
    }
    for k, p in game_folders.items():
        if k.endswith("_dir"):
            Path(p).mkdir(parents=True, exist_ok=True)
    pipeline.game_folders = game_folders
    pipeline.box_score = metadata.get("box_score") or {}
    if game_context:
        pipeline._refresh_game_context()

    original_overlay_enabled = getattr(config, "OVERLAY_ENABLED", True)
    config.OVERLAY_ENABLED = False
    try:
        pipeline._step6_create_clips(before_seconds=float(args.before_seconds), after_seconds=float(args.after_seconds))
    finally:
        config.OVERLAY_ENABLED = original_overlay_enabled
    print(f"[rebuild] Rebuilt clips in: {game_dir / 'clips'}")

    if not bool(args.keep_video) and downloaded_path is not None:
        # Only delete media we downloaded into the scratch dir.
        for p in [working_path, downloaded_path]:
            if p is None:
                continue
            try:
                Path(p).unlink(missing_ok=True)
            except Exception:
                pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
