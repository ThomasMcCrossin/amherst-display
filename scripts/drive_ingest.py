#!/usr/bin/env python3
"""
Drive ingest loop (Google Drive API) -> local processing -> optional upload back to Drive.

This script replaces the legacy ingest and uses the service-account
credentials pointed to by GOOGLE_APPLICATION_CREDENTIALS.

Folder workflow (inside the ingest folder):
- ingest/               : drop raw game videos here
- ingest/processing/    : files are moved here while processing (lock)
- ingest/failed/        : failures are moved here
- ingest/processed/     : successfully processed source videos are moved here (if no games upload configured)

If a games destination folder is configured, outputs are uploaded to:
  <games-folder>/<game-folder-name>/{data,clips,output,logs}/
and the source video is moved into:
  <games-folder>/<game-folder-name>/source/

Run with the repo virtualenv:
  ./venv/bin/python scripts/drive_ingest.py --once
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import unicodedata
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Ensure repo root is on sys.path so `import config` works when running from `scripts/`.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402
from highlight_extractor import HighlightPipeline  # noqa: E402
from highlight_extractor.amherst_integration import AmherstBoxScoreProvider  # noqa: E402
from highlight_extractor.file_manager import FileManager  # noqa: E402


@dataclass(frozen=True)
class DriveFile:
    id: str
    name: str
    size: int
    modified_time: Optional[str]
    parents: List[str]


def _run(cmd: List[str], *, check: bool = True, capture_output: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        check=check,
        capture_output=capture_output,
        text=True,
    )


def _append_log_line(log_path: Path, level: str, message: str) -> None:
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8", errors="ignore") as f:
            ts = datetime.now().isoformat(timespec="seconds")
            f.write(f"{ts} | {level.upper():5s} | {message}\n")
    except Exception:
        pass


def _preflight_snapshot() -> Dict[str, Any]:
    """
    Collect a lightweight environment snapshot to aid debugging.

    This should never raise.
    """
    out: Dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "python": {
            "executable": sys.executable,
            "version": sys.version,
        },
        "platform": {
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
        "paths": {},
        "env": {
            # Don't dump secrets; just presence.
            "GOOGLE_APPLICATION_CREDENTIALS_set": bool(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")),
            "RAMBLERS_DRIVE_ID_set": bool(os.environ.get("RAMBLERS_DRIVE_ID")),
            "DRIVE_INGEST_FOLDER_ID_set": bool(os.environ.get("DRIVE_INGEST_FOLDER_ID")),
            "DRIVE_GAMES_FOLDER_ID_set": bool(os.environ.get("DRIVE_GAMES_FOLDER_ID")),
            "DRIVE_HIGHLIGHTS_FOLDER_ID_set": bool(os.environ.get("DRIVE_HIGHLIGHTS_FOLDER_ID")),
            "HOCKEYTECH_API_KEY_set": bool(os.environ.get("HOCKEYTECH_API_KEY")),
        },
        "imports": {},
    }

    for bin_name in ["ffmpeg", "ffprobe", "tesseract"]:
        try:
            out["paths"][bin_name] = shutil.which(bin_name)
        except Exception:
            out["paths"][bin_name] = None

    for mod in ["moviepy", "PIL", "pytesseract"]:
        try:
            __import__(mod)
            out["imports"][mod] = True
        except Exception as e:
            out["imports"][mod] = False
            out["imports"][f"{mod}_error"] = str(e)

    # easyocr is optional.
    try:
        __import__("easyocr")
        out["imports"]["easyocr"] = True
    except Exception:
        out["imports"]["easyocr"] = False

    return out


def _disk_free_bytes(path: Path) -> int:
    return int(shutil.disk_usage(str(path)).free)


def _gb(n: int) -> float:
    return n / (1024**3)


def _try_remove_file(path: Path) -> None:
    try:
        path.unlink()
        return
    except FileNotFoundError:
        return
    except Exception:
        pass
    # Fallback: truncation still frees space even if deletion fails.
    try:
        with path.open("wb"):
            pass
    except Exception:
        pass


def _prune_local_incoming_dir(local_incoming_dir: Path, *, supported_exts: set[str], keep: set[Path]) -> None:
    if not local_incoming_dir.exists():
        return
    for child in local_incoming_dir.iterdir():
        if child in keep or not child.is_file():
            continue
        suffix = child.suffix.lower()
        if suffix in supported_exts or suffix in {".part", ".partial", ".tmp"}:
            _try_remove_file(child)


def _parse_min_age(value: str) -> timedelta:
    v = str(value or "").strip().lower()
    if not v:
        return timedelta(0)
    unit = v[-1]
    num = v[:-1]
    try:
        n = float(num)
    except Exception as e:
        raise ValueError(f"Invalid --min-age value: {value}") from e

    if unit == "s":
        return timedelta(seconds=n)
    if unit == "m":
        return timedelta(minutes=n)
    if unit == "h":
        return timedelta(hours=n)
    if unit == "d":
        return timedelta(days=n)
    raise ValueError(f"Invalid --min-age unit (use s/m/h/d): {value}")


def _normalize_drive_id(value: str) -> str:
    if not value:
        return ""
    value = str(value).strip()
    # Folder URL support
    if "/folders/" in value:
        m = re.search(r"/folders/([a-zA-Z0-9_-]+)", value)
        return m.group(1) if m else value
    return value


def _get_drive_service():
    try:
        from googleapiclient.discovery import build
        from google.oauth2 import service_account
    except ImportError as e:
        raise RuntimeError("Google API client not installed. Install: google-api-python-client google-auth") from e

    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path or not Path(creds_path).exists():
        raise FileNotFoundError(f"Service account credentials not found: {creds_path}")

    credentials = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=credentials)


def _list_files(
    service,
    *,
    parent_id: str,
    drive_id: str,
    query_extra: str = "",
    fields: str = "files(id,name,size,modifiedTime,parents,mimeType)",
) -> List[Dict[str, Any]]:
    q = f"'{parent_id}' in parents and trashed=false"
    if query_extra:
        q = f"{q} and ({query_extra})"

    params: Dict[str, Any] = {
        "q": q,
        "fields": fields,
        "pageSize": 1000,
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
    }
    if drive_id:
        params.update(
            {
                "corpora": "drive",
                "driveId": drive_id,
            }
        )

    out: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    while True:
        if page_token:
            params["pageToken"] = page_token
        res = service.files().list(**params).execute()
        out.extend(res.get("files", []) or [])
        page_token = res.get("nextPageToken")
        if not page_token:
            break
    return out


def _find_folder(service, *, parent_id: str, name: str, drive_id: str) -> Optional[str]:
    target = str(name or "").strip()
    if not target:
        return None

    items = _list_files(
        service,
        parent_id=parent_id,
        drive_id=drive_id,
        query_extra="mimeType='application/vnd.google-apps.folder'",
        fields="files(id,name)",
    )
    matches = [it for it in items if str(it.get("name") or "").strip().lower() == target.lower()]
    if not matches:
        return None
    # Prefer exact-case match if present.
    exact = next((it for it in matches if str(it.get("name") or "") == target), None)
    chosen = exact or matches[0]
    return str(chosen["id"])


def _ensure_folder(service, *, parent_id: str, name: str, drive_id: str) -> str:
    existing = _find_folder(service, parent_id=parent_id, name=name, drive_id=drive_id)
    if existing:
        return existing

    body = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    created = (
        service.files()
        .create(body=body, fields="id", supportsAllDrives=True)
        .execute()
    )
    return str(created["id"])


def _resolve_folder_path(service, *, drive_id: str, folder_path: str) -> str:
    path = str(folder_path or "").strip().strip("/")
    if not path:
        raise ValueError("Empty folder path")

    root_id = drive_id or "root"
    current = root_id
    for seg in [p for p in path.split("/") if p]:
        found = _find_folder(service, parent_id=current, name=seg, drive_id=drive_id)
        if found:
            current = found
            continue

        # Fallback: search globally within the drive by name (must be unique).
        name_esc = seg.replace("'", "\\'")
        items = _list_files(
            service,
            parent_id=root_id,
            drive_id=drive_id,
            query_extra=f"mimeType='application/vnd.google-apps.folder' and name='{name_esc}'",
            fields="files(id,name,parents)",
        )
        if len(items) == 1:
            current = str(items[0]["id"])
            continue
        raise FileNotFoundError(f"Could not resolve folder path segment: {seg}")

    return current


def _move_file(service, *, file_id: str, add_parent: str, remove_parent: str) -> None:
    service.files().update(
        fileId=file_id,
        addParents=add_parent,
        removeParents=remove_parent,
        fields="id,parents",
        supportsAllDrives=True,
    ).execute()


def _download_file(service, *, file_id: str, dst: Path) -> None:
    from googleapiclient.http import MediaIoBaseDownload

    dst.parent.mkdir(parents=True, exist_ok=True)
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(str(dst), "wb")
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024 * 16)  # 16MB
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            pct = int(status.progress() * 100)
            print(f"[ingest] Download {dst.name}: {pct}%")
    fh.close()


def _should_attempt_repair(status_payload: Dict[str, Any]) -> bool:
    if not isinstance(status_payload, dict):
        return False

    failed_step = status_payload.get("failed_step")
    failed_reason = str(status_payload.get("failed_reason") or "").strip().lower()

    if failed_step == 3 or failed_reason == "video_load_failed":
        return True

    # Legacy fallback: older payloads only had stringified errors.
    errors = status_payload.get("errors")
    if not isinstance(errors, list):
        return False
    for err in errors:
        if not isinstance(err, str):
            continue
        if "Failed to load video file" in err or "Step 3 failed" in err:
            return True
    return False


def _probe_av_streams(src: Path) -> Dict[str, Any]:
    """
    Best-effort ffprobe for stream metadata (never raises).
    """
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "stream=index,codec_type,start_time,disposition:format=start_time",
                "-of",
                "json",
                str(src),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(proc.stdout or "{}")
    except Exception:
        return {}


def _pick_default_stream_index(probe: Dict[str, Any], *, codec_type: str) -> Optional[int]:
    streams = probe.get("streams") if isinstance(probe, dict) else None
    if not isinstance(streams, list):
        return None
    candidates: List[Dict[str, Any]] = []
    for st in streams:
        if not isinstance(st, dict):
            continue
        if str(st.get("codec_type") or "") != codec_type:
            continue
        candidates.append(st)
    if not candidates:
        return None

    # Prefer streams marked as default.
    for st in candidates:
        disp = st.get("disposition")
        if isinstance(disp, dict) and int(disp.get("default") or 0) == 1:
            try:
                return int(st.get("index"))
            except Exception:
                break

    try:
        return int(candidates[0].get("index"))
    except Exception:
        return None


def _parse_start_time_seconds(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        s = str(value).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _estimate_audio_delay_seconds(
    src: Path,
    *,
    explicit_delay_seconds: float,
    auto_threshold_seconds: float,
    audio_stream_index: Optional[int] = None,
    video_stream_index: Optional[int] = None,
) -> Tuple[float, bool, Dict[str, Any]]:
    """
    Estimate a large A/V start offset and suggest a fixed audio delay.

    Returns: (delay_seconds, auto_applied, debug_info)
      - delay_seconds: positive delays audio relative to video, negative advances it
      - auto_applied: True only when we chose to auto-apply a non-zero delay
      - debug_info: probe-derived fields for logging/status
    """
    if abs(float(explicit_delay_seconds or 0.0)) > 1e-6:
        return float(explicit_delay_seconds), False, {"mode": "explicit"}

    probe = _probe_av_streams(src)
    a_idx = int(audio_stream_index) if audio_stream_index is not None else _pick_default_stream_index(probe, codec_type="audio")
    v_idx = int(video_stream_index) if video_stream_index is not None else _pick_default_stream_index(probe, codec_type="video")

    streams = probe.get("streams") if isinstance(probe, dict) else None
    if not isinstance(streams, list) or a_idx is None or v_idx is None:
        return 0.0, False, {"mode": "none", "reason": "missing_streams"}

    audio_start = None
    video_start = None
    for st in streams:
        if not isinstance(st, dict):
            continue
        try:
            idx = int(st.get("index"))
        except Exception:
            continue
        if idx == a_idx:
            audio_start = _parse_start_time_seconds(st.get("start_time"))
        if idx == v_idx:
            video_start = _parse_start_time_seconds(st.get("start_time"))

    if audio_start is None or video_start is None:
        return (
            0.0,
            False,
            {
                "mode": "none",
                "reason": "missing_start_time",
                "audio_index": a_idx,
                "video_index": v_idx,
                "audio_stream_index_override": audio_stream_index,
                "video_stream_index_override": video_stream_index,
            },
        )

    delay = float(video_start) - float(audio_start)
    auto_apply = abs(delay) >= float(auto_threshold_seconds)
    return (
        delay if auto_apply else 0.0,
        bool(auto_apply),
        {
            "mode": "auto" if auto_apply else "measured",
            "audio_index": a_idx,
            "video_index": v_idx,
            "audio_stream_index_override": audio_stream_index,
            "video_stream_index_override": video_stream_index,
            "audio_start_time": audio_start,
            "video_start_time": video_start,
            "delay_seconds": delay,
            "threshold_seconds": float(auto_threshold_seconds),
        },
    )


def _repair_video_file(
    src: Path,
    *,
    audio_delay_seconds: float = 0.0,
    audio_stream_index: Optional[int] = None,
    video_stream_index: Optional[int] = None,
) -> Optional[Path]:
    """
    Attempt a lightweight remux to fix broken TS/PTS headers.

    Returns repaired file path if successful.
    """
    try:
        repair_dir = src.parent / "repair"
        repair_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        return None

    def _has_video_stream(path: Path) -> bool:
        try:
            proc = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-select_streams",
                    "v:0",
                    "-show_entries",
                    "stream=codec_type",
                    "-of",
                    "default=noprint_wrappers=1:nokey=1",
                    str(path),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            return "video" in (proc.stdout or "").strip().lower()
        except Exception:
            return False

    probe = _probe_av_streams(src)
    a_idx = int(audio_stream_index) if audio_stream_index is not None else _pick_default_stream_index(probe, codec_type="audio")
    v_idx = int(video_stream_index) if video_stream_index is not None else _pick_default_stream_index(probe, codec_type="video")
    v_map = f"0:{v_idx}" if v_idx is not None else "0:v:0"
    a_map_in0 = f"0:{a_idx}?" if a_idx is not None else "0:a:0?"
    a_map_in1 = f"1:{a_idx}?" if a_idx is not None else "1:a:0?"

    # Attempt 1: remux copy (fast)
    dst = repair_dir / f"{src.stem}_fixed.mp4"
    if dst.exists() and dst.stat().st_size > 0 and _has_video_stream(dst):
        print(f"[ingest] Repair reuse (remux): {dst}")
        return dst
    try:
        # Keep video bitstream intact, but re-encode audio and force timestamps to start at 0.
        # This fixes many capture edge-cases where audio PTS/DTS drift or jump and MoviePy
        # ends up seeking audio from the wrong moment.
        #
        # Also: explicitly exclude TS data streams (SCTE35, timed_id3) which MP4 can't mux.
        cmd: List[str] = [
            "ffmpeg",
            "-y",
            "-fflags",
            "+genpts",
            "-analyzeduration",
            "2000M",
            "-probesize",
            "2000M",
        ]
        if abs(float(audio_delay_seconds or 0.0)) > 1e-6:
            # Use the same file twice so we can offset audio independently.
            cmd += ["-i", str(src), "-itsoffset", str(float(audio_delay_seconds)), "-i", str(src)]
            cmd += ["-map", v_map, "-map", a_map_in1]
        else:
            cmd += ["-i", str(src)]
            cmd += ["-map", v_map, "-map", a_map_in0]
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
        _run(cmd, check=True, capture_output=False)
    except Exception as e:
        print(f"[ingest] Repair attempt (remux) failed: {e}", file=sys.stderr)
        _try_remove_file(dst)
        dst = None

    if dst is not None and dst.exists() and dst.stat().st_size > 0 and _has_video_stream(dst):
        print(f"[ingest] Repair succeeded (remux): {dst}")
        return dst

    # Attempt 2: re-encode (slow but more resilient)
    dst_reencode = repair_dir / f"{src.stem}_reencode.mp4"
    if dst_reencode.exists() and dst_reencode.stat().st_size > 0 and _has_video_stream(dst_reencode):
        print(f"[ingest] Repair reuse (re-encode): {dst_reencode}")
        return dst_reencode
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-fflags",
            "+genpts+igndts",
            "-err_detect",
            "ignore_err",
            "-analyzeduration",
            "2000M",
            "-probesize",
            "2000M",
            "-i",
            str(src),
            "-map",
            v_map,
            "-map",
            a_map_in0,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "20",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-af",
            "aresample=async=1:first_pts=0",
            "-movflags",
            "+faststart",
            str(dst_reencode),
        ]
        _run(cmd, check=True, capture_output=False)
    except Exception as e:
        print(f"[ingest] Repair attempt (re-encode) failed: {e}", file=sys.stderr)
        _try_remove_file(dst_reencode)
        return None

    if dst_reencode.exists() and dst_reencode.stat().st_size > 0 and _has_video_stream(dst_reencode):
        print(f"[ingest] Repair succeeded (re-encode): {dst_reencode}")
        return dst_reencode
    _try_remove_file(dst_reencode)
    return None


def _find_child_file(service, *, parent_id: str, name: str, drive_id: str) -> Optional[str]:
    name_esc = name.replace("'", "\\'")
    items = _list_files(
        service,
        parent_id=parent_id,
        drive_id=drive_id,
        query_extra=f"name='{name_esc}'",
        fields="files(id,name,mimeType)",
    )
    for it in items:
        if it.get("mimeType") != "application/vnd.google-apps.folder":
            return str(it["id"])
    return None


def _upsert_file(
    service,
    *,
    local_path: Path,
    parent_id: str,
    drive_id: str,
    remote_name: Optional[str] = None,
) -> str:
    from googleapiclient.http import MediaFileUpload

    remote_name = str(remote_name or local_path.name)
    existing_id = _find_child_file(service, parent_id=parent_id, name=remote_name, drive_id=drive_id)
    media = MediaFileUpload(str(local_path), resumable=True)
    if existing_id:
        service.files().update(
            fileId=existing_id,
            media_body=media,
            supportsAllDrives=True,
        ).execute()
        return str(existing_id)

    created = (
        service.files()
        .create(
            body={"name": remote_name, "parents": [parent_id]},
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    return str(created.get("id") or "")


def _upload_tree(service, *, src_dir: Path, dst_parent_id: str, drive_id: str) -> None:
    if not src_dir.exists():
        return
    for child in sorted(src_dir.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
        if child.is_dir():
            sub_id = _ensure_folder(service, parent_id=dst_parent_id, name=child.name, drive_id=drive_id)
            _upload_tree(service, src_dir=child, dst_parent_id=sub_id, drive_id=drive_id)
        elif child.is_file():
            _upsert_file(service, local_path=child, parent_id=dst_parent_id, drive_id=drive_id)


def _create_shortcut(service, *, target_file_id: str, parent_id: str, name: str) -> str:
    """
    Create a Drive shortcut file pointing at an existing file.

    Useful when we want the source to remain in an ingest queue (e.g. failed/) while
    still surfacing it inside the per-game archive folder.
    """
    created = (
        service.files()
        .create(
            body={
                "name": str(name),
                "mimeType": "application/vnd.google-apps.shortcut",
                "parents": [parent_id],
                "shortcutDetails": {"targetId": str(target_file_id)},
            },
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    return str(created.get("id") or "")


def _build_debug_bundle_zip(game_dir: Path) -> Optional[Path]:
    """
    Zip up small, high-signal artifacts so we can debug runs remotely without
    uploading gigabytes of raw media.
    """
    output_dir = game_dir / "output"
    logs_dir = game_dir / "logs"
    data_dir = game_dir / "data"
    source_dir = game_dir / "source"

    candidates: List[Path] = []
    for p in [
        output_dir / "preflight.json",
        output_dir / "ingest_status.json",
        output_dir / "major_review_status.json",
        logs_dir / "ingest.log",
        logs_dir / "pipeline.log",
        logs_dir / "major_review_monitor.log",
        logs_dir / "production_reel.log",
        source_dir / "source_info.json",
    ]:
        if p.exists() and p.is_file():
            candidates.append(p)

    if data_dir.exists():
        for p in sorted(data_dir.iterdir()):
            if not p.is_file():
                continue
            # Keep JSON and text logs; skip large debug frames by default.
            if p.suffix.lower() in {".json", ".txt"}:
                candidates.append(p)

    if not candidates:
        return None

    dst = output_dir / "debug_bundle.zip"
    tmp = output_dir / (dst.name + ".tmp")
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
    except Exception:
        pass

    try:
        with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in candidates:
                try:
                    rel = p.relative_to(game_dir)
                except Exception:
                    rel = Path(p.name)
                zf.write(p, arcname=str(rel))
    except Exception:
        _try_remove_file(tmp)
        return None

    try:
        tmp.replace(dst)
    except Exception:
        try:
            shutil.move(str(tmp), str(dst))
        except Exception:
            _try_remove_file(tmp)
            return None
    return dst


def _sanitize_drive_name(value: str) -> str:
    raw = str(value or "")
    normalized = unicodedata.normalize("NFKD", raw)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_value = re.sub(r'[<>:"/\\|?*]', "_", ascii_value)
    ascii_value = re.sub(r"\s+", " ", ascii_value).strip(" .")
    return ascii_value or "Highlights"


def _load_game_metadata(game_dir: Path) -> Dict[str, Any]:
    metadata_path = game_dir / "data" / "game_metadata.json"
    if not metadata_path.exists():
        return {}
    try:
        return json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _build_highlight_folder_name(game_dir: Path) -> str:
    metadata = _load_game_metadata(game_dir)
    info = metadata.get("game_info") or {}
    date = str(info.get("date") or "").strip()
    home = str(info.get("home_team") or "").strip()
    away = str(info.get("away_team") or "").strip()
    if date and home and away:
        name = f"{date} - {home} vs {away}"
    else:
        name = game_dir.name
    return _sanitize_drive_name(name)


def _ensure_youtube_description(game_dir: Path) -> Optional[Path]:
    desc_path = game_dir / "output" / "youtube_description.txt"
    if desc_path.exists():
        return desc_path
    try:
        from highlight_extractor.description_generator import generate_description_from_game_dir
    except Exception:
        return None
    try:
        return generate_description_from_game_dir(game_dir, output_dir=game_dir / "output")
    except Exception:
        return None


def _upload_highlight_outputs(
    service,
    *,
    game_dir: Path,
    parent_folder_id: str,
    drive_id: str,
) -> Optional[str]:
    output_dir = game_dir / "output"
    production_path = output_dir / "highlights_production.mp4"
    if not output_dir.exists() or not production_path.exists():
        return None

    _ensure_youtube_description(game_dir)
    folder_name = _build_highlight_folder_name(game_dir)
    remote_folder_id = _ensure_folder(service, parent_id=parent_folder_id, name=folder_name, drive_id=drive_id)

    _upload_tree(service, src_dir=output_dir, dst_parent_id=remote_folder_id, drive_id=drive_id)

    return f"https://drive.google.com/drive/folders/{remote_folder_id}"


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _build_production_reel(game_dir: Path, *, fps: str, overlay_seconds: float, transition_seconds: float) -> Path:
    clips_dir = game_dir / "clips"
    events_json = game_dir / "data" / "matched_events.json"
    out_path = game_dir / "output" / "highlights_production.mp4"

    cmd = [
        sys.executable,
        str(Path("scripts") / "build_production_highlight_reel.py"),
        "--game-dir",
        str(game_dir),
        "--clips-dir",
        str(clips_dir),
        "--events-json",
        str(events_json),
        "--output",
        str(out_path),
        "--fps",
        str(fps),
        "--overlay-seconds",
        str(overlay_seconds),
        "--transition-seconds",
        str(transition_seconds),
    ]
    log_path = game_dir / "logs" / "production_reel.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8", errors="ignore") as f:
        f.write(f"\n=== {datetime.now().isoformat(timespec='seconds')} | build_production_reel ===\n")
        f.write(" ".join(cmd) + "\n")
        subprocess.run(cmd, check=True, stdout=f, stderr=f, text=True)
    return out_path


def _refresh_amherst_games_cache() -> None:
    _run(["node", str(REPO_ROOT / "scripts" / "games.mjs")], check=True, capture_output=False)


def _load_amherst_provider() -> AmherstBoxScoreProvider:
    games_json = REPO_ROOT / "games" / "amherst-ramblers.json"
    if not games_json.exists():
        raise FileNotFoundError(f"Missing games cache: {games_json}")
    return AmherstBoxScoreProvider(str(games_json))


def _parse_source_game_info(file_manager: FileManager, filename: str) -> Dict[str, Any]:
    return file_manager.parse_mhl_filename(filename) or file_manager.parse_generic_hockey_filename(filename)


def _find_matching_game(
    provider: AmherstBoxScoreProvider,
    *,
    source_game_info: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    home = str(source_game_info.get("home_team") or "")
    away = str(source_game_info.get("away_team") or "")
    date = str(source_game_info.get("date") or "")
    game = provider.find_game_by_teams(home, away, date)
    if game:
        return game
    # Some sources swap home/away in the filename; try the inverse.
    return provider.find_game_by_teams(away, home, date)


def _canonical_game_info_from_match(
    *,
    source_game_info: Dict[str, Any],
    game: Dict[str, Any],
    ramblers_name: str = "Amherst Ramblers",
) -> Dict[str, Any]:
    date = str(game.get("date") or source_game_info.get("date") or "").strip()
    opponent = (game.get("opponent") or {}).get("team_name") or ""
    opponent = str(opponent).strip() or "Opponent"
    is_home = bool(game.get("home_game"))

    if is_home:
        home_team = ramblers_name
        away_team = opponent
    else:
        home_team = opponent
        away_team = ramblers_name

    return {
        "date": date,
        "date_formatted": str(source_game_info.get("date_formatted") or date),
        "home_team": home_team,
        "away_team": away_team,
        "league": str(source_game_info.get("league") or "MHL"),
        "filename": str(source_game_info.get("filename") or ""),
        "home_away": str(source_game_info.get("home_away") or "unknown"),
        "time": str(source_game_info.get("time") or "unknown"),
    }


def _process_one_video(
    local_video: Path,
    *,
    game: Dict[str, Any],
    game_folders: Dict[str, Path],
    canonical_game_info: Dict[str, Any],
    source_game_info: Optional[Dict[str, Any]],
    sample_interval: int,
    tolerance_seconds: int,
    before_seconds: float,
    after_seconds: float,
    max_clips: int,
    broadcast_type: str,
    parallel_ocr: bool,
    ocr_workers: int,
) -> Tuple[bool, Optional[Path], Dict[str, Any]]:
    provider = _load_amherst_provider()
    fetcher = provider.create_fetcher(game)

    pipeline = HighlightPipeline(
        config=config,
        video_path=local_video,
        box_score_fetcher=fetcher,
        game_info_override=canonical_game_info,
        game_folders_override=game_folders,
        source_game_info_override=source_game_info,
    )
    result = pipeline.execute(
        sample_interval=sample_interval,
        tolerance_seconds=tolerance_seconds,
        before_seconds=before_seconds,
        after_seconds=after_seconds,
        max_clips=max_clips if max_clips > 0 else None,
        parallel_ocr=parallel_ocr,
        ocr_workers=ocr_workers,
        broadcast_type=broadcast_type,
        auto_detect_start=True,
    )

    game_dir = game_folders.get("game_dir") if isinstance(game_folders.get("game_dir"), Path) else None

    payload: Dict[str, Any] = {
        "success": bool(result.success),
        "paused_for_review": bool(getattr(result, "paused_for_review", False)),
        "resume_state_path": getattr(result, "resume_state_path", None),
        "major_review_folder_url": getattr(result, "major_review_folder_url", None),
        "failed_step": getattr(result, "failed_step", None),
        "failed_reason": getattr(result, "failed_reason", None),
        "exception_type": getattr(result, "exception_type", None),
        "game_id": str(game.get("game_id", "")),
        "game_info": getattr(result, "game_info", None).__dict__ if getattr(result, "game_info", None) else None,
        "events_found": result.events_found,
        "events_matched": result.events_matched,
        "clips_created": result.clips_created,
        "highlights_path": result.highlights_path,
        "errors": result.errors,
        "warnings": result.warnings,
        "total_duration_seconds": result.total_duration_seconds,
    }

    return bool(result.success), game_dir, payload


def _to_dt(value: str) -> Optional[datetime]:
    if not value:
        return None
    # Drive timestamps are RFC3339, often ending with 'Z'.
    v = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(v)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _list_ingest_videos(
    service,
    *,
    ingest_folder_id: str,
    drive_id: str,
    min_age: timedelta,
    supported_exts: set[str],
    name_contains: str = "",
) -> List[DriveFile]:
    now = datetime.now(timezone.utc)
    items = _list_files(
        service,
        parent_id=ingest_folder_id,
        drive_id=drive_id,
        query_extra="mimeType!='application/vnd.google-apps.folder'",
    )

    out: List[DriveFile] = []
    name_filter = str(name_contains or "").strip().lower()
    for it in items:
        name = str(it.get("name") or "")
        if not name or Path(name).suffix.lower() not in supported_exts:
            continue
        if name_filter and name_filter not in name.lower():
            continue
        mt = _to_dt(str(it.get("modifiedTime") or ""))
        if mt and (now - mt) < min_age:
            continue
        size = int(it.get("size") or 0)
        out.append(
            DriveFile(
                id=str(it.get("id") or ""),
                name=name,
                size=size,
                modified_time=it.get("modifiedTime"),
                parents=list(it.get("parents") or []),
            )
        )

    out.sort(key=lambda f: (f.modified_time or "", f.name))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Process videos dropped into a Drive folder (service account API).")
    parser.add_argument("--drive-id", default=os.environ.get("RAMBLERS_DRIVE_ID", ""), help="Shared Drive ID (optional)")

    parser.add_argument("--ingest-folder-id", default=os.environ.get("DRIVE_INGEST_FOLDER_ID", ""), help="Ingest folder ID (preferred)")
    parser.add_argument(
        "--ingest-folder-path",
        default=os.environ.get("DRIVE_INGEST_FOLDER_PATH", "Inbox"),
        help="Ingest folder path under the Drive root (used when --ingest-folder-id is not set)",
    )

    parser.add_argument("--games-folder-id", default=os.environ.get("DRIVE_GAMES_FOLDER_ID", ""), help="Games root folder ID for uploads (optional)")
    parser.add_argument("--games-folder-path", default=os.environ.get("DRIVE_GAMES_FOLDER_PATH", ""), help="Games root folder path under Drive root (optional)")
    parser.add_argument(
        "--highlights-folder-id",
        default=os.environ.get("DRIVE_HIGHLIGHTS_FOLDER_ID", ""),
        help="Highlights output root folder ID (optional)",
    )
    parser.add_argument(
        "--highlights-folder-path",
        default=os.environ.get("DRIVE_HIGHLIGHTS_FOLDER_PATH", ""),
        help="Highlights output root folder path under Drive root (optional)",
    )

    parser.add_argument("--min-age", default="2m", help="Only ingest files older than this (prevents partial uploads), e.g. 2m, 10m, 1h")
    parser.add_argument("--local-incoming-dir", type=Path, default=Path("temp/drive_ingest/incoming"), help="Where to download incoming videos locally")
    parser.add_argument("--keep-local-videos", action="store_true", help="Keep downloaded source videos locally")
    parser.add_argument("--min-free-gb", type=float, default=8.0, help="Minimum free disk space required to start processing a file (default: 8GB)")
    parser.add_argument("--disk-headroom-gb", type=float, default=4.0, help="Extra free space required beyond the source file size (default: 4GB)")
    parser.add_argument("--once", action="store_true", help="Run a single ingest pass, then exit")
    parser.add_argument("--poll-seconds", type=int, default=60, help="Polling interval when not using --once")
    parser.add_argument("--name-contains", default="", help="Only process files whose names contain this substring (case-insensitive)")
    parser.add_argument("--list", action="store_true", help="List eligible files and exit")
    parser.add_argument("--preflight", action="store_true", help="Print a preflight environment snapshot and exit")

    # Pipeline tuning
    parser.add_argument(
        "--broadcast-type",
        default="auto",
        help="OCR broadcast type: auto, flohockey, yarmouth, standard (default: auto)",
    )
    parser.add_argument("--sample-interval", type=int, default=5, help="OCR sampling interval in seconds")
    parser.add_argument("--tolerance-seconds", type=int, default=30, help="Matching tolerance in seconds")
    parser.add_argument("--before-seconds", type=float, default=15.0, help="Seconds before goal to include")
    parser.add_argument("--after-seconds", type=float, default=4.0, help="Seconds after goal to include")
    parser.add_argument("--max-clips", type=int, default=0, help="Max clips in basic highlights.mp4 (0 = unlimited)")
    parser.add_argument("--no-parallel-ocr", action="store_true", help="Disable parallel OCR")
    parser.add_argument("--ocr-workers", type=int, default=4, help="OCR worker threads when parallel OCR is enabled")
    parser.add_argument(
        "--audio-delay-seconds",
        type=float,
        default=0.0,
        help="Delay audio relative to video by this many seconds when repairing TS/PTS issues (default: 0)",
    )
    parser.add_argument(
        "--auto-av-sync-threshold-seconds",
        type=float,
        default=60.0,
        help="Auto-apply audio delay when ffprobe start_time suggests a large A/V offset >= this many seconds (default: 60)",
    )
    parser.add_argument(
        "--no-remux-ts",
        action="store_true",
        help="Disable pre-remuxing TS sources to a working MP4 before running the pipeline",
    )
    parser.add_argument(
        "--no-debug-bundle",
        action="store_true",
        help="Disable building/uploading output/debug_bundle.zip",
    )
    parser.add_argument(
        "--audio-stream-index",
        type=int,
        default=None,
        help="Force which input audio stream index to use (ffprobe/ffmpeg stream index). Default: use the file's default audio stream.",
    )
    parser.add_argument(
        "--video-stream-index",
        type=int,
        default=None,
        help="Force which input video stream index to use (ffprobe/ffmpeg stream index). Default: use the file's default video stream.",
    )

    # Production reel tuning
    parser.add_argument(
        "--reel-fps",
        default="source",
        help="Production reel FPS (e.g., 60, 30000/1001, or 'source' to match clips)",
    )
    parser.add_argument("--overlay-seconds", type=float, default=5.0, help="Overlay duration at clip start")
    parser.add_argument("--transition-seconds", type=float, default=0.25, help="Crossfade duration between clips")
    parser.add_argument(
        "--no-review-monitor",
        action="store_true",
        help="Disable running the major review monitor during ingest polling",
    )

    args = parser.parse_args()

    # Local-only diagnostics: should not require Drive credentials.
    preflight = _preflight_snapshot()
    if bool(getattr(args, "preflight", False)):
        print(json.dumps(preflight, indent=2))
        ok_bins = bool(preflight.get("paths", {}).get("ffmpeg")) and bool(preflight.get("paths", {}).get("ffprobe"))
        return 0 if ok_bins else 1

    drive_id = _normalize_drive_id(args.drive_id)
    ingest_folder_id = _normalize_drive_id(args.ingest_folder_id)
    games_folder_id = _normalize_drive_id(args.games_folder_id)
    highlights_folder_id = _normalize_drive_id(args.highlights_folder_id)

    try:
        service = _get_drive_service()
    except Exception as e:
        msg = str(e)
        hint = ""
        if "oauth2.googleapis.com" in msg or "name resolution" in msg.lower():
            hint = " (looks like DNS/network is down; cannot reach Google OAuth/Drive)"
        print(f"[ingest] ERROR: could not initialize Drive client: {e}{hint}", file=sys.stderr)
        return 1

    try:
        if not ingest_folder_id:
            ingest_folder_id = _resolve_folder_path(service, drive_id=drive_id, folder_path=str(args.ingest_folder_path))

        if not games_folder_id and args.games_folder_path:
            games_folder_id = _resolve_folder_path(service, drive_id=drive_id, folder_path=str(args.games_folder_path))
        if not highlights_folder_id and args.highlights_folder_path:
            highlights_folder_id = _resolve_folder_path(
                service,
                drive_id=drive_id,
                folder_path=str(args.highlights_folder_path),
            )

        # Ensure ingest subfolders exist
        processing_folder_id = _ensure_folder(service, parent_id=ingest_folder_id, name="processing", drive_id=drive_id)
        failed_folder_id = _ensure_folder(service, parent_id=ingest_folder_id, name="failed", drive_id=drive_id)
        processed_folder_id = _ensure_folder(service, parent_id=ingest_folder_id, name="processed", drive_id=drive_id)
    except Exception as e:
        msg = str(e)
        hint = ""
        if "oauth2.googleapis.com" in msg or "name resolution" in msg.lower():
            hint = " (DNS/network is down; cannot reach Google OAuth/Drive)"
        print(f"[ingest] ERROR: Drive access failed during startup: {e}{hint}", file=sys.stderr)
        return 1

    supported_exts = {ext.lower() for ext in getattr(config, "SUPPORTED_FORMATS", [".ts", ".mp4", ".mkv", ".mov"])}
    args.local_incoming_dir.mkdir(parents=True, exist_ok=True)
    parallel_ocr = not bool(args.no_parallel_ocr)
    min_age = _parse_min_age(str(args.min_age))
    file_manager = FileManager(config)
    unmatched_root = Path(config.GAMES_DIR) / "unmatched"
    unmatched_root.mkdir(parents=True, exist_ok=True)

    while True:
        # Opportunistically run the major-review auto-resume check while we're already awake.
        # This removes the need for a separate cron/systemd timer in most deployments.
        if not bool(args.no_review_monitor):
            try:
                _run([sys.executable, str(REPO_ROOT / "scripts" / "major_review_monitor.py")], check=False)
            except Exception as e:
                print(f"[ingest] WARNING: major review monitor check failed: {e}", file=sys.stderr)

        try:
            candidates = _list_ingest_videos(
                service,
                ingest_folder_id=ingest_folder_id,
                drive_id=drive_id,
                min_age=min_age,
                supported_exts=supported_exts,
                name_contains=str(args.name_contains),
            )
        except Exception as e:
            print(f"[ingest] Failed to list ingest folder: {e}", file=sys.stderr)
            candidates = []

        if not candidates:
            if args.once:
                print("[ingest] No files to process.")
                return 0
            time.sleep(max(1, int(args.poll_seconds)))
            continue

        if bool(args.list):
            for f in candidates:
                print(f"{f.modified_time or ''}\t{f.size}\t{f.name}")
            return 0

        for rf in candidates:
            source_game_info = _parse_source_game_info(file_manager, rf.name)

            provider = _load_amherst_provider()
            game = _find_matching_game(provider, source_game_info=source_game_info)
            if not game:
                # Cache is likely stale; refresh and try again.
                try:
                    _refresh_amherst_games_cache()
                except Exception:
                    pass
                try:
                    provider = _load_amherst_provider()
                    game = _find_matching_game(provider, source_game_info=source_game_info)
                except Exception:
                    game = None

            if game:
                canonical_game_info = _canonical_game_info_from_match(source_game_info=source_game_info, game=game)
                game_folders = file_manager.create_game_folder_from_teams(
                    date=canonical_game_info["date"],
                    home_team=canonical_game_info["home_team"],
                    away_team=canonical_game_info["away_team"],
                    league=canonical_game_info["league"],
                    filename=canonical_game_info["filename"],
                    home_away=canonical_game_info.get("home_away") or "unknown",
                    time_str=canonical_game_info.get("time") or "unknown",
                )
            else:
                canonical_game_info = dict(source_game_info)
                game_folders = file_manager.create_game_folder(source_game_info, base_dir=unmatched_root)

            game_dir = game_folders.get("game_dir") if isinstance(game_folders.get("game_dir"), Path) else None
            if not game_dir:
                print(f"[ingest] ERROR: could not create game folder for {rf.name}", file=sys.stderr)
                if args.once:
                    return 1
                continue

            ingest_log = game_dir / "logs" / "ingest.log"
            _append_log_line(ingest_log, "info", f"Drive file: id={rf.id} name={rf.name} size={rf.size}")

            # Write a per-game preflight snapshot for debugging.
            try:
                _write_json(game_dir / "output" / "preflight.json", preflight)
            except Exception:
                pass

            # Stage source video inside the game folder (keep on failure; delete on success unless configured).
            stage_ext = Path(rf.name).suffix or ".ts"
            staged_original = game_dir / "source" / f"original{stage_ext}"
            local_tmp = args.local_incoming_dir / rf.name

            reuse_existing = False
            if staged_original.exists():
                try:
                    reuse_existing = int(staged_original.stat().st_size) == int(rf.size) and int(rf.size) > 0
                except Exception:
                    reuse_existing = False

            # If the video is already staged, we don't need free space equal to source size again — only headroom.
            required_free_bytes = max(
                int(float(args.min_free_gb) * (1024**3)),
                (
                    int(float(args.disk_headroom_gb) * (1024**3))
                    if reuse_existing
                    else int(rf.size) + int(float(args.disk_headroom_gb) * (1024**3))
                ),
            )
            free_bytes = _disk_free_bytes(args.local_incoming_dir)
            if free_bytes < required_free_bytes and not bool(args.keep_local_videos):
                keep = {local_tmp} if local_tmp.exists() else set()
                _prune_local_incoming_dir(args.local_incoming_dir, supported_exts=supported_exts, keep=keep)
                free_bytes = _disk_free_bytes(args.local_incoming_dir)

            if free_bytes < required_free_bytes:
                msg = f"Skipping (low disk): free={_gb(free_bytes):.2f}GB required={_gb(required_free_bytes):.2f}GB"
                _append_log_line(ingest_log, "warn", msg)
                print(f"[ingest] Skipping (low disk): {rf.name} ({msg})", file=sys.stderr)
                if args.once:
                    return 1
                continue

            print(f"[ingest] Processing: {rf.name} ({rf.size} bytes)")

            try:
                # Lock (move into processing/)
                _move_file(service, file_id=rf.id, add_parent=processing_folder_id, remove_parent=ingest_folder_id)
            except Exception as e:
                _append_log_line(ingest_log, "error", f"Failed to lock file: {e}")
                print(f"[ingest] Failed to lock file: {rf.name}: {e}", file=sys.stderr)
                continue

            started_at = time.time()
            ok = False
            status_payload: Dict[str, Any] = {}
            repaired_video: Optional[Path] = None

            try:
                # Download if not already staged.
                if not reuse_existing:
                    if local_tmp.exists() and not bool(args.keep_local_videos):
                        _try_remove_file(local_tmp)
                    try:
                        _download_file(service, file_id=rf.id, dst=local_tmp)
                    except Exception as e:
                        _append_log_line(ingest_log, "error", f"Failed to download: {e}")
                        print(f"[ingest] Failed to download: {rf.name}: {e}", file=sys.stderr)
                        try:
                            _move_file(service, file_id=rf.id, add_parent=failed_folder_id, remove_parent=processing_folder_id)
                        except Exception:
                            pass
                        if args.once:
                            return 1
                        continue

                    staged_original.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        shutil.move(str(local_tmp), str(staged_original))
                    except Exception:
                        shutil.copy2(str(local_tmp), str(staged_original))
                        if not bool(args.keep_local_videos):
                            _try_remove_file(local_tmp)

                # Write source provenance.
                try:
                    _write_json(
                        game_dir / "source" / "source_info.json",
                        {
                            "drive": {
                                "file_id": rf.id,
                                "name": rf.name,
                                "size": rf.size,
                                "modified_time": rf.modified_time,
                            },
                            "local": {
                                "staged_original": str(staged_original),
                            },
                            "match": {
                                "matched": bool(game),
                                "game_id": str(game.get("game_id")) if game else None,
                            },
                            "source_game_info": source_game_info,
                            "canonical_game_info": canonical_game_info,
                        },
                    )
                except Exception:
                    pass

                if not game:
                    ok = False
                    status_payload = {
                        "success": False,
                        "failed_reason": "no_matching_game",
                        "errors": [f"No matching game found in games/amherst-ramblers.json for: {source_game_info}"],
                    }
                else:
                    # Some captures have broken timestamps and/or massive A/V start offsets (common with TS).
                    # If we can cheaply remux to a working MP4 first, do it so OCR + clip extraction are stable.
                    audio_delay_used, audio_delay_auto, av_probe = _estimate_audio_delay_seconds(
                        staged_original,
                        explicit_delay_seconds=float(args.audio_delay_seconds),
                        auto_threshold_seconds=float(args.auto_av_sync_threshold_seconds),
                        audio_stream_index=args.audio_stream_index,
                        video_stream_index=args.video_stream_index,
                    )

                    working_video = staged_original
                    if staged_original.suffix.lower() == ".ts" and not bool(args.no_remux_ts):
                        if abs(float(audio_delay_used)) > 1e-6 or bool(audio_delay_auto):
                            repaired_video = _repair_video_file(
                                staged_original,
                                audio_delay_seconds=float(audio_delay_used),
                                audio_stream_index=args.audio_stream_index,
                                video_stream_index=args.video_stream_index,
                            )
                            if repaired_video:
                                working_video = repaired_video

                    ok, _game_dir, status_payload = _process_one_video(
                        working_video,
                        game=game,
                        game_folders=game_folders,
                        canonical_game_info=canonical_game_info,
                        source_game_info=source_game_info,
                        sample_interval=int(args.sample_interval),
                        tolerance_seconds=int(args.tolerance_seconds),
                        before_seconds=float(args.before_seconds),
                        after_seconds=float(args.after_seconds),
                        max_clips=int(args.max_clips),
                        broadcast_type=str(args.broadcast_type),
                        parallel_ocr=parallel_ocr,
                        ocr_workers=int(args.ocr_workers),
                    )
                    if repaired_video and working_video == repaired_video and isinstance(status_payload, dict):
                        status_payload.setdefault("warnings", []).append(
                            f"Video remuxed to working MP4 before processing: {repaired_video.name}"
                        )

                    # If the video failed to load (common with some TS captures), attempt a remux repair.
                    if not ok and _should_attempt_repair(status_payload):
                        repaired_video = _repair_video_file(
                            staged_original,
                            audio_delay_seconds=float(audio_delay_used),
                            audio_stream_index=args.audio_stream_index,
                            video_stream_index=args.video_stream_index,
                        )
                        if repaired_video:
                            ok, _game_dir, status_payload = _process_one_video(
                                repaired_video,
                                game=game,
                                game_folders=game_folders,
                                canonical_game_info=canonical_game_info,
                                source_game_info=source_game_info,
                                sample_interval=int(args.sample_interval),
                                tolerance_seconds=int(args.tolerance_seconds),
                                before_seconds=float(args.before_seconds),
                                after_seconds=float(args.after_seconds),
                                max_clips=int(args.max_clips),
                                broadcast_type=str(args.broadcast_type),
                                parallel_ocr=parallel_ocr,
                                ocr_workers=int(args.ocr_workers),
                            )
                            status_payload.setdefault("warnings", []).append(
                                f"Video repaired via ffmpeg remux/re-encode: {repaired_video.name}"
                            )
                    else:
                        # Preserve the auto-sync/debug info even if no repair happened.
                        if isinstance(status_payload, dict) and isinstance(status_payload.get("warnings"), list):
                            if bool(audio_delay_auto):
                                status_payload["warnings"].append(
                                    f"Auto A/V sync: delaying audio by ~{audio_delay_used:.3f}s (ffprobe start_time)"
                                )

                    if isinstance(status_payload, dict):
                        status_payload.setdefault("ingest_media", {})
                        status_payload["ingest_media"].update(
                            {
                                "audio_delay_seconds_explicit": float(args.audio_delay_seconds),
                                "audio_delay_seconds_used": float(audio_delay_used),
                                "audio_delay_auto_applied": bool(audio_delay_auto),
                                "audio_stream_index": args.audio_stream_index,
                                "video_stream_index": args.video_stream_index,
                                "ffprobe_av_probe": av_probe,
                                "working_video_path": str(working_video),
                            }
                        )

                    if ok and not bool(status_payload.get("paused_for_review")):
                        _build_production_reel(
                            game_dir,
                            fps=str(args.reel_fps),
                            overlay_seconds=float(args.overlay_seconds),
                            transition_seconds=float(args.transition_seconds),
                        )
                        if highlights_folder_id:
                            highlight_url = _upload_highlight_outputs(
                                service,
                                game_dir=game_dir,
                                parent_folder_id=highlights_folder_id,
                                drive_id=drive_id,
                            )
                            if highlight_url:
                                status_payload["highlights_folder_url"] = highlight_url
                    elif ok and bool(status_payload.get("paused_for_review")):
                        print(f"[ingest] Paused for major review: {game_dir.name}")

            except Exception as e:
                ok = False
                status_payload = status_payload or {}
                status_payload.setdefault("errors", [])
                status_payload["errors"].append(f"Unhandled exception: {e}")
                _append_log_line(ingest_log, "error", f"Unhandled exception: {e}")

            finished_at = time.time()
            status_payload.setdefault("success", bool(ok))
            status_payload["ingest"] = {
                "drive_file_id": rf.id,
                "drive_ingest_folder_id": ingest_folder_id,
                "drive_processing_folder_id": processing_folder_id,
                "local_tmp": str(local_tmp),
                "staged_original": str(staged_original),
                "repaired_video": str(repaired_video) if repaired_video else None,
                "started_at_unix": started_at,
                "finished_at_unix": finished_at,
                "duration_seconds": round(finished_at - started_at, 3),
                "hostname": os.uname().nodename if hasattr(os, "uname") else None,
                "user": os.environ.get("USER") or os.environ.get("USERNAME"),
            }

            # Always write ingest status locally, even on failure.
            try:
                status_path = game_dir / "output" / "ingest_status.json"
                _write_json(status_path, status_payload)
            except Exception:
                pass

            # Upload outputs/status/logs (optional). Do this even on failure so we can debug remotely.
            remote_dest_parent_id = games_folder_id
            remote_source_folder_id = None
            remote_output_folder_id = None
            remote_game_id = None
            uploaded_working_id = None
            uploaded_debug_bundle_id = None
            if remote_dest_parent_id:
                try:
                    remote_game_name = _build_highlight_folder_name(game_dir)
                    remote_game_id = _ensure_folder(service, parent_id=remote_dest_parent_id, name=remote_game_name, drive_id=drive_id)
                    data_id = _ensure_folder(service, parent_id=remote_game_id, name="data", drive_id=drive_id)
                    clips_id = _ensure_folder(service, parent_id=remote_game_id, name="clips", drive_id=drive_id)
                    output_id = _ensure_folder(service, parent_id=remote_game_id, name="output", drive_id=drive_id)
                    logs_id = _ensure_folder(service, parent_id=remote_game_id, name="logs", drive_id=drive_id)
                    remote_source_folder_id = _ensure_folder(service, parent_id=remote_game_id, name="source", drive_id=drive_id)
                    remote_output_folder_id = output_id

                    status_payload.setdefault("drive", {})
                    status_payload["drive"].update(
                        {
                            "drive_id": drive_id,
                            "source_file_id": rf.id,
                            "source_file_name": rf.name,
                            "games_root_folder_id": remote_dest_parent_id,
                            "game_folder_id": remote_game_id,
                            "game_folder_url": f"https://drive.google.com/drive/folders/{remote_game_id}",
                            "folders": {
                                "data": data_id,
                                "clips": clips_id,
                                "output": output_id,
                                "logs": logs_id,
                                "source": remote_source_folder_id,
                            },
                        }
                    )

                    _upload_tree(service, src_dir=game_dir / "data", dst_parent_id=data_id, drive_id=drive_id)
                    _upload_tree(service, src_dir=game_dir / "clips", dst_parent_id=clips_id, drive_id=drive_id)
                    _upload_tree(service, src_dir=game_dir / "output", dst_parent_id=output_id, drive_id=drive_id)
                    _upload_tree(service, src_dir=game_dir / "logs", dst_parent_id=logs_id, drive_id=drive_id)
                    # Avoid uploading the full source video (large). The original Drive file will be
                    # moved into remote_source_folder_id on success. Keep only provenance metadata.
                    source_info_path = game_dir / "source" / "source_info.json"
                    if source_info_path.exists():
                        # Enrich local provenance with where we archived on Drive.
                        try:
                            payload = json.loads(source_info_path.read_text(encoding="utf-8"))
                            if isinstance(payload, dict):
                                payload.setdefault("drive_archive", {})
                                payload["drive_archive"].update(
                                    {
                                        "drive_id": drive_id,
                                        "game_folder_id": remote_game_id,
                                        "game_folder_url": f"https://drive.google.com/drive/folders/{remote_game_id}",
                                        "source_folder_id": remote_source_folder_id,
                                    }
                                )
                                if uploaded_working_id:
                                    payload["drive_archive"]["working_file_id"] = uploaded_working_id
                                if uploaded_debug_bundle_id:
                                    payload["drive_archive"]["debug_bundle_file_id"] = uploaded_debug_bundle_id
                                source_info_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
                        except Exception:
                            pass
                        _upsert_file(service, local_path=source_info_path, parent_id=remote_source_folder_id, drive_id=drive_id)

                    # If we created a repaired/working MP4 locally, archive it into the per-game Drive folder.
                    if repaired_video and repaired_video.exists() and repaired_video.is_file():
                        uploaded_working_id = _upsert_file(
                            service,
                            local_path=repaired_video,
                            parent_id=remote_source_folder_id,
                            drive_id=drive_id,
                            remote_name="working.mp4",
                        )
                        status_payload["drive"]["working_file_id"] = uploaded_working_id

                    # Build + upload a compact debug bundle so we can diagnose issues without the full video.
                    if not bool(args.no_debug_bundle):
                        debug_zip = _build_debug_bundle_zip(game_dir)
                        if debug_zip and debug_zip.exists():
                            uploaded_debug_bundle_id = _upsert_file(
                                service,
                                local_path=debug_zip,
                                parent_id=output_id,
                                drive_id=drive_id,
                                remote_name="debug_bundle.zip",
                            )
                            status_payload["drive"]["debug_bundle_file_id"] = uploaded_debug_bundle_id

                    # Upsert status explicitly (it lives in output/ already, but do it to ensure it's present).
                    status_path = game_dir / "output" / "ingest_status.json"
                    if status_path.exists():
                        _write_json(status_path, status_payload)
                        _upsert_file(service, local_path=status_path, parent_id=output_id, drive_id=drive_id)
                except Exception as e:
                    _append_log_line(ingest_log, "warn", f"Upload failed: {e}")
                    print(f"[ingest] Upload failed for {game_dir.name}: {e}", file=sys.stderr)

            if not ok:
                print(f"[ingest] FAILED: {rf.name}", file=sys.stderr)
                if remote_source_folder_id and remote_output_folder_id and remote_game_id:
                    # Surface the source file inside the per-game archive even if it stays in ingest/failed/.
                    try:
                        shortcut_id = _create_shortcut(
                            service,
                            target_file_id=rf.id,
                            parent_id=remote_source_folder_id,
                            name=f"source_link - {rf.name}",
                        )
                        if shortcut_id:
                            status_payload.setdefault("drive", {})
                            status_payload["drive"]["source_shortcut_file_id"] = shortcut_id
                            status_path = game_dir / "output" / "ingest_status.json"
                            try:
                                _write_json(status_path, status_payload)
                                _upsert_file(service, local_path=status_path, parent_id=remote_output_folder_id, drive_id=drive_id)
                            except Exception:
                                pass
                    except Exception:
                        pass
                try:
                    _move_file(service, file_id=rf.id, add_parent=failed_folder_id, remove_parent=processing_folder_id)
                except Exception:
                    pass
                if not bool(args.keep_local_videos):
                    _try_remove_file(local_tmp)
                if args.once:
                    return 1
                continue

            # Move the original Drive video out of processing/
            source_move_ok = False
            try:
                if remote_source_folder_id:
                    _move_file(service, file_id=rf.id, add_parent=remote_source_folder_id, remove_parent=processing_folder_id)
                else:
                    _move_file(service, file_id=rf.id, add_parent=processed_folder_id, remove_parent=processing_folder_id)
                source_move_ok = True
            except Exception as e:
                print(f"[ingest] WARNING: could not move source video after processing: {e}", file=sys.stderr)

            # Record archival outcome (used to decide local cleanup).
            # For local cleanup, we only require that the original source has been safely moved out of
            # ingest/processing on Drive. Uploading outputs is desirable, but not required to prevent
            # local disk exhaustion.
            drive_archive_ok = bool(source_move_ok)
            status_payload.setdefault("drive", {})
            status_payload["drive"].update(
                {
                    "source_move_ok": bool(source_move_ok),
                    "archival_ok": bool(drive_archive_ok),
                }
            )
            try:
                status_path = game_dir / "output" / "ingest_status.json"
                _write_json(status_path, status_payload)
                if remote_output_folder_id:
                    _upsert_file(service, local_path=status_path, parent_id=remote_output_folder_id, drive_id=drive_id)
            except Exception:
                pass

            # Delete local staged videos on success if Drive archival completed.
            # Local disk is disposable; Drive is source-of-truth.
            if not bool(args.keep_local_videos) and bool(drive_archive_ok):
                _try_remove_file(staged_original)
                if repaired_video:
                    _try_remove_file(repaired_video)
                _try_remove_file(local_tmp)

            print(f"[ingest] DONE: {game_dir.name}")

        if args.once:
            return 0
        time.sleep(max(1, int(args.poll_seconds)))


if __name__ == "__main__":
    raise SystemExit(main())
