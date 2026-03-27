#!/usr/bin/env python3
"""
Major Review Monitor - Cronjob script to monitor Drive for completed reviews

This script is designed to run as a cronjob (every 5 minutes) and:
1. Check if review is active (flag file exists)
2. Poll Google Drive folder for completed reviews
3. Re-cut clips based on user specifications
4. Move reviewed items to completed folder
5. Disable itself when all reviews are complete

Usage:
    # Check for completed reviews (normal cronjob mode)
    python scripts/major_review_monitor.py

    # Enable monitor for a specific game
    python scripts/major_review_monitor.py --enable --game-id 4820

    # Disable monitor
    python scripts/major_review_monitor.py --disable

    # Force check regardless of flag
    python scripts/major_review_monitor.py --force

Cronjob entry (runs every 5 minutes):
    */5 * * * * cd /path/to/repo && python scripts/major_review_monitor.py >> logs/major_review.log 2>&1
"""

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Dict, Optional, Tuple

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from drive_config import resolve_drive_config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _setup_game_log_handler(game_dir: Path) -> Optional[logging.Handler]:
    """
    Attach a per-game file handler so cron stdout isn't the only place logs go.

    Returns the handler so callers can detach it at the end of the run.
    """
    try:
        logs_dir = game_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_path = logs_dir / "major_review_monitor.log"
        handler = logging.FileHandler(log_path, encoding="utf-8")
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(handler)
        return handler
    except Exception:
        return None


def _update_ingest_status(game_dir: Path, patch: Dict[str, Any]) -> None:
    """
    Merge a status patch into output/ingest_status.json (best-effort).

    This avoids losing ingest info while still making major-review resumption observable.
    """
    try:
        status_path = game_dir / "output" / "ingest_status.json"
        current = _load_json(status_path)
        if not isinstance(current, dict):
            current = {}
        current.update(patch)
        _write_json(status_path, current)
    except Exception:
        pass


def _send_resend_email(*, subject: str, text: str, html: Optional[str] = None) -> bool:
    """
    Send an email via Resend, retrying with a verified fallback sender if needed.

    Some environments set NOTIFICATION_EMAIL_FROM to an unverified address; Resend will
    reject those. We attempt with the configured sender first, then fall back.
    """
    api_key = getattr(config, "RESEND_API_KEY", "") or os.environ.get("RESEND_API_KEY", "")
    to_email = getattr(config, "NOTIFICATION_EMAIL_TO", "") or os.environ.get("NOTIFICATION_EMAIL", "")
    preferred_from = getattr(config, "NOTIFICATION_EMAIL_FROM", "") or os.environ.get("NOTIFICATION_EMAIL_FROM", "")
    fallback_from = "onboarding@resend.dev"

    if not api_key or not to_email:
        return False

    try:
        import resend
    except Exception as e:
        logger.warning(f"Resend not available: {e}")
        return False

    resend.api_key = api_key

    def _attempt(sender: str) -> bool:
        if not sender:
            return False
        payload: Dict[str, Any] = {
            "from": sender,
            "to": [to_email],
            "subject": subject,
            "text": text,
        }
        if html:
            payload["html"] = html
        try:
            resend.Emails.send(payload)
            return True
        except Exception as e:
            logger.warning(f"Resend send failed (from={sender}): {e}")
            return False

    sender = preferred_from or fallback_from
    if _attempt(sender):
        return True
    if sender != fallback_from:
        return _attempt(fallback_from)
    return False


def normalize_drive_folder_id(value: str) -> str:
    """Allow config/env to provide either a folder ID or a full Drive folder URL."""
    if not value:
        return ''
    value = str(value).strip()
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", value)
    return m.group(1) if m else value


def _sanitize_drive_name(value: str) -> str:
    raw = str(value or "")
    normalized = unicodedata.normalize("NFKD", raw)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_value = re.sub(r'[<>:"/\\|?*]', "_", ascii_value)
    ascii_value = re.sub(r"\s+", " ", ascii_value).strip(" .")
    return ascii_value or "Highlights"


def _load_game_metadata(game_dir: Path) -> Dict[str, Any]:
    metadata_path = Path(game_dir) / "data" / "game_metadata.json"
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
        name = str(Path(game_dir).name)
    return _sanitize_drive_name(name)


def _ensure_youtube_description(game_dir: Path) -> Optional[Path]:
    desc_path = Path(game_dir) / "output" / "youtube_description.txt"
    if desc_path.exists():
        return desc_path
    try:
        from highlight_extractor.description_generator import generate_description_from_game_dir
    except Exception as e:
        logger.warning(f"Could not import description generator: {e}")
        return None
    try:
        return generate_description_from_game_dir(game_dir, output_dir=Path(game_dir) / "output")
    except Exception as e:
        logger.warning(f"Failed to generate YouTube description: {e}")
        return None


def upload_highlight_outputs(service, *, game_dir: Path, drive_id: str) -> Optional[str]:
    drive_cfg = resolve_drive_config()
    parent_folder_id = normalize_drive_folder_id(drive_cfg.reels_folder_id)
    if not parent_folder_id:
        folder_path = str(drive_cfg.reels_folder_path or "").strip()
        if folder_path:
            try:
                parent_folder_id = _resolve_folder_path(service, drive_id=drive_id, folder_path=folder_path)
            except Exception as e:
                logger.warning(f"Highlight upload skipped: could not resolve folder path ({e})")
                return None
        else:
            logger.info("Highlight upload skipped: HIGHLIGHTS_REELS_FOLDER_ID not set")
            return None

    output_dir = Path(game_dir) / "output"
    production_path = output_dir / "highlights_production.mp4"
    if not output_dir.exists() or not production_path.exists():
        logger.warning(f"Highlight upload skipped: missing {production_path}")
        return None

    _ensure_youtube_description(game_dir)
    folder_name = _build_highlight_folder_name(game_dir)
    remote_folder_id = _ensure_folder(service, parent_id=parent_folder_id, name=folder_name, drive_id=drive_id)

    _upload_tree(service, src_dir=output_dir, dst_parent_id=remote_folder_id, drive_id=drive_id)

    folder_url = f"https://drive.google.com/drive/folders/{remote_folder_id}"
    logger.info(f"Uploaded highlights to: {folder_url}")
    return folder_url


def get_flag_file() -> Path:
    """Get the flag file path from config"""
    return Path(getattr(config, 'MAJOR_REVIEW_FLAG_FILE', '/tmp/major_review_active'))


def is_review_active() -> bool:
    """Check if a review is currently active"""
    flag_file = get_flag_file()
    if not flag_file.exists():
        return False

    try:
        flag_data = json.loads(flag_file.read_text())
        return flag_data.get('enabled', False)
    except Exception:
        return False


def get_review_info() -> Optional[Dict]:
    """Get current review information from flag file"""
    flag_file = get_flag_file()
    if not flag_file.exists():
        return None

    try:
        return json.loads(flag_file.read_text())
    except Exception:
        return None


def resolve_game_paths(review_info: Dict[str, Any]) -> Tuple[Optional[Path], Optional[Path]]:
    """
    Resolve local game_dir and major_review_dir from the flag file and/or persisted state.

    Returns:
        (game_dir, major_review_dir)
    """
    game_dir_raw = str(review_info.get("game_dir") or "").strip()
    major_review_dir_raw = str(review_info.get("major_review_dir") or "").strip()
    resume_state_raw = str(review_info.get("resume_state_path") or "").strip()

    def _as_existing_dir(value: str) -> Optional[Path]:
        if not value:
            return None
        p = Path(value)
        return p if p.exists() and p.is_dir() else None

    game_dir = _as_existing_dir(game_dir_raw)
    major_review_dir = _as_existing_dir(major_review_dir_raw)

    if (game_dir is None or major_review_dir is None) and resume_state_raw:
        try:
            state_path = Path(resume_state_raw)
            if state_path.exists():
                state = json.loads(state_path.read_text(encoding="utf-8"))
                if game_dir is None:
                    game_dir = _as_existing_dir(str(state.get("game_dir") or ""))
                if major_review_dir is None:
                    major_review_dir = _as_existing_dir(str((state.get("major_review") or {}).get("local_dir") or ""))
        except Exception as e:
            logger.warning(f"Failed to resolve paths from resume_state_path: {e}")

    if game_dir is not None and major_review_dir is None:
        candidate = game_dir / "output" / "major_review"
        if candidate.exists() and candidate.is_dir():
            major_review_dir = candidate

    return game_dir, major_review_dir


def update_check_count():
    """Update the check count in flag file"""
    flag_file = get_flag_file()
    if not flag_file.exists():
        return

    try:
        flag_data = json.loads(flag_file.read_text())
        flag_data['check_count'] = flag_data.get('check_count', 0) + 1
        flag_data['last_check'] = datetime.now().isoformat()
        flag_file.write_text(json.dumps(flag_data, indent=2))
    except Exception as e:
        logger.warning(f"Failed to update check count: {e}")


def enable_monitor(game_id: str):
    """Enable the review monitor"""
    flag_file = get_flag_file()
    flag_data = {
        'enabled': True,
        'game_id': game_id,
        'enabled_at': datetime.now().isoformat(),
        'check_count': 0
    }
    flag_file.write_text(json.dumps(flag_data, indent=2))
    logger.info(f"Monitor enabled for game {game_id}")


def disable_monitor():
    """Disable the review monitor"""
    flag_file = get_flag_file()
    if flag_file.exists():
        flag_file.unlink()
    logger.info("Monitor disabled")


def check_review_timeout(review_info: Dict) -> bool:
    """Check if review has timed out"""
    timeout_days = getattr(config, 'MAJOR_REVIEW_TIMEOUT_DAYS', 7)
    enabled_at = review_info.get('enabled_at')

    if not enabled_at:
        return False

    try:
        enabled_time = datetime.fromisoformat(enabled_at)
        deadline = enabled_time + timedelta(days=timeout_days)
        return datetime.now() > deadline
    except Exception:
        return False


def get_drive_service():
    """Get authenticated Google Drive service"""
    try:
        from googleapiclient.discovery import build
        from google.oauth2 import service_account
    except ImportError:
        logger.error("Google API client not installed")
        return None

    creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if not creds_path or not Path(creds_path).exists():
        logger.error("Google service account credentials not found")
        return None

    try:
        credentials = service_account.Credentials.from_service_account_file(
            creds_path,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logger.error(f"Failed to create Drive service: {e}")
        return None


def _list_files(
    service,
    *,
    parent_id: str,
    drive_id: str,
    query_extra: str = "",
    fields: str = "files(id,name,mimeType)",
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
        params.update({"corpora": "drive", "driveId": drive_id})

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


def _find_child_file(service, *, parent_id: str, name: str, drive_id: str) -> Optional[str]:
    name_esc = str(name or "").replace("'", "\\'")
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


def _upsert_file(service, *, local_path: Path, parent_id: str, drive_id: str) -> None:
    from googleapiclient.http import MediaFileUpload

    existing_id = _find_child_file(service, parent_id=parent_id, name=local_path.name, drive_id=drive_id)
    media = MediaFileUpload(str(local_path), resumable=True)
    if existing_id:
        service.files().update(
            fileId=existing_id,
            media_body=media,
            supportsAllDrives=True,
        ).execute()
        return

    service.files().create(
        body={"name": local_path.name, "parents": [parent_id]},
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    ).execute()


def _upload_tree(service, *, src_dir: Path, dst_parent_id: str, drive_id: str) -> None:
    if not src_dir.exists():
        return
    for child in sorted(src_dir.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
        if child.is_dir():
            sub_id = _ensure_folder(service, parent_id=dst_parent_id, name=child.name, drive_id=drive_id)
            _upload_tree(service, src_dir=child, dst_parent_id=sub_id, drive_id=drive_id)
        elif child.is_file():
            _upsert_file(service, local_path=child, parent_id=dst_parent_id, drive_id=drive_id)


def find_review_folder(service, game_id: str) -> Optional[str]:
    """Find the review folder for a specific game"""
    parent_folder_id = normalize_drive_folder_id(getattr(config, 'MAJOR_REVIEW_DRIVE_FOLDER_ID', ''))
    if not parent_folder_id:
        return None

    try:
        query = f"'{parent_folder_id}' in parents and name contains '{game_id}' and mimeType='application/vnd.google-apps.folder'"
        results = service.files().list(
            q=query,
            fields='files(id, name)',
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()
        files = results.get('files', [])

        if files:
            return files[0]['id']
        return None
    except Exception as e:
        logger.error(f"Failed to find review folder: {e}")
        return None


def get_review_jsons(service, folder_id: str) -> List[Dict]:
    """Get all review JSON files from the folder"""
    try:
        query = f"'{folder_id}' in parents and name contains '.json'"
        results = service.files().list(
            q=query,
            fields='files(id, name, modifiedTime)',
            orderBy='modifiedTime desc',
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()
        files = results.get('files', []) or []

        # De-dup by filename, keeping the most recently modified copy. This prevents
        # stale duplicates (from past retries) from blocking completion.
        seen_names: set[str] = set()
        deduped: List[Dict[str, Any]] = []
        for file_info in files:
            name = str(file_info.get("name") or "")
            if not name or name in seen_names:
                continue
            seen_names.add(name)
            deduped.append(file_info)

        jsons = []
        for file_info in deduped:
            try:
                content = service.files().get_media(fileId=file_info['id'], supportsAllDrives=True).execute()
                review_data = json.loads(content.decode('utf-8'))
                review_data['_file_id'] = file_info['id']
                review_data['_file_name'] = file_info['name']
                review_data['_modified_time'] = file_info.get('modifiedTime')
                jsons.append(review_data)
            except Exception as e:
                logger.warning(f"Failed to read {file_info['name']}: {e}")

        return jsons
    except Exception as e:
        logger.error(f"Failed to list review JSONs: {e}")
        return []


def all_reviews_complete(review_jsons: List[Dict]) -> bool:
    """Check if all reviews are marked as reviewed"""
    if not review_jsons:
        return False
    return all(r.get('reviewed', False) for r in review_jsons)


def download_clip(service, folder_id: str, clip_filename: str, output_path: Path) -> bool:
    """Download a clip from Drive"""
    try:
        query = f"'{folder_id}' in parents and name='{clip_filename}'"
        results = service.files().list(
            q=query,
            fields='files(id, name, modifiedTime)',
            orderBy='modifiedTime desc',
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()
        files = results.get('files', [])

        if not files:
            logger.error(f"Clip not found: {clip_filename}")
            return False

        file_id = files[0]['id']
        content = service.files().get_media(fileId=file_id, supportsAllDrives=True).execute()

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(content)
        return True
    except Exception as e:
        logger.error(f"Failed to download {clip_filename}: {e}")
        return False


def process_approved_clips(
    service,
    folder_id: str,
    review_jsons: List[Dict],
    game_id: str,
    *,
    major_review_dir: Path,
) -> List[Dict[str, Any]]:
    """
    Process approved clips: download, trim if needed, return paths.

    Returns list of dicts ready for insertion into the production reel manifest.
    """
    approved_entries: List[Dict[str, Any]] = []
    major_review_dir.mkdir(parents=True, exist_ok=True)

    for review in review_jsons:
        if not review.get('include', False):
            logger.info(f"Skipping excluded clip: {review.get('clip_filename')}")
            continue

        clip_filename = review.get('clip_filename')
        if not clip_filename:
            continue

        # Download clip
        local_clip = major_review_dir / clip_filename
        if not download_clip(service, folder_id, clip_filename, local_clip):
            continue

        # Persist the updated review JSON alongside the clip for traceability
        json_filename = review.get("json_filename") or review.get("_file_name") or (Path(clip_filename).with_suffix(".json").name)
        try:
            (major_review_dir / str(json_filename)).write_text(json.dumps(review, indent=2), encoding="utf-8")
        except Exception:
            pass

        # Check if trimming is needed
        trim_start = review.get('trim_start')
        trim_end = review.get('trim_end')

        if trim_start is not None or trim_end is not None:
            # Trim the clip
            trimmed_clip = trim_clip(local_clip, trim_start, trim_end)
            final_clip = trimmed_clip or local_clip
        else:
            final_clip = local_clip

        clip_video_start = review.get("clip_video_start")
        try:
            clip_video_start_f = float(clip_video_start) if clip_video_start is not None else None
        except Exception:
            clip_video_start_f = None

        # If the reviewer trims off the start, the clip's new "start in game video"
        # shifts forward by trim_start seconds.
        try:
            trim_start_f = float(trim_start) if trim_start is not None else 0.0
        except Exception:
            trim_start_f = 0.0

        video_time = None
        if clip_video_start_f is not None:
            video_time = max(0.0, clip_video_start_f + trim_start_f)

        period = int(review.get("period") or 0)
        time_str = str(review.get("time") or "").strip()
        players = review.get("players") or []
        if not isinstance(players, list):
            players = []

        primary = players[0] if players else {}
        team = str(primary.get("team") or review.get("team") or "")
        minutes = primary.get("minutes") or review.get("minutes") or 5
        infraction = str(primary.get("infraction") or review.get("infraction") or "Major")
        player_names = " / ".join([str(p.get("name") or "").strip() for p in players if isinstance(p, dict)]) or "Unknown"

        event = {
            "type": "penalty",
            "subtype": "major_review",
            "period": period,
            "time": time_str,
            "team": team,
            "player": {"name": player_names, "number": None},
            "infraction": infraction,
            "minutes": minutes,
        }
        if video_time is not None:
            event["video_time"] = video_time

        # Prefer storing relpaths so the game folder is portable.
        relpath: str
        try:
            relpath = str(final_clip.relative_to(review_info_game_dir := major_review_dir.parent.parent))
        except Exception:
            relpath = str(final_clip)

        approved_entries.append(
            {
                "type": "penalty",
                "video_time": video_time,
                "path": relpath,
                "event": event,
                "source": "major_review",
                "review": {
                    "clip_filename": clip_filename,
                    "json_filename": json_filename,
                    "clip_video_start": clip_video_start,
                    "trim_start": trim_start,
                    "trim_end": trim_end,
                },
            }
        )

        logger.info(f"Processed approved clip: {final_clip.name}")

    return approved_entries


def write_major_approved_manifest(game_dir: Path, approved_entries: List[Dict[str, Any]]) -> Path:
    out_path = game_dir / "data" / "major_penalty_approved.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now().isoformat(),
        "approved": approved_entries,
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


def build_production_reel(game_dir: Path) -> Optional[Path]:
    clips_dir = game_dir / "clips"
    events_json = game_dir / "data" / "matched_events.json"
    out_path = game_dir / "output" / "highlights_production.mp4"

    if not clips_dir.exists():
        logger.error(f"Clips dir not found: {clips_dir}")
        return None
    if not events_json.exists():
        logger.error(f"Events JSON not found: {events_json}")
        return None

    cmd = [
        sys.executable,
        str(Path(__file__).parent / "build_production_highlight_reel.py"),
        "--game-dir",
        str(game_dir),
        "--clips-dir",
        str(clips_dir),
        "--events-json",
        str(events_json),
        "--output",
        str(out_path),
    ]
    log_path = game_dir / "logs" / "production_reel.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8", errors="ignore") as f:
        f.write(f"\n=== {datetime.now().isoformat(timespec='seconds')} | build_production_reel ===\n")
        f.write(" ".join(cmd) + "\n")
        subprocess.run(cmd, check=True, stdout=f, stderr=f, text=True)
    return out_path


def trim_clip(
    clip_path: Path,
    trim_start: Optional[float],
    trim_end: Optional[float]
) -> Optional[Path]:
    """Trim a clip to specified bounds"""
    try:
        from moviepy import VideoFileClip
    except ImportError:
        from moviepy.editor import VideoFileClip

    try:
        clip = VideoFileClip(str(clip_path))

        start = trim_start if trim_start is not None else 0
        end = trim_end if trim_end is not None else clip.duration

        if start >= end or start < 0 or end > clip.duration:
            logger.warning(f"Invalid trim bounds: {start}-{end} for clip duration {clip.duration}")
            clip.close()
            return None

        trimmed = clip.subclipped(start, end)
        output_path = clip_path.with_suffix('.trimmed.mp4')

        codec = getattr(config, "OUTPUT_CODEC", "libx264")
        preset = getattr(config, "OUTPUT_PRESET", "medium")
        audio_codec = getattr(config, "OUTPUT_AUDIO_CODEC", "aac")
        audio_bitrate = getattr(config, "OUTPUT_AUDIO_BITRATE", None)
        audio_fps = getattr(config, "OUTPUT_AUDIO_SAMPLE_RATE", 44100)
        pixel_format = getattr(config, "OUTPUT_PIXEL_FORMAT", None)
        threads = getattr(config, "OUTPUT_THREADS", None)
        ffmpeg_params = []
        crf = getattr(config, "OUTPUT_CRF", None)
        if crf is not None and str(codec).lower() in {"libx264", "libx265"}:
            ffmpeg_params += ["-crf", str(crf)]
        ffmpeg_params += ["-movflags", "+faststart"]

        try:
            trimmed.write_videofile(
                str(output_path),
                codec=codec,
                preset=preset,
                audio_codec=audio_codec,
                audio_bitrate=audio_bitrate,
                audio_fps=audio_fps,
                threads=threads,
                ffmpeg_params=ffmpeg_params or None,
                pixel_format=pixel_format,
                logger=None,
            )
        except TypeError:
            trimmed.write_videofile(
                str(output_path),
                codec=codec,
                preset=preset,
                audio_codec=audio_codec,
                audio_bitrate=audio_bitrate,
                audio_fps=audio_fps,
                threads=threads,
                ffmpeg_params=ffmpeg_params or None,
                pixel_format=pixel_format,
            )

        trimmed.close()
        clip.close()

        return output_path

    except Exception as e:
        logger.error(f"Failed to trim clip: {e}")
        return None


def move_to_completed(service, folder_id: str):
    """Move review folder to _completed subfolder"""
    parent_folder_id = normalize_drive_folder_id(getattr(config, 'MAJOR_REVIEW_DRIVE_FOLDER_ID', ''))

    try:
        # Find or create _completed folder
        query = f"'{parent_folder_id}' in parents and name='_completed' and mimeType='application/vnd.google-apps.folder'"
        results = service.files().list(
            q=query,
            fields='files(id)',
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()
        files = results.get('files', [])

        if files:
            completed_folder_id = files[0]['id']
        else:
            # Create _completed folder
            folder_metadata = {
                'name': '_completed',
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_folder_id]
            }
            completed_folder = service.files().create(body=folder_metadata, fields='id', supportsAllDrives=True).execute()
            completed_folder_id = completed_folder.get('id')

        # Move review folder to _completed
        service.files().update(
            fileId=folder_id,
            addParents=completed_folder_id,
            removeParents=parent_folder_id,
            fields='id, parents',
            supportsAllDrives=True,
        ).execute()

        logger.info(f"Moved review folder to _completed")

    except Exception as e:
        logger.error(f"Failed to move folder to completed: {e}")


def send_timeout_notification(game_id: str, review_info: Dict):
    """Send notification that review timed out"""
    subject = f"Major Penalty Review Timed Out - Game {game_id}"
    text = f"""
The major penalty review for game {game_id} has timed out after 7 days.

The clips have been excluded from the highlight reel.

If you still want to include them, please process them manually.
"""
    if _send_resend_email(subject=subject, text=text):
        logger.info("Timeout notification sent")


def send_highlights_ready_notification(*, folder_url: str, game_dir: Optional[Path]) -> None:
    """Send an email when the production highlights have been uploaded."""
    if not folder_url:
        return
    meta = _load_game_metadata(game_dir) if game_dir else {}
    info = meta.get("game_info") or {}
    date = str(info.get("date") or "").strip() or "Unknown Date"
    home = str(info.get("home_team") or "").strip() or "Home"
    away = str(info.get("away_team") or "").strip() or "Away"

    subject = f"Highlights Ready - {home} vs {away} ({date})"
    text = f"Your production highlights are ready:\n\n{folder_url}\n"
    if _send_resend_email(subject=subject, text=text):
        logger.info("Highlights-ready notification sent")


def resend_review_required_notification() -> None:
    """Resend the major-review-required email for the currently active review (if any)."""
    review_info = get_review_info() or {}
    if not bool(review_info.get("enabled")):
        logger.info("No active review; nothing to resend.")
        return

    game_id = str(review_info.get("game_id") or "").strip() or "unknown"
    folder_url = str(review_info.get("drive_folder_url") or "").strip()
    game_dir, _ = resolve_game_paths(review_info)

    meta = _load_game_metadata(game_dir) if game_dir else {}
    info = meta.get("game_info") or {}
    date = str(info.get("date") or "").strip() or str(review_info.get("game_date") or "").strip() or "Unknown Date"
    home = str(info.get("home_team") or "").strip() or "Home"
    away = str(info.get("away_team") or "").strip() or "Away"

    subject = f"Major Penalty Review Required - {home} vs {away} ({date})"
    text = (
        "Major penalty clips require review.\n\n"
        f"Game ID: {game_id}\n"
        f"Review folder: {folder_url}\n\n"
        "Instructions:\n"
        "1) Open the folder\n"
        "2) Watch each clip\n"
        "3) Edit the corresponding JSON file:\n"
        '   - Set "reviewed": true\n'
        '   - Set "include": true or false\n'
        '   - Optionally set "trim_start" and "trim_end" (seconds from clip start)\n'
    )
    if not folder_url:
        text += "\n(Note: drive_folder_url is missing in the local review state.)\n"

    if _send_resend_email(subject=subject, text=text):
        logger.info("Review-required notification sent")


def run_check(force: bool = False):
    """Run the main check logic"""
    if not force and not is_review_active():
        logger.debug("No active review, exiting")
        return

    review_info = get_review_info()
    if not review_info:
        logger.warning("Could not read review info")
        return

    game_id = review_info.get('game_id', 'unknown')
    logger.info(f"Checking review status for game {game_id}")

    game_dir, major_review_dir = resolve_game_paths(review_info)
    handler: Optional[logging.Handler] = None
    status: Dict[str, Any] = {
        "major_review_monitor": {
            "started_at": datetime.now().isoformat(),
            "game_id": str(game_id),
            "success": False,
            "errors": [],
            "warnings": [],
        }
    }

    try:
        if game_dir is not None:
            handler = _setup_game_log_handler(game_dir)
            # Persist a status breadcrumb immediately so failures are visible.
            _update_ingest_status(game_dir, status)
            try:
                _write_json(game_dir / "output" / "major_review_status.json", status)
            except Exception:
                pass

        if game_dir is None or major_review_dir is None:
            logger.warning("Missing local game paths; cannot auto-resume production build")

        # Check for timeout
        if check_review_timeout(review_info):
            logger.warning(f"Review for game {game_id} has timed out")
            send_timeout_notification(game_id, review_info)
            disable_monitor()
            status["major_review_monitor"]["success"] = True
            status["major_review_monitor"]["warnings"].append("Review timed out; monitor disabled; clips excluded.")
            return

        # Update check count
        update_check_count()

        # Get Drive service
        service = get_drive_service()
        if not service:
            logger.error("Could not get Drive service")
            status["major_review_monitor"]["errors"].append("Could not get Drive service")
            return

        # Find review folder: prefer explicit folder ID from the flag (avoids ambiguity when
        # multiple review runs exist), fall back to name-contains lookup.
        folder_id = normalize_drive_folder_id(str(review_info.get("drive_folder_id") or ""))
        if folder_id:
            try:
                service.files().get(fileId=folder_id, fields="id,name", supportsAllDrives=True).execute()
            except Exception:
                folder_id = ""

        if not folder_id:
            folder_id = find_review_folder(service, game_id)
            if not folder_id:
                logger.warning(f"Could not find review folder for game {game_id}")
                status["major_review_monitor"]["warnings"].append("Could not find review folder")
                return
        status["major_review_monitor"]["folder_id"] = folder_id

        # Get review JSONs
        review_jsons = get_review_jsons(service, folder_id)
        if not review_jsons:
            logger.info("No review JSONs found")
            status["major_review_monitor"]["warnings"].append("No review JSONs found")
            return

        logger.info(f"Found {len(review_jsons)} review files")
        status["major_review_monitor"]["review_files_found"] = len(review_jsons)

        # Check if all reviews are complete
        if not all_reviews_complete(review_jsons):
            reviewed_count = sum(1 for r in review_jsons if r.get('reviewed', False))
            logger.info(f"Reviews not complete: {reviewed_count}/{len(review_jsons)} reviewed")
            status["major_review_monitor"]["reviewed_count"] = reviewed_count
            return

        logger.info("All reviews complete! Processing approved clips...")

        # Process approved clips
        approved_entries: List[Dict[str, Any]] = []
        if game_dir is not None and major_review_dir is not None:
            approved_entries = process_approved_clips(
                service,
                folder_id,
                review_jsons,
                game_id,
                major_review_dir=major_review_dir,
            )
        logger.info(f"Processed {len(approved_entries)} approved clips")
        status["major_review_monitor"]["approved_entries"] = len(approved_entries)

        if game_dir is not None and approved_entries:
            try:
                approved_path = write_major_approved_manifest(game_dir, approved_entries)
                logger.info(f"Wrote approved major clip manifest: {approved_path}")
                status["major_review_monitor"]["approved_manifest"] = str(approved_path)
            except Exception as e:
                logger.error(f"Failed to write approved major manifest: {e}")
                status["major_review_monitor"]["errors"].append(f"Failed to write approved manifest: {e}")

        # Move folder to completed
        move_to_completed(service, folder_id)

        # Disable monitor
        disable_monitor()

        # Resume: Build the production reel with approved majors inserted.
        if game_dir is not None:
            try:
                out_path = build_production_reel(game_dir)
                if out_path:
                    logger.info(f"Built production reel: {out_path}")
                    status["major_review_monitor"]["production_reel"] = str(out_path)
                    drive_id = str(resolve_drive_config().drive_id or "").strip()
                    highlights_folder_url = upload_highlight_outputs(service, game_dir=game_dir, drive_id=drive_id)
                    if highlights_folder_url:
                        status["major_review_monitor"]["highlights_folder_url"] = highlights_folder_url
                        send_highlights_ready_notification(folder_url=highlights_folder_url, game_dir=game_dir)
            except Exception as e:
                logger.error(f"Failed to build production reel: {e}")
                status["major_review_monitor"]["errors"].append(f"Failed to build production reel: {e}")
                status["major_review_monitor"]["exception_type"] = type(e).__name__
                return

        status["major_review_monitor"]["success"] = True
    finally:
        status["major_review_monitor"]["finished_at"] = datetime.now().isoformat()
        if game_dir is not None:
            _update_ingest_status(game_dir, status)
            try:
                _write_json(game_dir / "output" / "major_review_status.json", status)
            except Exception:
                pass
        if handler is not None:
            try:
                logger.removeHandler(handler)
                handler.close()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(description='Major Penalty Review Monitor')
    parser.add_argument('--enable', action='store_true', help='Enable monitor')
    parser.add_argument('--disable', action='store_true', help='Disable monitor')
    parser.add_argument('--game-id', help='Game ID (required with --enable)')
    parser.add_argument('--force', action='store_true', help='Force check regardless of flag')
    parser.add_argument('--status', action='store_true', help='Show current status')
    parser.add_argument(
        '--resend-review-email',
        action='store_true',
        help='Resend the major-review-required email for the active review (if any)',
    )

    args = parser.parse_args()

    if args.enable:
        if not args.game_id:
            parser.error("--game-id required with --enable")
        enable_monitor(args.game_id)
    elif args.disable:
        disable_monitor()
    elif args.resend_review_email:
        resend_review_required_notification()
    elif args.status:
        if is_review_active():
            info = get_review_info()
            print(f"Review active: Yes")
            print(f"Game ID: {info.get('game_id')}")
            print(f"Enabled at: {info.get('enabled_at')}")
            print(f"Check count: {info.get('check_count', 0)}")
        else:
            print("Review active: No")
    else:
        run_check(force=args.force)


if __name__ == '__main__':
    main()
