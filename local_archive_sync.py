"""
Helpers for mirroring locally managed game archives into the configured Shared Drive.

This closes the gap between the ingest workflow, which already archived source media,
and the local source-based workflows used for Amherst series reels.
"""

from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, Optional

from drive_api import (
    ensure_folder,
    get_drive_service,
    list_child_files,
    resolve_folder_path,
    trash_file,
    upload_tree,
    upsert_file,
)
from drive_config import resolve_drive_config


def _sanitize_drive_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_value = re.sub(r'[<>:"/\\|?*]', "_", ascii_value)
    ascii_value = re.sub(r"\s+", " ", ascii_value).strip(" .")
    return ascii_value or "Highlights"


def _slug_token(value: str, *, fallback: str, limit: int = 48) -> str:
    cleaned = _sanitize_drive_name(value).lower()
    cleaned = re.sub(r"[^a-z0-9]+", "-", cleaned).strip("-")
    if not cleaned:
        cleaned = fallback
    return cleaned[:limit].strip("-") or fallback


def _time_token(value: str) -> str:
    token = str(value or "0:00").strip().replace(":", "-")
    token = re.sub(r"[^0-9-]+", "", token)
    return token or "0-00"


def _display_time_token(value: str) -> str:
    token = _time_token(value)
    parts = token.split("-", 1)
    if len(parts) != 2:
        return token
    minutes, seconds = parts
    return f"{int(minutes):02d}-{seconds.zfill(2)}"


def _person_label(value: str, *, fallback: str) -> str:
    safe = _sanitize_drive_name(value)
    parts = [part for part in re.split(r"\s+", safe) if part]
    if not parts:
        return fallback
    return parts[-1][:24]


def _clip_entry_local_path(game_dir: Path, entry: Dict[str, Any]) -> Optional[Path]:
    relpath = str(entry.get("path") or entry.get("clip_relpath") or "").strip()
    if relpath:
        candidate = game_dir / relpath
        if candidate.exists():
            return candidate

    filename = str(entry.get("clip_filename") or "").strip()
    if filename:
        candidate = game_dir / "clips" / filename
        if candidate.exists():
            return candidate

    return None


def build_review_clip_filename(entry: Dict[str, Any], *, index: int) -> str:
    event_type = str(entry.get("type") or "event").strip().lower()
    period = int(entry.get("period") or 0)
    time_token = _display_time_token(str(entry.get("time") or "0:00"))

    if event_type == "goal":
        scorer = _person_label(str(entry.get("scorer") or entry.get("player") or "Unknown"), fallback="Scorer")
        parts = [
            f"{int(index):02d}",
            f"P{period}",
            time_token,
            scorer,
        ]
        assist1 = str(entry.get("assist1") or "").strip()
        assist2 = str(entry.get("assist2") or "").strip()
        special = str(entry.get("special") or "").strip().upper()
        if assist1:
            parts.append(f"A1 {_person_label(assist1, fallback='Assist1')}")
        if assist2:
            parts.append(f"A2 {_person_label(assist2, fallback='Assist2')}")
        if special:
            parts.append(special)
        return " - ".join(parts) + ".mp4"

    player = entry.get("player")
    if isinstance(player, dict):
        player = player.get("name")
    player_name = _person_label(str(player or "Unknown"), fallback="Player")
    infraction = _sanitize_drive_name(str(entry.get("infraction") or event_type or "Event"))
    return " - ".join(
        [
            f"{int(index):02d}",
            event_type.title() or "Event",
            f"P{period}",
            time_token,
            player_name,
            infraction,
        ]
    ) + ".mp4"


def _upload_review_goal_clips(
    service,
    *,
    game_dir: Path,
    goal_review_parent_id: str,
    drive_id: str,
) -> int:
    manifest_path = game_dir / "data" / "clips_manifest.json"
    if not manifest_path.exists():
        return 0

    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return 0

    entries = payload.get("clips") if isinstance(payload, dict) else payload
    if not isinstance(entries, list):
        return 0

    uploaded = 0
    desired_names: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("type") or "").strip().lower() != "goal":
            continue
        clip_path = _clip_entry_local_path(game_dir, entry)
        if clip_path is None:
            continue
        remote_name = build_review_clip_filename(entry, index=int(entry.get("index") or uploaded + 1))
        desired_names.add(remote_name)
        upsert_file(
            service,
            local_path=clip_path,
            parent_id=goal_review_parent_id,
            drive_id=drive_id,
            remote_name=remote_name,
        )
        uploaded += 1

    for item in list_child_files(service, parent_id=goal_review_parent_id, drive_id=drive_id):
        mime_type = str(item.get("mimeType") or "")
        if mime_type == "application/vnd.google-apps.folder":
            continue
        name = str(item.get("name") or "")
        file_id = str(item.get("id") or "")
        if file_id and name not in desired_names:
            trash_file(service, file_id=file_id)

    return uploaded


def _trash_duplicate_raw_clips(service, *, clips_parent_id: str, drive_id: str) -> None:
    for item in list_child_files(service, parent_id=clips_parent_id, drive_id=drive_id):
        mime_type = str(item.get("mimeType") or "")
        if mime_type == "application/vnd.google-apps.folder":
            continue
        file_id = str(item.get("id") or "")
        if file_id:
            trash_file(service, file_id=file_id)


def _load_game_metadata(game_dir: Path) -> Dict[str, Any]:
    metadata_path = game_dir / "data" / "game_metadata.json"
    if not metadata_path.exists():
        return {}
    try:
        return json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def build_game_drive_folder_name(game_dir: Path, *, canonical_game_info: Optional[Dict[str, Any]] = None) -> str:
    info = dict(canonical_game_info or {})
    if not info:
        metadata = _load_game_metadata(game_dir)
        info = dict((metadata.get("game_info") or {}) if isinstance(metadata, dict) else {})
    date = str(info.get("date") or "").strip()
    home = str(info.get("home_team") or "").strip()
    away = str(info.get("away_team") or "").strip()
    if date and home and away:
        return _sanitize_drive_name(f"{date} - {home} vs {away}")
    return _sanitize_drive_name(game_dir.name)


def build_source_info_payload(
    *,
    source_video: Path,
    canonical_game_info: Dict[str, Any],
    game: Dict[str, Any],
) -> Dict[str, Any]:
    source_video = Path(source_video).expanduser().resolve()
    stat = source_video.stat()
    opponent = game.get("opponent") if isinstance(game.get("opponent"), dict) else {}
    return {
        "source": {
            "local_path": str(source_video),
            "filename": source_video.name,
            "size_bytes": int(stat.st_size),
            "mtime_unix": float(stat.st_mtime),
            "archive_mode": "local_source_sync",
        },
        "game": {
            "game_id": str(game.get("game_id") or "").strip(),
            "date": str(canonical_game_info.get("date") or "").strip(),
            "home_team": str(canonical_game_info.get("home_team") or "").strip(),
            "away_team": str(canonical_game_info.get("away_team") or "").strip(),
            "league": str(canonical_game_info.get("league") or "").strip(),
            "home_game": bool(game.get("home_game")),
            "opponent": str(opponent.get("team_name") or "").strip(),
            "venue": str(game.get("venue") or "").strip(),
            "attendance": game.get("attendance"),
            "schedule_notes": str(game.get("schedule_notes") or "").strip(),
        },
        "drive_archive": {},
    }


def build_archive_status_payload(
    *,
    game_folder_id: str,
    source_folder_id: str,
    source_file_id: str,
    source_file_name: str,
    goal_review_folder_id: str,
    goal_review_folder_url: str,
    goal_review_uploaded: int,
) -> Dict[str, Any]:
    return {
        "game_folder_url": f"https://drive.google.com/drive/folders/{game_folder_id}",
        "game_folder_id": game_folder_id,
        "source_folder_id": source_folder_id,
        "source_file_id": source_file_id,
        "source_file_name": source_file_name,
        "goal_review_folder_id": goal_review_folder_id,
        "goal_review_folder_url": goal_review_folder_url,
        "goal_review_uploaded": int(goal_review_uploaded),
        "archive_complete": bool(source_file_id),
    }


def _resolve_games_root(service, *, drive_id: str, configured_folder_id: str, configured_folder_path: str) -> str:
    if str(configured_folder_id or "").strip():
        return str(configured_folder_id).strip()
    if str(configured_folder_path or "").strip():
        return resolve_folder_path(service, drive_id=drive_id, folder_path=configured_folder_path)
    raise RuntimeError("HIGHLIGHTS_GAMES_FOLDER_ID or HIGHLIGHTS_GAMES_FOLDER_PATH must be configured")


def sync_local_game_archive_to_drive(
    *,
    game_dir: Path,
    source_video: Path,
    canonical_game_info: Dict[str, Any],
    game: Dict[str, Any],
    remote_source_name: str = "",
) -> str:
    game_dir = Path(game_dir).expanduser().resolve()
    source_video = Path(source_video).expanduser().resolve()
    if not game_dir.exists():
        raise FileNotFoundError(game_dir)
    if not source_video.exists():
        raise FileNotFoundError(source_video)

    drive_cfg = resolve_drive_config()
    drive_id = str(drive_cfg.drive_id or "").strip()
    if not drive_id:
        raise RuntimeError("HIGHLIGHTS_DRIVE_ID is not configured")
    credentials_path = str(drive_cfg.credentials_path or "").strip()
    if not credentials_path:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS is not configured")

    service = get_drive_service(credentials_path)
    games_root_id = _resolve_games_root(
        service,
        drive_id=drive_id,
        configured_folder_id=str(drive_cfg.games_folder_id or "").strip(),
        configured_folder_path=str(drive_cfg.games_folder_path or "").strip(),
    )

    folder_name = build_game_drive_folder_name(game_dir, canonical_game_info=canonical_game_info)
    game_folder_id = ensure_folder(service, parent_id=games_root_id, name=folder_name, drive_id=drive_id)
    data_id = ensure_folder(service, parent_id=game_folder_id, name="data", drive_id=drive_id)
    clips_id = ensure_folder(service, parent_id=game_folder_id, name="clips", drive_id=drive_id)
    goal_review_id = ensure_folder(service, parent_id=clips_id, name="goal_review", drive_id=drive_id)
    output_id = ensure_folder(service, parent_id=game_folder_id, name="output", drive_id=drive_id)
    logs_id = ensure_folder(service, parent_id=game_folder_id, name="logs", drive_id=drive_id)
    source_id = ensure_folder(service, parent_id=game_folder_id, name="source", drive_id=drive_id)

    goal_review_uploaded = _upload_review_goal_clips(
        service,
        game_dir=game_dir,
        goal_review_parent_id=goal_review_id,
        drive_id=drive_id,
    )

    source_dir = game_dir / "source"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_info_path = source_dir / "source_info.json"
    payload = build_source_info_payload(
        source_video=source_video,
        canonical_game_info=canonical_game_info,
        game=game,
    )
    payload["drive_archive"].update(
        {
            "drive_id": drive_id,
            "games_root_folder_id": games_root_id,
            "game_folder_id": game_folder_id,
            "game_folder_url": f"https://drive.google.com/drive/folders/{game_folder_id}",
            "source_folder_id": source_id,
            "goal_review_folder_id": goal_review_id,
            "goal_review_folder_url": f"https://drive.google.com/drive/folders/{goal_review_id}",
            "goal_review_uploaded": goal_review_uploaded,
        }
    )
    source_info_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    upload_tree(service, src_dir=source_dir, dst_parent_id=source_id, drive_id=drive_id)

    remote_source_name = str(remote_source_name or source_video.name).strip() or source_video.name
    archive_status_path = game_dir / "output" / "archive_sync.json"
    archive_status_path.parent.mkdir(parents=True, exist_ok=True)
    archive_status = build_archive_status_payload(
        game_folder_id=game_folder_id,
        source_folder_id=source_id,
        source_file_id="",
        source_file_name=remote_source_name,
        goal_review_folder_id=goal_review_id,
        goal_review_folder_url=payload["drive_archive"]["goal_review_folder_url"],
        goal_review_uploaded=goal_review_uploaded,
    )
    archive_status_path.write_text(json.dumps(archive_status, indent=2) + "\n", encoding="utf-8")
    upsert_file(service, local_path=archive_status_path, parent_id=output_id, drive_id=drive_id)

    upload_tree(service, src_dir=game_dir / "data", dst_parent_id=data_id, drive_id=drive_id)
    upload_tree(service, src_dir=game_dir / "output", dst_parent_id=output_id, drive_id=drive_id)
    upload_tree(service, src_dir=game_dir / "logs", dst_parent_id=logs_id, drive_id=drive_id)
    _trash_duplicate_raw_clips(service, clips_parent_id=clips_id, drive_id=drive_id)

    source_file_id = upsert_file(
        service,
        local_path=source_video,
        parent_id=source_id,
        drive_id=drive_id,
        remote_name=remote_source_name,
    )

    payload["drive_archive"]["source_file_id"] = source_file_id
    payload["drive_archive"]["source_file_name"] = remote_source_name
    source_info_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    upsert_file(service, local_path=source_info_path, parent_id=source_id, drive_id=drive_id)

    archive_status = build_archive_status_payload(
        game_folder_id=game_folder_id,
        source_folder_id=source_id,
        source_file_id=source_file_id,
        source_file_name=payload["drive_archive"]["source_file_name"],
        goal_review_folder_id=goal_review_id,
        goal_review_folder_url=payload["drive_archive"]["goal_review_folder_url"],
        goal_review_uploaded=goal_review_uploaded,
    )
    archive_status_path.write_text(json.dumps(archive_status, indent=2) + "\n", encoding="utf-8")
    upsert_file(service, local_path=archive_status_path, parent_id=output_id, drive_id=drive_id)

    return str(payload["drive_archive"]["game_folder_url"])
