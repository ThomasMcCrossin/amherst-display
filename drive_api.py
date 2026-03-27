"""
Small Google Drive API helpers shared by local highlight scripts.

These helpers are intentionally narrow:
- authenticate with a service account
- resolve/create folders within a Shared Drive
- upsert files by name inside a parent folder
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional


def get_drive_service(credentials_path: str):
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    credentials = service_account.Credentials.from_service_account_file(
        str(credentials_path),
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=credentials)


def list_child_folders(service, *, parent_id: str, drive_id: str) -> list[dict[str, Any]]:
    params: Dict[str, Any] = {
        "q": (
            f"'{parent_id}' in parents and trashed=false and "
            "mimeType='application/vnd.google-apps.folder'"
        ),
        "fields": "files(id,name)",
        "pageSize": 1000,
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
    }
    if drive_id:
        params["corpora"] = "drive"
        params["driveId"] = drive_id
    result = service.files().list(**params).execute()
    return list(result.get("files", []) or [])


def list_child_files(service, *, parent_id: str, drive_id: str) -> list[dict[str, Any]]:
    params: Dict[str, Any] = {
        "q": f"'{parent_id}' in parents and trashed=false",
        "fields": "files(id,name,mimeType)",
        "pageSize": 1000,
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
    }
    if drive_id:
        params["corpora"] = "drive"
        params["driveId"] = drive_id
    result = service.files().list(**params).execute()
    return list(result.get("files", []) or [])


def find_child_folder(service, *, parent_id: str, name: str, drive_id: str) -> str:
    target = str(name or "").strip().lower()
    if not target:
        return ""
    for item in list_child_folders(service, parent_id=parent_id, drive_id=drive_id):
        if str(item.get("name") or "").strip().lower() == target:
            return str(item.get("id") or "")
    return ""


def ensure_folder(service, *, parent_id: str, name: str, drive_id: str) -> str:
    existing = find_child_folder(service, parent_id=parent_id, name=name, drive_id=drive_id)
    if existing:
        return existing
    body: Dict[str, Any] = {
        "name": str(name),
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    created = (
        service.files()
        .create(body=body, fields="id", supportsAllDrives=True)
        .execute()
    )
    return str(created.get("id") or "")


def resolve_folder_path(service, *, drive_id: str, folder_path: str) -> str:
    current_parent = str(drive_id or "root").strip()
    for segment in [part for part in str(folder_path or "").strip("/").split("/") if part]:
        current_parent = ensure_folder(service, parent_id=current_parent, name=segment, drive_id=drive_id)
    return current_parent


def trash_file(service, *, file_id: str) -> None:
    service.files().update(
        fileId=str(file_id),
        body={"trashed": True},
        supportsAllDrives=True,
    ).execute()


def upsert_file(service, *, local_path: Path, parent_id: str, drive_id: str, remote_name: Optional[str] = None) -> str:
    from googleapiclient.http import MediaFileUpload

    local_path = Path(local_path).expanduser().resolve()
    if not local_path.exists():
        raise FileNotFoundError(local_path)

    remote_name = str(remote_name or local_path.name)
    safe_name = remote_name.replace("'", "\\'")
    query = f"'{parent_id}' in parents and trashed=false and name='{safe_name}'"
    params: Dict[str, Any] = {
        "q": query,
        "fields": "files(id,name)",
        "pageSize": 10,
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
    }
    if drive_id:
        params["corpora"] = "drive"
        params["driveId"] = drive_id

    existing = service.files().list(**params).execute().get("files", []) or []
    media = MediaFileUpload(str(local_path), resumable=False)
    if existing:
        file_id = str(existing[0].get("id") or "")
        service.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True,
        ).execute()
        return file_id

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


def upload_tree(service, *, src_dir: Path, dst_parent_id: str, drive_id: str) -> None:
    src_dir = Path(src_dir).expanduser().resolve()
    if not src_dir.exists() or not src_dir.is_dir():
        return

    for child in sorted(src_dir.iterdir(), key=lambda item: item.name.lower()):
        if child.name.startswith("."):
            continue
        if child.is_dir():
            subfolder_id = ensure_folder(service, parent_id=dst_parent_id, name=child.name, drive_id=drive_id)
            upload_tree(service, src_dir=child, dst_parent_id=subfolder_id, drive_id=drive_id)
            continue
        if child.is_file():
            upsert_file(service, local_path=child, parent_id=dst_parent_id, drive_id=drive_id)
