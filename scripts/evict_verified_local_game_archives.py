#!/usr/bin/env python3
"""Manifest and optionally evict local game sources already verified in Drive.

This is intentionally separate from archive upload.  A source is eligible only
when a completed archive receipt names its Drive object and the Drive API
reports the same name, byte size, and MD5 checksum as the local file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from drive_api import get_drive_service  # noqa: E402
from drive_config import resolve_drive_config  # noqa: E402


def _hashes(path: Path) -> tuple[str, str]:
    md5 = hashlib.md5(usedforsecurity=False)
    sha256 = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            md5.update(chunk)
            sha256.update(chunk)
    return md5.hexdigest(), sha256.hexdigest()


def _drive_metadata(service: Any, file_id: str) -> dict[str, Any]:
    return (
        service.files()
        .get(
            fileId=file_id,
            fields="id,name,size,md5Checksum,trashed,parents,modifiedTime",
            supportsAllDrives=True,
        )
        .execute()
    )


def _atomic_write_new(path: Path, payload: dict[str, Any]) -> None:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"refusing to replace existing manifest: {path}")
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _candidate_records(input_dir: Path, games_dir: Path, service: Any) -> list[dict[str, Any]]:
    receipts = sorted(games_dir.glob("*/output/archive_sync.json"))
    records: list[dict[str, Any]] = []
    seen_sources: set[Path] = set()
    for receipt_path in receipts:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if receipt.get("archive_complete") is not True:
            continue
        source_name = str(receipt.get("source_file_name") or "").strip()
        source_file_id = str(receipt.get("source_file_id") or "").strip()
        if not source_name or not source_file_id:
            raise RuntimeError(f"incomplete archive receipt: {receipt_path}")
        source_path = (input_dir / source_name).resolve()
        if source_path.parent != input_dir.resolve():
            raise RuntimeError(f"receipt source escapes the exact input directory: {source_path}")
        if not source_path.exists():
            continue
        if source_path in seen_sources:
            raise RuntimeError(f"duplicate source across receipts: {source_path}")
        seen_sources.add(source_path)
        if not source_path.is_file():
            raise RuntimeError(f"receipt source is not an exact local file: {source_path}")

        stat = source_path.stat()
        local_md5, local_sha256 = _hashes(source_path)
        remote = _drive_metadata(service, source_file_id)
        checks = {
            "not_trashed": remote.get("trashed") is not True,
            "name_equal": str(remote.get("name") or "") == source_name,
            "size_equal": int(remote.get("size") or -1) == stat.st_size,
            "md5_equal": str(remote.get("md5Checksum") or "").lower() == local_md5,
        }
        if not all(checks.values()):
            raise RuntimeError(
                f"Drive verification failed for {source_path}: "
                + json.dumps(checks, sort_keys=True)
            )
        records.append(
            {
                "decision": "verified-eligible",
                "local": {
                    "path": str(source_path),
                    "device": stat.st_dev,
                    "inode": stat.st_ino,
                    "size_bytes": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns,
                    "md5": local_md5,
                    "sha256": local_sha256,
                },
                "receipt": {
                    "path": str(receipt_path.resolve()),
                    "sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
                },
                "remote": {
                    "file_id": source_file_id,
                    "name": remote.get("name"),
                    "size_bytes": int(remote.get("size") or 0),
                    "md5": remote.get("md5Checksum"),
                    "modified_time": remote.get("modifiedTime"),
                    "parents": remote.get("parents") or [],
                },
                "verification": checks,
            }
        )
    return records


def _apply(records: list[dict[str, Any]], service: Any) -> None:
    for record in records:
        local = record["local"]
        path = Path(local["path"])
        stat = path.stat()
        identity = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
        expected = (
            local["device"],
            local["inode"],
            local["size_bytes"],
            local["mtime_ns"],
        )
        if identity != expected:
            raise RuntimeError(f"local source changed after manifest: {path}")
        remote = _drive_metadata(service, record["remote"]["file_id"])
        if (
            remote.get("trashed") is True
            or str(remote.get("name") or "") != path.name
            or int(remote.get("size") or -1) != stat.st_size
            or str(remote.get("md5Checksum") or "").lower() != local["md5"]
        ):
            raise RuntimeError(f"Drive object changed after manifest: {path}")
        path.unlink()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--games-dir", type=Path, default=REPO_ROOT / "Games")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    config = resolve_drive_config()
    if not config.credentials_path:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS is required")
    service = get_drive_service(config.credentials_path)
    records = _candidate_records(args.input_dir, args.games_dir, service)
    if not records:
        raise RuntimeError("no completed archive receipts resolved to local sources")
    manifest = {
        "schema": "ca.clarence.amherst-local-archive-eviction/v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "apply_requested": bool(args.apply),
        "record_count": len(records),
        "total_size_bytes": sum(item["local"]["size_bytes"] for item in records),
        "records": records,
    }
    _atomic_write_new(args.manifest, manifest)
    if args.apply:
        _apply(records, service)
    print(
        json.dumps(
            {
                "manifest": str(args.manifest.expanduser().resolve()),
                "record_count": len(records),
                "total_size_bytes": manifest["total_size_bytes"],
                "applied": bool(args.apply),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
