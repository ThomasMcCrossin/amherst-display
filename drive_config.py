"""
Generic Google Drive configuration helpers for the hockey highlight pipeline.

This module centralizes:
- backward-compatible env alias resolution
- canonical shared-drive folder layout generation
- env/manifest rendering for local bootstrap workflows
"""

from __future__ import annotations

import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional


ENV_ALIASES: Dict[str, tuple[str, ...]] = {
    "HIGHLIGHTS_DRIVE_ID": ("RAMBLERS_DRIVE_ID",),
    "HIGHLIGHTS_INGEST_FOLDER_ID": ("DRIVE_INGEST_FOLDER_ID",),
    "HIGHLIGHTS_INGEST_FOLDER_PATH": ("DRIVE_INGEST_FOLDER_PATH",),
    "HIGHLIGHTS_GAMES_FOLDER_ID": ("DRIVE_GAMES_FOLDER_ID",),
    "HIGHLIGHTS_GAMES_FOLDER_PATH": ("DRIVE_GAMES_FOLDER_PATH",),
    "HIGHLIGHTS_REELS_FOLDER_ID": ("DRIVE_HIGHLIGHTS_FOLDER_ID",),
    "HIGHLIGHTS_REELS_FOLDER_PATH": ("DRIVE_HIGHLIGHTS_FOLDER_PATH",),
    "HIGHLIGHTS_MAJOR_REVIEW_FOLDER_ID": ("MAJOR_REVIEW_DRIVE_FOLDER_ID",),
    "HIGHLIGHTS_MAJOR_REVIEW_FOLDER_PATH": ("MAJOR_REVIEW_DRIVE_FOLDER_PATH",),
    "HIGHLIGHTS_REFERENCE_FOLDER_ID": (),
    "HIGHLIGHTS_REFERENCE_FOLDER_PATH": (),
}

DEFAULT_PROGRAM_ROOT = "Programs"


def normalize_drive_folder_id(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", raw)
    return match.group(1) if match else raw


def normalize_drive_id(value: str) -> str:
    return normalize_drive_folder_id(value)


def _slugify_env_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned.upper()


def _first_env(name: str, env: Optional[Dict[str, str]] = None) -> str:
    source = env or os.environ
    for candidate in (name, *ENV_ALIASES.get(name, ())):
        value = str(source.get(candidate, "") or "").strip()
        if value:
            return value
    return ""


def default_state_env_path() -> Path:
    state_root = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
    return state_root / "amherst-display" / "highlight-drive.env"


@dataclass(frozen=True)
class ProgramDriveLayout:
    league: str
    team: str
    season: str
    root_folder: str
    program_root_path: str
    ingest_root_path: str
    ingest_inbox_path: str
    games_root_path: str
    reels_root_path: str
    reels_games_path: str
    reels_series_path: str
    reels_players_path: str
    reels_special_projects_path: str
    review_root_path: str
    major_review_root_path: str
    major_review_incoming_path: str
    major_review_approved_path: str
    reference_root_path: str
    ocr_samples_path: str
    ocr_notes_path: str
    run_manifests_path: str

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class ResolvedDriveConfig:
    drive_id: str = ""
    ingest_folder_id: str = ""
    ingest_folder_path: str = ""
    games_folder_id: str = ""
    games_folder_path: str = ""
    reels_folder_id: str = ""
    reels_folder_path: str = ""
    major_review_folder_id: str = ""
    major_review_folder_path: str = ""
    reference_folder_id: str = ""
    reference_folder_path: str = ""
    credentials_path: str = ""

    def to_env_dict(self, *, include_legacy: bool = True) -> Dict[str, str]:
        env = {
            "GOOGLE_APPLICATION_CREDENTIALS": self.credentials_path,
            "HIGHLIGHTS_DRIVE_ID": self.drive_id,
            "HIGHLIGHTS_INGEST_FOLDER_ID": self.ingest_folder_id,
            "HIGHLIGHTS_INGEST_FOLDER_PATH": self.ingest_folder_path,
            "HIGHLIGHTS_GAMES_FOLDER_ID": self.games_folder_id,
            "HIGHLIGHTS_GAMES_FOLDER_PATH": self.games_folder_path,
            "HIGHLIGHTS_REELS_FOLDER_ID": self.reels_folder_id,
            "HIGHLIGHTS_REELS_FOLDER_PATH": self.reels_folder_path,
            "HIGHLIGHTS_MAJOR_REVIEW_FOLDER_ID": self.major_review_folder_id,
            "HIGHLIGHTS_MAJOR_REVIEW_FOLDER_PATH": self.major_review_folder_path,
            "HIGHLIGHTS_REFERENCE_FOLDER_ID": self.reference_folder_id,
            "HIGHLIGHTS_REFERENCE_FOLDER_PATH": self.reference_folder_path,
        }
        if include_legacy:
            env.update(
                {
                    "RAMBLERS_DRIVE_ID": self.drive_id,
                    "DRIVE_INGEST_FOLDER_ID": self.ingest_folder_id,
                    "DRIVE_INGEST_FOLDER_PATH": self.ingest_folder_path,
                    "DRIVE_GAMES_FOLDER_ID": self.games_folder_id,
                    "DRIVE_GAMES_FOLDER_PATH": self.games_folder_path,
                    "DRIVE_HIGHLIGHTS_FOLDER_ID": self.reels_folder_id,
                    "DRIVE_HIGHLIGHTS_FOLDER_PATH": self.reels_folder_path,
                    "MAJOR_REVIEW_DRIVE_FOLDER_ID": self.major_review_folder_id,
                    "MAJOR_REVIEW_DRIVE_FOLDER_PATH": self.major_review_folder_path,
                }
            )
        return {key: value for key, value in env.items() if str(value or "").strip()}


def build_program_drive_layout(
    *,
    league: str,
    team: str,
    season: str,
    root_folder: str = DEFAULT_PROGRAM_ROOT,
) -> ProgramDriveLayout:
    program_root = "/".join([root_folder.strip("/"), str(league or "").strip(), str(team or "").strip(), str(season or "").strip()])
    ingest_root = f"{program_root}/01_Ingest"
    reels_root = f"{program_root}/03_Reels"
    review_root = f"{program_root}/04_Review"
    major_root = f"{review_root}/Major Penalties"
    reference_root = f"{program_root}/05_Reference"
    return ProgramDriveLayout(
        league=str(league or "").strip(),
        team=str(team or "").strip(),
        season=str(season or "").strip(),
        root_folder=str(root_folder or DEFAULT_PROGRAM_ROOT).strip("/"),
        program_root_path=program_root,
        ingest_root_path=ingest_root,
        ingest_inbox_path=f"{ingest_root}/Inbox",
        games_root_path=f"{program_root}/02_Games",
        reels_root_path=reels_root,
        reels_games_path=f"{reels_root}/Games",
        reels_series_path=f"{reels_root}/Series",
        reels_players_path=f"{reels_root}/Players",
        reels_special_projects_path=f"{reels_root}/Special Projects",
        review_root_path=review_root,
        major_review_root_path=major_root,
        major_review_incoming_path=f"{major_root}/Incoming",
        major_review_approved_path=f"{major_root}/Approved",
        reference_root_path=reference_root,
        ocr_samples_path=f"{reference_root}/OCR Samples",
        ocr_notes_path=f"{reference_root}/OCR Notes",
        run_manifests_path=f"{reference_root}/Run Manifests",
    )


def resolve_drive_config(env: Optional[Dict[str, str]] = None) -> ResolvedDriveConfig:
    source = env or os.environ
    return ResolvedDriveConfig(
        drive_id=normalize_drive_id(_first_env("HIGHLIGHTS_DRIVE_ID", source)),
        ingest_folder_id=normalize_drive_folder_id(_first_env("HIGHLIGHTS_INGEST_FOLDER_ID", source)),
        ingest_folder_path=_first_env("HIGHLIGHTS_INGEST_FOLDER_PATH", source),
        games_folder_id=normalize_drive_folder_id(_first_env("HIGHLIGHTS_GAMES_FOLDER_ID", source)),
        games_folder_path=_first_env("HIGHLIGHTS_GAMES_FOLDER_PATH", source),
        reels_folder_id=normalize_drive_folder_id(_first_env("HIGHLIGHTS_REELS_FOLDER_ID", source)),
        reels_folder_path=_first_env("HIGHLIGHTS_REELS_FOLDER_PATH", source),
        major_review_folder_id=normalize_drive_folder_id(_first_env("HIGHLIGHTS_MAJOR_REVIEW_FOLDER_ID", source)),
        major_review_folder_path=_first_env("HIGHLIGHTS_MAJOR_REVIEW_FOLDER_PATH", source),
        reference_folder_id=normalize_drive_folder_id(_first_env("HIGHLIGHTS_REFERENCE_FOLDER_ID", source)),
        reference_folder_path=_first_env("HIGHLIGHTS_REFERENCE_FOLDER_PATH", source),
        credentials_path=str(source.get("GOOGLE_APPLICATION_CREDENTIALS", "") or "").strip(),
    )


def build_bootstrap_env(
    *,
    layout: ProgramDriveLayout,
    folder_ids: Dict[str, str],
    drive_id: str,
    credentials_path: str,
) -> ResolvedDriveConfig:
    return ResolvedDriveConfig(
        drive_id=normalize_drive_id(drive_id),
        ingest_folder_id=normalize_drive_folder_id(folder_ids.get("ingest_inbox_path", "")),
        ingest_folder_path=layout.ingest_inbox_path,
        games_folder_id=normalize_drive_folder_id(folder_ids.get("games_root_path", "")),
        games_folder_path=layout.games_root_path,
        reels_folder_id=normalize_drive_folder_id(folder_ids.get("reels_games_path", "")),
        reels_folder_path=layout.reels_games_path,
        major_review_folder_id=normalize_drive_folder_id(folder_ids.get("major_review_incoming_path", "")),
        major_review_folder_path=layout.major_review_incoming_path,
        reference_folder_id=normalize_drive_folder_id(folder_ids.get("reference_root_path", "")),
        reference_folder_path=layout.reference_root_path,
        credentials_path=str(credentials_path or "").strip(),
    )


def render_env_lines(
    resolved: ResolvedDriveConfig,
    *,
    include_legacy: bool = True,
    extra: Optional[Dict[str, str]] = None,
) -> str:
    payload = resolved.to_env_dict(include_legacy=include_legacy)
    for key, value in (extra or {}).items():
        if str(value or "").strip():
            payload[key] = str(value)
    lines = ["# Generated by scripts/setup_highlight_drive.py"]
    for key in sorted(payload):
        value = str(payload[key]).replace("\n", " ").strip()
        lines.append(f'{key}="{value}"')
    lines.append("")
    return "\n".join(lines)


def program_env_metadata(*, league: str, team: str, season: str, layout: ProgramDriveLayout) -> Dict[str, str]:
    return {
        "HIGHLIGHTS_PROGRAM_LEAGUE": str(league or "").strip(),
        "HIGHLIGHTS_PROGRAM_TEAM": str(team or "").strip(),
        "HIGHLIGHTS_PROGRAM_SEASON": str(season or "").strip(),
        "HIGHLIGHTS_PROGRAM_SLUG": _slugify_env_name(f"{league}_{team}_{season}").lower(),
        "HIGHLIGHTS_PROGRAM_ROOT_PATH": layout.program_root_path,
    }


def iter_layout_paths(layout: ProgramDriveLayout) -> Iterable[tuple[str, str]]:
    ordered_keys = [
        "program_root_path",
        "ingest_root_path",
        "ingest_inbox_path",
        "games_root_path",
        "reels_root_path",
        "reels_games_path",
        "reels_series_path",
        "reels_players_path",
        "reels_special_projects_path",
        "review_root_path",
        "major_review_root_path",
        "major_review_incoming_path",
        "major_review_approved_path",
        "reference_root_path",
        "ocr_samples_path",
        "ocr_notes_path",
        "run_manifests_path",
    ]
    data = layout.to_dict()
    for key in ordered_keys:
        yield key, str(data[key])
