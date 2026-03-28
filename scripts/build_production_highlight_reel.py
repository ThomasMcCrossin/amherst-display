#!/usr/bin/env python3
"""
Build a production-style stitched highlight reel from per-goal clips.

Features:
- Per-clip lower-third overlay for the first N seconds (scorer, assists, score)
- Cross-dissolve transitions between clips (video + audio)

Designed to be metadata-driven using existing JSON outputs in `Games/<game>/data/`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import tempfile
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from PIL import Image, ImageDraw, ImageFont


REPO_ROOT = Path(__file__).resolve().parents[1]

TEAM_COLOR_PRESETS: Dict[str, Dict[str, str]] = {
    "amherst-ramblers": {"primary": "#19c37d", "secondary": "#0b6d49"},
    "summerside-western-capitals": {"primary": "#cf4859", "secondary": "#7e1827"},
    "yarmouth-mariners": {"primary": "#18b58f", "secondary": "#0f5f52"},
    "truro-bearcats": {"primary": "#6f51d8", "secondary": "#2f2758"},
    "edmundston-blizzard": {"primary": "#4ba4ff", "secondary": "#103d6f"},
}

TEAM_SHORT_LABELS: Dict[str, str] = {
    "amherst-ramblers": "AMH",
    "summerside-western-capitals": "SUM",
    "yarmouth-mariners": "YAR",
    "truro-bearcats": "TRU",
}


@dataclass(frozen=True)
class TeamInfo:
    name: str
    slug: str
    league: str
    logo_path: Path


@dataclass(frozen=True)
class ClipItem:
    index: int
    clip_path: Path
    event: Dict[str, Any]
    game_info: Dict[str, Any] | None = None
    overlay_game_label: str = ""
    overlay_series_title: str = ""
    series_context: Dict[str, Any] | None = None
    segment_kind: str = "clip"

    @property
    def type(self) -> str:
        return str(self.event.get("type") or "").strip().lower()


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _norm_name(value: str) -> str:
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _find_team_info(team_name: str, league: str, teams_db: Dict[str, Any]) -> TeamInfo:
    normalized = _norm_name(team_name)
    league_norm = league.strip().upper()

    for team in teams_db.get("teams", []):
        if team.get("league", "").strip().upper() != league_norm:
            continue
        candidates = [team.get("name", "")] + list(team.get("aliases", []) or [])
        if any(_norm_name(c) == normalized for c in candidates if c):
            slug = team.get("slug") or normalized.replace(" ", "-")
            logo_path = REPO_ROOT / "assets" / "logos" / league.lower() / f"{slug}.png"
            if not logo_path.exists():
                logo_path = REPO_ROOT / "assets" / "logos" / "fallback.png"
            return TeamInfo(name=team.get("name", team_name), slug=slug, league=league_norm, logo_path=logo_path)

    # Fallback: best-effort slug from name
    slug = normalized.replace(" ", "-") or "unknown"
    logo_path = REPO_ROOT / "assets" / "logos" / league.lower() / f"{slug}.png"
    if not logo_path.exists():
        logo_path = REPO_ROOT / "assets" / "logos" / "fallback.png"
    return TeamInfo(name=team_name, slug=slug, league=league_norm, logo_path=logo_path)


def _collect_name_values(obj: Any) -> List[str]:
    values: List[str] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "name" and isinstance(v, str) and v.strip():
                values.append(v)
            values.extend(_collect_name_values(v))
    elif isinstance(obj, list):
        for v in obj:
            values.extend(_collect_name_values(v))
    return values


def _load_roster_names(team_slug: str) -> List[str]:
    roster_path = REPO_ROOT / "rosters" / f"{team_slug}.json"
    if not roster_path.exists():
        return []
    data = _read_json(roster_path)
    return _collect_name_values(data)


def _infer_scoring_team(
    event: Dict[str, Any],
    *,
    home: TeamInfo,
    away: TeamInfo,
    home_roster: List[str],
    away_roster: List[str],
) -> Optional[TeamInfo]:
    scorer = _norm_name(str(event.get("scorer") or event.get("player") or ""))
    if scorer:
        if any(_norm_name(n) == scorer for n in home_roster):
            return home
        if any(_norm_name(n) == scorer for n in away_roster):
            return away

    team_name = str(event.get("team", "") or "")
    if _norm_name(team_name) == _norm_name(home.name):
        return home
    if _norm_name(team_name) == _norm_name(away.name):
        return away

    return None


def _infer_penalty_team(
    event: Dict[str, Any],
    *,
    home: TeamInfo,
    away: TeamInfo,
    home_roster: List[str],
    away_roster: List[str],
) -> Optional[TeamInfo]:
    player = event.get("player") or {}
    player_name = ""
    if isinstance(player, dict):
        player_name = str(player.get("name") or "")
    else:
        player_name = str(player or "")

    player_norm = _norm_name(player_name)
    if player_norm:
        if any(_norm_name(n) == player_norm for n in home_roster):
            return home
        if any(_norm_name(n) == player_norm for n in away_roster):
            return away

    team_name = _norm_name(str(event.get("team") or ""))
    if team_name:
        # Common pipeline slugs
        if team_name in {"ramblers", "amherst-ramblers", "amherst ramblers"}:
            if "ramblers" in _norm_name(home.name):
                return home
            if "ramblers" in _norm_name(away.name):
                return away
        if team_name in {"opponent", "opp"}:
            if "ramblers" in _norm_name(home.name):
                return away
            if "ramblers" in _norm_name(away.name):
                return home

        if team_name == _norm_name(home.name):
            return home
        if team_name == _norm_name(away.name):
            return away

    return None


def _ffprobe_duration_seconds(video_path: Path) -> float:
    proc = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(video_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return float(proc.stdout.strip())


def _ffprobe_fps_expr(video_path: Path) -> Optional[str]:
    """
    Return the clip's average frame rate as a ffmpeg-friendly expression.

    Examples:
      - "30000/1001"
      - "60/1"
    """
    proc = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=avg_frame_rate",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(video_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    expr = (proc.stdout or "").strip().splitlines()[0] if (proc.stdout or "").strip() else ""
    expr = expr.strip()
    if not expr:
        return None
    # Basic sanitization: allow digits, '/', and '.' only.
    if not re.match(r"^[0-9./]+$", expr):
        return None
    return expr


def _load_font(path: str, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(path, size=size)


def _fit_text_font(draw: ImageDraw.ImageDraw, text: str, font_path: str, max_size: int, min_size: int, max_width: int) -> ImageFont.FreeTypeFont:
    for size in range(max_size, min_size - 1, -1):
        font = _load_font(font_path, size)
        if int(draw.textlength(text, font=font)) <= max_width:
            return font
    return _load_font(font_path, min_size)


def _format_period_label(period: int) -> str:
    if not period or period < 0:
        return ""
    if period <= 3:
        return f"P{period}"
    if period == 4:
        return "OT"
    return f"{period - 3}OT"


def _format_attendance(value: Any) -> str:
    if value in ("", None):
        return ""
    try:
        return f"Attendance {int(value):,}"
    except Exception:
        return str(value).strip()


def _ffprobe_video_size(video_path: Path) -> Tuple[int, int]:
    proc = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height",
            "-of",
            "csv=p=0:s=x",
            str(video_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    raw = (proc.stdout or "").strip().splitlines()
    if not raw:
        return (1920, 1080)
    width_str, height_str = (raw[0].split("x", 1) + ["1080"])[:2]
    try:
        return (int(width_str), int(height_str))
    except Exception:
        return (1920, 1080)


def _design_scale(video_size: Tuple[int, int]) -> float:
    video_w, video_h = video_size
    return min(float(video_w) / 1920.0, float(video_h) / 1080.0)


def _scale_px(video_size: Tuple[int, int], value: int) -> int:
    return max(1, int(round(value * _design_scale(video_size))))


def _team_palette(team: TeamInfo) -> Dict[str, str]:
    palette = TEAM_COLOR_PRESETS.get(team.slug)
    if palette:
        return dict(palette)
    return {"primary": "#4ba4ff", "secondary": "#153656"}


def _team_short_label(team: TeamInfo) -> str:
    if team.slug in TEAM_SHORT_LABELS:
        return TEAM_SHORT_LABELS[team.slug]
    words = [part for part in re.split(r"[^A-Za-z0-9]+", team.name or "") if part]
    if not words:
        return "TEAM"
    if len(words) == 1:
        return words[0][:3].upper()
    return "".join(word[0] for word in words[:3]).upper()


def _series_round_label(series_title: str) -> str:
    title = str(series_title or "").strip()
    if not title:
        return "Playoff Series"
    if " vs " in title:
        return title.split(" vs ", 1)[0].strip()
    return title


def _build_matchup_label(home: TeamInfo, away: TeamInfo) -> str:
    return f"{away.name} vs {home.name}"


def _format_goal_secondary_text(assist1: str, assist2: str) -> str:
    assists = [value.strip() for value in [assist1, assist2] if value and value.strip()]
    if assists:
        return "A: " + ", ".join(assists)
    return "Unassisted"


def _format_goal_meta_text(period: int, time_str: str) -> str:
    parts = [_format_period_label(period), str(time_str or "").strip()]
    return " • ".join([part for part in parts if part])


def _goal_kicker_text(*, game_label: str, clip_type: str, is_power_play: bool, is_short_handed: bool, is_empty_net: bool) -> str:
    tag = "GOAL"
    if clip_type == "penalty":
        tag = "PENALTY"
    elif is_power_play:
        tag = "PP GOAL"
    elif is_short_handed:
        tag = "SH GOAL"
    elif is_empty_net:
        tag = "EN GOAL"
    pieces = [str(game_label or "").strip(), tag]
    return " • ".join([piece for piece in pieces if piece])


def _result_badge_text(result_display: str, *, fallback: str = "Final") -> str:
    normalized = str(result_display or "").upper()
    if "(SO" in normalized or "SO)" in normalized:
        return "Final / SO"
    if "(OT" in normalized or "OT)" in normalized:
        return "Final / OT"
    return fallback


def _render_browser_graphics(jobs: List[Dict[str, Any]]) -> None:
    if not jobs:
        return
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as handle:
        json.dump({"jobs": jobs}, handle, indent=2)
        spec_path = Path(handle.name)
    try:
        subprocess.run(
            [
                "node",
                str(REPO_ROOT / "scripts" / "render_overlay_asset.mjs"),
                "--spec",
                str(spec_path),
                "--output",
                str(jobs[0]["output_path"]),
            ],
            check=True,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
    finally:
        spec_path.unlink(missing_ok=True)


def _render_transparent_overlay_png(out_path: Path, overlay_size: Tuple[int, int]) -> None:
    Image.new("RGBA", overlay_size, (0, 0, 0, 0)).save(out_path, format="PNG")


def _render_game_intro_card_png(
    out_path: Path,
    *,
    video_size: Tuple[int, int],
    home: TeamInfo,
    away: TeamInfo,
    series_title: str,
    total_games: int,
    final_series_status: str,
) -> None:
    open_headline = "Road to Game 7" if str(final_series_status or "").strip().lower() == "series tied 3-3" else "Series Highlights"
    footer_parts = [f"Games 1-{max(1, int(total_games))} highlights"]
    if str(final_series_status or "").strip():
        if open_headline == "Road to Game 7":
            footer_parts.append(f"{str(final_series_status or '').strip()} entering tonight")
        else:
            footer_parts.append(str(final_series_status or "").strip())
    home_palette = _team_palette(home)
    away_palette = _team_palette(away)
    spec = {
        "type": "series_open",
        "width": int(video_size[0]),
        "height": int(video_size[1]),
        "eyebrow": _series_round_label(series_title),
        "headline": open_headline,
        "subheadline": _build_matchup_label(home, away),
        "footer": " • ".join([part for part in footer_parts if part]),
        "badgeText": str(final_series_status or "").strip(),
        "centerBadge": "Winner Take All" if open_headline == "Road to Game 7" else f"Games 1-{max(1, int(total_games))}",
        "homeName": home.name,
        "awayName": away.name,
        "homeLogoPath": str(home.logo_path),
        "awayLogoPath": str(away.logo_path),
        "homePrimary": home_palette["primary"],
        "awayPrimary": away_palette["primary"],
        "accentPrimary": home_palette["primary"],
        "accentSecondary": away_palette["secondary"],
    }
    _render_browser_graphics([{"output_path": str(out_path), "spec": spec}])


def _render_game_break_card_png(
    out_path: Path,
    *,
    video_size: Tuple[int, int],
    home: TeamInfo,
    away: TeamInfo,
    game_label: str,
    headline: str,
    series_status: str,
    home_score: int | None,
    away_score: int | None,
    final_score_display: str,
    venue: str,
    attendance: Any,
    next_game_label: str,
    winner_side: str,
) -> None:
    home_palette = _team_palette(home)
    away_palette = _team_palette(away)
    winner_palette = home_palette if winner_side == "home" else away_palette
    spec = {
        "type": "game_break",
        "width": int(video_size[0]),
        "height": int(video_size[1]),
        "kicker": f"{str(game_label or '').strip()} Final",
        "headline": str(headline or series_status or "Series Update").strip(),
        "seriesStatus": str(series_status or "").strip(),
        "venue": str(venue or "").strip(),
        "attendanceText": _format_attendance(attendance),
        "nextGameLabel": f"Up Next • {str(next_game_label or '').strip()}" if str(next_game_label or "").strip() else "",
        "homeName": home.name,
        "awayName": away.name,
        "homeScore": home_score if home_score is not None else 0,
        "awayScore": away_score if away_score is not None else 0,
        "centerBadge": _result_badge_text(final_score_display),
        "homeLogoPath": str(home.logo_path),
        "awayLogoPath": str(away.logo_path),
        "homePrimary": home_palette["primary"],
        "awayPrimary": away_palette["primary"],
        "accentPrimary": winner_palette["primary"],
        "accentSecondary": winner_palette["secondary"],
        "homeResultClass": "winner" if winner_side == "home" else "",
        "awayResultClass": "winner" if winner_side == "away" else "",
    }
    _render_browser_graphics([{"output_path": str(out_path), "spec": spec}])


def _render_series_outro_card_png(
    out_path: Path,
    *,
    video_size: Tuple[int, int],
    home: TeamInfo,
    away: TeamInfo,
    series_title: str,
    series_status: str,
    next_game_label: str,
    datetime_label: str,
    venue: str,
    location: str,
) -> None:
    home_palette = _team_palette(home)
    away_palette = _team_palette(away)
    spec = {
        "type": "series_outro",
        "width": int(video_size[0]),
        "height": int(video_size[1]),
        "eyebrow": _series_round_label(series_title),
        "headline": str(series_status or "Series Continues").strip(),
        "subheadline": str(next_game_label or "Game 7 Tonight").strip(),
        "datetimeLabel": str(datetime_label or "").strip(),
        "venue": str(venue or "").strip(),
        "location": str(location or "").strip(),
        "badgeText": "Tonight",
        "centerBadge": "Winner Take All",
        "homeName": home.name,
        "awayName": away.name,
        "homeLogoPath": str(home.logo_path),
        "awayLogoPath": str(away.logo_path),
        "homePrimary": home_palette["primary"],
        "awayPrimary": away_palette["primary"],
        "accentPrimary": home_palette["primary"],
        "accentSecondary": away_palette["secondary"],
    }
    _render_browser_graphics([{"output_path": str(out_path), "spec": spec}])


def _render_static_card_video(
    image_path: Path,
    out_path: Path,
    *,
    duration_seconds: float,
    fps: str,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "ffmpeg",
            "-hide_banner",
            "-y",
            "-loop",
            "1",
            "-framerate",
            str(fps),
            "-t",
            f"{float(duration_seconds):.3f}",
            "-i",
            str(image_path),
            "-f",
            "lavfi",
            "-t",
            f"{float(duration_seconds):.3f}",
            "-i",
            "anullsrc=channel_layout=stereo:sample_rate=48000",
            "-shortest",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "18",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            str(out_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def _render_overlay_png(
    out_path: Path,
    *,
    video_size: Tuple[int, int],
    overlay_size: Tuple[int, int],
    home: TeamInfo,
    away: TeamInfo,
    scoring_team: TeamInfo,
    home_score: int,
    away_score: int,
    period: int,
    time_str: str,
    scorer: str,
    assist1: str,
    assist2: str,
    is_power_play: bool,
    is_short_handed: bool,
    is_empty_net: bool,
    special: str = "",
    game_label: str = "",
    series_title: str = "",
) -> None:
    accent = _team_palette(scoring_team)
    special_text = str(special or "").strip()
    badge_text = special_text if "UNVERIFIED" in special_text.upper() else ""
    spec = {
        "type": "goal_overlay",
        "transparent": True,
        "width": int(overlay_size[0]),
        "height": int(overlay_size[1]),
        "kicker": _goal_kicker_text(
            game_label=game_label,
            clip_type="goal",
            is_power_play=is_power_play,
            is_short_handed=is_short_handed,
            is_empty_net=is_empty_net,
        ),
        "hero": str(scorer or "Unknown").strip(),
        "secondaryText": _format_goal_secondary_text(assist1, assist2),
        "metaText": _format_goal_meta_text(period, time_str),
        "badgeText": badge_text,
        "primaryLogoPath": str(scoring_team.logo_path),
        "homeLogoPath": str(home.logo_path),
        "awayLogoPath": str(away.logo_path),
        "homeScore": int(home_score),
        "awayScore": int(away_score),
        "homeShortLabel": _team_short_label(home),
        "awayShortLabel": _team_short_label(away),
        "scoreLabel": "Game State",
        "scoringSide": "home" if _norm_name(scoring_team.name) == _norm_name(home.name) else "away",
        "homePrimary": _team_palette(home)["primary"],
        "awayPrimary": _team_palette(away)["primary"],
        "accentPrimary": accent["primary"],
        "accentSecondary": accent["secondary"],
    }
    _render_browser_graphics([{"output_path": str(out_path), "spec": spec}])


def _render_penalty_overlay_png(
    out_path: Path,
    *,
    video_size: Tuple[int, int],
    overlay_size: Tuple[int, int],
    home: TeamInfo,
    away: TeamInfo,
    penalized_team: TeamInfo,
    home_score: int,
    away_score: int,
    period: int,
    time_str: str,
    player: str,
    infraction: str,
    minutes: int,
    game_label: str = "",
    series_title: str = "",
) -> None:
    accent = {"primary": "#f4b942", "secondary": "#805112"}
    details = f"{str(infraction or 'Penalty').strip()} • {int(minutes or 2)} min"
    spec = {
        "type": "penalty_overlay",
        "transparent": True,
        "width": int(overlay_size[0]),
        "height": int(overlay_size[1]),
        "kicker": _goal_kicker_text(
            game_label=game_label,
            clip_type="penalty",
            is_power_play=False,
            is_short_handed=False,
            is_empty_net=False,
        ),
        "hero": str(player or "Unknown").strip(),
        "secondaryText": details,
        "metaText": _format_goal_meta_text(period, time_str),
        "badgeText": "",
        "primaryLogoPath": str(penalized_team.logo_path),
        "homeLogoPath": str(home.logo_path),
        "awayLogoPath": str(away.logo_path),
        "homeScore": int(home_score),
        "awayScore": int(away_score),
        "homeShortLabel": _team_short_label(home),
        "awayShortLabel": _team_short_label(away),
        "scoreLabel": "Game State",
        "scoringSide": "home" if _norm_name(penalized_team.name) == _norm_name(home.name) else "away",
        "homePrimary": _team_palette(home)["primary"],
        "awayPrimary": _team_palette(away)["primary"],
        "accentPrimary": accent["primary"],
        "accentSecondary": accent["secondary"],
    }
    _render_browser_graphics([{"output_path": str(out_path), "spec": spec}])


def _parse_clip_prefix(path: Path) -> int:
    m = re.match(r"^(\d+)", path.name)
    return int(m.group(1)) if m else 10**9


def _load_clip_items(
    *,
    game_dir: Path,
    clips_dir: Path,
    events_json: Path,
    clips_manifest: Optional[Path],
    major_approved_json: Optional[Path] = None,
) -> List[ClipItem]:
    def _as_list_manifest(value: Any, *, label: Path) -> List[Dict[str, Any]]:
        if isinstance(value, list):
            return [e for e in value if isinstance(e, dict)]
        if isinstance(value, dict):
            clips = value.get("clips")
            if isinstance(clips, list):
                return [e for e in clips if isinstance(e, dict)]
            approved = value.get("approved")
            if isinstance(approved, list):
                return [e for e in approved if isinstance(e, dict)]
        raise ValueError(f"Invalid manifest (expected list or {{'clips': [...]}}): {label}")

    def _entry_event(entry: Dict[str, Any]) -> Dict[str, Any]:
        raw_event = entry.get("event")
        if isinstance(raw_event, dict):
            return raw_event
        # Newer manifest format: entry itself is the event payload (+ path/index).
        return dict(entry)

    def _event_video_time(event: Dict[str, Any]) -> Optional[float]:
        for key in ("video_time", "clip_video_start"):
            if event.get(key) is None:
                continue
            try:
                return float(event.get(key))
            except Exception:
                return None
        return None

    def _resolve_clip_path(entry: Dict[str, Any]) -> Path:
        clip_filename = str(entry.get("clip_filename") or "")
        relpath = str(entry.get("clip_relpath") or "")
        abs_path = str(entry.get("clip_path") or "")
        path_field = str(entry.get("path") or "")

        candidate_paths: List[Path] = []
        if path_field:
            p = Path(path_field)
            candidate_paths.append(p if p.is_absolute() else (game_dir / p))
            if not p.is_absolute():
                candidate_paths.append(clips_dir / p.name)
        if clip_filename:
            candidate_paths.append(clips_dir / clip_filename)
        if relpath:
            candidate_paths.append(game_dir / relpath)
        if abs_path:
            candidate_paths.append(Path(abs_path))

        clip_path = next((p for p in candidate_paths if p.exists()), None)
        if clip_path is None:
            raise FileNotFoundError(
                f"Clip referenced in manifest not found: {path_field or clip_filename or relpath or abs_path}"
            )
        return clip_path

    items: List[ClipItem] = []
    seen_paths: set[Path] = set()

    if clips_manifest and clips_manifest.exists():
        raw = _read_json(clips_manifest)
        entries = _as_list_manifest(raw, label=clips_manifest)

        for i, entry in enumerate(entries, 1):
            clip_path = _resolve_clip_path(entry)
            seen_paths.add(clip_path)
            index = int(entry.get("index") or i)
            items.append(ClipItem(index=index, clip_path=clip_path, event=_entry_event(entry)))

    if major_approved_json and major_approved_json.exists():
        raw = _read_json(major_approved_json)
        entries = _as_list_manifest(raw, label=major_approved_json)
        for i, entry in enumerate(entries, 1):
            clip_path = _resolve_clip_path(entry)
            if clip_path in seen_paths:
                continue
            seen_paths.add(clip_path)
            items.append(ClipItem(index=10_000 + i, clip_path=clip_path, event=_entry_event(entry)))

    if items:
        goal_times: List[Optional[float]] = []
        for it in items:
            if str(it.event.get("type") or "").strip().lower() == "goal":
                goal_times.append(_event_video_time(it.event))

        def _sort_key(it: ClipItem) -> Tuple[float, int, int, str]:
            video_time = _event_video_time(it.event)
            if str(it.event.get("type") or "").strip().lower() == "penalty":
                linked = it.event.get("linked_to_goal")
                if linked is not None:
                    try:
                        goal_idx = int(linked)
                        goal_vt = goal_times[goal_idx]
                        # ALWAYS place linked penalty clips immediately before their goal,
                        # regardless of video_time. The penalty's video_time might be wrong
                        # (matched to wrong period due to similar clock reading), but the
                        # overlay score must be correct (pre-goal score, not post-goal).
                        if goal_vt is not None:
                            video_time = float(goal_vt) - 0.1
                    except Exception:
                        pass
            if video_time is None:
                video_time = float("inf")
            return (
                float(video_time),
                it.index,
                _parse_clip_prefix(it.clip_path),
                it.clip_path.name,
            )

        items.sort(key=_sort_key)
        return items

    # Fallback: infer clip types from filenames and map goal events in order.
    clip_paths = sorted([p for p in clips_dir.glob("*.mp4") if p.is_file()], key=lambda p: (_parse_clip_prefix(p), p.name))
    goal_events: List[Dict[str, Any]] = list(_read_json(events_json))
    goal_events = [e for e in goal_events if e.get("type") == "goal" and e.get("video_time") is not None]
    goal_events.sort(key=lambda e: float(e["video_time"]))

    goal_idx = 0
    items: List[ClipItem] = []
    for i, clip_path in enumerate(clip_paths, 1):
        name = clip_path.name.upper()
        if "_GOAL_" in name:
            if goal_idx >= len(goal_events):
                raise ValueError(f"More GOAL clips than goal events (missing manifest?): {clip_path.name}")
            items.append(ClipItem(index=i, clip_path=clip_path, event=dict(goal_events[goal_idx])))
            goal_idx += 1
            continue

        # Minimal placeholder penalty event
        period = 0
        m = re.search(r"_P(\d+)_", name)
        if m:
            period = int(m.group(1))
        items.append(
            ClipItem(
                index=i,
                clip_path=clip_path,
                event={"type": "penalty", "period": period, "time": "", "player": {"name": ""}, "infraction": "", "minutes": 2},
            )
        )

    return items


def _load_reel_manifest_items(reel_manifest: Path) -> List[ClipItem]:
    raw = _read_json(reel_manifest)
    entries = raw.get("clips") if isinstance(raw, dict) else None
    if not isinstance(entries, list):
        raise ValueError(f"Invalid reel manifest: {reel_manifest}")

    items: List[ClipItem] = []
    for index, entry in enumerate(entries, 1):
        if not isinstance(entry, dict):
            continue
        clip_path = Path(str(entry.get("clip_path") or "")).expanduser().resolve()
        if not clip_path.exists():
            raise FileNotFoundError(f"Missing clip path in reel manifest: {clip_path}")
        event = entry.get("event") if isinstance(entry.get("event"), dict) else {}
        game_info = entry.get("game_info") if isinstance(entry.get("game_info"), dict) else {}
        items.append(
            ClipItem(
                index=int(entry.get("index") or index),
                clip_path=clip_path,
                event=dict(event),
                game_info=dict(game_info),
                overlay_game_label=str(entry.get("game_label") or "").strip(),
                overlay_series_title=str(
                    entry.get("series_title") or (raw.get("title") if isinstance(raw, dict) else "") or ""
                ).strip(),
                series_context=dict(entry.get("series_context") or {}) if isinstance(entry.get("series_context"), dict) else {},
            )
        )

    items.sort(key=lambda item: (item.index, item.clip_path.name))
    return items


def _insert_game_intro_cards(
    items: List[ClipItem],
    *,
    output_dir: Path,
    teams_db: Dict[str, Any],
    video_size: Tuple[int, int],
    fps: str,
    duration_seconds: float,
) -> List[ClipItem]:
    if not items:
        return items

    grouped: List[Tuple[ClipItem, List[ClipItem]]] = []
    current_group: List[ClipItem] = []
    current_key: Optional[Tuple[str, str, str]] = None

    for item in items:
        game_info = dict(item.game_info or {})
        series_context = dict(item.series_context or {})
        game_key = (
            str(series_context.get("game_date") or game_info.get("date") or "").strip(),
            str(item.overlay_game_label or "").strip(),
            str(game_info.get("home_team") or "") + "::" + str(game_info.get("away_team") or ""),
        )
        if current_key is None or game_key == current_key:
            current_group.append(item)
            current_key = game_key
            continue
        grouped.append((current_group[0], list(current_group)))
        current_group = [item]
        current_key = game_key

    if current_group:
        grouped.append((current_group[0], list(current_group)))

    render_items: List[ClipItem] = []
    total_games = len(grouped)
    final_context = dict(grouped[-1][0].series_context or {}) if grouped else {}

    for group_index, (anchor, group_items) in enumerate(grouped, 1):
        game_info = dict(anchor.game_info or {})
        series_context = dict(anchor.series_context or {})
        league = str(game_info.get("league") or "MHL")
        home_name = str(game_info.get("home_team") or "Home")
        away_name = str(game_info.get("away_team") or "Away")
        home = _find_team_info(home_name, league, teams_db)
        away = _find_team_info(away_name, league, teams_db)

        card_png = output_dir / f"game_intro_{group_index:02d}.png"
        card_mp4 = output_dir / f"game_intro_{group_index:02d}.mp4"

        if group_index == 1:
            _render_game_intro_card_png(
                card_png,
                video_size=video_size,
                home=home,
                away=away,
                series_title=str(series_context.get("series_title") or anchor.overlay_series_title or "").strip(),
                total_games=total_games,
                final_series_status=str(final_context.get("series_status_after") or final_context.get("series_status") or "").strip(),
            )
        else:
            previous_anchor, _ = grouped[group_index - 2]
            previous_info = dict(previous_anchor.game_info or {})
            previous_context = dict(previous_anchor.series_context or {})
            previous_home = _find_team_info(str(previous_info.get("home_team") or home_name), league, teams_db)
            previous_away = _find_team_info(str(previous_info.get("away_team") or away_name), league, teams_db)
            previous_home_score = previous_context.get("final_home_score")
            previous_away_score = previous_context.get("final_away_score")
            if previous_home_score is not None or previous_away_score is not None:
                winner_side = "home" if int(previous_home_score or 0) >= int(previous_away_score or 0) else "away"
            else:
                previous_won = previous_context.get("won")
                home_is_amherst = "ramblers" in _norm_name(previous_home.name)
                winner_side = "home"
                if previous_won is True:
                    winner_side = "home" if home_is_amherst else "away"
                elif previous_won is False:
                    winner_side = "away" if home_is_amherst else "home"
            _render_game_break_card_png(
                card_png,
                video_size=video_size,
                home=previous_home,
                away=previous_away,
                game_label=str(previous_context.get("game_label") or previous_anchor.overlay_game_label or "").strip(),
                headline=str(previous_context.get("momentum_headline") or previous_context.get("series_status_after") or "Series Update").strip(),
                series_status=str(previous_context.get("series_status_after") or previous_context.get("series_status") or "").strip(),
                home_score=int(previous_home_score) if previous_home_score is not None else None,
                away_score=int(previous_away_score) if previous_away_score is not None else None,
                final_score_display=str(previous_context.get("final_score_display") or "").strip(),
                venue=str(previous_context.get("venue") or "").strip(),
                attendance=previous_context.get("attendance"),
                next_game_label=str(series_context.get("game_label") or anchor.overlay_game_label or "").strip(),
                winner_side=winner_side,
            )

        _render_static_card_video(card_png, card_mp4, duration_seconds=duration_seconds, fps=fps)
        render_items.append(
            ClipItem(
                index=max(0, int(anchor.index) - 1),
                clip_path=card_mp4,
                event={"type": "game_intro"},
                game_info=game_info,
                overlay_game_label=str(anchor.overlay_game_label or "").strip(),
                overlay_series_title=str(anchor.overlay_series_title or "").strip(),
                series_context=series_context,
                segment_kind="game_intro",
            )
        )
        render_items.extend(group_items)

    return render_items


def _insert_series_outro_card(
    items: List[ClipItem],
    *,
    output_dir: Path,
    teams_db: Dict[str, Any],
    video_size: Tuple[int, int],
    fps: str,
    duration_seconds: float,
    series_status: str,
    next_game_label: str,
    datetime_label: str,
    venue: str,
    location: str,
    home_team_name: str,
    away_team_name: str,
) -> List[ClipItem]:
    if not items:
        return items

    reference = next((item for item in reversed(items) if item.segment_kind == "clip"), items[-1])
    game_info = dict(reference.game_info or {})
    league = str(game_info.get("league") or "MHL")
    home_name = str(home_team_name or game_info.get("home_team") or "Home").strip()
    away_name = str(away_team_name or game_info.get("away_team") or "Away").strip()
    home = _find_team_info(home_name, league, teams_db)
    away = _find_team_info(away_name, league, teams_db)

    card_png = output_dir / "series_outro.png"
    card_mp4 = output_dir / "series_outro.mp4"
    _render_series_outro_card_png(
        card_png,
        video_size=video_size,
        home=home,
        away=away,
        series_title=str(reference.overlay_series_title or "").strip(),
        series_status=str(series_status or "").strip(),
        next_game_label=str(next_game_label or "").strip(),
        datetime_label=str(datetime_label or "").strip(),
        venue=str(venue or "").strip(),
        location=str(location or "").strip(),
    )
    _render_static_card_video(card_png, card_mp4, duration_seconds=duration_seconds, fps=fps)

    outro_item = ClipItem(
        index=int(reference.index) + 1,
        clip_path=card_mp4,
        event={"type": "series_outro"},
        game_info={
            **game_info,
            "home_team": home_name,
            "away_team": away_name,
            "league": league,
        },
        overlay_series_title=str(reference.overlay_series_title or "").strip(),
        segment_kind="series_outro",
    )
    return [*items, outro_item]


def _coerce_score(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _build_ffmpeg_filter(
    *,
    num_clips: int,
    clip_durations: List[float],
    video_size: Tuple[int, int],
    overlay_w: int,
    overlay_h: int,
    overlay_margin: int,
    overlay_seconds: float,
    transition_seconds: float,
    fps: str,
) -> str:
    n = num_clips
    assert len(clip_durations) == n

    # Overlay placement (bottom-left)
    x = overlay_margin
    y = int(video_size[1]) - overlay_margin - overlay_h

    parts: List[str] = []

    # Per-clip overlay + audio normalization
    overlay_start_fade = 0.25
    overlay_end_fade = 0.25
    overlay_out_start = max(0.0, overlay_seconds - overlay_end_fade)

    for i in range(n):
        ov_in = n + i
        parts.append(f"[{i}:v]setpts=PTS-STARTPTS[v{i}base]")
        parts.append(
            f"[{ov_in}:v]format=rgba,"
            f"fade=t=in:st=0:d={overlay_start_fade}:alpha=1,"
            f"fade=t=out:st={overlay_out_start}:d={overlay_end_fade}:alpha=1"
            f"[ov{i}]"
        )
        parts.append(
            f"[v{i}base][ov{i}]overlay=x={x}:y={y}:eof_action=pass:format=auto,"
            f"fps={fps},format=yuv420p[v{i}]"
        )
        parts.append(
            f"[{i}:a]asetpts=PTS-STARTPTS,"
            f"aformat=sample_fmts=fltp:sample_rates=48000:channel_layouts=stereo[a{i}]"
        )

    # Crossfade chain
    v_prev = f"[v0]"
    a_prev = f"[a0]"
    total = clip_durations[0]

    for i in range(1, n):
        offset = max(0.0, total - transition_seconds)
        v_out = f"[vx{i}]"
        a_out = f"[ax{i}]"
        parts.append(
            f"{v_prev}[v{i}]xfade=transition=fade:duration={transition_seconds}:offset={offset}{v_out}"
        )
        parts.append(f"{a_prev}[a{i}]acrossfade=d={transition_seconds}:c1=tri:c2=tri{a_out}")
        v_prev = v_out
        a_prev = a_out
        total = total + clip_durations[i] - transition_seconds

    parts.append(f"{v_prev}copy[vout]")
    parts.append(f"{a_prev}acopy[aout]")

    return ";".join(parts)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a production highlight reel with overlays + transitions.")
    parser.add_argument("--game-dir", type=Path, default=None, help="Game folder under Games/...")
    parser.add_argument(
        "--reel-manifest",
        type=Path,
        default=None,
        help="Filtered/multi-game reel manifest JSON. When set, clips and metadata are loaded from the manifest instead of one game dir.",
    )
    parser.add_argument("--clips-dir", type=Path, default=None, help="Directory containing per-clip mp4s (default: <game-dir>/clips)")
    parser.add_argument("--events-json", type=Path, default=None, help="Matched events JSON (default: <game-dir>/data/matched_events.json)")
    parser.add_argument("--clips-manifest", type=Path, default=None, help="Clip manifest JSON (default: <game-dir>/data/clips_manifest.json)")
    parser.add_argument(
        "--major-approved-json",
        type=Path,
        default=None,
        help="Approved major penalty manifest (default: <game-dir>/data/major_penalty_approved.json)",
    )
    parser.add_argument(
        "--skip-major-approved",
        action="store_true",
        help="Ignore approved major penalty manifest even if it exists.",
    )
    parser.add_argument("--teams-json", type=Path, default=REPO_ROOT / "teams_highlights.json", help="Teams metadata JSON (default: teams_highlights.json)")
    parser.add_argument("--output", type=Path, default=None, help="Output mp4 path (default: <game-dir>/output/highlights_production.mp4)")
    parser.add_argument("--transition-seconds", type=float, default=0.25, help="Crossfade duration between clips")
    parser.add_argument("--overlay-seconds", type=float, default=5.0, help="How long to show the overlay at the start of each clip")
    parser.add_argument("--game-intro-cards", action="store_true", help="Insert the series-open card and between-game recap cards")
    parser.add_argument("--game-intro-card-seconds", type=float, default=3.5, help="Duration of each inserted series-open / between-game card in seconds")
    parser.add_argument("--series-outro-card", action="store_true", help="Append a full-screen series outro card after the final clip")
    parser.add_argument("--series-outro-card-seconds", type=float, default=4.5, help="Duration of the inserted series outro card in seconds")
    parser.add_argument("--series-outro-status", default="", help="Primary status line for the series outro card")
    parser.add_argument("--series-outro-game-label", default="", help="Upcoming game label for the series outro card")
    parser.add_argument("--series-outro-datetime-label", default="", help="Date/time label for the series outro card")
    parser.add_argument("--series-outro-venue", default="", help="Venue name for the series outro card")
    parser.add_argument("--series-outro-location", default="", help="Venue location text for the series outro card")
    parser.add_argument("--series-outro-home-team", default="", help="Home team name shown on the series outro card")
    parser.add_argument("--series-outro-away-team", default="", help="Away team name shown on the series outro card")
    parser.add_argument(
        "--fps",
        default="source",
        help="Output FPS (e.g., 60, 30000/1001, or 'source' to match input clips)",
    )
    parser.add_argument("--crf", type=int, default=18, help="H.264 CRF quality (lower=better, default: 18)")
    parser.add_argument("--dry-run", action="store_true", help="Print ffmpeg command without running it")
    args = parser.parse_args()

    if not args.reel_manifest and not args.game_dir:
        parser.error("one of --game-dir or --reel-manifest is required")

    reel_manifest = args.reel_manifest.expanduser().resolve() if args.reel_manifest else None
    game_dir = args.game_dir.expanduser().resolve() if args.game_dir else None
    teams_db = _read_json(args.teams_json)
    default_game_info: Dict[str, Any] = {}
    clips_dir: Optional[Path] = None

    if reel_manifest:
        clip_items = _load_reel_manifest_items(reel_manifest)
        output_path = args.output or reel_manifest.with_suffix(".mp4")
    else:
        assert game_dir is not None
        clips_dir = args.clips_dir or (game_dir / "clips")
        events_json = args.events_json or (game_dir / "data" / "matched_events.json")
        if not events_json.exists():
            legacy_events_json = game_dir / "data" / "matched_events_freezestart.json"
            if legacy_events_json.exists():
                events_json = legacy_events_json
        clips_manifest = args.clips_manifest or (game_dir / "data" / "clips_manifest.json")
        major_approved_json = args.major_approved_json or (game_dir / "data" / "major_penalty_approved.json")
        if args.skip_major_approved:
            major_approved_json = None
        output_path = args.output or (game_dir / "output" / "highlights_production.mp4")

        if not clips_dir.exists():
            raise FileNotFoundError(f"Clips dir not found: {clips_dir}")
        if not events_json.exists():
            raise FileNotFoundError(f"Events JSON not found: {events_json}")

        game_meta_path = game_dir / "data" / "game_metadata.json"
        if not game_meta_path.exists():
            raise FileNotFoundError(f"Missing game metadata: {game_meta_path}")

        game_meta = _read_json(game_meta_path)
        default_game_info = dict(game_meta.get("game_info", {}) or {})
        clip_items = _load_clip_items(
            game_dir=game_dir,
            clips_dir=clips_dir,
            events_json=events_json,
            clips_manifest=clips_manifest,
            major_approved_json=major_approved_json if (major_approved_json and major_approved_json.exists()) else None,
        )

    base_clip_paths = [it.clip_path for it in clip_items]
    if not base_clip_paths:
        raise ValueError(f"No mp4 clips found for production render")

    fps_expr = str(args.fps or "").strip()
    if not fps_expr or fps_expr.lower() in {"source", "auto"}:
        fps_expr = _ffprobe_fps_expr(base_clip_paths[0]) or "30"
    if not re.match(r"^[0-9./]+$", fps_expr):
        raise ValueError(f"Invalid --fps value: {args.fps}")

    # Render overlays / optional game intro cards
    overlays_dir = (game_dir / "output" / "overlays") if game_dir else (output_path.parent / f"{output_path.stem}_overlays")
    overlays_dir.mkdir(parents=True, exist_ok=True)

    video_size = _ffprobe_video_size(base_clip_paths[0])
    overlay_w = _scale_px(video_size, 1020)
    overlay_h = _scale_px(video_size, 228)
    overlay_margin = _scale_px(video_size, 64)

    if args.game_intro_cards:
        clip_items = _insert_game_intro_cards(
            clip_items,
            output_dir=overlays_dir,
            teams_db=teams_db,
            video_size=video_size,
            fps=fps_expr,
            duration_seconds=float(args.game_intro_card_seconds),
        )
    if args.series_outro_card:
        clip_items = _insert_series_outro_card(
            clip_items,
            output_dir=overlays_dir,
            teams_db=teams_db,
            video_size=video_size,
            fps=fps_expr,
            duration_seconds=float(args.series_outro_card_seconds),
            series_status=str(args.series_outro_status or "").strip(),
            next_game_label=str(args.series_outro_game_label or "").strip(),
            datetime_label=str(args.series_outro_datetime_label or "").strip(),
            venue=str(args.series_outro_venue or "").strip(),
            location=str(args.series_outro_location or "").strip(),
            home_team_name=str(args.series_outro_home_team or "").strip(),
            away_team_name=str(args.series_outro_away_team or "").strip(),
        )

    overlay_paths: List[Path] = []
    clip_paths: List[Path] = []
    score_cache: Dict[Tuple[str, str, str], Tuple[TeamInfo, TeamInfo, List[str], List[str]]] = {}

    unreliable_clips = []
    for idx, item in enumerate(clip_items, 1):
        e = item.event
        clip_type = item.type

        overlay_path = overlays_dir / f"overlay_{idx:02d}.png"
        clip_paths.append(item.clip_path)
        item_game_info = dict(default_game_info)
        if item.game_info:
            item_game_info.update(item.game_info)

        league = str(item_game_info.get("league", "MHL"))
        home_name = str(item_game_info.get("home_team", "Home"))
        away_name = str(item_game_info.get("away_team", "Away"))
        score_key = (league, home_name, away_name)
        if score_key not in score_cache:
            home = _find_team_info(home_name, league, teams_db)
            away = _find_team_info(away_name, league, teams_db)
            score_cache[score_key] = (
                home,
                away,
                _load_roster_names(home.slug),
                _load_roster_names(away.slug),
            )
        home, away, home_roster, away_roster = score_cache[score_key]

        if item.segment_kind in {"game_intro", "series_outro"} or clip_type in {"game_intro", "series_outro"}:
            _render_transparent_overlay_png(overlay_path, (overlay_w, overlay_h))
            overlay_paths.append(overlay_path)
            continue

        # Check for unreliable match (scoreboard issues detected)
        is_unreliable = bool(e.get("match_unreliable", False))
        match_confidence = e.get("match_confidence", 1.0)

        if is_unreliable:
            reason = e.get("match_unreliable_reason", "Unknown")
            unreliable_clips.append({
                "index": idx,
                "type": clip_type,
                "period": e.get("period"),
                "time": e.get("time"),
                "confidence": match_confidence,
                "reason": reason,
            })
            print(
                f"⚠️  WARNING: Clip {idx} ({clip_type} P{e.get('period')} {e.get('time')}) "
                f"has unreliable timing (confidence: {match_confidence:.0%}). "
                f"Reason: {reason}"
            )

        if clip_type == "goal":
            scoring_team = _infer_scoring_team(e, home=home, away=away, home_roster=home_roster, away_roster=away_roster)
            if scoring_team is None:
                scoring_team_name = str(e.get("team", "") or "")
                scoring_team = _find_team_info(scoring_team_name, league, teams_db)

            home_score = _coerce_score(e.get("home_score"))
            away_score = _coerce_score(e.get("away_score"))
            if home_score is None or away_score is None:
                home_score = 1 if scoring_team is home else 0
                away_score = 1 if scoring_team is away else 0

            special_str = str(e.get("special") or "").strip()
            special_norm = special_str.upper()
            is_power_play = bool(e.get("power_play") or e.get("is_power_play")) or ("PP" in special_norm)
            is_short_handed = bool(e.get("short_handed") or e.get("is_short_handed")) or ("SH" in special_norm)
            is_empty_net = bool(e.get("empty_net") or e.get("is_empty_net")) or ("EN" in special_norm)

            # FAIL-SAFE: Add warning indicator to overlay if match is unreliable
            special_overlay = special_str
            if is_unreliable and match_confidence < 0.5:
                special_overlay = "⚠️ UNVERIFIED"

            _render_overlay_png(
                overlay_path,
                video_size=video_size,
                overlay_size=(overlay_w, overlay_h),
                home=home,
                away=away,
                scoring_team=scoring_team,
                home_score=home_score,
                away_score=away_score,
                period=int(e.get("period") or 0),
                time_str=str(e.get("time") or "").strip(),
                scorer=str(e.get("scorer") or e.get("player") or "").strip(),
                assist1=str(e.get("assist1") or "").strip(),
                assist2=str(e.get("assist2") or "").strip(),
                is_power_play=is_power_play,
                is_short_handed=is_short_handed,
                is_empty_net=is_empty_net,
                special=special_overlay,
                game_label=item.overlay_game_label,
                series_title=item.overlay_series_title,
            )
            overlay_paths.append(overlay_path)
            continue

        penalized_team = _infer_penalty_team(e, home=home, away=away, home_roster=home_roster, away_roster=away_roster)
        if penalized_team is None:
            penalized_team = home

        home_score = _coerce_score(e.get("home_score")) or 0
        away_score = _coerce_score(e.get("away_score")) or 0
        player = e.get("player") or {}
        player_name = player.get("name") if isinstance(player, dict) else str(player or "")
        _render_penalty_overlay_png(
            overlay_path,
            video_size=video_size,
            overlay_size=(overlay_w, overlay_h),
            home=home,
            away=away,
            penalized_team=penalized_team,
            home_score=home_score,
            away_score=away_score,
            period=int(e.get("period") or 0),
            time_str=str(e.get("time") or "").strip(),
            player=str(player_name or "").strip(),
            infraction=str(e.get("infraction") or "").strip(),
            minutes=int(e.get("minutes") or 2),
            game_label=item.overlay_game_label,
            series_title=item.overlay_series_title,
        )
        overlay_paths.append(overlay_path)

    # Summary warning for unreliable clips
    if unreliable_clips:
        print("\n" + "=" * 70)
        print(f"⚠️  SCOREBOARD TIMING WARNING: {len(unreliable_clips)} clips have unreliable timing")
        print("=" * 70)
        for clip in unreliable_clips:
            print(f"  - Clip {clip['index']}: {clip['type']} P{clip['period']} {clip['time']} "
                  f"(confidence: {clip['confidence']:.0%})")
        print("\nRECOMMENDATION: Review these clips manually before publishing.")
        print("The scoreboard may have been broken or unreadable during recording.")
        print("=" * 70 + "\n")

        # Write unreliable clips report
        report_path = (game_dir / "output" / "UNRELIABLE_CLIPS_REPORT.txt") if game_dir else (output_path.parent / "UNRELIABLE_CLIPS_REPORT.txt")
        try:
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(f"UNRELIABLE CLIPS REPORT\n")
                f.write(f"Game: {(game_dir.name if game_dir else output_path.stem)}\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Total clips with timing issues: {len(unreliable_clips)}\n\n")
                for clip in unreliable_clips:
                    f.write(f"Clip {clip['index']}:\n")
                    f.write(f"  Type: {clip['type']}\n")
                    f.write(f"  Period: {clip['period']}\n")
                    f.write(f"  Time: {clip['time']}\n")
                    f.write(f"  Confidence: {clip['confidence']:.0%}\n")
                    f.write(f"  Reason: {clip['reason']}\n\n")
                f.write("\nThese clips may show incorrect timing or be cut from the wrong part of the video.\n")
                f.write("Manual review is recommended before publishing.\n")
            print(f"Wrote unreliable clips report to: {report_path}")
        except Exception as e:
            print(f"Warning: Could not write unreliable clips report: {e}")

    # Clip durations
    durations = [_ffprobe_duration_seconds(p) for p in clip_paths]

    # Build ffmpeg command
    output_path.parent.mkdir(parents=True, exist_ok=True)
    filter_complex = _build_ffmpeg_filter(
        num_clips=len(clip_paths),
        clip_durations=durations,
        video_size=video_size,
        overlay_w=overlay_w,
        overlay_h=overlay_h,
        overlay_margin=overlay_margin,
        overlay_seconds=float(args.overlay_seconds),
        transition_seconds=float(args.transition_seconds),
        fps=fps_expr,
    )

    cmd: List[str] = ["ffmpeg", "-hide_banner", "-y"]

    for clip in clip_paths:
        cmd += ["-i", str(clip)]

    # Each overlay is looped for overlay_seconds; overlay filter uses eof_action=pass
    for ov in overlay_paths:
        cmd += ["-loop", "1", "-framerate", fps_expr, "-t", f"{args.overlay_seconds}", "-i", str(ov)]

    cmd += [
        "-filter_complex",
        filter_complex,
        "-map",
        "[vout]",
        "-map",
        "[aout]",
        "-c:v",
        "libx264",
        "-preset",
        "slow",
        "-crf",
        str(int(args.crf)),
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-movflags",
        "+faststart",
        "-f",
        "mp4",
        # Write atomically: ffmpeg can leave a corrupt file if interrupted mid-run.
        str(output_path.with_name(output_path.stem + ".tmp" + output_path.suffix)),
    ]

    if args.dry_run:
        print(" ".join(cmd))
        return 0

    tmp_path = output_path.with_name(output_path.stem + ".tmp" + output_path.suffix)
    try:
        tmp_path.unlink(missing_ok=True)
    except Exception:
        pass

    env = os.environ.copy()
    subprocess.run(cmd, check=True, env=env)
    tmp_path.replace(output_path)
    print(f"Wrote: {output_path}")

    # Copy YouTube description alongside output if available.
    desc_src = (game_dir / "output" / "youtube_description.txt") if game_dir else None
    if desc_src and desc_src.exists():
        try:
            out_desc = output_path.parent / "youtube_description.txt"
            if out_desc.resolve() != desc_src.resolve():
                out_desc.write_text(desc_src.read_text(encoding="utf-8"), encoding="utf-8")
                print(f"Copied: {out_desc}")
        except Exception as e:
            print(f"Warning: could not copy YouTube description: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
