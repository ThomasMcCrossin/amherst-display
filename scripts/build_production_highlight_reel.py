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
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from PIL import Image, ImageDraw, ImageFont


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
            logo_path = Path("assets/logos") / league.lower() / f"{slug}.png"
            if not logo_path.exists():
                logo_path = Path("assets/logos/fallback.png")
            return TeamInfo(name=team.get("name", team_name), slug=slug, league=league_norm, logo_path=logo_path)

    # Fallback: best-effort slug from name
    slug = normalized.replace(" ", "-") or "unknown"
    logo_path = Path("assets/logos") / league.lower() / f"{slug}.png"
    if not logo_path.exists():
        logo_path = Path("assets/logos/fallback.png")
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
    roster_path = Path("rosters") / f"{team_slug}.json"
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
) -> None:
    video_w, video_h = video_size
    w, h = overlay_size

    font_bold = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    font_reg = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"

    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Background card
    radius = 18
    bg_alpha = 96  # ~0.38
    draw.rounded_rectangle([0, 0, w, h], radius=radius, fill=(0, 0, 0, bg_alpha))

    # Accent bar
    accent_w = 8
    draw.rounded_rectangle([0, 0, accent_w, h], radius=radius, fill=(25, 195, 125, 200))

    pad = 18
    logo_size = 88
    score_area_w = 200

    # Scoring team logo (left)
    logo = Image.open(scoring_team.logo_path).convert("RGBA")
    logo = logo.resize((logo_size, logo_size), Image.Resampling.LANCZOS)
    logo_x = pad
    logo_y = (h - logo_size) // 2
    img.paste(logo, (logo_x, logo_y), logo)

    # Score area (right)
    small_logo = 40
    home_logo = Image.open(home.logo_path).convert("RGBA").resize((small_logo, small_logo), Image.Resampling.LANCZOS)
    away_logo = Image.open(away.logo_path).convert("RGBA").resize((small_logo, small_logo), Image.Resampling.LANCZOS)

    score_x0 = w - pad - score_area_w
    row_gap = 10
    row_h = (h - row_gap) // 2
    away_row_y = 0
    home_row_y = row_h + row_gap

    # Highlight scoring team row slightly
    if _norm_name(scoring_team.name) == _norm_name(away.name):
        highlight_y = away_row_y
    elif _norm_name(scoring_team.name) == _norm_name(home.name):
        highlight_y = home_row_y
    else:
        highlight_y = None

    if highlight_y is not None:
        draw.rounded_rectangle(
            [score_x0, highlight_y + 6, w - pad, highlight_y + row_h - 6],
            radius=12,
            fill=(255, 255, 255, 28),
        )

    score_font = _load_font(font_bold, 34)
    label_font = _load_font(font_reg, 16)

    # Away row
    ax = score_x0 + 10
    ay = away_row_y + (row_h - small_logo) // 2
    img.paste(away_logo, (ax, ay), away_logo)
    draw.text((ax + small_logo + 10, away_row_y + 18), f"{away_score}", font=score_font, fill=(255, 255, 255, 230))
    draw.text((ax + small_logo + 70, away_row_y + 26), "AWAY", font=label_font, fill=(255, 255, 255, 160))

    # Home row
    hx = score_x0 + 10
    hy = home_row_y + (row_h - small_logo) // 2
    img.paste(home_logo, (hx, hy), home_logo)
    draw.text((hx + small_logo + 10, home_row_y + 18), f"{home_score}", font=score_font, fill=(255, 255, 255, 230))
    draw.text((hx + small_logo + 70, home_row_y + 26), "HOME", font=label_font, fill=(255, 255, 255, 160))

    # Text block (center)
    text_x0 = logo_x + logo_size + pad
    text_x1 = score_x0 - pad
    text_w = max(10, text_x1 - text_x0)

    badge_text = ""
    if is_power_play:
        badge_text = "PP"
    elif is_short_handed:
        badge_text = "SH"
    elif is_empty_net:
        badge_text = "EN"

    tag = "GOAL"
    if special and not badge_text:
        tag = f"{special.strip().upper()} GOAL"

    tag_font = _load_font(font_bold, 18)
    draw.text((text_x0, 10), tag, font=tag_font, fill=(25, 195, 125, 240))

    scorer = scorer.strip() or "Unknown"
    badge_reserve = 70 if badge_text else 0
    scorer_font = _fit_text_font(draw, scorer, font_bold, max_size=36, min_size=24, max_width=max(10, text_w - badge_reserve))
    scorer_y = 38
    draw.text((text_x0, scorer_y), scorer, font=scorer_font, fill=(255, 255, 255, 235))

    if badge_text:
        badge_font = _load_font(font_bold, 16)
        badge_pad_x = 10
        badge_pad_y = 6
        scorer_w = int(draw.textlength(scorer, font=scorer_font))
        badge_w = int(draw.textlength(badge_text, font=badge_font)) + 2 * badge_pad_x
        badge_h = int(badge_font.size) + 2 * badge_pad_y
        badge_x = min(text_x0 + scorer_w + 12, text_x1 - badge_w)
        badge_y = scorer_y + int((int(scorer_font.size) - badge_h) * 0.5) + 4

        badge_fill = (90, 90, 90, 200)
        if badge_text == "PP":
            badge_fill = (35, 140, 255, 220)
        elif badge_text == "SH":
            badge_fill = (255, 140, 0, 220)
        elif badge_text == "EN":
            badge_fill = (210, 210, 210, 200)

        draw.rounded_rectangle([badge_x, badge_y, badge_x + badge_w, badge_y + badge_h], radius=10, fill=badge_fill)
        draw.text((badge_x + badge_pad_x, badge_y + badge_pad_y), badge_text, font=badge_font, fill=(0, 0, 0, 220))

    meta_parts = [_format_period_label(period), time_str]
    meta = " • ".join([p for p in meta_parts if p])
    meta_font = _load_font(font_reg, 20)
    draw.text((text_x0, 82), meta, font=meta_font, fill=(255, 255, 255, 200))

    assists = [a.strip() for a in [assist1, assist2] if a and a.strip()]
    if assists:
        assists_text = "A: " + ", ".join(assists)
        assists_font = _fit_text_font(draw, assists_text, font_reg, max_size=18, min_size=14, max_width=text_w)
        draw.text((text_x0, 108), assists_text, font=assists_font, fill=(255, 255, 255, 180))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, format="PNG")


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
) -> None:
    video_w, video_h = video_size
    w, h = overlay_size

    font_bold = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    font_reg = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"

    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    radius = 18
    bg_alpha = 96
    draw.rounded_rectangle([0, 0, w, h], radius=radius, fill=(0, 0, 0, bg_alpha))

    # Amber accent bar for penalties
    accent_w = 8
    draw.rounded_rectangle([0, 0, accent_w, h], radius=radius, fill=(255, 193, 7, 220))

    pad = 18
    logo_size = 88
    score_area_w = 200

    logo = Image.open(penalized_team.logo_path).convert("RGBA")
    logo = logo.resize((logo_size, logo_size), Image.Resampling.LANCZOS)
    logo_x = pad
    logo_y = (h - logo_size) // 2
    img.paste(logo, (logo_x, logo_y), logo)

    small_logo = 40
    home_logo = Image.open(home.logo_path).convert("RGBA").resize((small_logo, small_logo), Image.Resampling.LANCZOS)
    away_logo = Image.open(away.logo_path).convert("RGBA").resize((small_logo, small_logo), Image.Resampling.LANCZOS)

    score_x0 = w - pad - score_area_w
    row_gap = 10
    row_h = (h - row_gap) // 2
    away_row_y = 0
    home_row_y = row_h + row_gap

    if _norm_name(penalized_team.name) == _norm_name(away.name):
        highlight_y = away_row_y
    elif _norm_name(penalized_team.name) == _norm_name(home.name):
        highlight_y = home_row_y
    else:
        highlight_y = None

    if highlight_y is not None:
        draw.rounded_rectangle(
            [score_x0, highlight_y + 6, w - pad, highlight_y + row_h - 6],
            radius=12,
            fill=(255, 255, 255, 28),
        )

    score_font = _load_font(font_bold, 34)
    label_font = _load_font(font_reg, 16)

    ax = score_x0 + 10
    ay = away_row_y + (row_h - small_logo) // 2
    img.paste(away_logo, (ax, ay), away_logo)
    draw.text((ax + small_logo + 10, away_row_y + 18), f"{away_score}", font=score_font, fill=(255, 255, 255, 230))
    draw.text((ax + small_logo + 70, away_row_y + 26), "AWAY", font=label_font, fill=(255, 255, 255, 160))

    hx = score_x0 + 10
    hy = home_row_y + (row_h - small_logo) // 2
    img.paste(home_logo, (hx, hy), home_logo)
    draw.text((hx + small_logo + 10, home_row_y + 18), f"{home_score}", font=score_font, fill=(255, 255, 255, 230))
    draw.text((hx + small_logo + 70, home_row_y + 26), "HOME", font=label_font, fill=(255, 255, 255, 160))

    text_x0 = logo_x + logo_size + pad
    text_x1 = score_x0 - pad
    text_w = max(10, text_x1 - text_x0)

    tag_font = _load_font(font_bold, 18)
    draw.text((text_x0, 10), "PENALTY", font=tag_font, fill=(255, 193, 7, 240))

    player = player.strip() or "Unknown"
    player_font = _fit_text_font(draw, player, font_bold, max_size=34, min_size=22, max_width=text_w)
    draw.text((text_x0, 38), player, font=player_font, fill=(255, 255, 255, 235))

    meta_parts = [_format_period_label(period), time_str]
    meta = " • ".join([p for p in meta_parts if p])
    meta_font = _load_font(font_reg, 20)
    draw.text((text_x0, 82), meta, font=meta_font, fill=(255, 255, 255, 200))

    detail = infraction.strip() or "Penalty"
    detail += f" ({int(minutes)} min)"
    detail_font = _fit_text_font(draw, detail, font_reg, max_size=18, min_size=14, max_width=text_w)
    draw.text((text_x0, 108), detail, font=detail_font, fill=(255, 255, 255, 180))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, format="PNG")


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


def _build_ffmpeg_filter(
    *,
    num_clips: int,
    clip_durations: List[float],
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
    y = 720 - overlay_margin - overlay_h  # tuned for 1280x720 source clips

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
    parser.add_argument("--game-dir", type=Path, required=True, help="Game folder under Games/...")
    parser.add_argument("--clips-dir", type=Path, default=None, help="Directory containing per-clip mp4s (default: <game-dir>/clips)")
    parser.add_argument("--events-json", type=Path, default=None, help="Matched events JSON (default: <game-dir>/data/matched_events_freezestart.json)")
    parser.add_argument("--clips-manifest", type=Path, default=None, help="Clip manifest JSON (default: <game-dir>/data/clips_manifest.json)")
    parser.add_argument(
        "--major-approved-json",
        type=Path,
        default=None,
        help="Approved major penalty manifest (default: <game-dir>/data/major_penalty_approved.json)",
    )
    parser.add_argument("--teams-json", type=Path, default=Path("teams_highlights.json"), help="Teams metadata JSON (default: teams_highlights.json)")
    parser.add_argument("--output", type=Path, default=None, help="Output mp4 path (default: <game-dir>/output/highlights_production.mp4)")
    parser.add_argument("--transition-seconds", type=float, default=0.25, help="Crossfade duration between clips")
    parser.add_argument("--overlay-seconds", type=float, default=5.0, help="How long to show the overlay at the start of each clip")
    parser.add_argument(
        "--fps",
        default="source",
        help="Output FPS (e.g., 60, 30000/1001, or 'source' to match input clips)",
    )
    parser.add_argument("--crf", type=int, default=18, help="H.264 CRF quality (lower=better, default: 18)")
    parser.add_argument("--dry-run", action="store_true", help="Print ffmpeg command without running it")
    args = parser.parse_args()

    game_dir: Path = args.game_dir
    clips_dir = args.clips_dir or (game_dir / "clips")
    events_json = args.events_json or (game_dir / "data" / "matched_events_freezestart.json")
    clips_manifest = args.clips_manifest or (game_dir / "data" / "clips_manifest.json")
    major_approved_json = args.major_approved_json or (game_dir / "data" / "major_penalty_approved.json")
    output_path = args.output or (game_dir / "output" / "highlights_production.mp4")

    if not clips_dir.exists():
        raise FileNotFoundError(f"Clips dir not found: {clips_dir}")
    if not events_json.exists():
        raise FileNotFoundError(f"Events JSON not found: {events_json}")

    game_meta_path = game_dir / "data" / "game_metadata.json"
    if not game_meta_path.exists():
        raise FileNotFoundError(f"Missing game metadata: {game_meta_path}")

    teams_db = _read_json(args.teams_json)
    game_meta = _read_json(game_meta_path)
    game_info = game_meta.get("game_info", {})

    league = str(game_info.get("league", "MHL"))
    home_name = str(game_info.get("home_team", "Home"))
    away_name = str(game_info.get("away_team", "Away"))

    home = _find_team_info(home_name, league, teams_db)
    away = _find_team_info(away_name, league, teams_db)

    home_roster = _load_roster_names(home.slug)
    away_roster = _load_roster_names(away.slug)

    clip_items = _load_clip_items(
        game_dir=game_dir,
        clips_dir=clips_dir,
        events_json=events_json,
        clips_manifest=clips_manifest,
        major_approved_json=major_approved_json if major_approved_json.exists() else None,
    )
    clip_paths = [it.clip_path for it in clip_items]
    if not clip_paths:
        raise ValueError(f"No mp4 clips found in: {clips_dir}")

    # Render overlays
    overlays_dir = game_dir / "output" / "overlays"
    overlays_dir.mkdir(parents=True, exist_ok=True)

    overlay_w, overlay_h = (780, 140)
    video_size = (1280, 720)

    overlay_paths: List[Path] = []
    home_score = 0
    away_score = 0

    unreliable_clips = []
    for idx, item in enumerate(clip_items, 1):
        e = item.event
        clip_type = item.type

        overlay_path = overlays_dir / f"overlay_{idx:02d}.png"

        # Check for unreliable match (scoreboard issues detected)
        is_unreliable = bool(e.get("match_unreliable", False))
        match_confidence = e.get("match_confidence", 1.0)
        scoreboard_health = e.get("scoreboard_health_score", 1.0)

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

            if scoring_team is home:
                home_score += 1
            elif scoring_team is away:
                away_score += 1

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
            )
            overlay_paths.append(overlay_path)
            continue

        penalized_team = _infer_penalty_team(e, home=home, away=away, home_roster=home_roster, away_roster=away_roster)
        if penalized_team is None:
            penalized_team = home

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
        report_path = game_dir / "output" / "UNRELIABLE_CLIPS_REPORT.txt"
        try:
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(f"UNRELIABLE CLIPS REPORT\n")
                f.write(f"Game: {game_dir.name}\n")
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

    fps_expr = str(args.fps or "").strip()
    if not fps_expr or fps_expr.lower() in {"source", "auto"}:
        fps_expr = _ffprobe_fps_expr(clip_paths[0]) or "30"
    if not re.match(r"^[0-9./]+$", fps_expr):
        raise ValueError(f"Invalid --fps value: {args.fps}")

    # Build ffmpeg command
    output_path.parent.mkdir(parents=True, exist_ok=True)
    filter_complex = _build_ffmpeg_filter(
        num_clips=len(clip_paths),
        clip_durations=durations,
        overlay_w=overlay_w,
        overlay_h=overlay_h,
        overlay_margin=24,
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
    desc_src = game_dir / "output" / "youtube_description.txt"
    if desc_src.exists():
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
