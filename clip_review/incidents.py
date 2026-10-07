"""Incidents (goals and grouped penalties) from an engine game dir, with the engine's clip windows."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import REPO

sys.path.insert(0, str(REPO))
import config  # noqa: E402

WANTED_RE = re.compile(r"fight|major|misconduct|match penalty|game misconduct|gross", re.I)

# Authority bounds per incident class (seconds; *_t values relative to the anchor). The code
# clamps a reviewer's window into these and rejects a verdict that still breaks them.
BOUNDS: Dict[str, Dict[str, float]] = {
    "goal":  {"lead_min": 3, "lead_max": 45, "tail_min": 8, "tail_max": 25, "len_min": 8, "len_max": 60,
              "near": 30, "relocate_max": 240, "drop_min_confidence": 0.8},
    "minor": {"lead_min": 1, "lead_max": 20, "tail_min": 2, "tail_max": 25, "len_min": 9, "len_max": 40,
              "near": 30, "relocate_max": 240, "drop_min_confidence": 0.6},
    "major": {"lead_min": 2, "lead_max": 45, "tail_min": 5, "tail_max": 60, "len_min": 10, "len_max": 90,
              "near": 45, "relocate_max": 360, "drop_min_confidence": 0.7},
    "fight": {"lead_min": 2, "lead_max": 45, "tail_min": 5, "tail_max": 60, "len_min": 10, "len_max": 75,
              "near": 45, "relocate_max": 360, "drop_min_confidence": 0.7, "fight_pre": 10, "fight_post": 10},
}
for _b in BOUNDS.values():
    _b.setdefault("fight_pre", 10)
    _b.setdefault("fight_post", 10)

# Coarse contact-sheet range per class: (before, after, step). Scorebug-alert games search wider
# for goals because the anchor can be far off.
COARSE = {"goal": (75.0, 40.0, 1.0), "minor": (60.0, 25.0, 1.5), "major": (120.0, 120.0, 2.0), "fight": (120.0, 120.0, 2.0)}
COARSE_ALERT_GOAL = (120.0, 60.0, 1.5)


def clock_to_seconds(text: Any) -> Optional[int]:
    m = re.match(r"^\s*(\d{1,2})[:.](\d{2})", str(text or ""))
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def fmt_clock(sec: Optional[int]) -> str:
    return "?" if sec is None else f"{sec // 60}:{sec % 60:02d}"


def penalty_class(e: Dict[str, Any]) -> str:
    text = str(e.get("infraction") or "")
    if "fight" in text.lower():
        return "fight"
    if int(e.get("minutes") or 0) >= 5 or WANTED_RE.search(text):
        return "major"
    return "minor"


CLASS_RANK = {"minor": 0, "major": 1, "fight": 2}


def game_info(game_dir: Path) -> Dict[str, Any]:
    meta_path = game_dir / "data" / "game_metadata.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    info = meta.get("game_info") or {}
    game_id = ""
    for rel, path in (("source/watch_rams_source.json", ("game_id",)), ("data/offline_run.json", ("game_id",)),
                      ("source/source_info.json", ("game", "game_id"))):
        try:
            node: Any = json.loads((game_dir / rel).read_text())
            for k in path:
                node = node.get(k)
            if node:
                game_id = str(node)
                break
        except Exception:  # noqa: BLE001
            continue
    return {"home": str(info.get("home_team") or ""), "away": str(info.get("away_team") or ""),
            "date": str(info.get("date") or ""), "game_id": game_id,
            "scorebug_alert": (game_dir / "data" / "SCOREBOARD_ALERT.txt").exists(), "name": game_dir.name}


def build_incidents(game_dir: Path) -> List[Dict[str, Any]]:
    data = game_dir / "data"
    events = json.loads((data / "matched_events.json").read_text())
    manifest_path = data / "clips_manifest.json"
    clips = json.loads(manifest_path.read_text()).get("clips", []) if manifest_path.exists() else []
    clip_by_key = {(c.get("type"), c.get("period"), c.get("time")): c for c in clips}

    incidents: List[Dict[str, Any]] = []
    groups: Dict[Tuple[Any, Any], Dict[str, Any]] = {}
    for e in events:
        etype = e.get("type")
        if etype == "goal":
            vt = e.get("video_time")
            clip = clip_by_key.get(("goal", e.get("period"), e.get("time")))
            before = float((clip or {}).get("before_seconds") or e.get("before_seconds") or config.DEFAULT_CLIP_BEFORE_TIME)
            after = float((clip or {}).get("after_seconds") or e.get("after_seconds") or config.DEFAULT_CLIP_AFTER_TIME)
            incidents.append({
                "kind": "goal", "class": "goal", "period": e.get("period"), "time_elapsed": e.get("time"),
                "video_time": vt, "engine_in": None if vt is None else vt - before, "engine_out": None if vt is None else vt + after,
                "engine_source": "clips_manifest" if clip else "engine_default",
                "clip_filename": (clip or {}).get("clip_filename"), "clip_path": (clip or {}).get("path"),
                "rows": [{"team": e.get("team"), "scorer": e.get("scorer"), "assist1": e.get("assist1"),
                          "assist2": e.get("assist2"), "special": e.get("special"), "empty_net": e.get("empty_net")}],
                "events": [e], "match": {"confidence": e.get("match_confidence"), "refined_by": e.get("refined_by"),
                                         "unreliable": bool(e.get("match_unreliable"))},
            })
        elif etype == "penalty":
            key = (e.get("period"), e.get("time"))
            g = groups.get(key)
            if g is None:
                g = {"kind": "penalty", "class": "minor", "period": e.get("period"), "time_elapsed": e.get("time"),
                     "video_time": e.get("video_time"), "rows": [], "events": [],
                     "match": {"confidence": e.get("match_confidence"), "refined_by": e.get("refined_by"),
                               "unreliable": bool(e.get("match_unreliable"))}}
                groups[key] = g
                incidents.append(g)
            name = e.get("player")
            name = name.get("name") if isinstance(name, dict) else name
            g["rows"].append({"team": e.get("team"), "player": name, "infraction": e.get("infraction"), "minutes": e.get("minutes")})
            g["events"].append(e)
            c = penalty_class(e)
            if CLASS_RANK[c] > CLASS_RANK[g["class"]]:
                g["class"] = c
            if g["video_time"] is None and e.get("video_time") is not None:
                g["video_time"] = e.get("video_time")
    for g in incidents:
        if g["kind"] != "penalty":
            continue
        vt = g["video_time"]
        big = g["class"] in ("major", "fight")
        before = float(config.MAJOR_PENALTY_BEFORE_SECONDS if big else config.PENALTY_ALL_BEFORE_SECONDS)
        after = float(config.MAJOR_PENALTY_AFTER_SECONDS if big else config.PENALTY_ALL_AFTER_SECONDS)
        g["engine_in"] = None if vt is None else vt - before
        g["engine_out"] = None if vt is None else vt + after
        g["engine_source"] = "config:major" if big else "config:penalty_all"
        g["clip_filename"] = None
        g["clip_path"] = None
    incidents.sort(key=lambda g: (g["video_time"] is None, g["video_time"] or 0.0))
    for i, g in enumerate(incidents, 1):
        g["id"] = f"{i:02d}_{g['class']}_p{g['period']}_{str(g['time_elapsed']).replace(':', '-')}"
    return incidents


def describe(inc: Dict[str, Any], teams: Dict[str, Any]) -> str:
    """Plain-text game-sheet description (used by the api backend prompts)."""
    p = int(inc["period"] or 0)
    el = clock_to_seconds(inc["time_elapsed"])
    length = 20 * 60 if p <= 3 else 5 * 60
    remaining = None if el is None else max(0, length - el)
    head = (f"Period {p}, {inc['time_elapsed']} elapsed ({fmt_clock(remaining)} left on the game clock). "
            f"Home: {teams.get('home', '?')}, away: {teams.get('away', '?')}.")
    lines = []
    for r in inc["rows"]:
        if inc["kind"] == "goal":
            assists = ", ".join(a for a in (r.get("assist1"), r.get("assist2")) if a)
            lines.append(f"GOAL by {r.get('team')}: {r.get('scorer')}" + (f" (assists: {assists})" if assists else "")
                         + (f" [{r['special']}]" if r.get("special") else "") + (" [empty net]" if r.get("empty_net") else ""))
        else:
            lines.append(f"PENALTY on {r.get('team')}: {r.get('player')} - {r.get('infraction')} ({r.get('minutes')} min)")
    return head + "\n" + "\n".join(lines)
