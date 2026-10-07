"""Stage 1 (code): one packet directory per incident: incident.json + coarse contact sheets."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import SKILL_DIR, frames
from .incidents import BOUNDS, COARSE, COARSE_ALERT_GOAL, build_incidents, clock_to_seconds, game_info

PACKET_SCHEMA = "hockey-clip-review/incident@1"


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def video_duration(video: Path) -> float:
    out = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(video)],
                         capture_output=True, text=True, check=True)
    return float(out.stdout.strip())


def _neighbours(inc: Dict[str, Any], incidents: List[Dict[str, Any]], span: float) -> List[Dict[str, Any]]:
    out = []
    t0 = inc["video_time"]
    for o in incidents:
        if o is inc or o["video_time"] is None or abs(o["video_time"] - t0) > span:
            continue
        if o["kind"] == "goal":
            what = f"goal by {o['rows'][0].get('team')} ({o['rows'][0].get('scorer')})"
        else:
            what = "penalty: " + "; ".join(f"{r.get('team')} {r.get('infraction')}" for r in o["rows"][:3])
        out.append({"id": o["id"], "what": what, "period": o["period"], "time_elapsed": o["time_elapsed"],
                    "placed_t": round(o["video_time"] - t0, 1)})
    return out


def build_packet(inc: Dict[str, Any], game: Dict[str, Any], video: Path, duration: float, root: Path,
                 incidents: List[Dict[str, Any]]) -> Optional[Path]:
    """Write (or reuse) the packet for one incident. None when the engine never placed it."""
    t0 = inc["video_time"]
    if t0 is None:
        return None
    pk = root / inc["id"]
    cls = inc["class"]
    before, after, step = COARSE_ALERT_GOAL if (cls == "goal" and game["scorebug_alert"]) else COARSE[cls]
    lo, hi = max(-t0, -before), min(duration - t0, after)
    if hi - lo < 10:  # placed outside what the recording covers
        return None
    p = int(inc["period"] or 0)
    el = clock_to_seconds(inc["time_elapsed"])
    remaining = None if el is None else max(0, (20 * 60 if p <= 3 else 5 * 60) - el)
    bounds = dict(BOUNDS[cls])
    bounds.update(engine_in=round(inc["engine_in"] - t0, 2), engine_out=round(inc["engine_out"] - t0, 2),
                  video_from=round(-t0, 2), video_to=round(duration - t0, 2))
    doc = {
        "schema": PACKET_SCHEMA, "incident_id": inc["id"], "kind": inc["kind"], "class": cls,
        "game": {k: game[k] for k in ("name", "date", "game_id")},
        "teams": {"home": game["home"], "away": game["away"]},
        "period": inc["period"], "time_elapsed": inc["time_elapsed"],
        "time_remaining": None if remaining is None else f"{remaining // 60}:{remaining % 60:02d}",
        "sheet_rows": inc["rows"],
        "video": str(video), "video_duration": round(duration, 2), "anchor": round(float(t0), 2),
        "engine_window": {"in_t": bounds["engine_in"], "out_t": bounds["engine_out"], "source": inc["engine_source"]},
        "engine_match": inc["match"], "scorebug_alert": game["scorebug_alert"],
        "neighbours": _neighbours(inc, incidents, before + after + 60),
        "bounds": bounds,
        "coarse": {"from": round(lo, 2), "to": round(hi, 2), "step": step},
        "tools": {"python": sys.executable, "skill_dir": str(SKILL_DIR)},
    }
    old = None
    if (pk / "incident.json").exists():
        try:
            old = json.loads((pk / "incident.json").read_text())
        except Exception:  # noqa: BLE001
            old = None
    sheets_ok = bool(old) and old.get("anchor") == doc["anchor"] and old.get("coarse") == doc["coarse"] \
        and old.get("video") == doc["video"] and all(Path(pk / s["path"]).exists() for s in old.get("coarse_sheets", []))
    if sheets_ok:
        doc["coarse_sheets"] = old["coarse_sheets"]
    else:
        fr = frames.extract(video, t0 + lo, t0 + hi, step)
        sheets = frames.make_sheets(fr, t0, pk / "sheets", "coarse")
        doc["coarse_sheets"] = [{"path": str(Path(s["path"]).relative_to(pk)), "from": s["from"], "to": s["to"],
                                 "frames": s["frames"], "step": step} for s in sheets]
    pk.mkdir(parents=True, exist_ok=True)
    (pk / "incident.json").write_text(json.dumps(doc, indent=2))
    return pk


def build_packets(game_dir: Path, video: Path, root: Path, kinds: Optional[set] = None,
                  only: Optional[List[str]] = None, scorebug_alert: Optional[bool] = None) -> Dict[str, Any]:
    """All packets for one game. Returns {"game", "incidents", "packets": {id: path}, "unplaced": [...]}.

    scorebug_alert overrides the game dir's SCOREBOARD_ALERT.txt (e.g. a known-bad broadcast)."""
    game = game_info(game_dir)
    if scorebug_alert is not None:
        game["scorebug_alert"] = bool(scorebug_alert)
    incidents = build_incidents(game_dir)
    duration = video_duration(video)
    sel = [g for g in incidents if not kinds or g["kind"] in kinds or g["class"] in kinds
           or ("wanted" in kinds and g["class"] in ("major", "fight"))]
    if only:
        sel = [g for g in sel if any(g["id"].startswith(w) for w in only)]
    packets, unplaced = {}, []
    for g in sel:
        pk = build_packet(g, game, video, duration, root, incidents)
        if pk is None:
            unplaced.append(g["id"])
        elif g["engine_in"] is not None and g["video_time"] is not None:
            packets[g["id"]] = pk
    return {"game": game, "incidents": incidents, "selected": sel, "packets": packets, "unplaced": unplaced,
            "video_duration": duration}
