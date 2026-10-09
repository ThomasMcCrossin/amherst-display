"""
Stage 4: write overrides.json and feed the reviewed windows to clip cutting and reel building.

  override        the reviewer's (enforced) window replaces the engine's
  keep / unsure / rejected / no_verdict / held_for_human
                  the engine window stays (held ones are listed for a human)
  drop            the clip leaves the reel

Reel modes (default unchanged: goals only):
  goals            main reel = goals
  with-rough       main reel = goals + fights/majors in game order
  separate-rough   main reel = goals; a separate "rough stuff" reel = fights/majors
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import REPO
from .incidents import ROUGH_CLASSES  # major, scrum, fight: the rough-stuff reel

REEL_MODES = ("goals", "with-rough", "separate-rough")
ENGINE_WINDOW_STATUSES = {"keep", "unsure", "rejected", "no_verdict", "held_for_human"}


def cut_clip(video: Path, start: float, end: float, dest: Path) -> None:
    """Same encode settings as the engine's clip cutter (libx264/aac, faststart)."""
    sys.path.insert(0, str(REPO))
    import config  # noqa: WPS433

    dest.parent.mkdir(parents=True, exist_ok=True)
    cmd = ["nice", "-n", "10", "ffmpeg", "-v", "error", "-nostdin", "-y", "-ss", f"{start:.3f}", "-t", f"{end - start:.3f}",
           "-i", str(video), "-map", "0:v:0", "-map", "0:a?", "-c:v", config.OUTPUT_CODEC, "-preset", config.OUTPUT_PRESET,
           "-crf", str(config.OUTPUT_CRF), "-c:a", config.OUTPUT_AUDIO_CODEC, "-b:a", config.OUTPUT_AUDIO_BITRATE,
           "-ar", str(config.OUTPUT_AUDIO_SAMPLE_RATE), "-pix_fmt", config.OUTPUT_PIXEL_FORMAT, "-threads", "3",
           "-movflags", "+faststart", "-avoid_negative_ts", "make_zero", str(dest)]
    subprocess.run(cmd, check=True)


def _primary_event(inc: Dict[str, Any]) -> Dict[str, Any]:
    if inc["kind"] == "goal" or len(inc["events"]) == 1:
        return dict(inc["events"][0])
    rank = {"minor": 0, "major": 1, "fight": 2}
    from .incidents import penalty_class
    return dict(max(inc["events"], key=lambda e: (rank[penalty_class(e)], int(e.get("minutes") or 0))))


def write_overrides(game_dir: Path, out_dir: Path, video: Path, incidents: List[Dict[str, Any]],
                    results: Dict[str, Dict[str, Any]], meta: Dict[str, Any]) -> Dict[str, Any]:
    rows = []
    for inc in incidents:
        r = results.get(inc["id"])
        final = (r or {}).get("final") or {}
        status = final.get("status") or ("unplaced" if inc["video_time"] is None else "not_reviewed")
        engine = None if inc["engine_in"] is None else {"in": round(inc["engine_in"], 2), "out": round(inc["engine_out"], 2)}
        if status == "override":
            window = {"in": final["in"], "out": final["out"]}
        elif status == "drop":
            window = None
        else:
            window = engine
        v = (r or {}).get("verdict") or {}
        rows.append({"incident_id": inc["id"], "kind": inc["kind"], "class": inc["class"], "period": inc["period"],
                     "time_elapsed": inc["time_elapsed"], "rows": inc["rows"], "status": status, "engine": engine,
                     "final": window, "event_video_time": final.get("event"), "decision": v.get("decision"),
                     "confidence": v.get("confidence"), "reason": v.get("reason"), "dispute": (r or {}).get("dispute"),
                     "objections": final.get("objections"), "clamps": final.get("clamps"), "errors": final.get("errors"),
                     "engine_clip": inc.get("clip_path")})
    doc = {"schema": "clip-review/overrides@1", "game_dir": str(game_dir), "video": str(video),
           "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"), **meta, "incidents": rows,
           "held_for_human": [x["incident_id"] for x in rows if x["status"] == "held_for_human"],
           "dropped": [x["incident_id"] for x in rows if x["status"] == "drop"]}
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "overrides.json").write_text(json.dumps(doc, indent=2, default=str))
    return doc


def apply_overrides(game_dir: Path, out_dir: Path, video: Path, incidents: List[Dict[str, Any]], overrides: Dict[str, Any],
                    reel_mode: str = "goals", build: bool = False, reel_output_dir: Optional[Path] = None) -> Dict[str, Any]:
    """Cut reviewed clips, write reel manifests, optionally build the reels."""
    if reel_mode not in REEL_MODES:
        raise ValueError(f"reel mode must be one of {REEL_MODES}")
    by_id = {inc["id"]: inc for inc in incidents}
    goals, rough = [], []
    for row in overrides["incidents"]:
        inc = by_id[row["incident_id"]]
        if row["final"] is None:  # dropped, or never placed by the engine
            continue
        if inc["kind"] == "goal":
            goals.append(row)
        elif inc["class"] in ROUGH_CLASSES:
            rough.append(row)
    clips_dir = out_dir / "clips"
    entries: Dict[str, Dict[str, Any]] = {}
    for row in goals + (rough if reel_mode != "goals" else []):
        inc = by_id[row["incident_id"]]
        w = row["final"]
        engine_clip = game_dir / inc["clip_path"] if inc.get("clip_path") else None
        if row["status"] != "override" and engine_clip is not None and engine_clip.exists():
            path = engine_clip
        else:
            path = clips_dir / f"{inc['id']}.mp4"
            stamp = path.with_suffix(".window")
            want = f"{w['in']:.2f}-{w['out']:.2f}"
            if not (path.exists() and stamp.exists() and stamp.read_text() == want):
                cut_clip(video, w["in"], w["out"], path)
                stamp.write_text(want)
        ev = _primary_event(inc)
        if inc["class"] == "scrum":  # one clip for the whole stoppage; overlays read these fields
            from penalty_incidents import scrum_summary
            ev.update(kind="scrum", penalties=inc["rows"], penalty_count=len(inc["rows"]),
                      infraction=scrum_summary(inc["rows"]), minutes=sum(int(r.get("minutes") or 0) for r in inc["rows"]))
        ev.update(path=str(path), clip_filename=path.name, review_status=row["status"],
                  clip_video_start=w["in"], clip_video_end=w["out"],
                  video_time=row.get("event_video_time") or ev.get("video_time"))
        entries[inc["id"]] = ev
    reels: Dict[str, List[Dict[str, Any]]] = {}
    order = lambda ids: sorted(ids, key=lambda i: entries[i]["clip_video_start"])  # noqa: E731
    goal_ids = [r["incident_id"] for r in goals if r["incident_id"] in entries]
    rough_ids = [r["incident_id"] for r in rough if r["incident_id"] in entries]
    reels["main"] = [entries[i] for i in order(goal_ids + (rough_ids if reel_mode == "with-rough" else []))]
    if reel_mode == "separate-rough":
        reels["rough_stuff"] = [entries[i] for i in order(rough_ids)]
    manifests = {}
    for name, items in reels.items():
        for i, e in enumerate(items, 1):
            e["index"] = i
        path = out_dir / f"reel_{name}.json"
        path.write_text(json.dumps({"reel_mode": reel_mode, "clips": items}, indent=2, default=str))
        manifests[name] = str(path)
    built: Dict[str, Any] = {}
    if build:
        dest = reel_output_dir or game_dir / "output"
        for name, mpath in manifests.items():
            if not reels[name]:
                continue
            outp = dest / ("highlights_reviewed.mp4" if name == "main" else "highlights_rough_stuff.mp4")
            cmd = [sys.executable, str(REPO / "scripts" / "build_production_highlight_reel.py"), "--game-dir", str(game_dir),
                   "--clips-manifest", mpath, "--skip-major-approved", "--output", str(outp)]
            proc = subprocess.run(["nice", "-n", "10"] + cmd, cwd=REPO, capture_output=True, text=True)
            built[name] = {"output": str(outp), "ok": proc.returncode == 0,
                           "error": None if proc.returncode == 0 else (proc.stderr or proc.stdout)[-1500:]}
    return {"reel_mode": reel_mode, "manifests": manifests, "clips": {k: v["path"] for k, v in entries.items()}, "built": built}
