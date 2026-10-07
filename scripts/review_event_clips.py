#!/usr/bin/env python3
"""
Vision review of every goal / penalty clip in a processed game, with clip boundaries chosen
from what happens on screen.

For each HockeyTech event the engine placed (data/matched_events.json), frames are pulled
from the source recording around the placed video time and tiled into timestamped contact
sheets. A cheap vision model (deepseek-flash, thinking off, same client as
validate_goal_clips.py) is asked, in two passes:

  coarse   -75 s .. +40 s at 1 fps (wider for fights/majors): is the event on screen, when
           did it happen, where does the play that produced it start (zone entry, possession
           change, faceoff win), where does it end (celebration over, cut to replay, whistle);
           for fights, when the gloves drop and when the linesmen separate the players.
  refine   dense (4 fps) sheets around each coarse boundary to pin the cut to ~0.5 s.

The model refines timing and visibility only. Events come from the HockeyTech game sheet,
never from the model, and the proposed window is clamped (see GUARDRAILS) and falls back to
the engine's current window when the model is unsure.

Penalties are grouped into incidents (one review per period+clock, e.g. a scrum with eight
rows). "Wanted" incidents (fighting, any major, misconduct, game misconduct, match penalty)
get a wider search and a fight-specific question.

Output (never inside the game dir):
  ~/.local/state/watch-rams/clip-review/<game>/review/events_review.json
  ~/.local/state/watch-rams/clip-review/<game>/sheets/   contact sheets sent to the model
  ~/.local/state/watch-rams/clip-review/<game>/clips/    with --render

  .venv/bin/python scripts/review_event_clips.py --game-dir "Games/<game>" --video <recording.mp4> [--render]

Needs DEEPSEEK_API_KEY (or SCOREBUG_VISION_API_KEY). Remove: delete this file; nothing imports it.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import cv2
import numpy as np

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
import config  # noqa: E402
from scorebug_detect import DEFAULT_VISION_BASE_URL, DEFAULT_VISION_MODEL  # noqa: E402

STATE_ROOT = Path(os.environ.get("CLIP_REVIEW_DIR") or Path.home() / ".local/state/watch-rams/clip-review")

# ---- search windows (seconds relative to the engine's placed video time) -------------------
# Goals are placed at the clock stop and the play is within ~60 s of it. Penalties are placed from
# the box-score clock, but the Flo bug often lags or jumps around a stoppage (09-12 P1 11:17: the
# stoppage was ~65 s before the engine's time), so penalties search much further back at 2 s steps
# and the refine pass pins the cut at 0.5 s. Wider was tried (-150 s) and made minors worse: with
# two penalties a minute apart the model picked the neighbour's stoppage, so the window stays
# tight, neighbours are named in the prompt, and a pick far from the engine's time is flagged.
GOAL_BEFORE, GOAL_AFTER, GOAL_STEP = 75.0, 40.0, 1.0
PENALTY_BEFORE, PENALTY_AFTER, PENALTY_STEP = 60.0, 25.0, 1.5
WANTED_BEFORE, WANTED_AFTER, WANTED_STEP = 120.0, 120.0, 2.0
MOVED_FAR_SECONDS = 30.0        # a proposed event this far from the engine's time is flagged for a human look
REFINE_HALF = 7.0               # +-7 s around each coarse boundary
REFINE_STEP = 0.5
SHEET_COLS, SHEET_ROWS = 5, 4   # 20 frames per sheet
CELL_W, CELL_H = 384, 216

# ---- GUARDRAILS ------------------------------------------------------------------------------
MAX_IN_BEFORE = 45.0            # in-point no earlier than this before the event ...
MAX_IN_BEFORE_FIGHT = 120.0     # ... unless it is a fight / wanted incident
MIN_CONFIDENCE = 0.5            # below this (or not visible) the current window is kept
GOAL_MIN_AFTER_MOMENT = 8.0     # a goal clip always shows at least 8 s after the puck goes in (the celebration)
GOAL_MAX_AFTER_MOMENT = 25.0
MIN_CLIP = 6.0
MAX_WANTED_AFTER_MOMENT = 60.0   # a major/misconduct that is not a fight: at most 60 s after it
FIGHT_TAIL = 12.0               # fights end this long after the linesmen have the players apart
MIN_CLIP_PENALTY = 9.0          # foul + whistle + referee signal reads badly under 9 s
MAX_CLIP = 180.0
NEXT_EVENT_GAP = 3.0            # out-point stops this far before the next incident's engine window

WANTED_RE = re.compile(r"fight|major|misconduct|match penalty|game misconduct|gross", re.I)


# ======================================================================================
# helpers
# ======================================================================================
def _clock_to_seconds(text: Any) -> Optional[int]:
    m = re.match(r"^\s*(\d{1,2})[:.](\d{2})", str(text or ""))
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def _fmt_clock(sec: Optional[int]) -> str:
    return "?" if sec is None else f"{sec // 60}:{sec % 60:02d}"


def _penalty_wanted(e: Dict[str, Any]) -> bool:
    if int(e.get("minutes") or 0) >= 5:
        return True
    return bool(WANTED_RE.search(str(e.get("infraction") or "")))


def _is_fight_text(e: Dict[str, Any]) -> bool:
    return "fight" in str(e.get("infraction") or "").lower()


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


# ======================================================================================
# event list: goals + penalty incidents, with the engine's current window
# ======================================================================================
def build_incidents(game_dir: Path) -> List[Dict[str, Any]]:
    data = game_dir / "data"
    events = json.loads((data / "matched_events.json").read_text())
    manifest_path = data / "clips_manifest.json"
    clips = json.loads(manifest_path.read_text()).get("clips", []) if manifest_path.exists() else []
    clip_by_key = {(c.get("type"), c.get("period"), c.get("time")): c for c in clips}

    incidents: List[Dict[str, Any]] = []
    penalty_groups: Dict[Tuple[Any, Any], Dict[str, Any]] = {}
    for e in events:
        if e.get("type") == "goal":
            vt = e.get("video_time")
            clip = clip_by_key.get(("goal", e.get("period"), e.get("time")))
            before = float((clip or {}).get("before_seconds") or e.get("before_seconds") or config.DEFAULT_CLIP_BEFORE_TIME)
            after = float((clip or {}).get("after_seconds") or e.get("after_seconds") or config.DEFAULT_CLIP_AFTER_TIME)
            incidents.append({
                "kind": "goal", "wanted": False, "fight": False, "period": e.get("period"), "time_elapsed": e.get("time"),
                "video_time": vt, "old_in": None if vt is None else vt - before, "old_out": None if vt is None else vt + after,
                "old_source": "clips_manifest" if clip else "engine_default",
                "clip_filename": (clip or {}).get("clip_filename"),
                "rows": [{"team": e.get("team"), "scorer": e.get("scorer"), "assist1": e.get("assist1"),
                          "assist2": e.get("assist2"), "special": e.get("special"), "empty_net": e.get("empty_net")}],
                "match_confidence": e.get("match_confidence"), "refined_by": e.get("refined_by"),
                "match_unreliable": bool(e.get("match_unreliable")),
            })
        elif e.get("type") == "penalty":
            key = (e.get("period"), e.get("time"))
            g = penalty_groups.get(key)
            if g is None:
                g = {"kind": "penalty", "wanted": False, "fight": False, "period": e.get("period"), "time_elapsed": e.get("time"),
                     "video_time": e.get("video_time"), "rows": [], "match_confidence": e.get("match_confidence"),
                     "match_unreliable": bool(e.get("match_unreliable"))}
                penalty_groups[key] = g
                incidents.append(g)
            name = e.get("player")
            name = name.get("name") if isinstance(name, dict) else name
            g["rows"].append({"team": e.get("team"), "player": name, "infraction": e.get("infraction"), "minutes": e.get("minutes")})
            g["wanted"] = g["wanted"] or _penalty_wanted(e)
            g["fight"] = g["fight"] or _is_fight_text(e)
    for g in incidents:
        if g["kind"] != "penalty":
            continue
        vt = g["video_time"]
        if g["wanted"]:
            before = float(config.MAJOR_PENALTY_BEFORE_SECONDS)
            after = float(config.MAJOR_PENALTY_AFTER_SECONDS)
        else:
            before = float(config.PENALTY_ALL_BEFORE_SECONDS)
            after = float(config.PENALTY_ALL_AFTER_SECONDS)
        g["old_in"] = None if vt is None else vt - before
        g["old_out"] = None if vt is None else vt + after
        g["old_source"] = "config:major" if g["wanted"] else "config:penalty_all"
    # chronological by placed video time; unplaced incidents last
    incidents.sort(key=lambda g: (g["video_time"] is None, g["video_time"] or 0.0))
    for i, g in enumerate(incidents, 1):
        g["id"] = f"{i:02d}_{g['kind']}_p{g['period']}_{str(g['time_elapsed']).replace(':', '-')}"
    return incidents


# ======================================================================================
# frames and contact sheets
# ======================================================================================
def extract_frames(video: Path, start: float, end: float, step: float, width: int) -> List[Tuple[float, np.ndarray]]:
    """Frames at start, start+step, ... (video seconds) via one niced ffmpeg decode."""
    start = max(0.0, start)
    if end <= start:
        return []
    with tempfile.TemporaryDirectory(prefix="clipreview_") as tmp:
        cmd = ["nice", "-n", "10", "ffmpeg", "-v", "error", "-nostdin", "-ss", f"{start:.3f}", "-t", f"{end - start:.3f}",
               "-i", str(video), "-an", "-sn", "-vf", f"fps=1/{step},scale={width}:-2", "-threads", "2",
               "-q:v", "4", f"{tmp}/f_%05d.jpg"]
        subprocess.run(cmd, check=True)
        frames = []
        for i, p in enumerate(sorted(Path(tmp).glob("f_*.jpg"))):
            img = cv2.imread(str(p))
            if img is not None:
                frames.append((start + i * step, img))
    return frames


def make_sheets(frames: List[Tuple[float, np.ndarray]], anchor: float, out_dir: Path, prefix: str,
                precise: bool = False) -> List[Dict[str, Any]]:
    """Tile frames into labelled sheets. Labels are seconds relative to the engine's placed time."""
    out_dir.mkdir(parents=True, exist_ok=True)
    per = SHEET_COLS * SHEET_ROWS
    sheets = []
    for s in range(0, len(frames), per):
        chunk = frames[s:s + per]
        canvas = np.zeros((SHEET_ROWS * CELL_H, SHEET_COLS * CELL_W, 3), np.uint8)
        for i, (t, img) in enumerate(chunk):
            cell = cv2.resize(img, (CELL_W, CELL_H))
            rel = t - anchor
            label = f"{rel:+.1f}" if precise else f"{rel:+.0f}" if abs(rel - round(rel)) < 0.05 else f"{rel:+.1f}"
            colour = (0, 255, 255) if abs(rel) < 0.01 else (255, 255, 255)
            cv2.putText(cell, label, (8, 34), cv2.FONT_HERSHEY_SIMPLEX, 1.1, (0, 0, 0), 6, cv2.LINE_AA)
            cv2.putText(cell, label, (8, 34), cv2.FONT_HERSHEY_SIMPLEX, 1.1, colour, 2, cv2.LINE_AA)
            r, c = divmod(i, SHEET_COLS)
            canvas[r * CELL_H:(r + 1) * CELL_H, c * CELL_W:(c + 1) * CELL_W] = cell
            cv2.rectangle(canvas, (c * CELL_W, r * CELL_H), ((c + 1) * CELL_W - 1, (r + 1) * CELL_H - 1), (60, 60, 60), 1)
        path = out_dir / f"{prefix}_{s // per + 1:02d}.jpg"
        cv2.imwrite(str(path), canvas, [cv2.IMWRITE_JPEG_QUALITY, 78])
        sheets.append({"path": path, "first": chunk[0][0] - anchor, "last": chunk[-1][0] - anchor, "n": len(chunk)})
    return sheets


def _data_url(path: Path) -> str:
    import base64
    return "data:image/jpeg;base64," + base64.b64encode(path.read_bytes()).decode()


# ======================================================================================
# model client (same endpoint/auth/model resolution as validate_goal_clips.ask_vision)
# ======================================================================================
def call_vision(content: List[Dict[str, Any]], required: Tuple[str, ...] = (), retries: int = 2) -> Dict[str, Any]:
    import requests

    api_key = os.environ.get("SCOREBUG_VISION_API_KEY") or os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("no vision API key (DEEPSEEK_API_KEY or SCOREBUG_VISION_API_KEY)")
    base_url = (os.environ.get("SCOREBUG_VISION_BASE_URL") or DEFAULT_VISION_BASE_URL).rstrip("/")
    model = os.environ.get("SCOREBUG_VISION_MODEL") or DEFAULT_VISION_MODEL
    body: Dict[str, Any] = {"model": model, "temperature": 0, "response_format": {"type": "json_object"},
                            "messages": [{"role": "user", "content": content}]}
    if "deepseek" in base_url:
        body["thinking"] = {"type": "disabled"}
    last: Optional[Exception] = None
    usage = {"prompt_tokens": 0, "completion_tokens": 0, "calls": 0}
    for attempt in range(retries + 1):
        try:
            resp = requests.post(f"{base_url}/chat/completions", json=body, timeout=240,
                                 headers={"Authorization": f"Bearer {api_key}"})
            resp.raise_for_status()
            payload = resp.json()
            u = payload.get("usage", {})
            usage["prompt_tokens"] += int(u.get("prompt_tokens") or 0)
            usage["completion_tokens"] += int(u.get("completion_tokens") or 0)
            usage["calls"] += 1
            answer = json.loads(payload["choices"][0]["message"]["content"])
            missing = [k for k in required if answer.get(k) is None]
            if missing:  # the model sometimes echoes the response_format instead of answering
                raise ValueError(f"answer lacks {missing}")
            return {"answer": answer, "usage": usage, "model": model}
        except Exception as exc:  # noqa: BLE001 - retry any transport/parse failure
            last = exc
            time.sleep(2 + 3 * attempt)
    raise RuntimeError(f"vision call failed: {last}")


def describe_incident(inc: Dict[str, Any], teams: Dict[str, str]) -> str:
    p = int(inc["period"] or 0)
    el = _clock_to_seconds(inc["time_elapsed"])
    length = 20 * 60 if p <= 3 else 5 * 60
    remaining = None if el is None else max(0, length - el)
    head = (f"Period {p}, {inc['time_elapsed']} elapsed ({_fmt_clock(remaining)} left on the game clock). "
            f"Home: {teams.get('home', '?')}, away: {teams.get('away', '?')}.")
    lines = []
    for r in inc["rows"]:
        if inc["kind"] == "goal":
            lines.append(f"GOAL by {r.get('team')}: {r.get('scorer')}"
                         + (f" (assists: {', '.join(a for a in (r.get('assist1'), r.get('assist2')) if a)})" if r.get("assist1") else "")
                         + (f" [{r['special']}]" if r.get("special") else "") + (" [empty net]" if r.get("empty_net") else ""))
        else:
            lines.append(f"PENALTY on {r.get('team')}: {r.get('player')} - {r.get('infraction')} ({r.get('minutes')} min)")
    return head + "\n" + "\n".join(lines)


# ======================================================================================
# prompts
# ======================================================================================
COARSE_SCHEMA = (
    '{"event_visible": true/false, "event_moment": <seconds label, goal: puck crosses the line / penalty: the infraction / fight: first punch>, '
    '"play_start": <seconds label where the PLAY that produced the event begins>, "play_start_kind": "faceoff|zone_entry|possession_change|'
    'rush|dump_in|pp_setup|other", "play_end": <seconds label where the live action/celebration/whistle ends>, '
    '"play_end_kind": "celebration_over|cut_to_replay|whistle|stoppage|other", "replay_start": <label of the first replay frame or null>, '
    '"fight": null or {"gloves_drop": <label>, "separated": <label when linesmen have the players apart>, "visible": true/false}, '
    '"confidence": 0.0-1.0, "reason": "one sentence"}'
)


def coarse_prompt(inc: Dict[str, Any], teams: Dict[str, str], sheets: List[Dict[str, Any]], neighbours: str = "") -> List[Dict[str, Any]]:
    goal = inc["kind"] == "goal"
    wanted = inc["wanted"]
    text = (
        "You are reviewing a hockey broadcast recording to choose clip boundaries for a highlight. The images are contact sheets of "
        "frames in time order, left to right, top to bottom. The number on each frame is its time in seconds relative to the "
        "moment our system THINKS the event happened (0 is highlighted yellow and is only an estimate: the real event can be "
        "tens of seconds earlier or later, or missing; for penalties the true stoppage is often 30-100 s BEFORE it, because the on-screen clock lags around stoppages). Frames are spaced evenly (see each sheet's label range); replays and graphics can interrupt live action.\n\n"
        "The event comes from the official game sheet (do not invent others):\n" + describe_incident(inc, teams) + "\n\n"
        "Our estimate (label 0) comes from matching the scoreboard clock and is usually within 10 s of the right stoppage; choose the "
        "candidate nearest 0 unless the frames clearly show the event elsewhere. Other sheet events near this one (their own "
        "stoppages are NOT this event; labels are where our system places them): " + (neighbours or "none") + ".\n\n"
    )
    if goal:
        text += ("Find the goal. play_start = the first frame of the possession that produced the goal: the faceoff win, the "
                 "turnover/possession change, or the zone entry that led directly to the shot (not an arbitrary earlier moment; if the "
                 "puck was in the offensive zone for a long cycle, start about 8 s before the shot). event_moment = puck crosses the "
                 "line. play_end = the last frame of the live celebration shots (players hugging, close-ups of the scorers) or the first replay/graphic, whichever comes first; a celebration that carries on while the broadcast shows the same players is still live. "
                 "A replay shows the same goal again from another angle: never treat a replay as the live event. The scoreboard clock "
                 "in the corner freezes when the goal is scored, so the frame where the clock stops is a strong cue for event_moment; "
                 "the score digits also change a while after.\n")
    elif wanted:
        text += ("This is a major / misconduct incident. Find it. If it is a fight, set fight.gloves_drop to when gloves come off or "
                 "players square up, fight.separated to when the linesmen have them apart, event_moment to the first punch; "
                 "play_start = a few seconds before the confrontation begins (the hit/scrum/words that triggered it), play_end = when "
                 "the players are sent off or the broadcast leaves the ice. If it is a scrum or a hit rather than a fight, set fight to null "
                 "and describe the incident the same way. A penalty is called at the next whistle, so the infraction is usually before "
                 "the whistle where the game clock stops.\n")
    else:
        text += ("Find where play stopped for this penalty: a whistle with a referee's arm raised, a delayed-penalty call, a player "
                 "skating to or sitting in the penalty box, or a scrum. event_moment = that stoppage (or the foul itself if you can see it). "
                 "The foul usually happens a few seconds BEFORE the whistle, and a trip or hook lasts under a second, so at this frame "
                 "spacing you often cannot see the foul itself: set event_visible true when a stoppage with a penalty signal is on screen, "
                 "even if the foul is not visible; a later pass with denser frames looks for the foul. play_start = about 8 s before the "
                 "stoppage, play_end = the referee's signal / the penalised player leaving the ice. Ignore stoppages with no penalty signal "
                 "(icing, offside, puck out of play, goalie freeze). Never end on a replay, a graphic wipe or the big FLO HOCKEY station ident.\n")
    text += ("Judge visibility strictly: event_visible is false when the sheets show no such event within the range (for example "
             "only a commercial, an intermission, or ordinary play). All time values are the labels printed on the frames "
             "(numbers, fractions allowed). Reply with JSON only: " + COARSE_SCHEMA)
    content: List[Dict[str, Any]] = [{"type": "text", "text": text}]
    for i, s in enumerate(sheets, 1):
        content.append({"type": "text", "text": f"Sheet {i}/{len(sheets)}: labels {s['first']:+.0f} to {s['last']:+.0f}"})
        content.append({"type": "image_url", "image_url": {"url": _data_url(s["path"])}})
    content.append({"type": "text", "text": "Now answer with the JSON object described above. All times are numbers taken from the frame labels."})
    return content


def refine_prompt(inc: Dict[str, Any], coarse: Dict[str, Any], boundaries: List[Tuple[str, float, List[Dict[str, Any]]]]) -> List[Dict[str, Any]]:
    kind = "goal" if inc["kind"] == "goal" else ("fight" if inc["fight"] else "penalty incident")
    if inc["kind"] == "penalty" and not inc["wanted"]:
        text = (
            "Hockey broadcast, one minor penalty: " + "; ".join(
                f"{r.get('team')} - {r.get('infraction')}" for r in inc["rows"]) + " (player names omitted on purpose: describe what you see, e.g. jersey colour and number)\n"
            "A first pass found the stoppage where it was called near label " + str(coarse.get("event_moment")) + ". The dense contact "
            "sheets (frames in time order, left to right then down, each labelled with its time in seconds, 0.5 s apart) cover the "
            "seconds before the whistle and the whistle itself. Find the foul that caused this penalty (trip, hook, slash, hold, hit, "
            "high stick...) and the whistle. start = 2 s before the foul (the cut begins with the play leading into it); if you cannot "
            "see a foul that fits, set foul_seen false and start = 8 s before the whistle. end = when the referee signals the penalty "
            "or the penalised player is led away, 2 s after the signal at most.\n"
            'Reply with JSON only: {"foul_seen": true/false, "foul_what": "what happens and who", "foul": <label or null>, '
            '"whistle": <label or null>, "start": <label>, "end": <label>, "end_what": "what is on screen at the cut"}'
        )
        content = [{"type": "text", "text": text}]
        for name, centre, sheets in boundaries:
            for sh in sheets:
                content.append({"type": "text", "text": f"{name.upper()} window around {centre:+.1f}: labels {sh['first']:+.1f} to {sh['last']:+.1f}"})
                content.append({"type": "image_url", "image_url": {"url": _data_url(sh["path"])}})
        content.append({"type": "text", "text": "Now answer with the JSON object described above (foul_seen, foul_what, foul, whistle, start, end, "
                                                "end_what). start and end must be numbers taken from the frame labels."})
        return content
    text = (
        f"Hockey broadcast, one {kind}. A first pass proposed cut points; confirm or correct each using the dense contact sheets "
        "(frames in time order, left to right then down, each labelled with its time in seconds; labels are on the same scale as "
        "before). For each boundary return the exact label of the best cut frame.\n"
        "  start = the first frame of the play that produced the event (the instant of the faceoff win, turnover, or the puck "
        "crossing the blue line); the cut should begin there, not before and not after.\n"
        "  end = the last frame worth keeping: the end of the celebration/the whistle/the linesmen getting the players apart; "
        "stop before a cut to a replay, graphic, or commercial.\n"
        f"First-pass proposal: start {coarse.get('play_start')} ({coarse.get('play_start_kind')}), end {coarse.get('play_end')} "
        f"({coarse.get('play_end_kind')}), event {coarse.get('event_moment')}. Reason given: {coarse.get('reason')}\n"
        'Reply with JSON only: {"start": <label>, "start_ok": true/false, "start_what": "what is on screen at the cut", '
        '"end": <label>, "end_ok": true/false, "end_what": "what is on screen at the cut"}'
    )
    content: List[Dict[str, Any]] = [{"type": "text", "text": text}]
    for name, centre, sheets in boundaries:
        for s in sheets:
            content.append({"type": "text", "text": f"{name.upper()} boundary candidates around {centre:+.1f}: labels {s['first']:+.1f} to {s['last']:+.1f}"})
            content.append({"type": "image_url", "image_url": {"url": _data_url(s["path"])}})
    content.append({"type": "text", "text": "Now answer with the JSON object described above (start, start_ok, start_what, end, end_ok, end_what). "
                                            "start and end must be numbers taken from the frame labels."})
    return content


# ======================================================================================
# review one incident
# ======================================================================================
def _num(v: Any) -> Optional[float]:
    try:
        f = float(v)
        return f if np.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def review_incident(inc: Dict[str, Any], video: Path, vid_duration: float, next_old_in: Optional[float],
                    teams: Dict[str, str], out_root: Path, others: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    res: Dict[str, Any] = {k: inc[k] for k in ("id", "kind", "wanted", "fight", "period", "time_elapsed", "video_time", "rows",
                                                "old_in", "old_out", "old_source", "clip_filename", "match_confidence",
                                                "match_unreliable", "refined_by") if k in inc}
    usage = {"prompt_tokens": 0, "completion_tokens": 0, "calls": 0}
    res["usage"] = usage
    t0 = inc["video_time"]
    if t0 is None:
        res.update(verdict="unplaced", note="engine has no video time for this event; nothing to review",
                   new_in=None, new_out=None, window_source="none")
        return res
    if inc["kind"] == "goal":
        before, after, step = GOAL_BEFORE, GOAL_AFTER, GOAL_STEP
    elif inc["wanted"]:
        before, after, step = WANTED_BEFORE, WANTED_AFTER, WANTED_STEP
    else:
        before, after, step = PENALTY_BEFORE, PENALTY_AFTER, PENALTY_STEP
    lo, hi = max(0.0, t0 - before), min(vid_duration, t0 + after)
    if hi - lo < 10:
        res.update(verdict="not_recorded", note="placed time is outside the recording", new_in=None, new_out=None, window_source="none")
        return res

    sheet_dir = out_root / "sheets" / inc["id"]
    parts = []
    for o in others or []:
        if o is inc or o["video_time"] is None or abs(o["video_time"] - t0) < 3 or abs(o["video_time"] - t0) > before + after + 30:
            continue
        what = "goal" if o["kind"] == "goal" else "penalty (" + "; ".join(str(r.get("infraction")) for r in o["rows"][:2]) + ")"
        parts.append(f'{what} at P{o["period"]} {o["time_elapsed"]} elapsed, placed at {o["video_time"] - t0:+.0f}')
    neighbours = "; ".join(parts)
    frames = extract_frames(video, lo, hi, step, CELL_W)
    sheets = make_sheets(frames, t0, sheet_dir, "coarse")
    res["coarse_frames"] = len(frames)
    r1 = call_vision(coarse_prompt(inc, teams, sheets, neighbours), required=("event_visible", "confidence"))
    for k in usage:
        usage[k] += r1["usage"][k]
    c = r1["answer"]
    res["coarse"] = c

    visible = bool(c.get("event_visible"))
    conf = _num(c.get("confidence")) or 0.0
    moment = _num(c.get("event_moment"))
    p_in, p_out = _num(c.get("play_start")), _num(c.get("play_end"))
    if not visible or conf < MIN_CONFIDENCE or moment is None or p_in is None or p_out is None:
        res.update(verdict="not_visible" if not visible else "unsure", new_in=inc["old_in"], new_out=inc["old_out"],
                   window_source="fallback_current", note=c.get("reason"))
        return res

    # ---- refine pass: dense frames around each coarse boundary ----
    try:
        bnds = []
        minor = inc["kind"] == "penalty" and not inc["wanted"]
        # minor penalty: one dense window covering the foul and the whistle, then the end boundary
        plan = ((("foul", moment - 20.0, moment + 2.0), ("end", p_out - REFINE_HALF, p_out + REFINE_HALF)) if minor else
                (("start", p_in - REFINE_HALF, p_in + REFINE_HALF), ("end", p_out - REFINE_HALF, p_out + REFINE_HALF)))
        for name, lo_rel, hi_rel in plan:
            centre = (lo_rel + hi_rel) / 2
            a = t0 + max(-before, lo_rel)
            b = t0 + min(after, hi_rel)
            fr = extract_frames(video, max(0.0, a), min(vid_duration, b), REFINE_STEP, CELL_W)
            bnds.append((name, centre, make_sheets(fr, t0, sheet_dir, f"refine_{name}", precise=True)))
        r2 = call_vision(refine_prompt(inc, c, bnds), required=("start", "end"))
        for k in usage:
            usage[k] += r2["usage"][k]
        rf = r2["answer"]
        res["refine"] = rf
        r_in, r_out = _num(rf.get("start")), _num(rf.get("end"))
        # accept a refined value only when it stays within the window we showed
        lo_in = moment - 20.0 if minor else p_in - REFINE_HALF - 0.5
        hi_in = moment + 2.0 if minor else p_in + REFINE_HALF + 0.5
        if r_in is not None and lo_in <= r_in <= hi_in:
            p_in = r_in
        elif minor:
            p_in = moment - 8.0
        if r_out is not None and abs(r_out - p_out) <= REFINE_HALF + 0.5:
            p_out = r_out
        if minor:
            res["infraction_seen"] = bool(rf.get("foul_seen"))
            res["infraction_what"] = rf.get("foul_what")
            f_t = _num(rf.get("foul"))
            if f_t is not None:
                res["infraction_video_time"] = round(t0 + f_t, 2)
    except Exception as exc:  # noqa: BLE001 - keep the coarse answer
        res["refine_error"] = str(exc)

    # ---- clamp ----
    notes: List[str] = []
    max_before = MAX_IN_BEFORE_FIGHT if (inc["fight"] or inc["wanted"]) else MAX_IN_BEFORE
    new_in = t0 + p_in
    new_out = t0 + p_out
    # "event" = where the model saw it happen (the engine's time can be off by a minute)
    if new_in < t0 + moment - max_before:
        new_in = t0 + moment - max_before
        notes.append(f"in-point clamped to {max_before:.0f}s before the event")
    if inc["kind"] == "goal":
        gm = t0 + moment
        if new_out < gm + GOAL_MIN_AFTER_MOMENT:
            new_out = gm + GOAL_MIN_AFTER_MOMENT
            notes.append("out-point extended to 8 s after the goal")
        if new_out > gm + GOAL_MAX_AFTER_MOMENT:
            new_out = gm + GOAL_MAX_AFTER_MOMENT
            notes.append("out-point clamped to 25 s after the goal")
        # The engine's clock-stop anchor is right more often than the model's read of the puck crossing
        # (09-12 P3 8:14: model said -8 s, the clock and the picture say the goal is at the anchor),
        # so never end before the engine's out-point when it is near the model's moment.
        if inc.get("old_out") is not None and new_out < inc["old_out"] <= gm + GOAL_MAX_AFTER_MOMENT:
            new_out = inc["old_out"]
            notes.append("out-point kept at the engine's (later) out-point")
        if new_in > gm - 3.0:
            new_in = gm - 3.0
            notes.append("in-point pulled back to 3 s before the goal")
    fight = c.get("fight") if isinstance(c.get("fight"), dict) else None
    if fight and inc["fight"]:
        sep = _num(fight.get("separated"))
        if sep is not None and new_out < t0 + sep + 2.0:
            new_out = t0 + sep + 2.0
            notes.append("out-point extended to 2 s past separation")
    if inc["wanted"] and fight is None and new_out > t0 + moment + MAX_WANTED_AFTER_MOMENT:
        new_out = t0 + moment + MAX_WANTED_AFTER_MOMENT
        notes.append(f"out-point capped {MAX_WANTED_AFTER_MOMENT:.0f}s after the incident")
    if fight and inc["wanted"]:
        sep = _num(fight.get("separated"))
        if sep is not None and new_out > t0 + sep + FIGHT_TAIL:
            new_out = t0 + sep + FIGHT_TAIL
            notes.append(f"out-point capped {FIGHT_TAIL:.0f}s after the players are separated")
    if next_old_in is not None and new_out > next_old_in - NEXT_EVENT_GAP and next_old_in > t0:
        new_out = max(t0 + 2.0, next_old_in - NEXT_EVENT_GAP)
        notes.append("out-point stopped before the next event's window")
    new_in = max(0.0, new_in)
    new_out = min(vid_duration, new_out)
    min_len = MIN_CLIP_PENALTY if inc["kind"] == "penalty" else MIN_CLIP
    if new_out - new_in < min_len:
        # lead-in is worth more than tail (a tail runs into replay wipes and station idents), so lengthen backwards first
        floor = max(0.0, t0 + moment - max_before)
        new_in = max(floor, new_out - min_len)
        if new_out - new_in < min_len:
            new_out = min(vid_duration, new_in + min_len)
            if next_old_in is not None and next_old_in > t0:
                new_out = min(new_out, max(new_in + 3.0, next_old_in - NEXT_EVENT_GAP))
        notes.append(f"clip lengthened to at least {min_len:.0f}s (lead-in first)")
    if new_out - new_in < 3.0 or new_out - new_in > MAX_CLIP:
        res.update(verdict="unsure", new_in=inc["old_in"], new_out=inc["old_out"], window_source="fallback_current",
                   note=f"proposed length {new_out - new_in:.1f}s outside {MIN_CLIP:.0f}-{MAX_CLIP:.0f}s; kept current window")
        return res
    if abs(moment) > MOVED_FAR_SECONDS:
        res["flag"] = f"moved_far: proposed event is {moment:+.0f}s from the engine's time; check by eye"
    res.update(verdict="reviewed", new_in=round(new_in, 2), new_out=round(new_out, 2), window_source="vision",
               event_moment_video=round(t0 + moment, 2), note=c.get("reason"), clamps=notes,
               play_start_kind=c.get("play_start_kind"), play_end_kind=c.get("play_end_kind"),
               replay_start_video=None if _num(c.get("replay_start")) is None else round(t0 + float(c["replay_start"]), 2))
    if fight:
        res["fight_detail"] = {k: (round(t0 + float(fight[k]), 2) if k in ("gloves_drop", "separated") and _num(fight.get(k)) is not None
                                   else fight.get(k)) for k in fight}
    return res


# ======================================================================================
# driver
# ======================================================================================
def render_clip(video: Path, start: float, end: float, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    cmd = ["nice", "-n", "10", "ffmpeg", "-v", "error", "-nostdin", "-y", "-ss", f"{start:.3f}", "-i", str(video),
           "-t", f"{end - start:.3f}", "-vf", "scale=1280:-2", "-c:v", "libx264", "-preset", "veryfast", "-crf", "24",
           "-threads", "3", "-c:a", "aac", "-b:a", "96k", str(dest)]
    subprocess.run(cmd, check=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--game-dir", required=True, type=Path)
    ap.add_argument("--video", required=True, type=Path)
    ap.add_argument("--out-dir", type=Path, default=None, help="default: ~/.local/state/watch-rams/clip-review/<game>")
    ap.add_argument("--render", action="store_true", help="cut the proposed (and, for comparison, the current) clips with ffmpeg")
    ap.add_argument("--only", default="", help="comma list of incident ids or prefixes (e.g. 01_goal) to review")
    ap.add_argument("--kinds", default="goal,penalty", help="goal, penalty, wanted (wanted = majors/fights/misconducts only)")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--render-only", action="store_true", help="render clips from an existing events_review.json without calling the model")
    ap.add_argument("--merge", action="store_true", help="merge these results into an existing events_review.json by incident id")
    args = ap.parse_args()

    game_dir = args.game_dir.resolve()
    video = args.video.resolve()
    out_root = (args.out_dir or STATE_ROOT / _slug(game_dir.name)).resolve()
    meta_path = game_dir / "data" / "game_metadata.json"
    info = (json.loads(meta_path.read_text()).get("game_info") or {}) if meta_path.exists() else {}
    teams = {"home": str(info.get("home_team") or ""), "away": str(info.get("away_team") or "")}

    if args.render_only:
        report = json.loads((out_root / "review" / "events_review.json").read_text())
        for r in report["events"]:
            if r.get("new_in") is None:
                continue
            render_clip(video, r["new_in"], r["new_out"], out_root / "clips" / f'{r["id"]}_new.mp4')
            if r.get("window_source") == "vision" and r.get("old_in") is not None:
                render_clip(video, r["old_in"], r["old_out"], out_root / "clips" / f'{r["id"]}_old.mp4')
        return 0

    probe = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(video)],
                           capture_output=True, text=True, check=True)
    vid_duration = float(probe.stdout.strip())

    incidents = build_incidents(game_dir)
    kinds = {k.strip() for k in args.kinds.split(",") if k.strip()}
    selected = [g for g in incidents if (g["kind"] in kinds) or ("wanted" in kinds and g["wanted"])]
    if args.only:
        wanted_ids = [s.strip() for s in args.only.split(",") if s.strip()]
        selected = [g for g in selected if any(g["id"].startswith(w) for w in wanted_ids)]

    def next_old_in(g: Dict[str, Any]) -> Optional[float]:
        later = [x["old_in"] for x in incidents if x["video_time"] is not None and g["video_time"] is not None
                 and x["video_time"] > g["video_time"] + 5 and x is not g]
        return min(later) if later else None

    (out_root / "review").mkdir(parents=True, exist_ok=True)
    results: List[Dict[str, Any]] = []
    t_start = time.time()

    def work(g: Dict[str, Any]) -> Dict[str, Any]:
        try:
            r = review_incident(g, video, vid_duration, next_old_in(g), teams, out_root, incidents)
            (out_root / "review").mkdir(parents=True, exist_ok=True)
            (out_root / "review" / f"{g['id']}.json").write_text(json.dumps(r, indent=2, default=str))
            return r
        except Exception as exc:  # noqa: BLE001 - one bad incident must not sink the game
            return {"id": g["id"], "kind": g["kind"], "verdict": "error", "error": str(exc), "video_time": g["video_time"],
                    "old_in": g["old_in"], "old_out": g["old_out"], "new_in": g["old_in"], "new_out": g["old_out"],
                    "window_source": "fallback_current", "rows": g["rows"], "wanted": g["wanted"], "fight": g["fight"],
                    "period": g["period"], "time_elapsed": g["time_elapsed"]}

    with cf.ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        for r in pool.map(work, selected):
            results.append(r)
            print(f'{r["id"]:34} {r["verdict"]:12} old {r.get("old_in")}..{r.get("old_out")} -> new {r.get("new_in")}..{r.get("new_out")}'
                  f' [{r.get("window_source")}]', flush=True)

    total = {"prompt_tokens": 0, "completion_tokens": 0, "calls": 0}
    for r in results:
        for k in total:
            total[k] += int((r.get("usage") or {}).get(k) or 0)
    counts: Dict[str, int] = {}
    for r in results:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    report = {"game_dir": str(game_dir), "video": str(video), "model": os.environ.get("SCOREBUG_VISION_MODEL") or DEFAULT_VISION_MODEL,
              "video_seconds": vid_duration, "counts": counts, "usage": total, "elapsed_seconds": round(time.time() - t_start, 1),
              "events": results}
    report_path = out_root / "review" / "events_review.json"
    if args.merge and report_path.exists():
        prev = json.loads(report_path.read_text())
        by_id = {e["id"]: e for e in prev.get("events", [])}
        by_id.update({e["id"]: e for e in results})
        report["events"] = [by_id[k] for k in sorted(by_id)]
        counts = {}
        total = {"prompt_tokens": 0, "completion_tokens": 0, "calls": 0}
        for e in report["events"]:
            counts[e["verdict"]] = counts.get(e["verdict"], 0) + 1
            for k in total:
                total[k] += int((e.get("usage") or {}).get(k) or 0)
        report["counts"], report["usage"] = counts, total
        results = report["events"]
    report_path.write_text(json.dumps(report, indent=2, default=str))

    if args.render:
        for r in results:
            if r.get("new_in") is None:
                continue
            render_clip(video, r["new_in"], r["new_out"], out_root / "clips" / f'{r["id"]}_new.mp4')
            if r.get("window_source") == "vision" and r.get("old_in") is not None:
                render_clip(video, r["old_in"], r["old_out"], out_root / "clips" / f'{r["id"]}_old.mp4')
    print(f"counts={counts} usage={total} -> {out_root / 'review' / 'events_review.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
