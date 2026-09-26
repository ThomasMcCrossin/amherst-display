#!/usr/bin/env python3
"""
Check every HockeyTech goal in a processed game against the source recording.

For each goal the pipeline matched, frames are pulled from the source video on both sides
of the matched time (the clip itself ends ~3 s after the goal, before the scorebug updates)
and a cheap vision model (DeepSeek by default, see scorebug_detect) reads the scorebug in
each frame and says whether a goal is visible. The verdict is then computed here, not by
the model:

  confirmed   the scoring team's score goes up by one across the goal time and the bug
              clock at the goal agrees with HockeyTech (remaining time, within tolerance)
  visual      the bug was frozen, so only the picture could be checked, and it shows a goal
  suspect     the model's reads disagree with HockeyTech (score or clock); look at it
  unclear     the bug could not be read well enough to decide
  missed      HockeyTech has the goal, the recording covers that moment, but no clip was made
  not_recorded the goal happened outside what the recording covers (joined late, ended early)

Writes <game-dir>/data/goal_validation.json and prints a one-line summary per goal.

  .venv/bin/python scripts/validate_goal_clips.py --game-dir "Games/<game>" --video <recording.mp4>

Remove: delete this file; nothing imports it.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import cv2

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scorebug_detect import DEFAULT_VISION_BASE_URL, DEFAULT_VISION_MODEL, _jpeg_data_url  # noqa: E402

# Seconds relative to the matched goal time. The bug usually updates the score within
# ~10-40 s of the goal (after the celebration / during the replay).
OFFSETS = [-30, -12, -3, 0, 3, 15, 35, 60]
FULL_FRAME_OFFSETS = {-3, 0, 3}
CLOCK_TOLERANCE_SECONDS = 6
PERIOD_SECONDS = 20 * 60


def _grab(cap, t: float):
    cap.set(cv2.CAP_PROP_POS_MSEC, max(0.0, t) * 1000.0)
    ok, frame = cap.read()
    return frame if ok else None


def _clock_seconds(text: Any) -> Optional[int]:
    m = re.match(r"^\s*(\d{1,2})[:.](\d{2})", str(text or ""))
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def _team_key(name: str) -> str:
    # "West Kent Steamers" -> "steamers"; bugs show nicknames or 3-letter codes.
    return (name or "").strip().split()[-1].lower() if name else ""


def ask_vision(frames: List[Dict[str, Any]], goal: Dict[str, Any]) -> Dict[str, Any]:
    import requests

    api_key = os.environ.get("SCOREBUG_VISION_API_KEY") or os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("no vision API key (DEEPSEEK_API_KEY or SCOREBUG_VISION_API_KEY)")
    base_url = (os.environ.get("SCOREBUG_VISION_BASE_URL") or DEFAULT_VISION_BASE_URL).rstrip("/")
    model = os.environ.get("SCOREBUG_VISION_MODEL") or DEFAULT_VISION_MODEL
    content: List[Dict[str, Any]] = [{"type": "text", "text": (
        "Frames from one hockey broadcast around a moment when a goal may have been scored. "
        "For each labelled frame, read the on-screen scorebug: both team labels with their "
        "scores, the period, and the game clock exactly as shown. Use null for anything not "
        "visible (replays and graphics often hide the bug). Full frames are included near the "
        "moment itself; say whether they show a goal (puck in net, goal light, celebration)."
    )}]
    for f in frames:
        content.append({"type": "text", "text": f'Frame "{f["label"]}" ({f["offset"]:+d} s), top band:'})
        content.append({"type": "image_url", "image_url": {"url": _jpeg_data_url(f["band"], width=1280)}})
        if f.get("full") is not None:
            content.append({"type": "text", "text": f'Frame "{f["label"]}" full picture:'})
            content.append({"type": "image_url", "image_url": {"url": _jpeg_data_url(f["full"], width=640)}})
    content.append({"type": "text", "text": (
        'Reply with JSON only: {"frames": [{"label": "...", "teams": [{"name": "...", "score": n}, '
        '{"name": "...", "score": n}] or null, "period": "..." or null, "clock": "m:ss" or null}], '
        '"goal_visible": true/false/null, "notes": "one sentence"}'
    )})
    body: Dict[str, Any] = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [{"role": "user", "content": content}],
    }
    if "deepseek" in base_url:
        body["thinking"] = {"type": "disabled"}
    resp = requests.post(f"{base_url}/chat/completions", json=body, timeout=180,
                         headers={"Authorization": f"Bearer {api_key}"})
    resp.raise_for_status()
    payload = resp.json()
    answer = json.loads(payload["choices"][0]["message"]["content"])
    return {"answer": answer, "usage": payload.get("usage", {}), "model": model}


def judge(event: Dict[str, Any], answer: Dict[str, Any]) -> Dict[str, Any]:
    """Deterministic verdict from the model's per-frame reads."""
    key = _team_key(event.get("team", ""))
    reads = {f.get("label"): f for f in answer.get("frames") or []}

    def score_for_team(read) -> Optional[int]:
        for t in (read or {}).get("teams") or []:
            name = str(t.get("name") or "").lower()
            if key and (key in name or name in key or name[:3] == key[:3]) and isinstance(t.get("score"), int):
                return t["score"]
        return None

    before = [score_for_team(reads.get(f"t{o:+d}")) for o in OFFSETS if o < 0]
    after = [score_for_team(reads.get(f"t{o:+d}")) for o in OFFSETS if o > 3]
    before = [s for s in before if s is not None]
    after = [s for s in after if s is not None]
    score_ok = None
    if before and after:
        score_ok = max(after) == before[-1] + 1

    clock_ok, clock_diff = None, None
    expected = event.get("time_remaining_seconds")
    at_goal = [_clock_seconds(reads.get(f"t{o:+d}", {}).get("clock")) for o in (-3, 0, 3)]
    at_goal = [c for c in at_goal if c is not None]
    if expected is not None and at_goal and int(event.get("period") or 0) <= 3:
        clock_diff = min(abs(c - int(expected)) for c in at_goal)
        clock_ok = clock_diff <= CLOCK_TOLERANCE_SECONDS

    clocks_seen = [f.get("clock") for f in reads.values() if f.get("clock")]
    frozen_bug = len(clocks_seen) >= 4 and len(set(clocks_seen)) == 1
    if frozen_bug:
        # The bug itself is stuck (Flo operator), so neither score nor clock can confirm;
        # fall back on what the picture shows.
        verdict = "visual" if answer.get("goal_visible") else "unclear"
    elif score_ok is True and clock_ok is not False:
        verdict = "confirmed"
    elif score_ok is False or clock_ok is False:
        verdict = "suspect"
    else:
        verdict = "unclear"
    return {"verdict": verdict, "frozen_bug": frozen_bug, "score_before": before, "score_after": after, "score_increment_ok": score_ok,
            "clock_at_goal": at_goal, "clock_diff_seconds": clock_diff, "clock_ok": clock_ok,
            "goal_visible": answer.get("goal_visible"), "notes": answer.get("notes")}


def _covered(event: Dict[str, Any], readings: List[Dict[str, Any]]) -> bool:
    """True when scorebug readings in the goal's period bracket its remaining time."""
    remaining = event.get("time_remaining_seconds")
    clocks = [int(r["game_time_seconds"]) for r in readings
              if r.get("period") == event.get("period") and r.get("game_time_seconds") is not None]
    return remaining is not None and bool(clocks) and min(clocks) <= int(remaining) <= max(clocks)


def validate(game_dir: Path, video: Path) -> Dict[str, Any]:
    log = json.loads((game_dir / "data" / "event_matching_log.json").read_text())
    readings_path = game_dir / "data" / "video_timestamps.json"
    readings = json.loads(readings_path.read_text()) if readings_path.exists() else []
    clips = json.loads((game_dir / "data" / "clips_manifest.json").read_text()).get("clips", [])
    clip_by_key = {(c.get("period"), c.get("time")): c for c in clips if c.get("type") == "goal"}
    cap = cv2.VideoCapture(str(video))
    results, usage = [], {"prompt_tokens": 0, "completion_tokens": 0}
    try:
        for entry in log.get("entries", []):
            event = entry.get("event") or {}
            if event.get("type") != "goal":
                continue
            clip = clip_by_key.get((event.get("period"), event.get("time_boxscore")))
            row: Dict[str, Any] = {"period": event.get("period"), "time_elapsed": event.get("time_boxscore"),
                                   "team": event.get("team"), "player": event.get("player"),
                                   "clip": (clip or {}).get("clip_filename")}
            if not clip:
                row.update(verdict="missed" if _covered(event, readings) else "not_recorded",
                           reason=(entry.get("match_result") or {}).get("unreliable_reason"))
                results.append(row)
                continue
            t0 = float(clip["video_time"])
            frames = []
            for o in OFFSETS:
                frame = _grab(cap, t0 + o)
                if frame is None:
                    continue
                band = frame[: max(1, int(frame.shape[0] * 0.22))]
                frames.append({"label": f"t{o:+d}", "offset": o, "band": band,
                               "full": frame if o in FULL_FRAME_OFFSETS else None})
            try:
                vision = ask_vision(frames, event)
            except Exception as exc:
                row.update(verdict="unclear", error=str(exc))
                results.append(row)
                continue
            for k in usage:
                usage[k] += int(vision["usage"].get(k) or 0)
            row.update(video_time=t0, **judge(event, vision["answer"]), reads=vision["answer"].get("frames"))
            results.append(row)
    finally:
        cap.release()
    counts: Dict[str, int] = {}
    for r in results:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    report = {"game_dir": str(game_dir), "video": str(video), "counts": counts, "usage": usage, "goals": results}
    (game_dir / "data" / "goal_validation.json").write_text(json.dumps(report, indent=2))
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--game-dir", required=True, type=Path)
    ap.add_argument("--video", required=True, type=Path)
    args = ap.parse_args()
    report = validate(args.game_dir.resolve(), args.video.resolve())
    for r in report["goals"]:
        extra = "" if r["verdict"] in ("missed", "not_recorded") else (
            f' score {r.get("score_before")}->{r.get("score_after")} clock_diff={r.get("clock_diff_seconds")}'
            f' visible={r.get("goal_visible")}')
        print(f'{r["verdict"]:12} P{r["period"]} {r["time_elapsed"]} {r["team"]} {r["player"]}{extra}')
    print(f'counts={report["counts"]} usage={report["usage"]}')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
