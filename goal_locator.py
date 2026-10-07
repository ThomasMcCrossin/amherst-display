"""
Find a goal in the video when the scorebug can't say when it happened.

Flo's bug is operator-driven and sometimes freezes (clock and score) for minutes of play,
so clock matching has nothing to anchor a goal to. What doesn't freeze is the broadcast:
after a goal the scoring team celebrates (group hug, close-ups of the scorer) and play
restarts with a faceoff at the centre-ice dot.

1. Bracket: from the scorebug readings in the goal's period, the goal lies after the clock
   first showed the closest value above the goal's remaining time and before it first
   showed a value below it (a frozen clock just makes the bracket wider).
2. Scan: frames every few seconds across the bracket are labelled by a cheap vision model
   (same OpenAI-compatible settings as scorebug_detect: DeepSeek by default).
3. A goal candidate is a run of celebration/close-up frames containing at least one group
   celebration. When the number of candidates equals the number of unverified goals in the
   bracket they are assigned in order; otherwise nothing is set.

Remove: delete this file and the `_locate_goals_by_vision` step in pipeline.py.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import cv2

from scorebug_detect import DEFAULT_VISION_BASE_URL, DEFAULT_VISION_MODEL, _jpeg_data_url

logger = logging.getLogger(__name__)

SCAN_STEP_SECONDS = 6
MAX_BRACKET_SECONDS = 1500
BATCH = 24
# Celebrations start a few seconds after the puck crosses the line.
CELEBRATION_LAG_SECONDS = 5
GOAL_LABELS = {"celebration", "closeup"}

PROMPT = (
    "Frames from one hockey broadcast, {step} s apart. Label every frame with exactly one of:\n"
    "- celebration: players of one team hugging, fist-bumping or skating to their bench in a group\n"
    "- closeup: tight shot of one or two players filling much of the picture\n"
    "- faceoff_center: players lined up for a faceoff on the centre-ice dot (red centre line, centre logo)\n"
    "- faceoff_other: a faceoff anywhere else\n"
    "- play: ordinary wide shot of play\n"
    "- other: replay, crowd, graphics, stoppage or anything else\n"
    'JSON only: {{"frames": [{{"t": <t>, "label": "..."}}]}}'
)


def _vision_settings() -> Optional[Tuple[str, str, str]]:
    api_key = os.environ.get("SCOREBUG_VISION_API_KEY") or os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        return None
    base_url = (os.environ.get("SCOREBUG_VISION_BASE_URL") or DEFAULT_VISION_BASE_URL).rstrip("/")
    return api_key, base_url, os.environ.get("SCOREBUG_VISION_MODEL") or DEFAULT_VISION_MODEL


def bracket(period: int, remaining: int, readings: List[Dict[str, Any]]) -> Optional[Tuple[float, float]]:
    rows = sorted(
        (float(r["video_time"]), int(r["game_time_seconds"]))
        for r in readings
        if r.get("period") == period and r.get("game_time_seconds") is not None and r.get("video_time") is not None
    )
    above = [(t, s) for t, s in rows if s > remaining]
    if not above:
        return None
    closest = min(s for _, s in above)
    lo = min(t for t, s in above if s == closest)
    below = [t for t, s in rows if s < remaining and t > lo]
    if not below:
        return None
    hi = min(below)
    return (lo, hi) if 0 < hi - lo <= MAX_BRACKET_SECONDS else None


def label_frames(video_path: str, lo: float, hi: float, usage: Dict[str, int]) -> List[Tuple[float, str]]:
    import requests

    settings = _vision_settings()
    if settings is None:
        return []
    api_key, base_url, model = settings
    times = [lo + i * SCAN_STEP_SECONDS for i in range(int((hi - lo) // SCAN_STEP_SECONDS) + 1)]
    cap = cv2.VideoCapture(str(video_path))
    labels: List[Tuple[float, str]] = []
    try:
        for i in range(0, len(times), BATCH):
            content: List[Dict[str, Any]] = [{"type": "text", "text": PROMPT.format(step=SCAN_STEP_SECONDS)}]
            for t in times[i:i + BATCH]:
                cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000.0)
                ok, frame = cap.read()
                if not ok or frame is None:
                    continue
                content.append({"type": "text", "text": f"t={int(t)}"})
                content.append({"type": "image_url", "image_url": {"url": _jpeg_data_url(frame, width=512)}})
            body: Dict[str, Any] = {"model": model, "temperature": 0, "response_format": {"type": "json_object"},
                                    "messages": [{"role": "user", "content": content}]}
            if "deepseek" in base_url:
                body["thinking"] = {"type": "disabled"}
            resp = requests.post(f"{base_url}/chat/completions", json=body, timeout=180,
                                 headers={"Authorization": f"Bearer {api_key}"})
            resp.raise_for_status()
            payload = resp.json()
            for k in ("prompt_tokens", "completion_tokens"):
                usage[k] = usage.get(k, 0) + int((payload.get("usage") or {}).get(k) or 0)
            for row in json.loads(payload["choices"][0]["message"]["content"]).get("frames", []):
                try:
                    labels.append((float(row["t"]), str(row["label"]).strip().lower()))
                except (KeyError, TypeError, ValueError):
                    continue
    finally:
        cap.release()
    return sorted(labels)


def candidates(labels: List[Tuple[float, str]]) -> List[float]:
    """Start times of runs of goal-ish frames that contain at least one celebration.

    Close-ups happen at every stoppage, so they only extend a run; a group celebration is
    what marks a goal. Centre-ice faceoffs are recorded in the report but not required:
    the model misses them too often to gate on.
    """
    found: List[float] = []
    run: List[Tuple[float, str]] = []

    def close_run():
        if any(label == "celebration" for _, label in run):
            found.append(run[0][0])

    for t, label in labels:
        if label in GOAL_LABELS:
            if run and t - run[-1][0] > 3 * SCAN_STEP_SECONDS:
                close_run()
                run = []
            run.append((t, label))
    if run:
        close_run()
    return found


def locate_goals(video_path: str, goals: List[Dict[str, Any]], readings: List[Dict[str, Any]],
                 remaining_of) -> Dict[str, Any]:
    """
    Set `video_time` / `refined_by="vision_celebration"` on goals it can place.
    `remaining_of(goal)` returns the goal's remaining seconds in its period.
    """
    report: Dict[str, Any] = {"usage": {}, "brackets": []}
    if _vision_settings() is None:
        report["skipped"] = "no vision API key"
        return report
    groups: Dict[Tuple[float, float], List[Dict[str, Any]]] = {}
    for goal in goals:
        span = bracket(int(goal.get("period") or 0), int(remaining_of(goal)), readings)
        if span is None:
            report["brackets"].append({"goal": f'P{goal.get("period")} {goal.get("time")}', "bracket": None})
            continue
        groups.setdefault(span, []).append(goal)
    for (lo, hi), members in groups.items():
        labels = label_frames(video_path, lo, hi, report["usage"])
        found = candidates(labels)
        entry = {"bracket": [lo, hi], "goals": [f'P{g.get("period")} {g.get("time")}' for g in members],
                 "candidates": found, "labels": [[t, lab] for t, lab in labels if lab != "play"]}
        report["brackets"].append(entry)
        if len(found) != len(members):
            logger.warning("Vision goal locator: %d candidate(s) for %d goal(s) in %.0f-%.0fs; leaving them unverified",
                           len(found), len(members), lo, hi)
            continue
        ordered = sorted(members, key=lambda g: -int(remaining_of(g)))  # earlier in the period first
        for goal, start in zip(ordered, found):
            goal["video_time_original"] = goal.get("video_time")
            goal["video_time"] = max(lo, start - CELEBRATION_LAG_SECONDS)
            goal["refined_by"] = "vision_celebration"
            logger.info("Vision goal locator: P%s %s at %.0fs", goal.get("period"), goal.get("time"), goal["video_time"])
    return report
