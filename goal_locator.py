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
   (same OpenAI-compatible settings as scorebug_detect: DeepSeek by default), one request per
   BATCH frames with GOAL_LOCATOR_WORKERS requests in flight (amherst-display#48: 24-45 s a
   request, 6 requests for the 2026-11-07 game, run one after another).
3. A goal candidate is a run of celebration/close-up frames containing at least one group
   celebration. When the number of candidates equals the number of unverified goals in the
   bracket they are assigned in order; otherwise nothing is set.

Remove: delete this file and the `_locate_goals_by_vision` step in pipeline.py.
"""

from __future__ import annotations

import json
import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

import cv2

from scorebug_detect import DEFAULT_VISION_BASE_URL, DEFAULT_VISION_MODEL, _jpeg_data_url

logger = logging.getLogger(__name__)

SCAN_STEP_SECONDS = 6
MAX_BRACKET_SECONDS = 1500
BATCH = 24
DEFAULT_WORKERS = 6
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


def _batch_bodies(video_path: str, lo: float, hi: float, model: str) -> List[Dict[str, Any]]:
    """One vision request per BATCH frames of a bracket: capture the frames now, send them later.

    Capturing all of a bracket's frames first (a few MB of 512 px JPEGs) is what lets every
    request go out at once instead of one per capture loop.
    """
    times = [lo + i * SCAN_STEP_SECONDS for i in range(int((hi - lo) // SCAN_STEP_SECONDS) + 1)]
    bodies: List[Dict[str, Any]] = []
    cap = cv2.VideoCapture(str(video_path))
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
            bodies.append({"model": model, "temperature": 0, "response_format": {"type": "json_object"},
                           "messages": [{"role": "user", "content": content}]})
    finally:
        cap.release()
    return bodies


def _request(base_url: str, api_key: str, body: Dict[str, Any]) -> Dict[str, Any]:
    import vision_api

    if "deepseek" in base_url:
        body["thinking"] = {"type": "disabled"}
    return vision_api.chat(base_url, api_key, body, 180)


def _read_labels(payloads: List[Dict[str, Any]], usage: Dict[str, int]) -> List[Tuple[float, str]]:
    labels: List[Tuple[float, str]] = []
    for payload in payloads:
        for k in ("prompt_tokens", "completion_tokens"):
            usage[k] = usage.get(k, 0) + int((payload.get("usage") or {}).get(k) or 0)
        for row in json.loads(payload["choices"][0]["message"]["content"]).get("frames", []):
            try:
                labels.append((float(row["t"]), str(row["label"]).strip().lower()))
            except (KeyError, TypeError, ValueError):
                continue
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
                 remaining_of, progress=None) -> Dict[str, Any]:
    """
    Set `video_time` / `refined_by="vision_celebration"` on goals it can place.
    `remaining_of(goal)` returns the goal's remaining seconds in its period.
    `progress` is optional: an object with plan(n, detail) / tick(n, detail) / note(detail), the
    pipeline's live-progress adapter; without it the scan is silent.
    """
    report: Dict[str, Any] = {"usage": {}, "brackets": []}
    settings = _vision_settings()
    if settings is None:
        report["skipped"] = "no vision API key"
        return report
    groups: Dict[Tuple[float, float], List[Dict[str, Any]]] = {}
    for goal in goals:
        span = bracket(int(goal.get("period") or 0), int(remaining_of(goal)), readings)
        if span is None:
            report["brackets"].append({"goal": f'P{goal.get("period")} {goal.get("time")}', "bracket": None})
            continue
        groups.setdefault(span, []).append(goal)
    if not groups:
        return report
    api_key, base_url, model = settings
    workers = max(1, int(os.environ.get("GOAL_LOCATOR_WORKERS", str(DEFAULT_WORKERS)) or DEFAULT_WORKERS))
    pending: Dict[Tuple[float, float], List[Future]] = {}
    batches = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        # Every bracket's requests are in flight together: a bracket can be one request, and the
        # last one used to wait for the first bracket's whole scan.
        for (lo, hi) in groups:
            bodies = _batch_bodies(video_path, lo, hi, model)
            batches += len(bodies)
            if progress is not None:
                progress.plan(len(bodies), f"vision locator: {len(bodies)} frame batch(es) at {lo:.0f}-{hi:.0f}s")
            pending[(lo, hi)] = [pool.submit(_request, base_url, api_key, body) for body in bodies]
        read = 0
        payloads: Dict[Tuple[float, float], List[Dict[str, Any]]] = {}
        for span, futures in pending.items():
            payloads[span] = []
            for future in futures:
                try:
                    payloads[span].append(future.result())
                except Exception as exc:  # one failed batch: label what came back, place nothing on a partial bracket
                    logger.warning("Vision goal locator: request for %.0f-%.0fs failed: %s", span[0], span[1], exc)
                read += 1
                if progress is not None:
                    progress.tick(1, f"vision locator: {read}/{batches} frame batches read")
    for (lo, hi), members in groups.items():
        labels = _read_labels(payloads.get((lo, hi)) or [], report["usage"])
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
