#!/usr/bin/env python3
"""Time step 5 (match_events) on one real recording, and compare two runs (amherst-display#48).

Step 5 is the OCR clock-stop pass plus the vision goal locator: about 7 minutes on the
2026-11-07 Ramblers vs Truro run (180 s clock stop, 237 s vision locator). Re-running the whole
pipeline to time it costs another 9 minutes of OCR extraction, and the joined recording is
deleted once the game is archived. This replays the events and OCR readings that production
produced against a recording timeline that still exists on disk, so the pass can be timed and
checked for identical anchors before and after a change.

  .venv/bin/python scripts/bench_match_events.py \
      --game-dir "/home/canteenhub/amherst-display/Games/2026-11-07_Amherst Ramblers_vs_Truro Bearcats" \
      --video ~/.local/state/watch-rams/archive/20261107T231505Z_e258346/recording.mp4 \
      --video-offset 2438.472 \
      --out ~/.local/state/watch-rams/issue-48/match/baseline-1 --label baseline-1

  ...then --compare-with baseline-1 on a later run.

The game folder is read only. Everything the pipeline writes goes to --out. `--video-offset` is
the production joined-timeline second where --video starts: the 2026-11-07 join is six live
capture runs (postgame/20261107T223121Z_e258346/joined.json) and the archive keeps the fifth
segment's recording as .../archive/20261107T231505Z_e258346/recording.mp4, which covers joined
2438.472 s to 9903.947 s (six of the eight goals and the P2 vision bracket).

Vision labels are model output: two runs of the same code can disagree (temperature 0 is not a
determinism guarantee), so compare the clock-stop anchors, which must be identical, separately
from the vision-placed ones.

Remove: delete this file; nothing imports it.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

# Timed pipeline phases. Everything in step 5 that is not one of these is matching + saving,
# which is a fraction of a second.
PHASES = (
    ("clock_stop", "_refine_goal_events_by_clock_stop"),
    ("local_ocr", "_refine_low_confidence_events_by_local_ocr"),
    ("vision", "_locate_goals_by_vision"),
)


def _cpu_seconds() -> float:
    me = resource.getrusage(resource.RUSAGE_SELF)
    kids = resource.getrusage(resource.RUSAGE_CHILDREN)
    return me.ru_utime + me.ru_stime + kids.ru_utime + kids.ru_stime


def _read_json(path: Path):
    return json.loads(path.read_text())


def _load_inputs(game_dir: Path, offset: float, duration: float) -> tuple[list, list]:
    """Production readings and events, shifted onto the --video timeline."""
    readings = []
    for row in _read_json(game_dir / "data" / "video_timestamps.json"):
        t = float(row.get("video_time") or 0.0)-offset
        if 0.0 <= t <= duration:
            readings.append(dict(row, video_time=t))

    events = []
    for row in _read_json(game_dir / "data" / "matched_events.json"):
        event = dict(row)
        # Undo the clock-stop / vision refinement: replay starts where matching left the events.
        if event.get("video_time_original") is not None:
            event["video_time"] = event.pop("video_time_original")
        else:
            event.pop("video_time_original", None)
        for key in ("refined_by", "goal_clock_verified", "goal_timing_source"):
            event.pop(key, None)
        if event.get("video_time") is None:
            continue
        t = float(event["video_time"])-offset
        if not (0.0 <= t <= duration):
            continue
        event["video_time"] = t
        events.append(event)
    return readings, events


def _vision_env() -> None:
    """Same vision endpoint as the postgame runner (ollama-cloud DeepSeek Flash, low think)."""
    os.environ.setdefault("SCOREBUG_VISION_BASE_URL", "https://ollama.com/v1")
    os.environ.setdefault("SCOREBUG_VISION_MODEL", "deepseek-v4.1-flash:1017")
    os.environ.setdefault("SCOREBUG_VISION_THINK", "low")
    if os.environ.get("SCOREBUG_VISION_API_KEY"):
        return
    env_file = Path(os.environ.get("RAMS_VISION_ENV_FILE", Path.home() / ".config/curlys/ollama-cloud/active.env"))
    for line in env_file.read_text().splitlines() if env_file.exists() else []:
        if line.startswith("OLLAMA_CLOUD_API_KEY="):
            os.environ["SCOREBUG_VISION_API_KEY"] = line.split("=", 1)[1].strip().strip('"').strip("'")


def _compare(a: Path, b: Path) -> bool:
    """Per-event diff of two runs, clock-stop placements treated as the contract and vision apart."""

    def key(events):
        return {(e.get("type"), e.get("period"), e.get("time")): e for e in events}

    left, right = key(_read_json(a / "matched_events.json")), key(_read_json(b / "matched_events.json"))
    keys = sorted(set(left) | set(right), key=lambda k: (str(k[0]), str(k[1]), str(k[2])))
    moved = vision = 0
    print(f"{'event':26s} {'video_time':>20s} -> {'video_time':<20s} refined_by")
    for k in keys:
        la, rb = left.get(k, {}), right.get(k, {})
        va, vb = la.get("video_time"), rb.get("video_time")
        same = va == vb or (va is not None and vb is not None and abs(float(va) - float(vb)) < 0.01)
        by = str(rb.get("refined_by"))
        if not same:
            if "vision" in by or "vision" in str(la.get("refined_by")):
                vision += 1
                note = "  vision-placed, model labels differ"
            else:
                moved += 1
                note = "  DIFF"
        else:
            note = ""
        print(f"{f'{k[0]} P{k[1]} {k[2]}':26s} {str(va):>20s} -> {str(vb):<20s} {by}{note}")
    print(f"{len(keys)} events: {moved} anchor difference(s), {vision} vision-placed difference(s)")
    if not moved:
        print("clock-stop and matched anchors identical")
    if vision:
        print("vision labels are model output and vary between runs; compare placements, not labels")
    return moved == 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--game-dir", type=Path, help="production game folder (read only)")
    ap.add_argument("--video", type=Path, help="recording to replay against")
    ap.add_argument("--video-offset", type=float, default=0.0,
                    help="production joined-timeline second where --video starts")
    ap.add_argument("--out", type=Path, help="output folder for this run")
    ap.add_argument("--label", default="", help="name recorded in bench.json")
    ap.add_argument("--broadcast", default="flo_stacked_topleft",
                    help="pinned broadcast/scorebug layout (execution profile)")
    ap.add_argument("--compare-with", type=Path, help="another run's --out folder to diff against")
    args = ap.parse_args()

    if args.compare_with:
        ok = _compare(args.out or args.game_dir / "data", args.compare_with)
        return 0 if ok else 1
    if args.out is None:
        ap.error("--out is required unless --compare-with is used")
    if args.game_dir is None or args.video is None:
        ap.error("--game-dir and --video are required unless --compare-with is used")

    out = args.out.resolve()
    for sub in ("data", "game", "output"):
        (out / sub).mkdir(parents=True, exist_ok=True)
    # Live progress from the pipeline lands here, never in the production status file.
    os.environ["PIPELINE_PROGRESS_JSON"] = str(out / "progress.json")
    _vision_env()

    sys.path.insert(0, str(REPO))
    import config  # noqa: PLC0415
    from highlight_extractor import event_matcher  # noqa: PLC0415
    from highlight_extractor.ocr_engine import OCREngine  # noqa: PLC0415
    from highlight_extractor.pipeline import HighlightPipeline  # noqa: PLC0415
    from highlight_extractor.video_processor import VideoProcessor  # noqa: PLC0415

    config.AUTO_UPLOAD_GAME_ARCHIVES_TO_DRIVE = False
    config.RESEND_API_KEY = ""
    config.NOTIFICATION_EMAIL_TO = ""
    for cls in vars(event_matcher).values():
        if isinstance(cls, type) and hasattr(cls, "send_email_alert"):
            cls.send_email_alert = lambda self, *a, **k: False

    print(f"loading {args.video}")
    video = VideoProcessor(args.video, config)
    if not video.load_video():
        print("could not load --video", file=sys.stderr)
        return 2
    readings, events = _load_inputs(args.game_dir, args.video_offset, float(video.duration))
    print(f"{len(readings)} readings, {len(events)} events on a {video.duration:.0f}s timeline")

    pipeline = HighlightPipeline(config, args.video, video_processor=video, ocr_engine=OCREngine(config))
    pipeline.game_folders = {"game_dir": out / "game", "data_dir": out / "data", "output_dir": out / "output"}
    pipeline.video_timestamps = readings
    pipeline.events = events
    pipeline._goals = []          # typed-goal matching already ran in production; events are hydrated
    pipeline.reel_mode = "goals_only"
    pipeline._pending_broadcast_type = args.broadcast
    pipeline._detected_game_start_time = 0.0

    timings: dict[str, float] = {}
    for name, attr in PHASES:
        original = getattr(pipeline, attr)

        def timed(*a, _original=original, _name=name, **k):
            started = time.time()
            try:
                return _original(*a, **k)
            finally:
                timings[_name] = timings.get(_name, 0.0) + time.time() - started

        setattr(pipeline, attr, timed)

    from pipeline_progress import progress  # noqa: PLC0415

    progress.stage("match_events")
    cpu_before, wall_before = _cpu_seconds(), time.time()
    pipeline._step5_match_events(recording_game_start_time=0.0)
    wall = time.time() - wall_before
    cpu = _cpu_seconds() - cpu_before
    progress.finish("bench run complete")

    phases = {"match_and_save": round(wall - sum(timings.values()), 2)}
    phases.update({k: round(v, 2) for k, v in timings.items()})
    progress_doc = _read_json(out / "progress.json").get("progress", {}) if (out / "progress.json").exists() else {}
    locator = _read_json(out / "data" / "goal_locator.json") if (out / "data" / "goal_locator.json").exists() else {}
    bench = {
        "label": args.label or out.name,
        "game_dir": str(args.game_dir),
        "video": str(args.video),
        "video_offset": args.video_offset,
        "broadcast": args.broadcast,
        "events": len(events),
        "readings": len(readings),
        "wall_s": round(wall, 2),
        "cpu_s": round(cpu, 2),
        "phases": phases,
        "step_timings": getattr(pipeline, "_step_timings", {}),
        "loadavg": [round(v, 2) for v in os.getloadavg()],
        "progress_line": progress_doc.get("line"),
        "progress_history": [row.get("summary") for row in progress_doc.get("history") or []],
        "vision_usage": (locator.get("usage") or {}),
        "vision_brackets": [
            {"bracket": row.get("bracket"), "goals": row.get("goals"), "candidates": row.get("candidates"),
             "labels": len(row.get("labels") or [])}
            for row in locator.get("brackets") or []
        ],
        "matched_events": [
            {k: e.get(k) for k in ("type", "period", "time", "video_time", "video_time_original",
                                   "refined_by", "goal_timing_source", "goal_clock_verified",
                                   "match_confidence", "match_unreliable")}
            for e in pipeline.matched_events
        ],
    }
    (out / "bench.json").write_text(json.dumps(bench, indent=2) + "\n")
    (out / "matched_events.json").write_text(json.dumps(pipeline.matched_events, indent=2) + "\n")
    if locator:
        (out / "goal_locator.json").write_text(json.dumps(locator, indent=2) + "\n")

    print(json.dumps({k: bench[k] for k in ("label", "wall_s", "cpu_s", "phases", "loadavg",
                                          "vision_usage", "vision_brackets")}, indent=2))
    print("progress:", bench["progress_line"])
    for line in bench["progress_history"]:
        if line:
            print("  history:", line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
