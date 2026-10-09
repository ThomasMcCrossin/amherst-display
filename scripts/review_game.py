#!/usr/bin/env python3
"""
Vision review of one processed game's highlight clips: the single entry point.

Code builds an incident packet per game-sheet goal/penalty the engine placed; a cheap vision
reviewer (api or agent backend) picks in/out points from the play on screen and may overrule
the engine (adjust, relocate within bounds, drop); an optional adversary tries to refute each
goal/major verdict (dispute -> second review -> held_for_human); --apply writes overrides and
cuts reviewed clips and reel manifests; --build-reel renders them.

  .venv/bin/python scripts/review_game.py --game-dir "Games/<game>" --video <recording.mp4> \
      [--backend api|agent|escalate] [--adversary] [--apply [--reel-mode goals|with-rough|separate-rough] [--build-reel]]

Backends:
  api    OpenAI-compatible vision endpoint (default DeepSeek). Env SCOREBUG_VISION_API_KEY or
         DEEPSEEK_API_KEY, SCOREBUG_VISION_BASE_URL, SCOREBUG_VISION_MODEL.
  agent  any agent harness that can Read images and run bash: CLIP_REVIEW_AGENT_CMD (reviewer),
         CLIP_REVIEW_ADVERSARY_CMD (adversary). "{prompt}" in the command is replaced by the
         prompt; without it the prompt goes on stdin; "{skill}" is the skill dir. Examples in README.md.
  escalate  api for every incident; the agent (CLIP_REVIEW_AGENT_CMD) re-reviews only a low-confidence,
         unsure or failed call, a scorebug-alert game, or a fight/major. summary.json reports the hand-off rate.

Writes <out-dir>/summary.json (default out-dir: <game-dir>/data/review), packets under
<out-dir>/packets/, overrides.json with --apply. Exits 0 once the summary is written, even when
some incidents failed (they keep the engine window); 2 on bad arguments or missing inputs.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import sys
import time
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from clip_review.apply import REEL_MODES, apply_overrides, write_overrides  # noqa: E402
from clip_review.backends import AgentBackend, ApiBackend, EscalateBackend  # noqa: E402
from clip_review.packet import build_packets, slug  # noqa: E402
from clip_review.review import review_incident  # noqa: E402


def make_backend(kind: str, cmd: str, name: str, timeout: float):
    if kind == "api":
        return ApiBackend(name=name or None)
    if kind == "escalate":
        return EscalateBackend(ApiBackend(), AgentBackend(cmd, "escalate-agent", timeout=timeout), name=name or "escalate")
    return AgentBackend(cmd, name or ("agent-" + slug(" ".join(cmd.split()[:1] + [w for w in cmd.split() if "/" in w or ":" in w][:1]))[:40]),
                        timeout=timeout)


def main() -> int:
    env = os.environ.get
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--game-dir", required=True, type=Path)
    ap.add_argument("--video", required=True, type=Path)
    ap.add_argument("--out-dir", type=Path, default=None, help="default: <game-dir>/data/review")
    ap.add_argument("--backend", choices=("api", "agent", "escalate"), default=env("CLIP_REVIEW_BACKEND") or "api")
    ap.add_argument("--agent-cmd", default=env("CLIP_REVIEW_AGENT_CMD") or "")
    ap.add_argument("--agent-name", default=env("CLIP_REVIEW_AGENT_NAME") or "", help="label for this reviewer's results")
    ap.add_argument("--adversary", action="store_true", default=bool(env("CLIP_REVIEW_ADVERSARY")))
    ap.add_argument("--adversary-backend", choices=("api", "agent"), default=None,
                    help="default: agent when CLIP_REVIEW_ADVERSARY_CMD is set, else api")
    ap.add_argument("--adversary-cmd", default=env("CLIP_REVIEW_ADVERSARY_CMD") or "")
    ap.add_argument("--adversary-name", default=env("CLIP_REVIEW_ADVERSARY_NAME") or "")
    ap.add_argument("--adversary-all", action="store_true", help="also challenge minor-penalty verdicts")
    ap.add_argument("--kinds", default="goal,penalty", help="goal, penalty, minor, major, fight, wanted (= major+fight)")
    ap.add_argument("--only", default="", help="comma list of incident id prefixes")
    ap.add_argument("--workers", type=int, default=int(env("CLIP_REVIEW_WORKERS") or 4))
    ap.add_argument("--attempts", type=int, default=3, help="tries per role on malformed/invalid output")
    ap.add_argument("--timeout", type=float, default=float(env("CLIP_REVIEW_TIMEOUT") or 900), help="seconds per agent run")
    ap.add_argument("--run-tag", default="run1")
    ap.add_argument("--apply", action="store_true", help="write overrides.json, cut reviewed clips, write reel manifests")
    ap.add_argument("--reel-mode", choices=REEL_MODES, default=env("CLIP_REVIEW_REEL_MODE") or "goals")
    ap.add_argument("--build-reel", action="store_true", help="with --apply: render the reviewed reel(s) into <game-dir>/output/")
    args = ap.parse_args()

    game_dir, video = args.game_dir.resolve(), args.video.resolve()
    if not (game_dir / "data" / "matched_events.json").exists():
        print(f"no data/matched_events.json in {game_dir}", file=sys.stderr)
        return 2
    if not video.exists():
        print(f"video not found: {video}", file=sys.stderr)
        return 2
    if args.backend in ("agent", "escalate") and not args.agent_cmd:
        print(f"--backend {args.backend} needs --agent-cmd or CLIP_REVIEW_AGENT_CMD", file=sys.stderr)
        return 2
    out_dir = (args.out_dir or game_dir / "data" / "review").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    t_start = time.time()
    summary = {"ok": False, "game_dir": str(game_dir), "video": str(video), "started_at": time.strftime("%Y-%m-%dT%H:%M:%S")}

    try:
        reviewer = make_backend(args.backend, args.agent_cmd, args.agent_name, args.timeout)
        adv = None
        if args.adversary:
            adv_kind = args.adversary_backend or ("agent" if args.adversary_cmd else "api")
            if adv_kind == "agent" and not args.adversary_cmd:
                raise SystemExit("adversary backend agent needs --adversary-cmd or CLIP_REVIEW_ADVERSARY_CMD")
            adv = make_backend(adv_kind, args.adversary_cmd, args.adversary_name or "", args.timeout)
            if adv.name == reviewer.name:
                adv.name = adv.name + "-adv"
        summary.update(reviewer=reviewer.name, adversary=getattr(adv, "name", None), run_tag=args.run_tag)
        kinds = {k.strip() for k in args.kinds.split(",") if k.strip()}
        only = [s.strip() for s in args.only.split(",") if s.strip()]
        built = build_packets(game_dir, video, out_dir / "packets", kinds=kinds, only=only or None)
        results = {}

        def work(item):
            iid, pk = item
            try:
                return iid, review_incident(pk, reviewer, adv, run_tag=args.run_tag, attempts=args.attempts)
            except Exception as exc:  # noqa: BLE001 - one incident must not sink the game
                return iid, {"incident_id": iid, "final": {"status": "error"}, "error": f"{exc}\n{traceback.format_exc()[-800:]}"}

        with cf.ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            for iid, r in pool.map(work, sorted(built["packets"].items())):
                results[iid] = r
                f = r.get("final") or {}
                print(f'{iid:28} {f.get("status", "?"):15} {f.get("in_t")}..{f.get("out_t")} {r.get("dispute") or ""}', flush=True)

        incidents = built["incidents"]
        overrides = write_overrides(game_dir, out_dir, video, incidents, results,
                                    {"reviewer": reviewer.name, "adversary": getattr(adv, "name", None)}) if args.apply else None
        applied = apply_overrides(game_dir, out_dir, video, incidents, overrides, reel_mode=args.reel_mode,
                                  build=args.build_reel) if args.apply else None
        counts, usage = {}, {}
        for r in results.values():
            st = (r.get("final") or {}).get("status", "error")
            counts[st] = counts.get(st, 0) + 1
            for k, v in (r.get("usage") or {}).items():
                usage[k] = round(usage.get(k, 0) + v, 6)
        if getattr(reviewer, "kind", "") == "escalate":
            why = {i: ((r.get("steps") or [{}])[0].get("stats") or {}).get("escalated") for i, r in results.items()}
            summary["escalation"] = {"handed_off": sum(1 for w in why.values() if w), "incidents": len(why),
                                     "rate": round(sum(1 for w in why.values() if w) / len(why), 3) if why else None,
                                     "why": {i: w for i, w in sorted(why.items()) if w}}
        summary.update(
            ok=True, counts=counts, usage=usage, unplaced=built["unplaced"],
            held_for_human=[i for i, r in results.items() if (r.get("final") or {}).get("status") == "held_for_human"],
            dropped=[i for i, r in results.items() if (r.get("final") or {}).get("status") == "drop"],
            errors={i: r["error"] for i, r in results.items() if r.get("error")},
            incidents=[{"id": i, "class": r.get("class"), "status": (r.get("final") or {}).get("status"),
                        "engine": r.get("engine_window"), "final_in_t": (r.get("final") or {}).get("in_t"),
                        "final_out_t": (r.get("final") or {}).get("out_t"), "dispute": r.get("dispute"),
                        "wall_s": r.get("wall_s")} for i, r in sorted(results.items())],
            overrides=str(out_dir / "overrides.json") if overrides else None, apply=applied)
    except SystemExit as exc:
        summary["error"] = str(exc)
        (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
        print(exc, file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        summary["error"] = f"{exc}\n{traceback.format_exc()[-1500:]}"
    summary["elapsed_seconds"] = round(time.time() - t_start, 1)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    print(f"summary: {out_dir / 'summary.json'} counts={summary.get('counts')} usage={summary.get('usage')}"
          + (f" ERROR {summary['error'][:300]}" if summary.get("error") else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
