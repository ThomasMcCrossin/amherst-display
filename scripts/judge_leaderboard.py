#!/usr/bin/env python3
"""
Leaderboard from blinded judge verdicts on the clip-review bake-off.

Reads <root>/packets/<incident>/judge-*.json (schema skills/hockey-clip-review/judge.schema.json)
and the unblinding key <root>/key.json (written by clip_review_bakeoff.py judge-packets), then
per contestant (the engine's original window is one of them):

  mean_score       THE HEADLINE: mean over incidents of the judges' mean 0-10 overall highlight
                   score for its clip (how good a highlight it is, not just whether the event is in it)
  build_up, moment, ending, cleanliness   mean sub-scores (judge@2)
  cost per incident  from <root>/costs.json (written by clip_review_bakeoff.py judge-packets)
  win_rate         share of incidents where a judge picked its letter as best (averaged over judges)
  win_rate_equiv   same, but a pick also counts for every contestant whose window is within 0.5 s
                   of the picked one (identical windows got separate letters)
  flag rates       share of its judged clips carrying each flag
  dropped          incidents where it dropped the clip; scored 0 when the judges saw the event
                   (some candidate scored >= 6), else left out

Judge agreement: share of incident pairs-of-judges picking the same best letter (or an
equivalent window), and the mean absolute score difference between judges on the same clip.

  python3 scripts/judge_leaderboard.py [--root ~/.local/state/watch-rams/clip-judge] [--out leaderboard]
"""

from __future__ import annotations

import argparse
import itertools
import json
import statistics
from pathlib import Path
from typing import Any, Dict, List

SUBS = ("build_up", "moment", "ending", "cleanliness")
FLAGS = ("correct", "wrong_event", "missing_goal", "starts_late", "ends_early", "too_long")  # starts_late = missing the build-up (primary)


def load_verdicts(pk: Path) -> List[Dict[str, Any]]:
    out = []
    for f in sorted(pk.glob("judge-*.json")):
        try:
            v = json.loads(f.read_text())
        except ValueError:
            print(f"skip {f}: not JSON")
            continue
        cands = v.get("candidates") if isinstance(v, dict) else None
        if not isinstance(cands, dict):
            print(f"skip {f}: no candidates")
            continue
        clean = {}
        for letter, c in cands.items():
            try:
                score = float(c.get("score"))
            except (TypeError, ValueError, AttributeError):
                continue
            subs = {}
            for k in SUBS:
                try:
                    subs[k] = max(0.0, min(10.0, float((c.get("subscores") or {}).get(k))))
                except (TypeError, ValueError):
                    pass
            clean[letter] = {"score": max(0.0, min(10.0, score)), "subs": subs,
                             "flags": [x for x in c.get("flags") or [] if x in FLAGS]}
        out.append({"judge": v.get("judge") or f.stem[6:], "candidates": clean, "best": v.get("best"), "file": str(f)})
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--root", type=Path, default=Path.home() / ".local/state/watch-rams/clip-judge")
    ap.add_argument("--out", default="leaderboard", help="writes <root>/<out>.json and .md")
    a = ap.parse_args()
    root = a.root.expanduser()
    key = json.loads((root / "key.json").read_text())

    per: Dict[str, Dict[str, Any]] = {}
    agree_pairs = agree_hits = 0
    score_diffs: List[float] = []
    judged = 0
    judges_seen = set()

    def row(who: str) -> Dict[str, Any]:
        return per.setdefault(who, {"scores": [], "wins": [], "wins_eq": [], "flags": {f: 0 for f in FLAGS}, "clips": 0, "dropped": 0,
                                    "dropped_scored": 0, "subs": {k: [] for k in SUBS}})

    for jid, k in key.items():
        verdicts = load_verdicts(root / "packets" / jid)
        if not verdicts:
            continue
        judged += 1
        letters = k["letters"]
        for v in verdicts:
            judges_seen.add(v["judge"])

        def equiv(l1: str, l2: str) -> bool:
            if l1 == l2:
                return True
            if l1 not in letters or l2 not in letters:
                return False
            a1, a2 = letters[l1], letters[l2]
            return abs(a1["in_t"] - a2["in_t"]) <= 0.5 and abs(a1["out_t"] - a2["out_t"]) <= 0.5

        best_any = 0.0
        for letter, info in letters.items():
            s = [v["candidates"][letter]["score"] for v in verdicts if letter in v["candidates"]]
            if not s:
                continue
            m = statistics.mean(s)
            best_any = max(best_any, m)
            r = row(info["contestant"])
            r["scores"].append(m)
            for k in SUBS:
                sv = [v["candidates"][letter]["subs"][k] for v in verdicts if k in v["candidates"].get(letter, {}).get("subs", {})]
                if sv:
                    r["subs"][k].append(statistics.mean(sv))
            r["wins"].append(statistics.mean([1.0 if v.get("best") == letter else 0.0 for v in verdicts]))
            r["wins_eq"].append(statistics.mean([1.0 if v.get("best") and equiv(v["best"], letter) else 0.0 for v in verdicts]))
            for v in verdicts:
                c = v["candidates"].get(letter)
                if c:
                    r["clips"] += 1
                    for f in c["flags"]:
                        r["flags"][f] += 1
        for who in k.get("dropped_by") or []:
            r = row(who)
            r["dropped"] += 1
            if best_any >= 6:  # judges saw the event, so the drop lost it
                r["dropped_scored"] += 1
                r["scores"].append(0.0)
                r["wins"].append(0.0)
                r["wins_eq"].append(0.0)
        for v1, v2 in itertools.combinations(verdicts, 2):
            agree_pairs += 1
            if v1.get("best") and v2.get("best") and equiv(v1["best"], v2["best"]):
                agree_hits += 1
            for letter in letters:
                if letter in v1["candidates"] and letter in v2["candidates"]:
                    score_diffs.append(abs(v1["candidates"][letter]["score"] - v2["candidates"][letter]["score"]))

    costs_f = root / "costs.json"
    costs = json.loads(costs_f.read_text()) if costs_f.exists() else {}
    rows = []
    for who, r in per.items():
        n = r["clips"] or 1
        rows.append({"contestant": who, "incidents": len(r["scores"]),
                     "mean_score": round(statistics.mean(r["scores"]), 2) if r["scores"] else None,
                     "win_rate": round(statistics.mean(r["wins"]), 3) if r["wins"] else None,
                     "win_rate_equiv": round(statistics.mean(r["wins_eq"]), 3) if r["wins_eq"] else None,
                     **{f"{f}_rate": round(r["flags"][f] / n, 3) for f in FLAGS},
                     **{k: (round(statistics.mean(r["subs"][k]), 2) if r["subs"][k] else None) for k in SUBS},
                     "dropped": r["dropped"], "dropped_but_event_seen": r["dropped_scored"],
                     "tokens_per_incident": (round((costs.get(who) or {}).get("tokens_in_mean", 0) + (costs.get(who) or {}).get("tokens_out_mean", 0))
                                             if costs.get(who) else None),
                     "cost_usd_per_incident": (costs.get(who) or {}).get("cost_usd_mean"),
                     "wall_s_per_incident": (costs.get(who) or {}).get("wall_s_median"),
                     "handoff_rate": (costs.get(who) or {}).get("handoff_rate")})
    rows.sort(key=lambda x: (-(x["mean_score"] or 0), -(x["win_rate_equiv"] or 0)))
    agreement = {"judges": sorted(judges_seen), "incidents_judged": judged, "judge_pairs": agree_pairs,
                 "best_pick_agreement": round(agree_hits / agree_pairs, 3) if agree_pairs else None,
                 "mean_abs_score_diff": round(statistics.mean(score_diffs), 2) if score_diffs else None}
    out = {"rows": rows, "agreement": agreement}
    (root / f"{a.out}.json").write_text(json.dumps(out, indent=2))
    md = [f"# Judge leaderboard: highlight quality ({judged} incidents, judges: {', '.join(sorted(judges_seen)) or '-'})", "",
          "Headline = mean overall highlight score (0-10) from the blinded judges, with spend per incident beside it. "
          "Sub-scores and flags are diagnostics.", "",
          "| contestant | incidents | **highlight score** | tokens / incident | cost $ / incident | wall s / incident | build-up | moment | ending | cleanliness | win rate (equiv.) | starts late (missing build-up) | wrong event | missing event | ends early | too long | dropped (event seen) |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        md.append(f'| {r["contestant"]}' + (f' (hand-off {r["handoff_rate"]:.0%})' if r.get("handoff_rate") is not None else "") +
                  f' | {r["incidents"]} | **{r["mean_score"]}** | {r["tokens_per_incident"]} | {r["cost_usd_per_incident"]} | {r["wall_s_per_incident"]} | '
                  f'{r["build_up"]} | {r["moment"]} | {r["ending"]} | {r["cleanliness"]} | {r["win_rate_equiv"]} | {r["starts_late_rate"]} | '
                  f'{r["wrong_event_rate"]} | {r["missing_goal_rate"]} | {r["ends_early_rate"]} | {r["too_long_rate"]} | '
                  f'{r["dropped"]} ({r["dropped_but_event_seen"]}) |')
    md += ["", f"Judge agreement: best pick {agreement['best_pick_agreement']} over {agree_pairs} judge pairs; "
               f"mean |score difference| {agreement['mean_abs_score_diff']}."]
    (root / f"{a.out}.md").write_text("\n".join(md) + "\n")
    print("\n".join(md))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
