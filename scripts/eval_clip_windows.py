#!/usr/bin/env python3
"""
Offline evaluation of highlight clip windows against what the blinded judges and the vision
reviewers said was right (issue #25).  No video, OCR or model calls: it reads the saved judge
packets and bake-off verdicts; the committed docs/clip-window-labels.json is the derived label set.

Labels (one set per judged incident, all times in seconds relative to the engine's anchor,
the clock-stop / box-score video time):

  play start  median in-point of the top-judged windows (score within 1.0 of the best, and
              build_up >= 8): where the scoring play / foul lead-in begins according to the
              judges' build_up marks.  Several windows can be "right" at different in-points
              (a faceoff 39 s back and a zone entry 19 s back both score build_up 8-10), so
              this is a central estimate, not a point truth.
  moment      median ``event_t`` of the reviewers who saw the event (goal: puck in net;
              penalty: the whistle / foul they pinned; needs >= 3 reviewers, else the anchor).
  end         median out-point of the top-judged windows with ending >= 8 (celebration over /
              the call shown, before the replay wipe or dead air).

Label noise, stated rather than hidden: two Sonnet judges per incident (three on the ten most
disputed), mean |score diff| 0.84; the top windows' in-points spread by the "label_spread"
fields printed below; an incident whose best window scores < 5 (stream-outage voids, wrong
events) is "unlabelled" and left out of timing metrics.  Metrics therefore use tolerances
(``TOL_*``) instead of exact seconds, and are reported over incidents with best score >= 7
("firm") as well as all labelled ones.

Metrics per window set (``in``/``out`` relative to the anchor):

  build_up_ok   window starts no later than the play start + TOL_START_S
  moment_ok     moment lies inside the window with MOMENT_MARGIN_S on both sides
  ending_ok     window ends no earlier than the labelled end - TOL_END_S
  all_ok        the three together and no wrong incident
  lead_gap_s    seconds the window starts after the play start (0 when covered)
  end_gap_s     seconds the window ends before the labelled end
  excess_s      dead air: seconds earlier than play start - EXCESS_LEAD_GRACE_S plus
                seconds later than end + EXCESS_TAIL_GRACE_S
  wrong         moment not inside the window at all
  proxy_score   mean judge score of the judged window nearest to this one (within
                PROXY_TOL_S on both edges); only available where such a window exists

  python3 scripts/eval_clip_windows.py                   # current engine windows (as recorded)
  python3 scripts/eval_clip_windows.py --policy replay   # windows from this checkout's code
  python3 scripts/eval_clip_windows.py --policy oracle   # the labels themselves (upper bound)
  python3 scripts/eval_clip_windows.py --write-labels docs/clip-window-labels.json

Remove: delete this file; nothing imports it.
"""

from __future__ import annotations

import argparse
import glob
import json
import statistics as st
import sys
from types import SimpleNamespace
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

HOME = Path.home()
JUDGE_DIR = HOME / ".local/state/watch-rams/clip-judge"
BAKEOFF_DIR = HOME / ".local/state/watch-rams/clip-bakeoff"
DEFAULT_LABELS = REPO / "docs" / "clip-window-labels.json"

TOP_SCORE_MARGIN = 1.0   # windows within this of the best score define the targets
MIN_LABEL_SCORE = 5.0    # best window below this: unlabelled (void / wrong event)
FIRM_LABEL_SCORE = 7.0   # best window at or above this: "firm" label
MIN_REVIEWERS_FOR_MOMENT = 3
TOL_START_S = 3.0        # a window may start this much after the labelled play start
TOL_END_S = 2.0          # ... and end this much before the labelled end
MOMENT_MARGIN_S = 1.0
EXCESS_LEAD_GRACE_S = 8.0
EXCESS_TAIL_GRACE_S = 5.0
PROXY_TOL_S = 2.5

# Games held out when tuning constants (never used to choose a constant); chosen before the
# first run: one spring and one 2026-27 game with goals, minors and a major each where possible.
HOLDOUT_GAMES = ("2026-09-16", "2026-03-14", "2026-10-03")


# ---------------------------------------------------------------------------- labels


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return st.mean(xs) if xs else None


def build_labels(judge_dir: Path = JUDGE_DIR, bakeoff_dir: Path = BAKEOFF_DIR) -> Dict[str, Dict[str, Any]]:
    key = json.loads((judge_dir / "key.json").read_text())
    corpus = json.loads((bakeoff_dir / "corpus.json").read_text())["games"]
    game_dirs = {g["slug"]: g["game_dir"] for g in corpus}
    labels: Dict[str, Dict[str, Any]] = {}
    for inc, k in key.items():
        src = k["source"]
        pk = Path(src["packet"])
        incident = json.loads((pk / "incident.json").read_text())
        judges = [json.loads(Path(f).read_text()) for f in sorted(glob.glob(str(judge_dir / "packets" / inc / "judge-*.json")))]
        windows: Dict[str, Dict[str, Any]] = {}
        for letter, v in k["letters"].items():
            ratings = [j["candidates"][letter] for j in judges if letter in j.get("candidates", {})]
            if not ratings:
                continue
            windows[v["contestant"]] = {
                "in": v["in_t"],
                "out": v["out_t"],
                "score": round(st.mean(r["score"] for r in ratings), 2),
                "build_up": round(st.mean(r["subscores"]["build_up"] for r in ratings), 2),
                "ending": round(st.mean(r["subscores"]["ending"] for r in ratings), 2),
                "flags": sorted({f for r in ratings for f in r["flags"]}),
            }
        if "engine" not in windows:
            continue
        reviewer_events = []
        for res in glob.glob(str(pk / "out" / "*" / "run1" / "result.json")):
            r1 = (json.loads(Path(res).read_text()).get("r1") or {})
            if r1.get("event_visible") and r1.get("event_t") is not None:
                reviewer_events.append(float(r1["event_t"]))
        best = max(w["score"] for w in windows.values())
        label: Dict[str, Any] = {
            "incident": inc,
            "game": src["game"],
            "date": incident["game"]["date"],
            "class": incident["class"],
            "kind": incident["kind"],
            "period": incident["period"],
            "time_elapsed": incident["time_elapsed"],
            "anchor": incident["anchor"],
            "refined_by": (incident.get("engine_match") or {}).get("refined_by"),
            "match_unreliable": bool((incident.get("engine_match") or {}).get("unreliable")),
            "scorebug_alert": bool(incident.get("scorebug_alert")),
            "n_penalties": len(incident.get("sheet_rows") or []) if incident["kind"] == "penalty" else 0,
            "n_judges": len(judges),
            "best_score": best,
            "windows": windows,
            "n_reviewer_events": len(reviewer_events),
        }
        label["moment"] = (
            round(st.median(reviewer_events), 1) if len(reviewer_events) >= MIN_REVIEWERS_FOR_MOMENT else 0.0
        )
        if best < MIN_LABEL_SCORE:
            label["label"] = "unlabelled"
        else:
            label["label"] = "firm" if best >= FIRM_LABEL_SCORE else "weak"
            top = {n: w for n, w in windows.items() if w["score"] >= best - TOP_SCORE_MARGIN}
            starters = [w["in"] for w in top.values() if w["build_up"] >= 8] or [w["in"] for w in top.values()]
            enders = [w["out"] for w in top.values() if w["ending"] >= 8] or [w["out"] for w in top.values()]
            label["play_start"] = round(st.median(starters), 1)
            label["end"] = round(st.median(enders), 1)
            if not (label["play_start"] <= label["moment"] <= label["end"]):
                # reviewers and judges disagree about where the incident is: no usable timing label
                label["label"] = "conflict"
            label["label_spread"] = {
                "start_iqr_s": round(_iqr(starters), 1),
                "end_iqr_s": round(_iqr(enders), 1),
                "n_top": len(top),
            }
        labels[inc] = label
    return labels


def _iqr(xs: List[float]) -> float:
    if len(xs) < 4:
        return (max(xs) - min(xs)) if len(xs) > 1 else 0.0
    q = st.quantiles(xs, n=4)
    return q[2] - q[0]


# ---------------------------------------------------------------------------- scoring


def score_window(lab: Dict[str, Any], w_in: float, w_out: float) -> Dict[str, Any]:
    moment = lab["moment"]
    m: Dict[str, Any] = {}
    m["moment_ok"] = (w_in <= moment - MOMENT_MARGIN_S) and (w_out >= moment + MOMENT_MARGIN_S)
    m["wrong"] = not (w_in <= moment <= w_out)
    m["length_s"] = w_out - w_in
    if lab["label"] in ("unlabelled", "conflict"):
        return m
    start, end = lab["play_start"], lab["end"]
    m["lead_gap_s"] = max(0.0, w_in - start)
    m["end_gap_s"] = max(0.0, end - w_out)
    m["build_up_ok"] = w_in <= start + TOL_START_S
    m["ending_ok"] = w_out >= end - TOL_END_S
    m["excess_s"] = max(0.0, (start - EXCESS_LEAD_GRACE_S) - w_in) + max(0.0, w_out - (end + EXCESS_TAIL_GRACE_S))
    m["all_ok"] = m["build_up_ok"] and m["moment_ok"] and m["ending_ok"] and not m["wrong"]
    near = [
        w["score"]
        for w in lab["windows"].values()
        if abs(w["in"] - w_in) <= PROXY_TOL_S and abs(w["out"] - w_out) <= PROXY_TOL_S
    ]
    if near:
        m["proxy_score"] = st.mean(near)
    return m


def aggregate(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"n": len(rows)}
    if not rows:
        return out
    for k in ("build_up_ok", "moment_ok", "ending_ok", "all_ok", "wrong"):
        vals = [r[k] for r in rows if k in r]
        if vals:
            out[k] = round(100.0 * sum(vals) / len(vals), 1)
    for k in ("lead_gap_s", "end_gap_s", "excess_s", "length_s"):
        vals = [r[k] for r in rows if k in r]
        if vals:
            out[k] = round(st.mean(vals), 1)
    prox = [r["proxy_score"] for r in rows if "proxy_score" in r]
    if prox:
        out["proxy_score"] = round(st.mean(prox), 2)
        out["proxy_n"] = len(prox)
    return out


# ---------------------------------------------------------------------------- policies


def policy_engine(lab: Dict[str, Any], ctx: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    w = lab["windows"]["engine"]
    return (w["in"], w["out"])


def policy_oracle(lab: Dict[str, Any], ctx: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    if lab["label"] in ("unlabelled", "conflict"):
        return None
    return (lab["play_start"], lab["end"])


def policy_replay(lab: Dict[str, Any], ctx: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    """The window this checkout's engine code would cut for the incident (saved data only).

    Goals go through ``HighlightPipeline._goal_clip_window`` with the incident's recorded match
    fields; penalties use the configured all-penalty / scrum windows.  Goals the engine placed by a
    fallback (no clock stop) and the stream-outage voids keep their recorded window.
    """
    import config
    from highlight_extractor.pipeline import HighlightPipeline

    eng = lab["windows"]["engine"]
    if lab["class"] == "goal":
        goal = {
            "refined_by": lab["refined_by"],
            "match_confidence": 0.0 if lab["match_unreliable"] else 1.0,
            "match_unreliable": lab["match_unreliable"],
            "period": lab["period"],
        }
        if lab["refined_by"] != "clock_stop":
            return (eng["in"], eng["out"])
        before, after = HighlightPipeline._goal_clip_window(
            SimpleNamespace(config=config), goal,
            before_seconds=config.DEFAULT_CLIP_BEFORE_TIME, after_seconds=config.DEFAULT_CLIP_AFTER_TIME,
        )
        return (-before, after)
    if lab["class"] == "minor" and lab.get("n_penalties", 1) < 2:
        return (-config.PENALTY_ALL_BEFORE_SECONDS, config.PENALTY_ALL_AFTER_SECONDS)
    # major / fight / several penalties at one stoppage: one scrum clip
    return (-config.SCRUM_BEFORE_SECONDS, config.SCRUM_AFTER_SECONDS)


POLICIES = {"engine": policy_engine, "oracle": policy_oracle, "replay": policy_replay}


# ---------------------------------------------------------------------------- report


def evaluate(labels: Dict[str, Dict[str, Any]], policy: str, windows_file: Optional[Path] = None,
             bakeoff_dir: Path = BAKEOFF_DIR) -> Dict[str, Dict[str, Any]]:
    ctx: Dict[str, Any] = {}
    external = json.loads(windows_file.read_text()) if windows_file else None
    res: Dict[str, Dict[str, Any]] = {}
    for inc, lab in labels.items():
        if external is not None:
            w = external.get(inc)
            win = tuple(w) if w else None
        else:
            win = POLICIES[policy](lab, ctx)
        if win is None:
            continue
        r = score_window(lab, win[0], win[1])
        r.update(window=[round(win[0], 1), round(win[1], 1)], cls=lab["class"], date=lab["date"],
                 label=lab["label"], game=lab["game"])
        res[inc] = r
    return res


def _group(cls: str) -> str:
    return {"goal": "goal", "minor": "minor"}.get(cls, "major/fight")


def report(labels, res, title: str) -> str:
    lines = [f"## {title}", ""]
    cols = ("n", "build_up_ok", "moment_ok", "ending_ok", "all_ok", "wrong", "lead_gap_s", "end_gap_s", "excess_s", "length_s", "proxy_score", "proxy_n")
    lines.append("| subset | " + " | ".join(cols) + " |")
    lines.append("|" + "---|" * (len(cols) + 1))

    def row(name, rows):
        a = aggregate(rows)
        lines.append(f"| {name} | " + " | ".join(str(a.get(c, "-")) for c in cols) + " |")

    labelled = [r for r in res.values() if r["label"] not in ("unlabelled", "conflict")]
    firm = [r for r in labelled if r["label"] == "firm"]
    held = lambda r: any(r["date"] == d for d in HOLDOUT_GAMES)  # noqa: E731
    for grp in ("goal", "minor", "major/fight"):
        row(f"{grp} (labelled)", [r for r in labelled if _group(r["cls"]) == grp])
    row("all labelled", labelled)
    row("all firm (best >= 7)", firm)
    row("tuning games", [r for r in labelled if not held(r)])
    row("held-out games", [r for r in labelled if held(r)])
    lines.append("")
    lines.append("Percent columns are shares of incidents; *_s are mean seconds; proxy_score is the mean judged score of the nearest judged window where one exists (proxy_n varies).")
    return "\n".join(lines)


def label_summary(labels) -> str:
    c = defaultdict(int)
    for lab in labels.values():
        c[(_group(lab["class"]), lab["label"])] += 1
    lines = ["Labels: " + ", ".join(f"{g}/{l}={n}" for (g, l), n in sorted(c.items()))]
    sp = [lab["label_spread"]["start_iqr_s"] for lab in labels.values() if "label_spread" in lab]
    ep = [lab["label_spread"]["end_iqr_s"] for lab in labels.values() if "label_spread" in lab]
    if sp:
        lines.append(f"Label noise: median IQR of top-window in-points {st.median(sp):.1f} s, of out-points {st.median(ep):.1f} s.")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--policy", choices=sorted(POLICIES), default="engine")
    ap.add_argument("--windows", type=Path, help="JSON {incident: [in, out]} relative to the anchor; overrides --policy")
    ap.add_argument("--labels", type=Path, default=DEFAULT_LABELS, help="committed labels file (default) or 'rebuild'")
    ap.add_argument("--rebuild", action="store_true", help="derive labels from the judge/bake-off state dirs")
    ap.add_argument("--write-labels", type=Path)
    ap.add_argument("--per-incident", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.rebuild or args.write_labels or not args.labels.exists():
        labels = build_labels()
    else:
        labels = json.loads(args.labels.read_text())
    if args.write_labels:
        args.write_labels.write_text(json.dumps(labels, indent=1, sort_keys=True) + "\n")
        print(f"wrote {args.write_labels} ({len(labels)} incidents)")

    res = evaluate(labels, args.policy, args.windows)
    if args.json:
        print(json.dumps({"policy": args.policy, "results": res}, indent=1))
        return 0
    print(label_summary(labels))
    print()
    print(report(labels, res, f"policy: {args.windows or args.policy}"))
    if args.per_incident:
        print()
        for inc, r in res.items():
            lab = labels[inc]
            print(f"{inc:36s} {r['label'][:4]} win={r['window']} start={lab.get('play_start')} end={lab.get('end')} moment={lab['moment']} "
                  f"b={r.get('build_up_ok')} m={r['moment_ok']} e={r.get('ending_ok')} ex={r.get('excess_s')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
