#!/usr/bin/env python3
"""
Bake-off of cheap vision reviewers for the clip review (scripts/review_game.py).

Every contestant reviews the same packets (same incidents, same coarse sheets); adversary
pairings reuse the reviewers' cached first verdicts. A static judging page shows each
contestant's proposed clip next to the engine's so a human can mark them; score turns the
exported marks into the final leaderboard. Until then, provisional ranks from objective checks
and cross-model agreement.

  B=~/.local/state/watch-rams/clip-bakeoff
  clip_review_bakeoff.py corpus      --bake $B [--games-root DIR ...]      # discover game dirs + recordings
  clip_review_bakeoff.py packets     --bake $B                              # all packets, all games
  clip_review_bakeoff.py sample      --bake $B [--n 40] [--seed 7]          # stratified sample
  clip_review_bakeoff.py run         --bake $B [--contestants a,b] [--run-tag run2 --subset 12]
  clip_review_bakeoff.py adversary   --bake $B --pairs reviewer:adversary,...
  clip_review_bakeoff.py render      --bake $B                              # clips + site/
  clip_review_bakeoff.py provisional --bake $B                              # leaderboard_provisional.{json,md}
  clip_review_bakeoff.py score       --bake $B --marks marks.json           # leaderboard_final.{json,md}

Contestant commands are below (CONTESTANTS); add one with --contestant name=kind:command.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from clip_review.backends import AgentBackend, ApiBackend  # noqa: E402
from clip_review.packet import build_packets, slug  # noqa: E402
from clip_review.review import enforce, review_incident  # noqa: E402

PI = "pi -p --mode json --no-session --no-context-files --no-skills --no-extensions --tools read,bash --model {model} {{prompt}}"
CONTESTANTS: Dict[str, Dict[str, str]] = {
    "api-deepseek-flash": {"kind": "api", "cmd": ""},
    "pi-deepseek-v4.1-flash": {"kind": "agent", "cmd": PI.format(model="ollama-cloud/deepseek-v4.1-flash")},
    "pi-gemma4-31b": {"kind": "agent", "cmd": PI.format(model="ollama-cloud/gemma4:31b")},
    "pi-glm-5.3-flash": {"kind": "agent", "cmd": PI.format(model="ollama-cloud/glm-5.3-flash")},
    "cu-deepseek-v4.1-flash": {"kind": "agent", "cmd": "claude-unified -p --model deepseek-v4.1-flash --output-format json "
                               "--tools Read,Bash --permission-mode bypassPermissions --no-session-persistence {prompt}"},
}
ENGINE = "engine"
STRATA = {"goal_normal": 12, "goal_alert": 10, "minor": 8, "rough": 10}
MARKS = ("correct", "wrong_event", "missing_goal", "starts_late", "ends_early", "too_long")


def jload(p: Path) -> Any:
    return json.loads(Path(p).read_text())


def jdump(p: Path, obj: Any) -> None:
    Path(p).parent.mkdir(parents=True, exist_ok=True)
    Path(p).write_text(json.dumps(obj, indent=2, default=str))


def backend(name: str, timeout: float = 900):
    c = CONTESTANTS[name]
    return ApiBackend(name=name) if c["kind"] == "api" else AgentBackend(c["cmd"], name, timeout=timeout)


# ======================================================================================
# corpus / packets / sample
# ======================================================================================
def cmd_corpus(a) -> None:
    games = []
    roots = [Path(r).expanduser() for r in (a.games_root or [])] or [
        Path.home() / "amherst-display/Games", Path("/mnt/rams-archive/watch-rams/games-2025-26-rerun/Games")]
    old_spring = Path("/mnt/rams-archive/watch-rams/amherst-display-clarencehub-2026-spring/Games")
    for root in roots:
        for gd in sorted(p for p in root.iterdir() if p.is_dir() and (p / "data" / "matched_events.json").exists()):
            video = None
            for rel, key in (("source/watch_rams_source.json", "watch_rams_video_path"), ("data/offline_run.json", "video")):
                f = gd / rel
                if f.exists():
                    video = jload(f).get(key)
                    break
            if not video or not Path(video).exists():
                print(f"skip {gd.name}: no recording")
                continue
            hard = (gd / "data" / "SCOREBOARD_ALERT.txt").exists() or (old_spring / gd.name / "data" / "SCOREBOARD_ALERT.txt").exists()
            games.append({"slug": slug(gd.name), "game_dir": str(gd), "video": video, "scorebug_alert": hard,
                          "season": "2025-26 spring" if gd.name.startswith("2026-03") else "2026-27"})
    jdump(a.bake / "corpus.json", {"games": games})
    for g in games:
        print(f'{g["slug"]:70} alert={g["scorebug_alert"]}')


def cmd_packets(a) -> None:
    corpus = jload(a.bake / "corpus.json")["games"]
    index = []
    for g in corpus:
        if a.only_game and a.only_game not in g["slug"]:
            continue
        t0 = time.time()
        built = build_packets(Path(g["game_dir"]), Path(g["video"]), a.bake / "games" / g["slug"] / "packets",
                              scorebug_alert=g["scorebug_alert"])
        for inc in built["selected"]:
            pk = built["packets"].get(inc["id"])
            index.append({"game": g["slug"], "incident_id": inc["id"], "class": inc["class"], "kind": inc["kind"],
                          "packet": str(pk) if pk else None, "scorebug_alert": g["scorebug_alert"],
                          "rows": inc["rows"], "period": inc["period"], "time_elapsed": inc["time_elapsed"]})
        print(f'{g["slug"]:70} {len(built["packets"]):3} packets {len(built["unplaced"])} unplaced {time.time() - t0:.0f}s', flush=True)
    if a.only_game and (a.bake / "packets.json").exists():
        old = [x for x in jload(a.bake / "packets.json") if a.only_game not in x["game"]]
        index = old + index
    jdump(a.bake / "packets.json", index)


def stratum(x: Dict[str, Any]) -> str:
    if x["kind"] == "goal":
        return "goal_alert" if x["scorebug_alert"] else "goal_normal"
    return "rough" if x["class"] in ("major", "fight") else "minor"


def cmd_sample(a) -> None:
    idx = [x for x in jload(a.bake / "packets.json") if x["packet"]]
    rng = random.Random(a.seed)
    by: Dict[str, List[Dict[str, Any]]] = {}
    for x in idx:
        by.setdefault(stratum(x), []).append(x)
    want = dict(STRATA)
    scale = a.n / sum(want.values())
    want = {k: max(1, round(v * scale)) for k, v in want.items()}
    picked: List[Dict[str, Any]] = []
    spare = 0
    for k, n in want.items():
        pool = by.get(k, [])
        # fights first inside rough: they are the rarest and the most wanted
        if k == "rough":
            fights = [x for x in pool if x["class"] == "fight"]
            others = [x for x in pool if x["class"] != "fight"]
            rng.shuffle(fights)
            rng.shuffle(others)
            pool = fights + others
        else:
            pool = pool[:]
            rng.shuffle(pool)
        take = pool[:n]
        spare += n - len(take)
        picked += [dict(x, stratum=k) for x in take]
    if spare:  # fill shortfalls from the largest goal stratum
        rest = [x for x in idx if (x["game"], x["incident_id"]) not in {(p["game"], p["incident_id"]) for p in picked} and x["kind"] == "goal"]
        rng.shuffle(rest)
        picked += [dict(x, stratum=stratum(x)) for x in rest[:spare]]
    picked.sort(key=lambda x: (x["stratum"], x["game"], x["incident_id"]))
    jdump(a.bake / "sample.json", picked)
    counts: Dict[str, int] = {}
    for p in picked:
        counts[p["stratum"]] = counts.get(p["stratum"], 0) + 1
    print(f"{len(picked)} sampled: {counts}; available: { {k: len(v) for k, v in by.items()} }")


# ======================================================================================
# run reviewers / adversaries
# ======================================================================================
def _subset(sample: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    if not n or n >= len(sample):
        return sample
    by: Dict[str, List[Dict[str, Any]]] = {}
    for x in sample:
        by.setdefault(x["stratum"], []).append(x)
    out, i = [], 0
    while len(out) < n:  # round-robin over strata
        for k in sorted(by):
            if i < len(by[k]) and len(out) < n:
                out.append(by[k][i])
        i += 1
    return out


def cmd_run(a) -> None:
    sample = _subset(jload(a.bake / "sample.json"), a.subset)
    names = [c for c in (a.contestants.split(",") if a.contestants else CONTESTANTS)]

    def one_contestant(name: str) -> str:
        be = backend(name, a.timeout)
        t0 = time.time()
        def work(x):
            try:
                r = review_incident(Path(x["packet"]), be, None, run_tag=a.run_tag, attempts=a.attempts)
                return f'{name:24} {x["incident_id"]:26} {r["final"]["status"]:10} {r.get("wall_s")}s'
            except Exception as exc:  # noqa: BLE001
                return f'{name:24} {x["incident_id"]:26} ERROR {exc}'
        with cf.ThreadPoolExecutor(max_workers=a.workers) as pool:
            for line in pool.map(work, sample):
                print(line, flush=True)
        return f"{name} done in {time.time() - t0:.0f}s"

    with cf.ThreadPoolExecutor(max_workers=len(names)) as pool:
        for msg in pool.map(one_contestant, names):
            print(msg, flush=True)


def cmd_adversary(a) -> None:
    sample = [x for x in jload(a.bake / "sample.json") if x["class"] in ("goal", "major", "fight")]
    pairs = [p.split(":") for p in a.pairs.split(",")]

    def one_pair(pair):
        rname, aname = pair
        rv, av = backend(rname, a.timeout), backend(aname, a.timeout)
        if av.name == rv.name:
            av.name = rv.name  # same-model pairing: results land under adv-<same name>
        def work(x):
            try:
                r = review_incident(Path(x["packet"]), rv, av, run_tag="run1", attempts=a.attempts)
                return f'{rname}<-{aname} {x["incident_id"]:26} {r["final"]["status"]:15} {r.get("dispute")}'
            except Exception as exc:  # noqa: BLE001
                return f'{rname}<-{aname} {x["incident_id"]:26} ERROR {exc}'
        with cf.ThreadPoolExecutor(max_workers=a.workers) as pool:
            for line in pool.map(work, sample):
                print(line, flush=True)
        return f"pair {rname}<-{aname} done"

    with cf.ThreadPoolExecutor(max_workers=len(pairs)) as pool:
        for msg in pool.map(one_pair, pairs):
            print(msg, flush=True)


# ======================================================================================
# collect results
# ======================================================================================
def load_result(x: Dict[str, Any], name: str, run_tag: str = "run1", adversary: Optional[str] = None) -> Optional[Dict[str, Any]]:
    f = Path(x["packet"]) / "out" / name / run_tag / ("result.json" if not adversary else f"result_adv-{adversary}.json")
    return jload(f) if f.exists() else None


def windows_for(x: Dict[str, Any], names: List[str]) -> Dict[str, Dict[str, Any]]:
    inc = jload(Path(x["packet"]) / "incident.json")
    out = {ENGINE: {"status": "engine", "in_t": inc["engine_window"]["in_t"], "out_t": inc["engine_window"]["out_t"]}}
    for n in names:
        r = load_result(x, n)
        if r:
            f = r["final"]
            v = r.get("r1") or {}
            out[n] = {"status": f.get("status"), "in_t": f.get("in_t"), "out_t": f.get("out_t"), "event_t": v.get("event_t"),
                      "event_visible": v.get("event_visible"), "decision": v.get("decision"), "foul_visible": v.get("foul_visible"),
                      "fight": v.get("fight"), "malformed": r.get("malformed"), "wall_s": r.get("wall_s"), "usage": r.get("usage"),
                      "attempts": (r.get("steps") or [{}])[0].get("attempts"), "reason": v.get("reason")}
    return out


# ======================================================================================
# render + judging site
# ======================================================================================
def render_small(video: Path, start: float, end: float, anchor: float, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    font = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    label = (f"drawtext=fontfile={font}:fontsize=22:fontcolor=white:box=1:boxcolor=black@0.55:x=8:y=h-34:"
             f"text='%{{eif\\:t+{start - anchor:.3f}\\:d}}s'") if Path(font).exists() else "null"
    vf = f"scale=-2:480,{label}"
    cmd = ["nice", "-n", "10", "ffmpeg", "-v", "error", "-nostdin", "-y", "-ss", f"{start:.3f}", "-t", f"{end - start:.3f}",
           "-i", str(video), "-vf", vf, "-c:v", "libx264", "-preset", "veryfast", "-crf", "28", "-pix_fmt", "yuv420p",
           "-c:a", "aac", "-b:a", "64k", "-ac", "1", "-movflags", "+faststart", "-threads", "2", str(dest)]
    subprocess.run(cmd, check=True)


def cmd_render(a) -> None:
    sample = jload(a.bake / "sample.json")
    names = list(CONTESTANTS)
    site = a.bake / "site"
    (site / "clips").mkdir(parents=True, exist_ok=True)
    items, jobs = [], []
    for x in sample:
        inc = jload(Path(x["packet"]) / "incident.json")
        anchor, video = float(inc["anchor"]), Path(inc["video"])
        ws = windows_for(x, names)
        clips: Dict[str, Dict[str, Any]] = {}
        for who, w in ws.items():
            if w.get("in_t") is None:  # dropped: nothing to render; listed on the card
                continue
            key = f"{round(w['in_t'] * 2) / 2:+.1f}_{round(w['out_t'] * 2) / 2:+.1f}"
            c = clips.setdefault(key, {"in_t": w["in_t"], "out_t": w["out_t"], "who": []})
            c["who"].append(who)
        rng = random.Random(hashlib.sha256(x["incident_id"].encode() + x["game"].encode()).hexdigest())
        keys = list(clips)
        rng.shuffle(keys)
        cards = []
        for i, key in enumerate(keys, 1):
            c = clips[key]
            cid = f'{x["game"][:10]}_{x["incident_id"]}_{i}'
            fname = f"{hashlib.sha1(cid.encode()).hexdigest()[:12]}.mp4"
            jobs.append((video, anchor + c["in_t"], anchor + c["out_t"], anchor, site / "clips" / fname))
            cards.append({"clip_id": cid, "label": f"Clip {chr(64 + i)}", "file": f"clips/{fname}", "in_t": c["in_t"],
                          "out_t": c["out_t"], "length": round(c["out_t"] - c["in_t"], 1), "who": c["who"]})
        dropped = [w for w, v in ws.items() if v.get("in_t") is None]
        items.append({"key": f'{x["game"]}/{x["incident_id"]}', "game": x["game"], "incident_id": x["incident_id"],
                      "stratum": x["stratum"], "class": x["class"], "period": inc["period"], "time_elapsed": inc["time_elapsed"],
                      "teams": inc["teams"], "rows": inc["sheet_rows"], "scorebug_alert": inc["scorebug_alert"],
                      "cards": cards, "dropped_by": dropped})
    todo = [j for j in jobs if not j[4].exists()]
    print(f"rendering {len(todo)} of {len(jobs)} clips", flush=True)
    with cf.ThreadPoolExecutor(max_workers=a.workers) as pool:
        list(pool.map(lambda j: render_small(*j), todo))
    data = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"), "marks": list(MARKS), "contestants": [ENGINE] + names,
            "items": items}
    jdump(site / "data.json", data)
    html = (REPO / "scripts" / "clip_review_bakeoff_site.html").read_text()
    (site / "index.html").write_text(html.replace("/*__DATA__*/null", json.dumps(data)))
    print(f"site: {site / 'index.html'} ({len(items)} incidents, {len(jobs)} clips)")


# ======================================================================================
# provisional leaderboard: objective checks + cross-model agreement
# ======================================================================================
def _median(v: List[float]) -> Optional[float]:
    return statistics.median(v) if v else None


def consensus_event(ws: Dict[str, Dict[str, Any]], tol: float = 5.0) -> Optional[float]:
    """Median event time of the largest cluster of reviewers that saw the event, if >= 3 agree."""
    ts = sorted(w["event_t"] for n, w in ws.items() if n != ENGINE and w.get("event_visible") and isinstance(w.get("event_t"), (int, float))
                and w.get("status") in ("override", "keep", "unsure", "held_for_human", "rejected"))
    best: List[float] = []
    for t in ts:
        grp = [u for u in ts if abs(u - t) <= tol]
        if len(grp) > len(best):
            best = grp
    return _median(best) if len(best) >= 3 else None


def cmd_provisional(a) -> None:
    sample = jload(a.bake / "sample.json")
    names = list(CONTESTANTS)
    who = [ENGINE] + names
    agg: Dict[str, Dict[str, List[float]]] = {n: {} for n in who}

    def add(n: str, k: str, v: Any) -> None:
        if v is not None:
            agg[n].setdefault(k, []).append(float(v))

    for x in sample:
        ws = windows_for(x, names)
        ev = consensus_event(ws)
        ins = [w["in_t"] for n, w in ws.items() if n != ENGINE and w.get("status") == "override"]
        outs = [w["out_t"] for n, w in ws.items() if n != ENGINE and w.get("status") == "override"]
        c_in, c_out = _median(ins) if len(ins) >= 3 else None, _median(outs) if len(outs) >= 3 else None
        fouls = [w.get("foul_visible") for n, w in ws.items() if n != ENGINE and isinstance(w.get("foul_visible"), bool)]
        foul_major = (sum(fouls) * 2 > len(fouls)) if len(fouls) >= 3 and sum(fouls) * 2 != len(fouls) else None
        for n in who:
            w = ws.get(n)
            if w is None:
                add(n, "missing", 1)
                continue
            add(n, "n", 1)
            if n != ENGINE:
                add(n, "malformed", 1 if w.get("malformed") else 0)
                add(n, "retried", 1 if (w.get("attempts") or 1) > 1 else 0)
                add(n, "override", 1 if w.get("status") == "override" else 0)
                add(n, "wall_s", w.get("wall_s"))
                u = w.get("usage") or {}
                add(n, "tokens_in", (u.get("input_tokens") or 0) + (u.get("cache_read_tokens") or 0))
                add(n, "tokens_out", u.get("output_tokens"))
                add(n, "cost_usd", u.get("cost_usd"))
            if ev is not None:
                dropped = w.get("in_t") is None
                if x["kind"] == "goal" or x["class"] in ("major", "fight"):
                    contains = (not dropped) and w["in_t"] + 2 <= ev <= w["out_t"] - 3
                    add(n, "event_in_clip", 1 if contains else 0)
                    if x["kind"] == "goal":
                        add(n, "goal_in_clip", 1 if contains else 0)
                    if x["scorebug_alert"] and x["kind"] == "goal":
                        add(n, "goal_in_clip_alert", 1 if contains else 0)
                add(n, "false_drop", 1 if dropped else 0)
            if c_in is not None and w.get("in_t") is not None:
                add(n, "in_dev", abs(w["in_t"] - c_in))
            if c_out is not None and w.get("out_t") is not None:
                add(n, "out_dev", abs(w["out_t"] - c_out))
            if foul_major is not None and n != ENGINE and isinstance(w.get("foul_visible"), bool) and x["kind"] == "penalty":
                add(n, "foul_agree", 1 if w["foul_visible"] == foul_major else 0)
            if x["class"] == "fight" and n != ENGINE:
                f = w.get("fight")
                add(n, "fight_bounds", 1 if isinstance(f, dict) and w.get("in_t") is not None and w["in_t"] <= f["gloves_drop_t"]
                    and w["out_t"] >= f["separated_t"] else 0)
    # stability: run1 vs run2 on whatever subset has a run2
    for n in names:
        for x in sample:
            r1, r2 = load_result(x, n, "run1"), load_result(x, n, "run2")
            if not (r1 and r2):
                continue
            f1, f2 = r1["final"], r2["final"]
            add(n, "stab_n", 1)
            add(n, "stab_same_status", 1 if f1.get("status") == f2.get("status") else 0)
            if f1.get("in_t") is not None and f2.get("in_t") is not None:
                add(n, "stab_in_dev", abs(f1["in_t"] - f2["in_t"]))
                add(n, "stab_out_dev", abs(f1["out_t"] - f2["out_t"]))
    # adversary pairings
    pairs = []
    for f in (a.bake / "adversary_pairs.json",):
        if f.exists():
            pairs = jload(f)
    adv_rows = []
    for rname, aname in pairs:
        raised = right = held = resolved = n_inc = 0
        for x in sample:
            r = load_result(x, rname, "run1", aname)
            if not r:
                continue
            n_inc += 1
            if r.get("dispute") in ("held_for_human", "resolved_by_second_review"):
                raised += 1
                held += r["dispute"] == "held_for_human"
                resolved += r["dispute"] == "resolved_by_second_review"
                ws = windows_for(x, names)
                ev = consensus_event(ws)
                f1 = r.get("r1_final") or {}
                if ev is not None and (f1.get("in_t") is None or not f1["in_t"] + 2 <= ev <= f1["out_t"] - 3):
                    right += 1
        adv_rows.append({"reviewer": rname, "adversary": aname, "incidents": n_inc, "disputes": raised,
                         "provisionally_right": right, "resolved": resolved, "held_for_human": held})

    def rate(n: str, k: str) -> Optional[float]:
        v = agg[n].get(k)
        return round(sum(v) / len(v), 3) if v else None

    def med(n: str, k: str) -> Optional[float]:
        v = agg[n].get(k)
        return round(statistics.median(v), 1) if v else None

    rows = []
    for n in who:
        rows.append({"contestant": n, "reviewed": int(sum(agg[n].get("n", []))),
                     "goal_in_clip": rate(n, "goal_in_clip"), "goal_in_clip_alert": rate(n, "goal_in_clip_alert"),
                     "event_in_clip": rate(n, "event_in_clip"), "false_drop": rate(n, "false_drop"),
                     "in_dev_s": med(n, "in_dev"), "out_dev_s": med(n, "out_dev"), "foul_agree": rate(n, "foul_agree"),
                     "fight_bounds": rate(n, "fight_bounds"), "malformed": rate(n, "malformed"), "retried": rate(n, "retried"),
                     "override_rate": rate(n, "override"), "stability_same_status": rate(n, "stab_same_status"),
                     "stability_in_out_dev_s": (med(n, "stab_in_dev"), med(n, "stab_out_dev")) if agg[n].get("stab_n") else None,
                     "wall_s_median": med(n, "wall_s"), "tokens_in_median": med(n, "tokens_in"), "tokens_out_median": med(n, "tokens_out"),
                     "cost_usd_total": round(sum(agg[n].get("cost_usd", [])), 4) if agg[n].get("cost_usd") else None})
    rows.sort(key=lambda r: (-(r["goal_in_clip"] or 0), -(r["event_in_clip"] or 0), r["in_dev_s"] or 99))
    jdump(a.bake / "leaderboard_provisional.json", {"provisional": True, "rows": rows, "adversary": adv_rows,
                                                    "note": "objective checks + cross-model agreement; not human-judged"})
    md = ["# PROVISIONAL clip-review leaderboard (no human marks yet)", "",
          "Consensus event = median event time of the largest group of >=3 reviewers within 5 s; "
          "'in clip' = consensus event >= 2 s after in and >= 3 s before out. Deviations are from the reviewers' median window.", "",
          "| contestant | goal in clip | goal in clip (bug-alert games) | event in clip (goals+majors) | false drop | in dev s | out dev s | foul agree | fight bounds | malformed | retried | same status run2 | wall s | tokens in | tokens out |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        md.append("| " + " | ".join(str(v) if v is not None else "-" for v in (
            r["contestant"], r["goal_in_clip"], r["goal_in_clip_alert"], r["event_in_clip"], r["false_drop"], r["in_dev_s"],
            r["out_dev_s"], r["foul_agree"], r["fight_bounds"], r["malformed"], r["retried"], r["stability_same_status"],
            r["wall_s_median"], r["tokens_in_median"], r["tokens_out_median"])) + " |")
    if adv_rows:
        md += ["", "| reviewer | adversary | incidents | disputes | provisionally right | resolved by 2nd review | held for human |",
               "|---|---|---|---|---|---|---|"]
        for r in adv_rows:
            md.append(f'| {r["reviewer"]} | {r["adversary"]} | {r["incidents"]} | {r["disputes"]} | {r["provisionally_right"]} | {r["resolved"]} | {r["held_for_human"]} |')
    (a.bake / "leaderboard_provisional.md").write_text("\n".join(md) + "\n")
    print("\n".join(md))


# ======================================================================================
# final score from exported human marks
# ======================================================================================
def cmd_score(a) -> None:
    marks = jload(a.marks)
    data = jload(a.bake / "site" / "data.json")
    by_clip = {c["clip_id"]: (it, c) for it in data["items"] for c in it["cards"]}
    agg: Dict[str, Dict[str, float]] = {}
    judged_items = set()
    for clip_id, m in (marks.get("clips") or {}).items():
        if clip_id not in by_clip:
            continue
        it, c = by_clip[clip_id]
        flags = [k for k in MARKS if m.get(k)]
        if not flags:
            continue
        judged_items.add(it["key"])
        for who in c["who"]:
            s = agg.setdefault(who, {"judged": 0, **{k: 0 for k in MARKS}, "best": 0, "goal_judged": 0, "goal_correct": 0})
            s["judged"] += 1
            for k in flags:
                s[k] += 1
            if it["class"] == "goal":
                s["goal_judged"] += 1
                s["goal_correct"] += 1 if m.get("correct") else 0
    for key, best_clip in (marks.get("best") or {}).items():
        if best_clip in by_clip:
            for who in by_clip[best_clip][1]["who"]:
                agg.setdefault(who, {"judged": 0, **{k: 0 for k in MARKS}, "best": 0, "goal_judged": 0, "goal_correct": 0})["best"] += 1
    rows = []
    for who, s in agg.items():
        j = s["judged"] or 1
        rows.append({"contestant": who, "judged": s["judged"], "correct_rate": round(s["correct"] / j, 3),
                     "goal_correct_rate": round(s["goal_correct"] / s["goal_judged"], 3) if s["goal_judged"] else None,
                     **{k: s[k] for k in MARKS if k != "correct"}, "best_picks": s["best"]})
    rows.sort(key=lambda r: (-r["correct_rate"], -r["best_picks"]))
    jdump(a.bake / "leaderboard_final.json", {"judged_incidents": len(judged_items), "rows": rows})
    md = [f"# Clip-review leaderboard (human-judged, {len(judged_items)} incidents)", "",
          "| contestant | judged clips | correct | goal correct | wrong event | missing goal | starts late | ends early | too long | best picks |",
          "|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        md.append(f'| {r["contestant"]} | {r["judged"]} | {r["correct_rate"]} | {r["goal_correct_rate"]} | {r["wrong_event"]} | '
                  f'{r["missing_goal"]} | {r["starts_late"]} | {r["ends_early"]} | {r["too_long"]} | {r["best_picks"]} |')
    (a.bake / "leaderboard_final.md").write_text("\n".join(md) + "\n")
    print("\n".join(md))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("cmd", choices=("corpus", "packets", "sample", "run", "adversary", "render", "provisional", "score"))
    ap.add_argument("--bake", type=Path, default=Path.home() / ".local/state/watch-rams/clip-bakeoff")
    ap.add_argument("--games-root", action="append")
    ap.add_argument("--only-game", default="")
    ap.add_argument("--n", type=int, default=40)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--contestants", default="")
    ap.add_argument("--contestant", action="append", default=[], help="extra contestant name=kind:command")
    ap.add_argument("--run-tag", default="run1")
    ap.add_argument("--subset", type=int, default=0)
    ap.add_argument("--pairs", default="")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--attempts", type=int, default=3)
    ap.add_argument("--timeout", type=float, default=900)
    ap.add_argument("--marks", type=Path)
    a = ap.parse_args()
    a.bake = a.bake.expanduser().resolve()
    for spec in a.contestant:
        name, rest = spec.split("=", 1)
        kind, _, cmd = rest.partition(":")
        CONTESTANTS[name] = {"kind": kind, "cmd": cmd}
    if a.cmd == "adversary":
        prev = jload(a.bake / "adversary_pairs.json") if (a.bake / "adversary_pairs.json").exists() else []
        new = [p.split(":") for p in a.pairs.split(",") if p]
        jdump(a.bake / "adversary_pairs.json", prev + [p for p in new if p not in prev])
    globals()[f"cmd_{a.cmd}"](a)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
