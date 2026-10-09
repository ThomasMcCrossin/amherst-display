"""
Review backends. Each returns {"verdict": dict|None, "stats": {...}, "error": str|None, "log": path}.

  api    one OpenAI-compatible vision endpoint, two passes (coarse sheets, then dense sheets
         around the boundaries). Provider-neutral; the public default. Env: SCOREBUG_VISION_API_KEY
         (or DEEPSEEK_API_KEY), SCOREBUG_VISION_BASE_URL, SCOREBUG_VISION_MODEL.
  agent  an agent harness command (CLIP_REVIEW_AGENT_CMD / CLIP_REVIEW_ADVERSARY_CMD) run in the
         packet dir; it reads SKILL.md, looks at frames itself and writes the verdict file.
         `{prompt}` in the command is the prompt (else stdin); `{skill}` is the skill dir.
  escalate  the api backend for every incident, handing off to an agent backend only when
         should_escalate() says so (low confidence, scorebug-alert game, fight or major).

No harness command lives here; commands are configuration (see README for examples).
"""

from __future__ import annotations

import base64
import json
import os
import shlex
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import SKILL_DIR, frames
from .incidents import describe

DEFAULT_BASE_URL = "https://api.deepseek.com"
DEFAULT_MODEL = "deepseek-flash"


def _data_url(path: Path) -> str:
    return "data:image/jpeg;base64," + base64.b64encode(Path(path).read_bytes()).decode()


def _num(v: Any) -> Optional[float]:
    try:
        f = float(v)
        return f if f == f and abs(f) != float("inf") else None
    except (TypeError, ValueError):
        return None


# ======================================================================================
# api backend
# ======================================================================================
class ApiBackend:
    kind = "api"

    def __init__(self, model: Optional[str] = None, base_url: Optional[str] = None, name: Optional[str] = None):
        self.model = model or os.environ.get("SCOREBUG_VISION_MODEL") or DEFAULT_MODEL
        self.base_url = (base_url or os.environ.get("SCOREBUG_VISION_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.name = name or f"api-{self.model}"

    def identity(self) -> str:
        return f"api|{self.base_url}|{self.model}|v1"

    def _call(self, content: List[Dict[str, Any]], required: Tuple[str, ...], stats: Dict[str, Any], retries: int = 2) -> Dict[str, Any]:
        import requests

        key = os.environ.get("SCOREBUG_VISION_API_KEY") or os.environ.get("DEEPSEEK_API_KEY")
        if not key:
            raise RuntimeError("no vision API key (SCOREBUG_VISION_API_KEY or DEEPSEEK_API_KEY)")
        body: Dict[str, Any] = {"model": self.model, "temperature": 0, "response_format": {"type": "json_object"},
                                "messages": [{"role": "user", "content": content}]}
        if "deepseek" in self.base_url:
            body["thinking"] = {"type": "disabled"}
        last: Optional[Exception] = None
        for attempt in range(retries + 1):
            try:
                resp = requests.post(f"{self.base_url}/chat/completions", json=body, timeout=240,
                                     headers={"Authorization": f"Bearer {key}"})
                resp.raise_for_status()
                payload = resp.json()
                u = payload.get("usage", {})
                stats["input_tokens"] += int(u.get("prompt_tokens") or 0)
                stats["output_tokens"] += int(u.get("completion_tokens") or 0)
                stats["calls"] += 1
                answer = json.loads(payload["choices"][0]["message"]["content"])
                missing = [k for k in required if answer.get(k) is None]
                if missing:
                    raise ValueError(f"answer lacks {missing}")
                return answer
            except Exception as exc:  # noqa: BLE001 - retry transport and parse failures
                last = exc
                stats["retries"] += 1
                time.sleep(2 + 3 * attempt)
        raise RuntimeError(f"vision call failed: {last}")

    # ---- reviewer ----
    def review(self, packet: Path, out_path: Path, objection: Optional[Dict[str, Any]] = None, **_: Any) -> Dict[str, Any]:
        inc = json.loads((packet / "incident.json").read_text())
        stats = {"wall_s": 0.0, "input_tokens": 0, "output_tokens": 0, "cache_read_tokens": 0, "calls": 0, "retries": 0,
                 "cost_usd": None, "cost_basis": "tokens only"}
        t_start = time.time()
        work = out_path.parent / (out_path.stem + "_work")
        try:
            verdict = self._review(packet, inc, work, stats, objection)
            out_path.write_text(json.dumps(verdict, indent=2))
            err = None
        except Exception as exc:  # noqa: BLE001
            verdict, err = None, str(exc)
        stats["wall_s"] = round(time.time() - t_start, 1)
        return {"verdict": verdict, "stats": stats, "error": err, "log": None}

    def _review(self, packet: Path, inc: Dict[str, Any], work: Path, stats: Dict[str, Any],
                objection: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        cls, kind = inc["class"], inc["kind"]
        anchor, video = float(inc["anchor"]), Path(inc["video"])
        b = inc["bounds"]
        text = (
            "You are reviewing a hockey broadcast recording to choose clip boundaries for a highlight. The images are contact "
            "sheets of frames in time order, left to right, top to bottom. The number on each frame is its time in seconds "
            "relative to the moment our system THINKS the event happened (0, highlighted yellow, is only an estimate from the "
            "scorebug clock: the real event can be tens of seconds earlier or later, or missing; the on-screen clock lags around "
            "stoppages" + (", and in THIS game the scorebug was stuck or glitching, so trust the picture" if inc["scorebug_alert"] else "")
            + "). Replays and graphics interrupt live action; a replay is never the live event.\n\n"
            "The event, from the official game sheet (do not invent others):\n" + describe(
                {"kind": kind, "period": inc["period"], "time_elapsed": inc["time_elapsed"], "rows": inc["sheet_rows"]}, inc["teams"])
            + "\n\nOther game-sheet events nearby (NOT this event; labels are where our system places them): "
            + ("; ".join(f"{n['what']} at {n['placed_t']:+.0f}" for n in inc["neighbours"]) or "none") + ".\n\n")
        if kind == "goal":
            text += ("Find the goal. event_moment = puck crosses the line. play_start = first frame of the possession that "
                     "produced the goal, to show the build-up: the zone entry, the possession change, or the faceoff win "
                     "that led to the goal. When in doubt go earlier: never less than 15 s before the goal, up to 45 s when "
                     "the play builds longer (cycle, sustained pressure, rush from its own zone). play_end = last frame of the live celebration, or the "
                     "first replay/graphic, whichever comes first. The clock freezes at the goal; the score digit changes later.\n")
        elif cls in ("major", "fight"):
            text += ("This is a major / misconduct / fight incident. Find it. If it is a fight, set fight.gloves_drop to when gloves "
                     "come off, fight.separated to when the linesmen have them apart, event_moment to the gloves drop; play_start = "
                     "a few seconds before the confrontation begins, play_end = when the players are separated or sent off. "
                     "If it is a scrum or a hit, set fight to null.\n")
        else:
            text += ("Find where play stopped for this penalty: whistle with a referee's arm raised, a delayed-penalty call, a "
                     "player going to the penalty box. event_moment = the foul if visible, else that stoppage. play_start = about "
                     "8 s before the stoppage, play_end = the referee's signal / player heading to the box. Ignore stoppages with "
                     "no penalty (icing, offside, puck out of play).\n")
        if objection:
            text += ("\nA previous review of this incident was disputed. The objection (check it against the frames yourself; it "
                     "can be wrong): " + json.dumps(objection.get("objections") or objection) + "\n")
        text += ('Reply with JSON only: {"event_visible": true/false, "event_moment": <label>, "play_start": <label>, '
                 '"play_start_kind": "faceoff|zone_entry|possession_change|rush|dump_in|cycle|pre_foul|pre_whistle|pre_confrontation|other", '
                 '"play_end": <label>, "play_end_kind": "celebration_over|cut_to_replay|graphic|whistle|referee_signal|to_penalty_box|'
                 'players_separated|sent_off|other", "replay_start": <label or null>, "foul_visible": true/false/null, '
                 '"fight": null or {"gloves_drop": <label>, "separated": <label>}, '
                 '"evidence": [{"t": <label>, "what": "what is on screen"}, ...2-4 entries around the event], '
                 '"confidence": 0.0-1.0, "reason": "one sentence"}')
        content: List[Dict[str, Any]] = [{"type": "text", "text": text}]
        for i, s in enumerate(inc["coarse_sheets"], 1):
            content.append({"type": "text", "text": f"Sheet {i}/{len(inc['coarse_sheets'])}: labels {s['from']:+.0f} to {s['to']:+.0f}"})
            content.append({"type": "image_url", "image_url": {"url": _data_url(packet / s["path"])}})
        content.append({"type": "text", "text": "Now answer with the JSON object. All times are numbers taken from the frame labels."})
        c = self._call(content, ("event_visible", "confidence"), stats)

        base = {"schema": "hockey-clip-review/verdict@1", "role": "reviewer", "incident_id": inc["incident_id"],
                "scorebug": "unknown", "replay_t": _num(c.get("replay_start")), "whistle_t": None,
                "foul_visible": (bool(c.get("foul_visible")) if kind == "penalty" else None), "foul_what": None, "fight": None,
                "evidence": [e for e in (c.get("evidence") or []) if isinstance(e, dict) and _num(e.get("t")) is not None],
                "confidence": max(0.0, min(1.0, _num(c.get("confidence")) or 0.0)), "reason": str(c.get("reason") or "")}
        for e in base["evidence"]:
            e["t"] = _num(e["t"])
            e["what"] = str(e.get("what") or "")
        moment, p_in, p_out = _num(c.get("event_moment")), _num(c.get("play_start")), _num(c.get("play_end"))
        if not c.get("event_visible") or base["confidence"] < 0.5 or None in (moment, p_in, p_out):
            return dict(base, decision="unsure", event_visible=bool(c.get("event_visible")), event_t=moment, in_t=None, out_t=None,
                        in_kind=None, out_kind=None)
        fight = c.get("fight") if isinstance(c.get("fight"), dict) else None
        if fight and _num(fight.get("gloves_drop")) is not None and _num(fight.get("separated")) is not None:
            base["fight"] = {"gloves_drop_t": _num(fight["gloves_drop"]), "separated_t": _num(fight["separated"]), "who": None}

        # ---- refine on dense sheets ----
        minor = cls == "minor"
        plan = ((("foul", moment - 20.0, moment + 2.0), ("end", p_out - 7.0, p_out + 7.0)) if minor else
                (("start", p_in - 7.0, p_in + 7.0), ("end", p_out - 7.0, p_out + 7.0)))
        bnds = []
        for name, lo, hi in plan:
            lo, hi = max(lo, b["video_from"]), min(hi, b["video_to"])
            fr = frames.extract(video, anchor + lo, anchor + hi, 0.5)
            bnds.append((name, (lo + hi) / 2, frames.make_sheets(fr, anchor, work, f"refine_{name}", precise=True)))
        if minor:
            rtext = ("Hockey broadcast, one minor penalty: " + "; ".join(f"{r.get('team')} - {r.get('infraction')}" for r in inc["sheet_rows"])
                     + ". A first pass found the stoppage near label " + f"{moment:+.1f}" + ". Dense sheets (0.5 s apart, labelled) cover "
                     "the seconds before the whistle and the end. Find the foul (trip, hook, slash, hold, hit, high stick...) and the "
                     "whistle. start = 2 s before the foul; if no foul fits, foul_seen false and start = 8 s before the whistle. end = "
                     "the referee's signal or the player led away.\n"
                     'Reply with JSON only: {"foul_seen": true/false, "foul_what": "...", "foul": <label or null>, "whistle": <label or null>, '
                     '"start": <label>, "end": <label>}')
        else:
            rtext = (f"Hockey broadcast, one {cls}. A first pass proposed: start {p_in:+.1f} ({c.get('play_start_kind')}), end "
                     f"{p_out:+.1f} ({c.get('play_end_kind')}), event {moment:+.1f}. Confirm or correct each boundary on the dense sheets "
                     "(0.5 s apart, labelled on the same scale). start = the first frame of the play that produced the event (for a goal the build-up: zone entry, possession change or "
                     "faceoff win, at least 15 s before the goal; when in doubt earlier); end = last "
                     "frame worth keeping, before any replay/graphic/commercial.\n"
                     'Reply with JSON only: {"start": <label>, "end": <label>, "start_what": "...", "end_what": "..."}')
        rcontent: List[Dict[str, Any]] = [{"type": "text", "text": rtext}]
        for name, centre, sheets in bnds:
            for sh in sheets:
                rcontent.append({"type": "text", "text": f"{name.upper()} window around {centre:+.1f}: labels {sh['from']:+.1f} to {sh['to']:+.1f}"})
                rcontent.append({"type": "image_url", "image_url": {"url": _data_url(Path(sh["path"]))}})
        rcontent.append({"type": "text", "text": "Now answer with the JSON object; start and end are numbers from the labels."})
        try:
            rf = self._call(rcontent, ("start", "end"), stats)
            r_in, r_out = _num(rf.get("start")), _num(rf.get("end"))
            if r_in is not None and (moment - 20.5 <= r_in <= moment + 2.5 if minor else abs(r_in - p_in) <= 7.5):
                p_in = r_in
            elif minor:
                p_in = moment - 8.0
            if r_out is not None and abs(r_out - p_out) <= 7.5:
                p_out = r_out
            if minor:
                base["foul_visible"] = bool(rf.get("foul_seen"))
                base["foul_what"] = rf.get("foul_what")
                base["whistle_t"] = _num(rf.get("whistle"))
                f_t = _num(rf.get("foul"))
                if base["foul_visible"] and f_t is not None:
                    moment = f_t
        except Exception as exc:  # noqa: BLE001 - keep the coarse answer
            base["reason"] += f" (refine failed: {exc})"
        if p_in >= moment:
            p_in = moment - b["lead_min"]
        if p_out <= moment:
            p_out = moment + b["tail_min"]
        decision = "relocate" if abs(moment) > b["near"] else "adjust"
        return dict(base, decision=decision, event_visible=True, event_t=moment, in_t=p_in, out_t=p_out,
                    in_kind=c.get("play_start_kind") if c.get("play_start_kind") in _IN_KINDS else "other",
                    out_kind=c.get("play_end_kind") if c.get("play_end_kind") in _OUT_KINDS else "other")

    # ---- adversary ----
    def adversary(self, packet: Path, out_path: Path, adv_input: Path, **_: Any) -> Dict[str, Any]:
        inc = json.loads((packet / "incident.json").read_text())
        ai = json.loads(adv_input.read_text())
        rv = json.loads(Path(ai["verdict_path"]).read_text())
        stats = {"wall_s": 0.0, "input_tokens": 0, "output_tokens": 0, "cache_read_tokens": 0, "calls": 0, "retries": 0,
                 "cost_usd": None, "cost_basis": "tokens only"}
        t_start = time.time()
        text = ("You are the adversary checking a proposed hockey highlight clip. Try to refute it. The event, from the official "
                "game sheet:\n" + describe({"kind": inc["kind"], "period": inc["period"], "time_elapsed": inc["time_elapsed"],
                                            "rows": inc["sheet_rows"]}, inc["teams"])
                + f"\nThe reviewer's verdict: {json.dumps({k: rv.get(k) for k in ('decision', 'event_visible', 'event_t', 'in_t', 'out_t', 'replay_t', 'fight', 'reason')})}.\n"
                f"The final clip is labels {ai['final_in_t']:+.1f} to {ai['final_out_t']:+.1f}; the sheets show it with ~2 s margin. "
                "Look for: the event not in the clip, the clip ending before the puck crosses the line / the foul, starting mid-play, "
                "a long unrelated lead-in, the celebration or fight cut off, a replay included, a neighbour's event instead of this "
                "one, a dropped/unsure event that is actually visible. Object ONLY to material faults: starts_too_early only with "
                "more than ~20 s of play unrelated to this play before it (a 5-30 s lead-in showing the play develop is wanted); "
                f"ends_early only when the clip stops less than {inc['bounds']['tail_min']:.0f} s after the event, mid-fight, or "
                "mid-celebration with the scorer still celebrating on screen; replay_included only with at least 1.5 s of replay "
                "inside the clip (a replay starting at the out-point is correct); too_long only past the length cap or with more "
                "than ~15 s of dead time. Boundaries within ~3 s of where you would put them are fine. A weak objection is a wrong one. "
                'Reply with JSON only: {"agree": true/false, "objections": [{"kind": "event_not_in_clip|cut_before_event|starts_mid_play|'
                'starts_too_early|ends_early|replay_included|wrong_incident|fight_cut_off|foul_not_in_clip|too_long|drop_unjustified|other", '
                '"t": <label or null>, "detail": "..."}], "suggested_in_t": <label or null>, "suggested_out_t": <label or null>, '
                '"confidence": 0.0-1.0, "reason": "one sentence"}')
        content: List[Dict[str, Any]] = [{"type": "text", "text": text}]
        sheets = ai.get("final_sheets") or []
        if not sheets:  # drop/unsure: show the coarse search instead
            sheets = [dict(s, path=str(packet / s["path"])) for s in inc["coarse_sheets"]]
        for s in sheets:
            content.append({"type": "text", "text": f"labels {s['from']:+.1f} to {s['to']:+.1f}"})
            content.append({"type": "image_url", "image_url": {"url": _data_url(Path(s["path"]))}})
        try:
            a = self._call(content, ("agree",), stats)
            objections = [o for o in (a.get("objections") or []) if isinstance(o, dict)]
            for o in objections:
                if o.get("kind") not in _OBJ_KINDS:
                    o["kind"] = "other"
                o["t"] = _num(o.get("t"))
                o["detail"] = str(o.get("detail") or "")
            agree = bool(a.get("agree"))
            if not agree and not objections:
                objections = [{"kind": "other", "t": None, "detail": str(a.get("reason") or "disputed")}]
            v = {"schema": "hockey-clip-review/adversary@1", "role": "adversary", "incident_id": inc["incident_id"], "agree": agree,
                 "objections": [] if agree else objections, "suggested_in_t": _num(a.get("suggested_in_t")),
                 "suggested_out_t": _num(a.get("suggested_out_t")),
                 "confidence": max(0.0, min(1.0, _num(a.get("confidence")) or 0.0)), "reason": str(a.get("reason") or "")}
            out_path.write_text(json.dumps(v, indent=2))
            err = None
        except Exception as exc:  # noqa: BLE001
            v, err = None, str(exc)
        stats["wall_s"] = round(time.time() - t_start, 1)
        return {"verdict": v, "stats": stats, "error": err, "log": None}


_IN_KINDS = {"faceoff", "zone_entry", "possession_change", "rush", "dump_in", "cycle", "pre_foul", "pre_whistle", "pre_confrontation", "other"}
_OUT_KINDS = {"celebration_over", "cut_to_replay", "graphic", "whistle", "referee_signal", "to_penalty_box", "players_separated", "sent_off", "other"}
_OBJ_KINDS = {"event_not_in_clip", "cut_before_event", "starts_mid_play", "starts_too_early", "ends_early", "replay_included",
              "wrong_incident", "fight_cut_off", "foul_not_in_clip", "too_long", "drop_unjustified", "other"}


# ======================================================================================
# agent backend
# ======================================================================================
REVIEW_PROMPT = """You are the REVIEWER for one hockey highlight incident.
Read the skill file {skill}/SKILL.md and follow its reviewer procedure exactly (SKILL_DIR={skill}).
Packet directory: {packet} . Start by reading {packet}/incident.json.
Run the skill scripts with: {python}
Write the verdict JSON to: {out}
Then run: {python} {skill}/scripts/check_verdict.py {out} --packet {packet}
and fix the verdict until it prints OK.
Budget: at most ~20 turns and ~12 frame pulls; coarse sheets first, dense frames only around the boundaries.
{extra}Finish with one line: VERDICT {out}"""

ADVERSARY_PROMPT = """You are the ADVERSARY for one hockey highlight incident.
Read the skill file {skill}/SKILL.md and follow its "Adversary role" section exactly (SKILL_DIR={skill}).
Packet directory: {packet} . Read {packet}/incident.json, then the adversary input {adv_input}
(it names the reviewer's verdict and the contact sheets of the proposed final clip).
Run the skill scripts with: {python}
Write your adversary JSON to: {out}
Then run: {python} {skill}/scripts/check_verdict.py {out} --packet {packet}
and fix it until it prints OK.
Budget: at most ~12 turns and ~6 frame pulls.
Finish with one line: VERDICT {out}"""


def parse_harness_stats(stdout: str) -> Dict[str, Any]:
    """Token/cost figures from pi --mode json (JSONL events) or claude -p --output-format json."""
    stats: Dict[str, Any] = {"input_tokens": 0, "output_tokens": 0, "cache_read_tokens": 0, "turns": 0, "image_reads": 0,
                             "cost_usd": None, "cost_basis": "unavailable", "final_text": ""}
    text = stdout.strip()
    if not text:
        return stats
    try:  # claude-style single JSON result
        d = json.loads(text)
        if isinstance(d, dict) and "usage" in d:
            u = d.get("usage") or {}
            stats.update(input_tokens=int(u.get("input_tokens") or 0) + int(u.get("cache_creation_input_tokens") or 0),
                         output_tokens=int(u.get("output_tokens") or 0), cache_read_tokens=int(u.get("cache_read_input_tokens") or 0),
                         turns=int(d.get("num_turns") or 0), cost_usd=d.get("total_cost_usd"),
                         cost_basis="harness-reported (may be list price of a mapped model, not the real bill)",
                         final_text=str(d.get("result") or "")[-500:])
            return stats
    except ValueError:
        pass
    cost, saw_cost = 0.0, False
    for line in text.splitlines():  # pi-style JSONL
        try:
            d = json.loads(line)
        except ValueError:
            continue
        if not isinstance(d, dict) or d.get("type") != "message_end":
            continue
        m = d.get("message") or {}
        if m.get("role") == "assistant":
            u = m.get("usage") or {}
            stats["input_tokens"] += int(u.get("input") or 0)
            stats["output_tokens"] += int(u.get("output") or 0)
            stats["cache_read_tokens"] += int(u.get("cacheRead") or 0)
            stats["turns"] += 1
            c = (u.get("cost") or {}).get("total")
            if isinstance(c, (int, float)):
                cost += c
                saw_cost = True
            for part in m.get("content") or []:
                if isinstance(part, dict) and part.get("type") == "text":
                    stats["final_text"] = str(part.get("text") or "")[-500:]
        elif m.get("role") == "toolResult":
            if any(isinstance(p, dict) and p.get("type") == "image" for p in m.get("content") or []):
                stats["image_reads"] += 1
    if saw_cost:
        stats["cost_usd"] = round(cost, 6)
        stats["cost_basis"] = "harness-reported (0 = subscription/flat-rate provider)"
    return stats


class AgentBackend:
    kind = "agent"

    def __init__(self, cmd: str, name: str, timeout: float = 900.0):
        if not cmd:
            raise ValueError("agent backend needs a command (CLIP_REVIEW_AGENT_CMD or --agent-cmd)")
        self.cmd = cmd
        self.name = name
        self.timeout = timeout

    def identity(self) -> str:
        return f"agent|{self.cmd}"

    def _argv(self, prompt: str) -> Tuple[List[str], Optional[str]]:
        argv = [a.replace("{skill}", str(SKILL_DIR)) for a in shlex.split(self.cmd)]
        if "{prompt}" in argv:
            return [prompt if a == "{prompt}" else a for a in argv], None
        return argv, prompt  # no placeholder: prompt goes on stdin

    def _run(self, packet: Path, out_path: Path, prompt: str) -> Dict[str, Any]:
        argv, stdin = self._argv(prompt)
        log = out_path.with_suffix(".log")
        work = out_path.parent / (out_path.stem + "_work")
        work.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ, HCR_WORK_DIR=str(work))
        t0 = time.time()
        err = None
        try:
            proc = subprocess.run(argv, cwd=packet, input=stdin, capture_output=True, text=True, timeout=self.timeout, env=env)
            stdout, stderr, rc = proc.stdout, proc.stderr, proc.returncode
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")
            stderr, rc, err = "", -9, f"timeout after {self.timeout:.0f}s"
        wall = round(time.time() - t0, 1)
        log.write_text(stdout + ("\n--- stderr ---\n" + stderr if stderr else ""))
        stats = parse_harness_stats(stdout)
        stats.update(wall_s=wall, returncode=rc)
        verdict = None
        if out_path.exists():
            try:
                verdict = json.loads(out_path.read_text())
            except ValueError as exc:
                err = err or f"verdict is not valid JSON: {exc}"
        else:
            err = err or f"agent wrote no verdict (rc={rc}): {stats.get('final_text', '')[-200:]}"
        return {"verdict": verdict, "stats": stats, "error": err, "log": str(log)}

    def review(self, packet: Path, out_path: Path, objection_path: Optional[Path] = None, retry_note: str = "", **_: Any) -> Dict[str, Any]:
        inc = json.loads((packet / "incident.json").read_text())
        extra = ""
        if objection_path:
            extra += (f"An adversary disputed a first review of this incident. Its objection is in {objection_path} "
                      "(see the skill's 'Second review' section); answer it in objection_answer.\n")
        if retry_note:
            extra += retry_note + "\n"
        prompt = REVIEW_PROMPT.format(skill=SKILL_DIR, packet=packet, python=inc["tools"]["python"], out=out_path, extra=extra)
        out_path.unlink(missing_ok=True)
        return self._run(packet, out_path, prompt)

    def adversary(self, packet: Path, out_path: Path, adv_input: Path, **_: Any) -> Dict[str, Any]:
        inc = json.loads((packet / "incident.json").read_text())
        prompt = ADVERSARY_PROMPT.format(skill=SKILL_DIR, packet=packet, python=inc["tools"]["python"], out=out_path, adv_input=adv_input)
        out_path.unlink(missing_ok=True)
        return self._run(packet, out_path, prompt)

    @staticmethod
    def prompt_text() -> str:
        return REVIEW_PROMPT + ADVERSARY_PROMPT


# ======================================================================================
# escalate: api first, an agent only where the cheap call is weak
# ======================================================================================
ESCALATE_MIN_CONFIDENCE = float(os.environ.get("CLIP_REVIEW_ESCALATE_MIN_CONFIDENCE", "0.6"))


def should_escalate(verdict: Optional[Dict[str, Any]], incident: Dict[str, Any], error: Optional[str] = None) -> Optional[str]:
    """Why the api verdict needs the agent, or None. Shared by EscalateBackend and the bake-off."""
    if incident.get("class") in ("major", "fight"):
        return "fight_or_major"
    if incident.get("scorebug_alert"):
        return "scorebug_alert"
    if error or not verdict:
        return "no_verdict"
    if verdict.get("decision") in ("unsure", "drop"):
        return "unsure_or_drop"
    if (_num(verdict.get("confidence")) or 0.0) < ESCALATE_MIN_CONFIDENCE:
        return "low_confidence"
    return None


class EscalateBackend:
    kind = "escalate"

    def __init__(self, primary: ApiBackend, fallback: AgentBackend, name: str = "escalate"):
        self.primary, self.fallback, self.name = primary, fallback, name

    def identity(self) -> str:
        return f"escalate|{self.primary.identity()}|{self.fallback.identity()}|{ESCALATE_MIN_CONFIDENCE}"

    def review(self, packet: Path, out_path: Path, **kw: Any) -> Dict[str, Any]:
        inc = json.loads((packet / "incident.json").read_text())
        res = self.primary.review(packet, out_path, **kw)
        why = should_escalate(res.get("verdict"), inc, res.get("error"))
        if not why:
            res["stats"] = dict(res.get("stats") or {}, escalated=None)
            return res
        first = res.get("stats") or {}
        out = self.fallback.review(packet, out_path, **kw)
        st = dict(out.get("stats") or {})
        for k in ("input_tokens", "output_tokens", "cache_read_tokens", "cost_usd", "wall_s"):
            if isinstance(first.get(k), (int, float)):
                st[k] = round((st.get(k) or 0) + first[k], 6)
        st["escalated"] = why
        out["stats"] = st
        return out

    def adversary(self, packet: Path, out_path: Path, adv_input: Path, **kw: Any) -> Dict[str, Any]:
        return self.primary.adversary(packet, out_path, adv_input, **kw)

    @staticmethod
    def prompt_text() -> str:
        return AgentBackend.prompt_text()
