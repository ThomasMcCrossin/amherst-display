"""
Stages 2-3: reviewer and adversary per incident, with the authority bounds enforced in code.

Flow per incident (results under <packet>/out/<contestant>/):
  r1 reviewer -> enforce -> [adversary a1 on the final clip]
     a1 agrees            -> accepted
     a1 disputes          -> r2 fresh reviewer with the objection -> enforce -> a2
        a2 agrees         -> accepted (r2)
        a2 disputes       -> held_for_human (engine window kept)
Malformed or invalid output is retried (with the check errors) up to `attempts` times.
Every role result is cached by (contestant identity, incident, prompt hash, run tag).
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import SKILL_DIR, checker, frames

ADVERSARY_CLASSES = {"goal", "major", "fight"}


def prompt_hash(backend: Any, role: str) -> str:
    h = hashlib.sha256()
    h.update(backend.identity().encode())
    h.update(role.encode())
    for p in (SKILL_DIR / "SKILL.md", SKILL_DIR / "scripts" / "check_verdict.py", SKILL_DIR / "verdict.schema.json",
              Path(__file__).with_name("backends.py")):
        h.update(p.read_bytes())
    return h.hexdigest()[:16]


# ---------------------------------------------------------------------------------------
# enforcement: the code's authority over the reviewer
# ---------------------------------------------------------------------------------------
def enforce(verdict: Optional[Dict[str, Any]], incident: Dict[str, Any]) -> Dict[str, Any]:
    """Final window (relative + absolute) for a reviewer verdict, or the engine window."""
    b, anchor = incident["bounds"], float(incident["anchor"])
    eng = {"in_t": b["engine_in"], "out_t": b["engine_out"]}

    def out(status: str, w: Optional[Dict[str, float]], **kw: Any) -> Dict[str, Any]:
        res = {"status": status, "in_t": None, "out_t": None, "in": None, "out": None}
        if w:
            res.update(in_t=round(w["in_t"], 2), out_t=round(w["out_t"], 2), **{"in": round(anchor + w["in_t"], 2),
                                                                                "out": round(anchor + w["out_t"], 2)})
        res.update(kw)
        return res

    if not verdict:
        return out("no_verdict", eng)
    clamped, notes = checker.clamp(verdict, incident)
    errors, warnings = checker.check(clamped, incident)
    if errors:
        return out("rejected", eng, errors=errors, warnings=warnings)
    dec = clamped["decision"]
    if dec == "keep":
        return out("keep", eng, warnings=warnings)
    if dec == "unsure":
        return out("unsure", eng, warnings=warnings)
    if dec == "drop":
        return out("drop", None, warnings=warnings)
    return out("override", {"in_t": clamped["in_t"], "out_t": clamped["out_t"]}, clamps=notes, warnings=warnings,
               event_t=clamped.get("event_t"), event=round(anchor + float(clamped["event_t"]), 2), decision=dec)


def final_sheets(incident: Dict[str, Any], window: Dict[str, Any], dest: Path) -> List[Dict[str, Any]]:
    """Contact sheets of the proposed final clip (+-2 s margin), at most ~40 frames."""
    if window.get("in_t") is None:
        return []
    anchor, video = float(incident["anchor"]), Path(incident["video"])
    b = incident["bounds"]
    lo, hi = max(b["video_from"], window["in_t"] - 2.0), min(b["video_to"], window["out_t"] + 2.0)
    step = max(0.5, math.ceil((hi - lo) / 39 * 2) / 2)
    fr = frames.extract(video, anchor + lo, anchor + hi, step)
    return frames.make_sheets(fr, anchor, dest, "final", precise=step < 1)


# ---------------------------------------------------------------------------------------
# one role, cached, with retries
# ---------------------------------------------------------------------------------------
def _cached(path: Path, key: str) -> Optional[Dict[str, Any]]:
    meta = path.with_suffix(".meta.json")
    if path.exists() and meta.exists():
        try:
            m = json.loads(meta.read_text())
            if m.get("key") == key and m.get("ok"):
                return {"verdict": json.loads(path.read_text()), "stats": m.get("stats") or {}, "error": None,
                        "attempts": m.get("attempts"), "cached": True, "log": m.get("log")}
        except ValueError:
            pass
    return None


def run_role(backend: Any, role: str, packet: Path, out_dir: Path, tag: str, incident: Dict[str, Any],
             attempts: int = 3, **kw: Any) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{tag}.json"
    key = f"{prompt_hash(backend, role)}|{incident['incident_id']}|{tag}|{kw.get('objection_path') or ''}|{kw.get('cache_salt') or ''}"
    hit = _cached(path, key)
    if hit:
        return hit
    total: Dict[str, Any] = {}
    note, res, errors = "", None, []
    n = 0
    for n in range(1, attempts + 1):
        call = getattr(backend, "review" if role == "reviewer" else "adversary")
        res = call(packet, path, retry_note=note, **{k: v for k, v in kw.items() if k != "cache_salt"})
        for k, v in (res.get("stats") or {}).items():
            if isinstance(v, (int, float)) and not isinstance(v, bool) and k not in ("returncode",):
                total[k] = round(total.get(k, 0) + v, 6) if v is not None else total.get(k)
            elif k not in total:
                total[k] = v
        v = res.get("verdict")
        if v is None:
            errors = [res.get("error") or "no verdict"]
        else:
            if role == "reviewer":
                v2, _ = checker.clamp(v, incident) if not checker.schema_errors(v) else (v, [])
                errors, _w = checker.check(v2, incident)
            else:
                errors, _w = checker.check(v, incident)
        if not errors:
            break
        note = ("Your previous attempt failed the check: " + "; ".join(errors[:6]) +
                ". Write a corrected verdict and run check_verdict.py until it prints OK.")
    ok = not errors
    meta = {"key": key, "ok": ok, "attempts": n, "errors": errors, "stats": total, "log": (res or {}).get("log"),
            "backend": backend.name, "role": role, "tag": tag, "at": time.strftime("%Y-%m-%dT%H:%M:%S")}
    path.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2))
    verdict = (res or {}).get("verdict")
    return {"verdict": verdict, "valid": ok, "stats": total, "error": None if ok else "; ".join(errors),
            "attempts": n, "cached": False, "log": meta["log"]}


# ---------------------------------------------------------------------------------------
# one incident end to end
# ---------------------------------------------------------------------------------------
def review_incident(packet: Path, reviewer: Any, adversary: Optional[Any] = None, run_tag: str = "run1",
                    adversary_all: bool = False, attempts: int = 3) -> Dict[str, Any]:
    incident = json.loads((packet / "incident.json").read_text())
    out_dir = packet / "out" / reviewer.name / run_tag
    t0 = time.time()
    res: Dict[str, Any] = {"incident_id": incident["incident_id"], "class": incident["class"], "kind": incident["kind"],
                           "reviewer": reviewer.name, "adversary": getattr(adversary, "name", None), "run": run_tag,
                           "engine_window": incident["engine_window"], "anchor": incident["anchor"], "steps": []}

    def step(name: str, r: Dict[str, Any]) -> None:
        res["steps"].append({"step": name, "valid": r.get("valid", r.get("error") is None), "error": r.get("error"),
                             "attempts": r.get("attempts"), "cached": r.get("cached"), "stats": r.get("stats")})

    r1 = run_role(reviewer, "reviewer", packet, out_dir, "r1", incident, attempts=attempts)
    step("r1", r1)
    valid1 = r1.get("error") is None
    final = enforce(r1["verdict"], incident)
    v1 = r1["verdict"] if valid1 else None
    res.update(r1=r1["verdict"], r1_final=final)
    res["final"], res["verdict"] = final, v1
    res["malformed"] = not valid1

    if adversary and (incident["class"] in ADVERSARY_CLASSES or adversary_all) and v1 is not None:
        adv_dir = packet / "out" / reviewer.name / run_tag / f"adv-{adversary.name}"
        a1 = _adversary(adversary, packet, adv_dir, "a1", incident, out_dir / "r1.json", final, attempts)
        step("a1", a1)
        res["a1"] = a1["verdict"]
        if a1["verdict"] is not None and not a1["verdict"].get("agree"):
            objection = adv_dir / "objection_a1.json"
            objection.write_text(json.dumps(a1["verdict"], indent=2))
            r2 = run_role(reviewer, "reviewer", packet, adv_dir, "r2", incident, attempts=attempts, objection_path=objection,
                          objection=a1["verdict"])
            step("r2", r2)
            v2 = r2["verdict"] if r2.get("error") is None else None
            final2 = enforce(r2["verdict"], incident)
            res.update(r2=v2, r2_final=final2)
            a2 = _adversary(adversary, packet, adv_dir, "a2", incident, adv_dir / "r2.json", final2, attempts) if v2 else None
            if a2:
                step("a2", a2)
                res["a2"] = a2["verdict"]
            if v2 is not None and a2 and a2["verdict"] is not None and a2["verdict"].get("agree"):
                res["final"], res["verdict"] = final2, v2
                res["dispute"] = "resolved_by_second_review"
            else:
                res["final"] = dict(enforce(None, incident), status="held_for_human",
                                    disputed=final, objections=a1["verdict"].get("objections"))
                res["dispute"] = "held_for_human"
        elif a1["verdict"] is not None:
            res["dispute"] = "none"
    res["wall_s"] = round(time.time() - t0, 1)
    tot: Dict[str, float] = {}
    for s in res["steps"]:
        for k in ("input_tokens", "output_tokens", "cache_read_tokens", "cost_usd", "wall_s"):
            v = (s.get("stats") or {}).get(k)
            if isinstance(v, (int, float)) and not s.get("cached"):
                tot[k] = round(tot.get(k, 0) + v, 6)
    res["usage"] = tot
    name = "result.json" if not adversary else f"result_adv-{adversary.name}.json"
    (out_dir / name).write_text(json.dumps(res, indent=2, default=str))
    return res


def _adversary(adversary: Any, packet: Path, adv_dir: Path, tag: str, incident: Dict[str, Any], verdict_path: Path,
               final: Dict[str, Any], attempts: int) -> Dict[str, Any]:
    adv_dir.mkdir(parents=True, exist_ok=True)
    sheets = final_sheets(incident, final, adv_dir / f"{tag}_final") if final.get("in_t") is not None else []
    adv_input = adv_dir / f"{tag}_input.json"
    adv_input.write_text(json.dumps({
        "verdict_path": str(verdict_path), "final_status": final["status"],
        "final_in_t": final.get("in_t") if final.get("in_t") is not None else 0.0,
        "final_out_t": final.get("out_t") if final.get("out_t") is not None else 0.0,
        "final_sheets": sheets,
        "note": ("The reviewer dropped this clip or was unsure; check the coarse sheets for the event (drop_unjustified)."
                 if not sheets else "Sheets cover the final clip with ~2 s margin on each side."),
    }, indent=2))
    salt = f"{final.get('in_t')}|{final.get('out_t')}|{final['status']}"
    return run_role(adversary, "adversary", packet, adv_dir, tag, incident, attempts=attempts, adv_input=adv_input, cache_salt=salt)
