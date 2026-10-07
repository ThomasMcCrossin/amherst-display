#!/usr/bin/env python3
"""
Check a reviewer or adversary verdict against verdict.schema.json and the incident's bounds.

  python3 check_verdict.py <verdict.json> --packet <packet-dir> [--json]

Prints OK (exit 0) or one ERROR line per problem (exit 1); fix them and run it again. Also
prints what the code will do to a reviewer window (clamps), so nothing surprises you.
Standard library only. The pipeline imports check() and clamp() from this file, so the rules
here are the rules the code enforces.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "verdict.schema.json"
TYPES = {"number": (int, float), "string": str, "boolean": bool, "object": dict, "array": list, "null": type(None)}


# ---- tiny JSON-schema subset: $ref, type, enum, required, properties, items, minimum, maximum ----
def _resolve(node: Dict[str, Any], root: Dict[str, Any]) -> Dict[str, Any]:
    while "$ref" in node:
        ref = node["$ref"].lstrip("#/").split("/")
        node = root
        for part in ref:
            node = node[part]
    return node


def _validate(value: Any, node: Dict[str, Any], root: Dict[str, Any], where: str, errors: List[str]) -> None:
    node = _resolve(node, root)
    if "enum" in node and value not in node["enum"]:
        errors.append(f"{where}: {value!r} not one of {node['enum']}")
        return
    if "type" in node:
        allowed = node["type"] if isinstance(node["type"], list) else [node["type"]]
        ok = any(isinstance(value, TYPES[t]) and not (t == "number" and isinstance(value, bool)) for t in allowed)
        if not ok:
            errors.append(f"{where}: expected {'/'.join(allowed)}, got {type(value).__name__}")
            return
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in node and value < node["minimum"]:
            errors.append(f"{where}: {value} < {node['minimum']}")
        if "maximum" in node and value > node["maximum"]:
            errors.append(f"{where}: {value} > {node['maximum']}")
    if isinstance(value, dict):
        for key in node.get("required", []):
            if key not in value:
                errors.append(f"{where}: missing required field {key!r}")
        for key, sub in node.get("properties", {}).items():
            if key in value:
                _validate(value[key], sub, root, f"{where}.{key}", errors)
    if isinstance(value, list) and "items" in node:
        for i, item in enumerate(value):
            _validate(item, node["items"], root, f"{where}[{i}]", errors)


def schema_errors(verdict: Any) -> List[str]:
    root = json.loads(SCHEMA_PATH.read_text())
    if not isinstance(verdict, dict):
        return ["verdict is not a JSON object"]
    role = verdict.get("role")
    if role not in ("reviewer", "adversary"):
        return [f"role must be 'reviewer' or 'adversary', got {role!r}"]
    errors: List[str] = []
    _validate(verdict, root["$defs"][role], root, "verdict", errors)
    return errors


def _num(v: Any) -> Optional[float]:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


# ---- invariants ----
def check(verdict: Dict[str, Any], incident: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """(errors, warnings) for a verdict against its incident packet."""
    errors = schema_errors(verdict)
    warnings: List[str] = []
    if errors:
        return errors, warnings
    if verdict["incident_id"] != incident["incident_id"]:
        errors.append(f"incident_id {verdict['incident_id']!r} is not this packet's {incident['incident_id']!r}")
    if verdict["role"] == "adversary":
        if not verdict["agree"] and not verdict["objections"]:
            errors.append("agree is false but objections is empty: name at least one objection")
        return errors, warnings

    b = incident["bounds"]
    cls = incident["class"]
    dec = verdict["decision"]
    ev, t_in, t_out = _num(verdict.get("event_t")), _num(verdict.get("in_t")), _num(verdict.get("out_t"))
    evidence = verdict.get("evidence") or []
    if dec == "unsure":
        return errors, warnings
    if dec == "keep":
        for name, val, eng in (("in_t", t_in, b["engine_in"]), ("out_t", t_out, b["engine_out"])):
            if val is not None and abs(val - eng) > 1.0:
                errors.append(f"decision keep means the engine window ({b['engine_in']:+.1f}..{b['engine_out']:+.1f}); "
                              f"{name}={val:+.1f} differs: use adjust")
        return errors, warnings
    if dec == "drop":
        if verdict["event_visible"]:
            errors.append("drop needs event_visible false (a visible event is clipped, not dropped)")
        if not evidence:
            errors.append("drop needs evidence: frames (t + what is on screen) showing why the event is not there")
        if verdict["confidence"] < b["drop_min_confidence"]:
            errors.append(f"drop needs confidence >= {b['drop_min_confidence']}; otherwise use unsure")
        return errors, warnings

    # adjust / relocate
    if not verdict["event_visible"]:
        errors.append(f"{dec} needs event_visible true; if you cannot see the event use unsure or drop")
    if ev is None or t_in is None or t_out is None:
        errors.append(f"{dec} needs numeric event_t, in_t and out_t")
        return errors, warnings
    if not t_in < ev < t_out:
        errors.append(f"need in_t < event_t < out_t, got {t_in:+.1f} / {ev:+.1f} / {t_out:+.1f}")
    lo, hi = b["video_from"], b["video_to"]
    for name, val in (("in_t", t_in), ("out_t", t_out), ("event_t", ev)):
        if not lo - 0.01 <= val <= hi + 0.01:
            errors.append(f"{name}={val:+.1f} is outside the recording ({lo:+.0f}..{hi:+.0f})")
    if abs(ev) > b["relocate_max"]:
        errors.append(f"event_t={ev:+.1f} is beyond the relocation limit (+-{b['relocate_max']:.0f} s from the anchor)")
    if abs(ev) > b["near"]:
        if dec != "relocate":
            errors.append(f"event_t={ev:+.1f} is more than {b['near']:.0f} s from the anchor: decision must be relocate")
        near_ev = [e for e in evidence if isinstance(e.get("t"), (int, float)) and abs(e["t"] - ev) <= 15]
        if len(near_ev) < 2:
            errors.append("relocate needs at least 2 evidence frames within 15 s of event_t showing the event")
    length = t_out - t_in
    fight = verdict.get("fight") if cls == "fight" else None
    if cls == "fight" and verdict["event_visible"] and not fight:
        warnings.append("fight incident without a fight object: bounded as a major (no fight seen?)")
    if fight:
        g, s = _num(fight.get("gloves_drop_t")), _num(fight.get("separated_t"))
        if g is None or s is None or not g < s:
            errors.append("fight needs gloves_drop_t < separated_t")
        else:
            if t_in < g - b["fight_pre"] - 0.5:
                errors.append(f"fight in_t must be >= gloves_drop_t - {b['fight_pre']:.0f} s ({g - b['fight_pre']:+.1f})")
            if t_out < s - 0.5:
                errors.append(f"out_t {t_out:+.1f} cuts the fight off before the players are separated ({s:+.1f})")
            if t_out > s + b["fight_post"] + 0.5:
                errors.append(f"fight out_t must be <= separated_t + {b['fight_post']:.0f} s ({s + b['fight_post']:+.1f})")
    else:
        lead, tail = ev - t_in, t_out - ev
        if lead < b["lead_min"] - 0.01:
            errors.append(f"in_t is only {lead:.1f} s before the event; need >= {b['lead_min']:.0f} s")
        if lead > b["lead_max"] + 0.01:
            errors.append(f"in_t is {lead:.1f} s before the event; at most {b['lead_max']:.0f} s")
        replay = _num(verdict.get("replay_t"))
        tail_min = b["tail_min"] if replay is None or replay <= ev else min(b["tail_min"], max(b.get("tail_min_replay", 2.0), replay - ev))
        if tail < tail_min - 0.01:
            errors.append(f"out_t is only {tail:.1f} s after the event; need >= {tail_min:.0f} s")
        if tail > b["tail_max"] + 0.01:
            errors.append(f"out_t is {tail:.1f} s after the event; at most {b['tail_max']:.0f} s")
    if length < b["len_min"] - 0.01:
        errors.append(f"clip is {length:.1f} s; minimum {b['len_min']:.0f} s")
    if length > b["len_max"] + 0.01:
        errors.append(f"clip is {length:.1f} s; maximum {b['len_max']:.0f} s")
    replay = _num(verdict.get("replay_t"))
    if replay is not None and ev + b.get("tail_min_replay", 2.0) < replay < t_out - 0.5:
        errors.append(f"replay starts at {replay:+.1f}, inside the clip: end at or before the replay")
    if incident["kind"] == "penalty" and verdict.get("foul_visible") is None:
        errors.append("penalties need foul_visible true/false")
    return errors, warnings


def clamp(verdict: Dict[str, Any], incident: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """Pull a reviewer's adjust/relocate window inside the bounds (what the code does before check())."""
    v = copy.deepcopy(verdict)
    notes: List[str] = []
    if v.get("role") != "reviewer" or v.get("decision") not in ("adjust", "relocate"):
        return v, notes
    ev, t_in, t_out = _num(v.get("event_t")), _num(v.get("in_t")), _num(v.get("out_t"))
    if ev is None or t_in is None or t_out is None:
        return v, notes
    b = incident["bounds"]
    fight = v.get("fight") if incident["class"] == "fight" and isinstance(v.get("fight"), dict) else None
    g = _num((fight or {}).get("gloves_drop_t"))
    s = _num((fight or {}).get("separated_t"))
    replay = _num(v.get("replay_t"))

    def note(msg: str) -> None:
        notes.append(msg)

    if fight and g is not None and s is not None and g < s:
        if t_in < g - b["fight_pre"]:
            t_in = g - b["fight_pre"]; note(f"in-point clamped to {b['fight_pre']:.0f} s before the gloves drop")
        if t_out < s:
            t_out = s + 2.0; note("out-point extended past the separation")
        if t_out > s + b["fight_post"]:
            t_out = s + b["fight_post"]; note(f"out-point clamped to {b['fight_post']:.0f} s after the separation")
        if t_out - t_in > b["len_max"]:
            t_out = t_in + b["len_max"]; note(f"fight clip capped at {b['len_max']:.0f} s")
    else:
        if ev - t_in > b["lead_max"]:
            t_in = ev - b["lead_max"]; note(f"in-point clamped to {b['lead_max']:.0f} s before the event")
        if ev - t_in < b["lead_min"]:
            t_in = ev - b["lead_min"]; note(f"in-point pulled back to {b['lead_min']:.0f} s before the event")
        if replay is not None and ev + b.get("tail_min_replay", 2.0) < replay < t_out:
            t_out = replay; note("out-point moved to the start of the replay")
        tail_min = b["tail_min"] if replay is None or replay <= ev else min(b["tail_min"], max(b.get("tail_min_replay", 2.0), replay - ev))
        if t_out - ev < tail_min:
            t_out = ev + tail_min; note(f"out-point extended to {tail_min:.0f} s after the event")
        if t_out - ev > b["tail_max"]:
            t_out = ev + b["tail_max"]; note(f"out-point clamped to {b['tail_max']:.0f} s after the event")
        if t_out - t_in > b["len_max"]:
            t_in = t_out - b["len_max"]; note(f"clip capped at {b['len_max']:.0f} s (lead-in trimmed)")
        if t_out - t_in < b["len_min"]:
            t_in = max(ev - b["lead_max"], t_out - b["len_min"])
            if t_out - t_in < b["len_min"]:
                t_out = min(ev + b["tail_max"], t_in + b["len_min"])
            note(f"clip lengthened to {b['len_min']:.0f} s (lead-in first)")
    t_in, t_out = max(t_in, b["video_from"]), min(t_out, b["video_to"])
    v["in_t"], v["out_t"] = round(t_in, 2), round(t_out, 2)
    return v, notes


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("verdict", type=Path)
    ap.add_argument("--packet", type=Path, default=None, help="packet dir with incident.json (default: the verdict's parent dirs)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    packet = args.packet
    if packet is None:
        packet = next((p for p in args.verdict.resolve().parents if (p / "incident.json").exists()), None)
        if packet is None:
            print("ERROR: no --packet and no incident.json above the verdict")
            return 1
    incident = json.loads((packet / "incident.json").read_text())
    try:
        verdict = json.loads(args.verdict.read_text())
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: verdict is not valid JSON: {exc}")
        return 1
    errors, warnings = check(verdict, incident)
    clamped, notes = clamp(verdict, incident) if not schema_errors(verdict) else (verdict, [])
    if args.json:
        print(json.dumps({"ok": not errors, "errors": errors, "warnings": warnings, "clamps": notes,
                          "in_t": clamped.get("in_t"), "out_t": clamped.get("out_t")}, indent=2))
    else:
        for e in errors:
            print(f"ERROR: {e}")
        for w in warnings:
            print(f"WARNING: {w}")
        if notes:
            print(f"NOTE: the code will adjust the window to {clamped['in_t']:+.1f}..{clamped['out_t']:+.1f}: " + "; ".join(notes))
        if not errors:
            print("OK")
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
