"""Build overlay specs from a league pack and render them with a theme.

    from overlays.spec import League, render
    mhl = League("mhl")
    spec = mhl.spec("score", home="AMH", away="PCC", side="away", score=(1, 0),
                    period="1", time="14:10", headline="GOAL",
                    subject=("Trent Stewart", "47"), lines=["Assists: Walsh, MacKenzie"])
    render(spec, "baseline", "overlay.png")

Teams resolve by short code, provider id, slug, full name, nickname or city, so a
producer can pass whatever its data source uses. Unknown teams get the fallback logo
and neutral colours instead of failing.
"""
from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
TEAM_KEYS = ("id", "name", "city", "nickname", "short", "logo", "primary", "secondary", "text")


class League:
    def __init__(self, league_id: str):
        self.pack = json.loads((HERE / "leagues" / league_id / "league.json").read_text())

    def team(self, key) -> dict:
        k = str(key or "").strip().lower()
        for t in self.pack["teams"]:
            if k in {str(t.get(f, "")).lower() for f in ("short", "provider_id", "id", "name", "nickname", "city")}:
                return {f: t[f] for f in TEAM_KEYS}
        name = str(key or "Team")
        return {"id": k or "unknown", "name": name, "city": "", "nickname": name, "short": name[:3].upper(),
                "logo": self.pack.get("fallback_logo", ""), "primary": "#3a4a5e", "secondary": "#1c2633", "text": "#ffffff"}

    def clock(self, period, time: str | None = None, text: str | None = None) -> dict:
        label = self.pack.get("periods", {}).get(str(period), str(period or ""))
        return {"period": label, "time": time or "", "text": text or " · ".join(x for x in (label, time) if x)}

    def spec(self, kind: str, *, home, away, side=None, score=None, period=None, time=None, clock_text=None,
             headline="", subject=None, others=(), lines=(), tags=(), badge=None, context="", stats=(),
             width=1920, height=1080) -> dict:
        lg = {k: self.pack[k] for k in ("id", "name", "short", "logo", "primary", "secondary")}
        person = lambda p: None if not p else ({"name": p[0], "number": str(p[1] or "")} if isinstance(p, (tuple, list)) else dict(p))  # noqa: E731
        return {
            "kind": kind, "sport": self.pack["sport"], "width": width, "height": height, "league": lg,
            "home": self.team(home), "away": self.team(away), "side": side,
            "score": {"home": score[0], "away": score[1]} if score else None,
            "clock": self.clock(period, time, clock_text) if (period or clock_text) else None,
            "headline": headline, "subject": person(subject), "others": [person(o) for o in others],
            "lines": list(lines), "tags": list(tags), "badge": badge, "context": context, "stats": list(stats),
        }


def render(spec: dict, theme: str, out_png: str | Path) -> Path:
    """Render one spec to a transparent PNG with the given theme (node + playwright)."""
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(spec, fh)
    try:
        subprocess.run(["node", str(HERE / "render.mjs"), "--theme", theme, "--spec", fh.name, "--output", str(out_png)],
                       check=True, cwd=ROOT, capture_output=True, text=True)
    finally:
        Path(fh.name).unlink(missing_ok=True)
    return Path(out_png)
