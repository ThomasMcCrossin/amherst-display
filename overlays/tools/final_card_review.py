#!/usr/bin/env python3
"""Render the final-score card review set into a folder with an index.html (watch-rams#31).

    python3 overlays/tools/final_card_review.py OUT_DIR

Real finals by HockeyTech game id, plus edge cases built from a real summary: the longest
names in a full column, an unknown team whose logo URL fails, and an unknown team with no
logo at all. Each frame is the exact 1920x1080 PNG final_card.mjs writes.
"""
from __future__ import annotations

import copy
import html
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CACHE = Path(os.environ.get("FINAL_CARD_CACHE", Path.home() / ".local/state/amherst-display/final-card"))

RAMBLERS = [("5072", "Oct 24 at Chaleur, W 6-3"), ("5067", "Oct 23 at Campbellton, W 3-2 OT"),
            ("5064", "Oct 18 at Yarmouth, L 0-4 (shutout against)"), ("5057", "Oct 17 vs Campbellton, L 2-5"),
            ("5042", "Oct 9 vs Chaleur, L 6-7 OT (13 goals)")]
LEAGUE = [("5073", "Oct 24: West Kent at Miramichi"), ("5074", "Oct 24: Yarmouth at Summerside (shutout)"),
          ("5075", "Oct 24: Truro at Valley"), ("5077", "Oct 25: Yarmouth at Summerside, SO"),
          ("4994", "Sep 11: West Kent at Truro, 13 goals")]


def render(out: Path, name: str, *, game: str | None = None, fixture: Path | None = None, assists=False) -> str:
    cmd = ["node", str(ROOT / "overlays/final_card.mjs"), "--out", str(out / f"{name}.png")]
    cmd += ["--game", game] if game else ["--fixture", str(fixture)]
    if assists:
        cmd.append("--assists")
    r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    return "ok" if r.returncode == 0 else f"exit {r.returncode}: {r.stderr.strip()}"


def edge_fixtures(out: Path) -> list[tuple[str, str, Path]]:
    base = json.loads((CACHE / "gameSummary_5072.json").read_text())
    long = copy.deepcopy(base)
    names = [("Jean-Christophe", "Archambault-Thibodeau"), ("Maximilien", "Vaillancourt-Desjardins"),
             ("Alexander", "Christopherson-MacDonald"), ("Xavier-Olivier", "Bourgeois-Leblanc")]
    k = 0
    for p in long["periods"]:
        for g in p["goals"]:
            if g["team"]["id"] == 1:
                g["scoredBy"]["firstName"], g["scoredBy"]["lastName"] = names[k % len(names)]
                k += 1
    broken = copy.deepcopy(base)
    for t in ("homeTeam",):
        broken[t]["info"].update(id=99, abbreviation="NEW", city="Newtown", nickname="Expansion", logo="https://assets.leaguestat.com/mhl/logos/does-not-exist.jpg")
    for p in broken["periods"]:
        for g in p["goals"]:
            if g["team"]["id"] == 21:
                g["team"]["id"] = 99
    nologo = copy.deepcopy(broken)
    nologo["homeTeam"]["info"]["logo"] = ""
    cases = [("edge-long-names", "Edge: longest names, 4 scorers (5072 with names replaced)", long),
             ("edge-logo-404", "Edge: unknown team, logo URL fails", broken),
             ("edge-no-logo", "Edge: unknown team, no logo at all", nologo)]
    res = []
    for name, label, data in cases:
        f = out / "fixtures" / f"{name}.json"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(json.dumps(data))
        res.append((name, label, f))
    return res


def main() -> None:
    out = Path(sys.argv[1]).resolve()
    out.mkdir(parents=True, exist_ok=True)
    sections = []
    rows = [(g, label, render(out, g, game=g)) for g, label in RAMBLERS]
    rows.append(("5072-assists", "Option: Oct 24 with assists under each goal", render(out, "5072-assists", game="5072", assists=True)))
    sections.append(("Ramblers finals: the closing frame of each highlight reel", rows))
    sections.append(("Around the league (same card, other MHL finals)", [(g, label, render(out, g, game=g)) for g, label in LEAGUE]))
    sections.append(("Edge cases", [(n, label, render(out, n, fixture=f)) for n, label, f in edge_fixtures(out)]))
    body = ""
    for title, items in sections:
        body += f"<h2>{html.escape(title)}</h2>"
        for name, label, status in items:
            img = f'<a href="{name}.png"><img src="{name}.png" alt="" loading="lazy"></a>' if status == "ok" else ""
            body += f'<figure>{img}<figcaption><b>{html.escape(label)}</b> · {html.escape(name)}.png' \
                    f'{"" if status == "ok" else " · " + html.escape(status)}</figcaption></figure>'
    (out / "index.html").write_text(f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Final-score cards</title>
<style>:root{{color-scheme:dark}}body{{margin:0;padding:16px;background:#0b0613;color:#eee;font:16px/1.4 system-ui,sans-serif}}
main{{max-width:1240px;margin:auto}}h1{{font-size:24px}}h2{{font-size:19px;margin:32px 0 8px;color:#f4b932}}
figure{{margin:0 0 22px}}img{{width:100%;height:auto;display:block;border:1px solid #333}}figcaption{{margin-top:6px;color:#c4b8dc}}
p{{color:#c4b8dc;max-width:70ch}}</style></head><body><main>
<h1>Final-score cards for the Game TV</h1>
<p>Each image is the exact 1920x1080 frame. Tap one for full size. The reel shows it at the end of a Ramblers
highlight reel for about 8 seconds. Nothing is on the Game TV until Tom says so.</p>
{body}</main></body></html>""")
    print(out / "index.html")


if __name__ == "__main__":
    main()
