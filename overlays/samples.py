#!/usr/bin/env python3
"""Build the bake-off sample specs from the league packs into overlays/samples/*.json.

Each sample is a fully resolved overlay spec (see README.md): teams carry their own
names, short codes, colours and logo paths, so a theme never needs a team table.
"""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent


def pack(league_id):
    return json.loads((HERE / "leagues" / league_id / "league.json").read_text())


def team(league, short):
    t = next(t for t in league["teams"] if t["short"] == short)
    return {k: t[k] for k in ("id", "name", "city", "nickname", "short", "logo", "primary", "secondary", "text")}


def lg(league):
    return {k: league[k] for k in ("id", "name", "short", "logo", "primary", "secondary")}


def base(league, home, away, **kw):
    spec = {"kind": "moment", "sport": league["sport"], "width": 1920, "height": 1080,
            "league": lg(league), "home": team(league, home), "away": team(league, away),
            "side": None, "score": None, "clock": None, "headline": "", "subject": None,
            "others": [], "lines": [], "tags": [], "badge": None, "context": "", "stats": []}
    spec.update(kw)
    return spec


mhl, soc = pack("mhl"), pack("demo-soccer")
SAMPLES = {
    "goal": base(mhl, "AMH", "PCC", kind="score", side="home", score={"home": 1, "away": 0},
                 clock={"period": "1st", "time": "14:10", "text": "1st · 14:10"}, headline="GOAL",
                 subject={"name": "Trent Stewart", "number": "47"}, lines=["Assists: Ryan Walsh, Cole MacKenzie"],
                 context="MHL Regular Season"),
    "goal-pp-long-name": base(mhl, "AMH", "SWC", kind="score", side="away", score={"home": 2, "away": 3},
                 clock={"period": "3rd", "time": "02:31", "text": "3rd · 2:31"}, headline="POWER-PLAY GOAL",
                 subject={"name": "Jean-Philippe Arsenault-Thibodeau", "number": "91"},
                 lines=["Assists: Maximilien Bourgeois-Leblanc, Christopher Vanderhoeven"], tags=["PP", "GWG"]),
    "goal-unassisted-light-team": base(mhl, "CAM", "AMH", kind="score", side="home", score={"home": 4, "away": 1},
                 clock={"period": "OT", "time": "00:33", "text": "OT · 0:33"}, headline="GOAL",
                 subject={"name": "Liam Fraser", "number": "8"}, lines=["Unassisted"], tags=["EN"],
                 badge="UNVERIFIED"),
    "penalty": base(mhl, "AMH", "TRU", kind="penalty", side="away", score={"home": 2, "away": 2},
                 clock={"period": "2nd", "time": "08:18", "text": "2nd · 8:18"}, headline="PENALTY",
                 subject={"name": "Owen Doucette", "number": "17"}, lines=["Tripping · 2 min"]),
    "fight": base(mhl, "AMH", "PCC", kind="fight", side=None, score={"home": 0, "away": 3},
                 clock={"period": "3rd", "time": "10:38", "text": "3rd · 10:38"}, headline="FIGHTING MAJORS",
                 subject={"name": "Brody Ford", "number": "23", "side": "away"},
                 others=[{"name": "Nathan MacIsaac", "number": "4", "side": "home"}], lines=["5 min each"]),
    "save": base(mhl, "AMH", "MIR", kind="save", side="home", score={"home": 1, "away": 1},
                 clock={"period": "2nd", "time": "11:42", "text": "2nd · 11:42"}, headline="BIG SAVE",
                 subject={"name": "Kaden Burke", "number": "30"}, lines=["Breakaway stop"]),
    "intermission": base(mhl, "AMH", "CHA", kind="break", score={"home": 2, "away": 1},
                 clock={"period": "1st", "time": "00:00", "text": "End of 1st"}, headline="INTERMISSION",
                 lines=["Highlights from the 1st period"],
                 stats=[{"label": "Shots", "home": 12, "away": 9}, {"label": "Power play", "home": "1/3", "away": "0/2"}]),
    "final": base(mhl, "AMH", "PCC", kind="final", score={"home": 1, "away": 4},
                 clock={"period": "3rd", "time": "00:00", "text": "Final"}, headline="FINAL",
                 lines=["Stars: Stewart (PCC), Walsh (PCC), Fraser (AMH)"],
                 stats=[{"label": "Shots", "home": 28, "away": 35}]),
    "intro": base(mhl, "AMH", "CHA", kind="intro", headline="TONIGHT",
                 lines=["Amherst Stadium · 7:00 PM"], context="MHL Regular Season · Fri Oct 9"),
    "moment": base(mhl, "AMH", "YAR", kind="moment", side="home", headline="MILESTONE",
                 subject={"name": "Cole MacKenzie", "number": "12"}, lines=["100th career MHL game"]),
    "soccer-goal": base(soc, "HFC", "SCA", kind="score", side="away", score={"home": 0, "away": 1},
                 clock={"period": "2nd half", "time": "67'", "text": "67'"}, headline="GOAL",
                 subject={"name": "Amélie Côté-Gagnon", "number": "9"}, lines=["Assist: Rosa Mendes"]),
    "soccer-final-light": base(soc, "RDG", "SCA", kind="final", score={"home": 2, "away": 2},
                 clock={"period": "2nd half", "time": "90+4'", "text": "Full time"}, headline="FULL TIME",
                 lines=["Ridgeway United 2 – 2 Saint-Clair-de-la-Rivière Athletic"]),
}

if __name__ == "__main__":
    out = HERE / "samples"
    out.mkdir(exist_ok=True)
    for name, spec in SAMPLES.items():
        (out / f"{name}.json").write_text(json.dumps(spec, indent=2, ensure_ascii=False) + "\n")
    print(f"{len(SAMPLES)} samples -> {out}")
