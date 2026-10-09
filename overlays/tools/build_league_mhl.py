#!/usr/bin/env python3
"""Regenerate overlays/leagues/mhl/league.json from the repo's MHL logos and team list.

Codes and names come from HockeyTech teamsbyseason (season 46). Colours come from the
hand-picked presets where they exist, else the dominant saturated colour of the logo.
Edit league.json by hand afterwards if a sampled colour is wrong; this is a one-off seed.
"""
import colorsys
import json
from collections import Counter
from pathlib import Path

from PIL import Image

ROOT = Path(__file__).resolve().parents[2]
TEAMS = [  # HockeyTech id, code, city, nickname, logo slug
    ("1", "AMH", "Amherst", "Ramblers", "amherst-ramblers"),
    ("8", "CAM", "Campbellton", "Tigers", "campbellton-tigers"),
    ("21", "CHA", "Chaleur", "Lightning", "chaleur-lightning"),
    ("2", "EDM", "Edmundston", "Blizzard", "edmundston-blizzard"),
    ("12", "GFR", "Grand Falls", "Rapids", "grand-falls-rapids"),
    ("9", "MIR", "Miramichi", "Timberwolves", "miramichi-timberwolves"),
    ("7", "PCC", "Pictou County", "Weeks Crushers", "pictou-county-weeks-crushers"),
    ("10", "SWC", "Summerside", "Western Capitals", "summerside-western-capitals"),
    ("3", "TRU", "Truro", "Bearcats", "truro-bearcats"),
    ("4", "VAL", "Valley", "Wildcats", "valley-wildcats"),
    ("6", "WKS", "West Kent", "Steamers", "west-kent-steamers"),
    ("5", "YAR", "Yarmouth", "Mariners", "yarmouth-mariners"),
]
PRESETS = {  # from scripts/build_production_highlight_reel.py TEAM_COLOR_PRESETS
    "amherst-ramblers": ("#412580", "#24114f"),
    "summerside-western-capitals": ("#cf4859", "#7e1827"),
    "yarmouth-mariners": ("#18b58f", "#0f5f52"),
    "truro-bearcats": ("#6f51d8", "#2f2758"),
    "edmundston-blizzard": ("#4ba4ff", "#103d6f"),
    # picked by eye from the logos (2026-10-09); the logo sampler was too dark for these
    "campbellton-tigers": ("#f2a900", "#1a1a1a"),
    "chaleur-lightning": ("#1f3f8f", "#0d1f4a"),
    "grand-falls-rapids": ("#1f8a3c", "#0c3d1a"),
    "miramichi-timberwolves": ("#7a1f2b", "#3b0d14"),
    "pictou-county-weeks-crushers": ("#c8102e", "#5a0a14"),
    "valley-wildcats": ("#1c1c1c", "#6d1a24"),
    "west-kent-steamers": ("#1b2a6b", "#0b1438"),
}


def hexc(rgb):
    return "#%02x%02x%02x" % tuple(int(c) for c in rgb)


def sampled(logo: Path):
    img = Image.open(logo).convert("RGBA").resize((96, 96))
    counts = Counter()
    for r, g, b, a in img.getdata():
        if a < 200:
            continue
        h, l, s = colorsys.rgb_to_hls(r / 255, g / 255, b / 255)
        if s < 0.35 or l < 0.12 or l > 0.85:
            continue
        counts[(r // 24 * 24, g // 24 * 24, b // 24 * 24)] += 1
    if not counts:
        return "#4ba4ff", "#153656"
    r, g, b = counts.most_common(1)[0][0]
    h, l, s = colorsys.rgb_to_hls(r / 255, g / 255, b / 255)
    dark = colorsys.hls_to_rgb(h, max(0.1, l * 0.45), s)
    return hexc((r, g, b)), hexc([c * 255 for c in dark])


def text_on(hex_color: str) -> str:
    r, g, b = (int(hex_color[i:i + 2], 16) / 255 for i in (1, 3, 5))
    lum = 0.2126 * r + 0.7152 * g + 0.0722 * b
    return "#111111" if lum > 0.6 else "#ffffff"


teams = []
for tid, code, city, nick, slug in TEAMS:
    logo = f"assets/logos/mhl/{slug}.png"
    primary, secondary = PRESETS.get(slug) or sampled(ROOT / logo)
    teams.append({"id": slug, "provider_id": tid, "short": code, "city": city, "nickname": nick,
                  "name": f"{city} {nick}", "logo": logo, "primary": primary, "secondary": secondary,
                  "text": text_on(primary)})

league = {
    "id": "mhl",
    "name": "Maritime Junior Hockey League",
    "short": "MHL",
    "sport": "hockey",
    "logo": "assets/logos/league-mhl.png",
    "primary": "#0b2d6b",
    "secondary": "#c8102e",
    "fallback_logo": "assets/logos/fallback.png",
    "periods": {"1": "1st", "2": "2nd", "3": "3rd", "4": "OT", "5": "SO"},
    "clock": "remaining",
    "provider": {"type": "hockeytech", "client_code": "mhl", "league_id": "1"},
    "teams": teams,
}
out = ROOT / "overlays/leagues/mhl/league.json"
out.write_text(json.dumps(league, indent=2) + "\n")
print(out, len(teams))
