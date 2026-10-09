#!/usr/bin/env python3
"""Composite rendered overlay PNGs over real broadcast frames for judging.

  python3 overlays/composite.py --theme baseline [--bg overlays/.bakeoff/bg]

Backgrounds are local frames from a recorded game (not committed: broadcast footage).
Writes <out>/<theme>/<sample>.jpg and a contact sheet <out>/<theme>/sheet.jpg.
"""
import argparse
import json
from pathlib import Path

from PIL import Image, ImageDraw

HERE = Path(__file__).resolve().parent
BG_FOR = {"score": ["play", "celebration"], "penalty": ["faceoff"], "fight": ["bench"], "save": ["play"],
          "moment": ["faceoff"], "break": ["faceoff"], "final": ["bench"], "intro": ["faceoff"]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--theme", required=True)
    ap.add_argument("--bg", default=str(HERE / ".bakeoff/bg"))
    ap.add_argument("--out", default=str(HERE / ".bakeoff/out"))
    a = ap.parse_args()
    out = Path(a.out) / a.theme
    bgdir = Path(a.bg)
    thumbs = []
    for i, png in enumerate(sorted(out.glob("*.png"))):
        spec = json.loads((HERE / "samples" / f"{png.stem}.json").read_text())
        choices = BG_FOR.get(spec["kind"], ["play"])
        bg_path = bgdir / f"{choices[i % len(choices)]}.jpg"
        bg = Image.open(bg_path).convert("RGBA") if bg_path.exists() else Image.new("RGBA", (1920, 1080), "#35506b")
        frame = Image.alpha_composite(bg.resize((1920, 1080)), Image.open(png).convert("RGBA")).convert("RGB")
        frame.save(out / f"{png.stem}.jpg", quality=88)
        t = frame.resize((640, 360))
        ImageDraw.Draw(t).text((8, 340), png.stem, fill="yellow")
        thumbs.append(t)
    if thumbs:
        cols = 3
        sheet = Image.new("RGB", (640 * cols, 360 * ((len(thumbs) + cols - 1) // cols)), "black")
        for i, t in enumerate(thumbs):
            sheet.paste(t, (640 * (i % cols), 360 * (i // cols)))
        sheet.save(out / "sheet.jpg", quality=85)
    print(f"{a.theme}: {len(thumbs)} composites -> {out}")


if __name__ == "__main__":
    main()
