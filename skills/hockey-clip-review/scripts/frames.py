#!/usr/bin/env python3
"""
Frames and timestamped contact sheets from a hockey recording, for an agent to Read.

Needs ffmpeg on PATH and Pillow. Every time is in seconds RELATIVE TO THE PACKET ANCHOR (the
moment the code thinks the event happened), the same numbers printed on the frames.

  # contact sheet(s): 5x4 frames per sheet, each labelled with its time
  python3 frames.py sheet --packet <packet-dir> --from -30 --to 10 --step 0.5 [--name start]
  # single full frames (bigger, for reading a scorebug or a jersey number)
  python3 frames.py frame --packet <packet-dir> --at -3.5 --at 2 [--width 960]

Without a packet: --video <file> --anchor <video seconds>. Output goes to --out-dir, else
$HCR_WORK_DIR (set by the pipeline per reviewer), else <packet>/frames/. Every written path is printed with the range it covers. A call returns at most
--max-frames frames (default 80); a wider range gets a coarser step, said in the output.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple

from PIL import Image, ImageDraw, ImageFont

COLS, ROWS = 5, 4
CELL_W, CELL_H = 384, 216
FONT_CANDIDATES = ("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                   "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
                   "/System/Library/Fonts/Supplemental/Arial Bold.ttf")


def _font(size: int):
    for path in FONT_CANDIDATES:
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    try:
        return ImageFont.load_default(size=size)
    except TypeError:  # Pillow < 10.1
        return ImageFont.load_default()


def video_duration(video: Path) -> float:
    out = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(video)],
                         capture_output=True, text=True, check=True)
    return float(out.stdout.strip())


def extract(video: Path, start: float, end: float, step: float, width: int = CELL_W,
            nice: bool = True) -> List[Tuple[float, Image.Image]]:
    """Frames at start, start+step, ... (absolute video seconds) from one ffmpeg decode."""
    start = max(0.0, start)
    if end <= start:
        return []
    with tempfile.TemporaryDirectory(prefix="hcr_frames_") as tmp:
        cmd = (["nice", "-n", "10"] if nice else []) + [
            "ffmpeg", "-v", "error", "-nostdin", "-ss", f"{start:.3f}", "-t", f"{end - start + step / 2:.3f}",
            "-i", str(video), "-an", "-sn", "-vf", f"fps=1/{step},scale={width}:-2", "-threads", "2", "-q:v", "4",
            f"{tmp}/f_%05d.jpg"]
        subprocess.run(cmd, check=True)
        frames = []
        for i, p in enumerate(sorted(Path(tmp).glob("f_*.jpg"))):
            t = start + i * step
            if t > end + 1e-6:
                break
            with Image.open(p) as im:
                frames.append((t, im.convert("RGB").copy()))
    return frames


def grab(video: Path, t: float, width: int = 960) -> Optional[Image.Image]:
    with tempfile.TemporaryDirectory(prefix="hcr_frame_") as tmp:
        out = Path(tmp) / "f.jpg"
        subprocess.run(["nice", "-n", "10", "ffmpeg", "-v", "error", "-nostdin", "-ss", f"{max(0.0, t):.3f}", "-i", str(video),
                        "-frames:v", "1", "-vf", f"scale={width}:-2", "-q:v", "3", str(out)], check=True)
        if not out.exists():
            return None
        with Image.open(out) as im:
            return im.convert("RGB").copy()


def _label(t_rel: float, precise: bool) -> str:
    if precise or abs(t_rel - round(t_rel)) > 0.05:
        return f"{t_rel:+.1f}"
    return f"{t_rel:+.0f}"


def _draw_label(img: Image.Image, text: str, highlight: bool, size: int) -> None:
    d = ImageDraw.Draw(img)
    font = _font(size)
    d.text((8, 6), text, font=font, fill=(255, 230, 0) if highlight else (255, 255, 255),
           stroke_width=max(2, size // 10), stroke_fill=(0, 0, 0))


def make_sheets(frames: List[Tuple[float, Image.Image]], anchor: float, out_dir: Path, prefix: str,
                precise: bool = False) -> List[dict]:
    """Tile frames into labelled 5x4 sheets; labels are seconds relative to anchor (0 in yellow)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    per = COLS * ROWS
    sheets = []
    for s in range(0, len(frames), per):
        chunk = frames[s:s + per]
        canvas = Image.new("RGB", (COLS * CELL_W, ROWS * CELL_H), (0, 0, 0))
        draw = ImageDraw.Draw(canvas)
        for i, (t, img) in enumerate(chunk):
            cell = img.resize((CELL_W, CELL_H))
            rel = t - anchor
            _draw_label(cell, _label(rel, precise), abs(rel) < 0.01, 34)
            r, c = divmod(i, COLS)
            canvas.paste(cell, (c * CELL_W, r * CELL_H))
            draw.rectangle([c * CELL_W, r * CELL_H, (c + 1) * CELL_W - 1, (r + 1) * CELL_H - 1], outline=(70, 70, 70))
        path = out_dir / f"{prefix}_{s // per + 1:02d}.jpg"
        canvas.save(path, quality=80)
        sheets.append({"path": str(path), "from": round(chunk[0][0] - anchor, 2), "to": round(chunk[-1][0] - anchor, 2),
                       "frames": len(chunk)})
    return sheets


def _packet(args) -> Tuple[Path, float, Path, float]:
    if args.packet:
        pk = Path(args.packet).resolve()
        inc = json.loads((pk / "incident.json").read_text())
        video, anchor = Path(inc["video"]), float(inc["anchor"])
        duration = float(inc.get("video_duration") or video_duration(video))
        out_dir = Path(args.out_dir or os.environ.get("HCR_WORK_DIR") or pk / "frames")
    else:
        if not (args.video and args.anchor is not None):
            sys.exit("need --packet, or --video and --anchor")
        video, anchor = Path(args.video), float(args.anchor)
        duration = video_duration(video)
        out_dir = Path(args.out_dir or os.environ.get("HCR_WORK_DIR") or ".")
    return video, anchor, out_dir, duration


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("sheet", "frame"):
        p = sub.add_parser(name)
        p.add_argument("--packet")
        p.add_argument("--video")
        p.add_argument("--anchor", type=float)
        p.add_argument("--out-dir")
    s = sub.choices["sheet"]
    s.add_argument("--from", dest="t_from", type=float, required=True)
    s.add_argument("--to", dest="t_to", type=float, required=True)
    s.add_argument("--step", type=float, default=1.0)
    s.add_argument("--name", default="")
    s.add_argument("--max-frames", type=int, default=80)
    f = sub.choices["frame"]
    f.add_argument("--at", type=float, action="append", required=True)
    f.add_argument("--width", type=int, default=960)
    args = ap.parse_args()
    video, anchor, out_dir, duration = _packet(args)

    if args.cmd == "frame":
        out_dir.mkdir(parents=True, exist_ok=True)
        for t in args.at:
            a = anchor + t
            if not 0 <= a <= duration:
                print(f"skip {t:+.1f}: outside the recording (0..{duration - anchor:+.0f})")
                continue
            img = grab(video, a, args.width)
            if img is None:
                print(f"skip {t:+.1f}: no frame")
                continue
            _draw_label(img, _label(t, True), abs(t) < 0.01, max(24, args.width // 24))
            path = out_dir / f"frame_{t:+08.2f}.jpg".replace("+", "p").replace("-", "m")
            img.save(path, quality=85)
            print(f"{path}  t={t:+.2f}")
        return 0

    lo, hi = sorted((args.t_from, args.t_to))
    lo, hi = max(lo, -anchor), min(hi, duration - anchor)
    if hi <= lo:
        print(f"range outside the recording (0..{duration - anchor:+.0f} relative to the anchor)")
        return 1
    step = max(0.25, args.step)
    n = math.floor((hi - lo) / step) + 1
    if n > args.max_frames:
        step = math.ceil((hi - lo) / (args.max_frames - 1) * 4) / 4
        print(f"note: {n} frames requested; step widened to {step:g}s")
    frames = extract(video, anchor + lo, anchor + hi, step)
    name = args.name or f"sheet_{lo:+.0f}_{hi:+.0f}_s{step:g}".replace("+", "p").replace("-", "m")
    for sh in make_sheets(frames, anchor, out_dir, name, precise=step < 1):
        print(f'{sh["path"]}  labels {sh["from"]:+.1f} .. {sh["to"]:+.1f}  ({sh["frames"]} frames, step {step:g}s)')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
