"""
Agentic highlight-clip review.

Code finds candidates cheaply (the engine's placed game-sheet events); cheap vision reviewers
look at the frames and may overrule the engine's clip window, relocate the event within bounds
or drop the clip; an adversary tries to refute each goal/major verdict; apply writes overrides
that clip cutting and reel building consume.

Stages: packet -> review -> adversary -> apply. Entry point: scripts/review_game.py.
The portable skill (reviewer/adversary instructions, frame and verdict-check scripts) lives in
skills/hockey-clip-review/; this package imports its scripts so code and agents share one set
of rules.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SKILL_DIR = REPO / "skills" / "hockey-clip-review"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"hcr_{name}", SKILL_DIR / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


frames = _load("frames")
checker = _load("check_verdict")
