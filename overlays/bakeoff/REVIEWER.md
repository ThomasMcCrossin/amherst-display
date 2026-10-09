# Overlay reviewer brief

Review the overlay theme `{name}` as a broadcast graphics lead would. Do not edit any theme
file. The repo root is the current directory.

Inputs:
- `overlays/README.md`: the contract and the rules.
- `overlays/.bakeoff/out/{name}/sheet.jpg`, and every `overlays/.bakeoff/out/{name}/<sample>.jpg`
  (the theme over real game frames). Look at each single frame at full size.
- `overlays/samples/<sample>.json`: the truth for each frame.
- `overlays/.bakeoff/out/{name}/checks.json`: the automatic layout checks.

Check every sample:
- **Correct**: every name, number, score, team attribution (whose goal, whose penalty, which
  side each score sits on), clock and tag matches the spec exactly. No invented text.
- **Complete**: nothing clipped, cut off, overlapping or off-frame. Long names fit, and both
  fighters show.
- **Legible**: readable on a TV across a room. Contrast holds for the yellow (CAM, RDG) and
  white (SCA) teams. Logos are visible on their backgrounds.
- **Safe**: lower thirds stay clear of the broadcast scorebug (top-left) and inside title-safe.
- **Neutral**: the soccer samples are as good as the hockey ones.
- **Quality**: hierarchy, typography, use of colour. Would a real broadcast air it?

Write `overlays/.bakeoff/reviews/{name}{suffix}.json` (create the directory if needed):
```json
{"theme": "{name}", "score": 0-10, "verdict": "one line",
 "defects": [{"sample": "...", "severity": "blocker|major|minor", "what": "...", "fix": "..."}],
 "strengths": ["..."]}
```
Blocker: wrong or missing information, clipped text, unreadable. Major: hurts legibility or
looks broken. Minor: polish. Be specific enough that the designer can fix it without guessing.
Then validate the file with `python3 -m json.tool` and stop.
