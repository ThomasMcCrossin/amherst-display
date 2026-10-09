# clean-modern

**Intent.** A calm, premium streaming-app look: flat, fully opaque near-black panels (no
bleed-through from bright ice), generous spacing, and one team-colour accent per panel.
The player name is the hero on lower thirds (Bebas, filled jersey chip, logo on a slate
plate); the score is the hero on full-screen cards (190px figures, top colour split, soft
team-tinted glows). Fights show both players against a shared score coin. Team colour is
always carried by `team.text` on filled shapes, so yellow (CAM/RDG) and white (SCA) teams
stay readable, and the same slate plate keeps dark and light logos equally visible.

**Type.** Display: **Bebas Neue** (SIL OFL 1.1) for names, numbers and scores. Text:
**Lato** 400/700/900 (SIL OFL 1.1, © Łukasz Dziedzic), bundled in `fonts/` with the licence
at `fonts/OFL-Lato.txt`. Long names and supporting lines shrink by a length step and clamp
with an ellipsis so a long producer-formatted line can never run into the score coin.

**Known weaknesses.** The slate plate is a fixed tone, so a slate-grey logo would blend in
and very wide logos letterbox inside the square. The kicker accent is auto-brightened to
clear 4.5:1 on the panel, so for dark team primaries it drifts lighter than the exact team
hue (the vivid colour remains on the rail, chip and dots). Specs that provide no `context`
show none on lower thirds; no motion — output is a still PNG.
