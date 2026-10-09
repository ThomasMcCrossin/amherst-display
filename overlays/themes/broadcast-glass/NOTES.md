# broadcast-glass

## Design intent
Network-sports look (TSN / Sportsnet flavour): every element sits on a near-opaque dark
glass panel with a top sheen, a hairline border, one diagonal light streak, and a soft
drop shadow so it reads over bright ice or stadium video across a canteen TV. Team colour
is applied with intent, not decor — as the wedged logo plate, the kicker and number chips,
per-fighter underlines, the score-chip hairline, and the bottom trim bar that mirrors the
wedge + secondary stripe; light-team primaries (CAM yellow, SCA white) always pair with the
pack's `team.text` colour. Logos never vanish: they sit on a frosted dark inset plate inside
the colour wedge (cards: frosted inset inside the plate). Hierarchy is fixed: the player
name is the hero on lower thirds (Bebas 68px with skewed number chip), the score is the hero
on full-screen cards (Bebas ~250px with team-colour slash/sash accents and a colour-gradient
divider). Lower thirds hug their content (`width:fit-content`, capped at the title-safe
width) so the score chip closes the panel with no dead-air band. Fights show both players
with their own logos, colour-coded chips and underlines; unknown kinds fall back to the
moment layout.

## Fix round (r1 review)
- Lower thirds now size to content instead of spanning the full title-safe width; the score
  chip closes the panel (no dead-air band).
- Bottom trim bar is a clean two-tone wedge/secondary accent aligned to the plate geometry;
  the stray pure-white sliver is gone.
- Card team names wrap only at word boundaries (`white-space:nowrap` tokens); the name size
  steps down (46 → 30px) so the longest token still sets on one line, e.g. the soccer city
  name no longer breaks mid-word at a hyphen. Both plate name blocks share a min-height with
  vertical centring, so one-line and two-line names read as a balanced row.

## Fonts
- Bebas Neue (display: names, scores, kickers, clocks) — SIL Open Font License 1.1.
- Barlow Semi Condensed 400/600 (supporting lines, shorts, labels) — SIL Open Font License 1.1.
- Both are already bundled in `overlays/assets/fonts/`; the theme adds no fonts and no runtime JS.

## Known weaknesses
- The wedge/logo plates use the pack's primary as-is; a team whose primary is very close to
  its logo's fill loses contrast on the wedge (mitigated by the dark frosted inset).
- On full-screen cards, plate name sizes scale per team (30–46px by token length), so a very
  long name renders slightly smaller than its opponent's; the row height stays balanced.
- The score chip assumes scores are short (2–3 digits); a 3-digit score would compress the
  divider spacing slightly.
- Long unbroken name words wrap with `overflow-wrap:anywhere` only as a last resort; a single
  token longer than ~30 characters would wrap mid-word. Acceptable but not elegant.
- Skewed chips are a still-frame treatment; if this theme ever gains motion the skew
  geometry should be re-checked for animated slides.