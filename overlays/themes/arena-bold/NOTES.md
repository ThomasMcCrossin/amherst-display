# arena-bold — design notes

**Beeper:** "arena-bold — big angled slabs, heavy condensed capitals, team colour floods the panel; loud but clean."

## Design intent

Jumbotron energy, built from one motif: clip-path parallelogram slabs on a consistent
~13° slant, white keyline plates, and a fixed near-black ink (#0b0e14) that makes every
team colour — and the white tabs — pop identically. The player name is the hero on
lower thirds (Bebas Neue, 94 px); the score is the hero on full-screen cards (290 px
digits with team-colour bars + glow). Team colour floods the lower-third deck from the
spec's `home/away/subject` side, with `team.text` as the type colour so yellow (CAM,
RDG) and white (SCA) teams get dark ink instead of white mud. Logos always sit on a
white backing plate with an ink keyline, so marks that vanish on light or dark panels
stay legible everywhere. Everything the spec provides is laid out as given — a theme
never invents wording; long strings shrink to fit from a `.fit` pass instead of wrapping.

## Layouts

- **Lower thirds** (`score penalty fight save moment`, + unknown fallback): white ribbon
  tab (ink headline, ink backing keyline, sliver trails) + team-colour deck slab (logo
  plate, `#NN` ink chip, name hero, supporting line) + ink meta slab (clock over a mini
  away `0 - 1` home score with dim shorts). No meta → decorative tail slivers.
- **Fight:** two face slabs (each: plate + `#NN` + name, both players side by side) with
  a white `VS` tab in the seam, sharing an ink strip for the suspension line, over the
  full meta slab.
- **Full-screen cards** (`break final intro`, opaque #0b0f17): white ribbon headline,
  clock chip, two team-colour slabs (big plate + team name) and a centre score `4 - 1`
  (or `VS`), optional ink stats panel and a white bottom strip for context/lines/tags.
  Corner washes tint the card edges with both team primaries (opacity .4).

## Fix round (r1)

- Review r1 flagged the `soccer-final-light` final card as "white team name on a light
  slab". Re-rendered and pixel-sampled: the SCA name already renders as `team.text`
  (`#111`, sampled (2,2,2)) on the `#e8e8e8` slab (232,232,232) — contrast ≈ 17:1.
  No code change needed; `.sname` is bound to `--pt` which is `team.text ||` derived.
- `node overlays/render.mjs --theme arena-bold`: **12 rendered, 0 with problems**.
- All 12 composite frames reviewed at full size (names/numbers/clock verified against
  the sample specs); unknown-kind fallback verified with a one-off spec (neutral league
  flood, league logo, no meta, tail slivers).

## Fonts

- **Bebas Neue** (display: heroes, ribbon, digits, chips) — SIL Open Font License 1.1,
  © Ryoichi Tsunekawa (Font Diner). Built into the overlay harness; no files bundled here.
- **Barlow Semi Condensed** 400/600 (copy, labels, strips) — SIL Open Font License 1.1,
  © Jeremy Tribby. Built into the harness; no files bundled here.

## Engineering notes

- `.fit` elements run a shrink-to-fit pass (nowrap, ≤22 px floor) after
  `document.fonts.ready`, so very long names (e.g. Jean-Philippe Arsenault-Thibodeau)
  stay on one line inside their box.
- `clip-path` slabs paint over static content: everything that must sit on a slab is
  `position:relative;z-index:2`. Ribbon tabs get an ink `::before` backing (`inset:-4px`)
  so white tabs survive light slabs.
- Lower thirds are anchored 96 px from the sides, 54 px from the bottom, inside the
  bottom ~30 % — clear of the broadcast scorebug zone.

## Known weaknesses

- Pathological strings longer than the fit floor (22 px) can still overflow; the
  producers' wording is usually sane, but there is no hard wrap.
- Fight rows give each fighter half the deck: two very long names shrink together
  rather than rebalancing.
- Team primaries darker than the ink (near-black clubs) make the meta/deck boundary and
  the score-bar glow subtle — readable, but the flood reads quieter for them.
- `.sep` dash between the score digits is a decorative low-contrast slab (#3c475c);
  intentional quiet punctuation in a loud layout.