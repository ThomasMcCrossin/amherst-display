# Overlays: league-neutral broadcast graphics

Overlays are transparent 1920x1080 layers drawn over game video: lower thirds for goals,
penalties, fights, saves and other moments, and full-screen cards for intros, intermissions
and finals. One **spec** describes what to show. A **theme** decides how it looks. A
**league pack** supplies teams, logos and colours. Nothing in a theme knows about hockey,
the MHL or any particular team.

```
overlays/
  leagues/<id>/league.json   league pack (MHL is the first-class one; demo-soccer proves neutrality)
  samples.py -> samples/     resolved sample specs built from the packs
  themes/<name>/theme.mjs    a theme (baseline = reference implementation)
  render.mjs                 spec + theme -> PNG, with layout checks
  composite.py               PNG over real frames + contact sheet (local judging only)
```

## League pack (`leagues/<id>/league.json`)

`id, name, short, sport, logo, primary, secondary, fallback_logo, periods{n: label},
clock ("remaining" | "elapsed"), provider{...}, teams[]`. Each team has
`id, provider_id, short, city, nickname, name, logo, primary, secondary, text`. `text` is
the colour that reads on `primary`. Logo paths are relative to the repo root (PNG or SVG).
Adding a league means adding a pack and logos. No code changes.

## Spec

```jsonc
{
  "kind": "score|penalty|fight|save|moment|break|final|intro",
  "sport": "hockey", "width": 1920, "height": 1080,
  "league": {"id","name","short","logo","primary","secondary"},
  "home": {team}, "away": {team},          // team = {id,name,city,nickname,short,logo,primary,secondary,text}
  "side": "home|away|null",                // whose moment it is (null: neutral, e.g. a fight)
  "score": {"home": 1, "away": 0} | null,  // score after the moment
  "clock": {"period": "1st", "time": "14:10", "text": "1st · 14:10"} | null,
  "headline": "GOAL",                      // already worded for the sport/league; show as given
  "subject": {"name", "number", "side"?} | null,   // the player the moment is about
  "others": [{"name", "number", "side"}],  // e.g. the other fighter
  "lines": ["Assists: ..."],               // supporting lines, pre-formatted
  "tags": ["PP", "GWG"], "badge": "UNVERIFIED" | null,
  "context": "MHL Regular Season", "stats": [{"label", "home", "away"}]
}
```

The producer (the highlight pipeline) does all the wording: sport terms, period names,
clock format, assist text. A theme lays out what it is given and never invents text.
At render time every `logo` is a `data:` URI. A missing logo becomes the fallback.

## Theme (`themes/<name>/theme.mjs`)

```js
export const meta = { name, author, description };
export async function render(spec, ctx) { return { css, html }; }
// ctx.esc(text)                     HTML-escape (use it on every string from the spec)
// await ctx.asset("tex.png")         data: URI for a file in the theme dir
// await ctx.fontFace(family, "fonts/X.woff2", weight)   @font-face CSS for a bundled font
```

The html goes into `<body>` of a transparent 1920x1080 page. Built-in fonts: "Bebas Neue",
"Barlow Semi Condensed" (400, 600). A theme may bundle other fonts only under an open
licence (OFL/Apache), with the licence file next to them. No network at render time,
and no JavaScript animation (the output is a still PNG; a theme may later gain motion).

Rules every theme must meet (`render.mjs` checks the first three):
- Text is never clipped or cut off. Long names shrink or wrap within their box.
- Nothing goes off-frame. Keep a 96 px side and 54 px top/bottom title-safe margin.
- Lower-third kinds (`score penalty fight save moment`) stay clear of the top-left
  broadcast scorebug zone (0,0)-(720,190). Keep them in the bottom ~30% of the frame.
- They are readable on a canteen TV across the room. Text over video sits on a solid or
  near-solid panel.
- Team colours come from the spec. Light colours (e.g. a yellow or white team) still have
  readable text: use `team.text`.
- Every kind renders, and so does an unknown kind (fall back to the `moment` layout).

## Run

```
python3 overlays/samples.py
node overlays/render.mjs --theme <name>      # PNGs + checks.json under overlays/.bakeoff/out/<name>/
python3 overlays/composite.py --theme <name> # over real frames in overlays/.bakeoff/bg/ (local only)
```

## Producing overlays (pipelines)

```python
from overlays.spec import League, render
mhl = League("mhl")                     # any pack under overlays/leagues/
spec = mhl.spec("score", home="AMH", away="PCC", side="away", score=(1, 0), period="1",
                time="14:10", headline="GOAL", subject=("Trent Stewart", "47"),
                lines=["Assists: Ryan Walsh, Cole MacKenzie"])
render(spec, "<theme>", "overlay.png")  # transparent 1920x1080; composite with ffmpeg overlay
```

Teams resolve by short code, provider id, slug, name, nickname or city. An unknown team
gets the fallback logo and neutral colours.
CLI: `node overlays/render.mjs --theme <name> --spec one.json --output one.png`.
