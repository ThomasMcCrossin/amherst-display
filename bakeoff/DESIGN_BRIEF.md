# Game TV board: design brief (bake-off, amherst-display#20)

The board runs full-screen on the **Game TV** in the Amherst Stadium canteen: a 1920x1080 TV
driven by a **Raspberry Pi 4 running Chromium in kiosk mode**. Fans read it from 3 to 10 m away
while they queue for food. It plays between games and on game days before puck drop.

## Hard constraints
- Fixed 1920x1080 canvas, no scrolling, no user input. Must look right at exactly that size.
- Pi 4 is weak and memory-tight (a leaking page has wedged it before). So: plain HTML/CSS/JS,
  no frameworks, no build step, no canvas/WebGL, no video. Animate only `transform`/`opacity`.
  Reuse DOM when rotating panels; don't keep appending nodes. Total own JS < 60 KB.
- Readability at distance: body text >= 28px, key numbers >= 72px, strong contrast, no thin
  light weights on dark backgrounds, at most ~6 items per list.
- Data: `fetch('../../data/board.json')` and `fetch('../../data/insights.json')` (relative to
  your `index.html`). Refetch both every 5 minutes without reloading the page. If insights.json
  is missing or `stub:true`, still render well. If a field is null, hide it gracefully; never
  print "undefined", "null" or "NaN".
- External assets allowed: team logos/headshots from `assets.leaguestat.com` (URLs in the data)
  and Google Fonts. Everything else inline/local. Logos are JPGs on white; design for that
  (e.g. put them on a white disc/tile) rather than pretending they're transparent.
- Times are America/Halifax. Show times as "7:00 PM", dates as "Fri Oct 9".
- Footer credit somewhere small: "Official statistics provided by Maritime Hockey League. Powered by HockeyTech.com" (it's `board.copyright`).

## Goalie rule (non-negotiable)
Never name, imply or "project" a starting goalie for an upcoming game. Use
`next_game...official_starters` only: if null, show "Starter TBA" (or omit). Goalie season
stats and recent-start history are fine as plainly labelled facts ("Last 5 starts"), never as a
prediction ("likely starter", "expected", "projected" are banned words).

## Brand
Amherst Ramblers (MHL, Eastlink South). HockeyTech has no team colours; the current board uses
Ramblers purple `#5B2D8C` with gold accents. Check the logo at `team.logo_url` and choose a
palette that matches it. Use team abbreviations and logos for opponents.

## Content to consider (choose what fits your concept)
Next game (countdown, matchup comparison, head-to-head, opponent form), upcoming schedule,
recent results with scorers and three stars, standings (division), team stats/special teams,
Ramblers scoring leaders with headshots, goalie stats, league leaders and Ramblers' ranks,
milestones near, streaks, and insight/narrative items from insights.json (each has `headline`,
`detail`, optional `stat:{value,label}`, `kind`, `priority`). Read `data/SCHEMA.md` and look at the
real `data/board.json` before designing.

## Deliverable
`bakeoff/designs/<slug>/index.html` (+ optional local css/js/svg in that folder) and
`bakeoff/designs/<slug>/meta.json`:
`{"name": "...", "concept": "one sentence", "panels": ["..."], "frames": 4, "frame_ms": 12000}`
where frames x frame_ms lets the screenshot tool capture each rotation state (max 6 frames).

Self-check: the gallery server runs at http://127.0.0.1:8794 (serves `bakeoff/`). Screenshot your
design with `cd ~/worktrees/amherst-display-board-bakeoff && node bakeoff/shoot.mjs <slug>`, then open
`bakeoff/shots/<slug>-*.png` with your image reader and fix what looks wrong (overflow, clipping,
tiny text, empty panels, console errors in `designs/manifest.json`). Iterate at least twice.
Do not edit anything outside your own `designs/<slug>/` folder.
