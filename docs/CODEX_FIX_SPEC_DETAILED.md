# Codex Prompt/Spec (Detailed): Fix Amherst MHL Display v3

Use this document as the exact prompt/spec for Codex to fix the v3 display.

## Goal

Fix the outstanding bugs in `index_mhl_redesign_v3_fixed.html` and ensure all **P0** + **P1** features work reliably.

## Hard Constraints (non-negotiable)

1) **Do not use external `display.js`** (no `<script src="display.js">`, no imports). All code must remain **inline** in `index_mhl_redesign_v3_fixed.html`.
2) Keep the display **self-contained**: no new build step required to run the HTML.
3) Do not guess API shapes. **Use Playwright exploration** + **inspect local JSON files** in this repo as the source of truth.
4) Preserve the fetch model + resilience:
   - Fetch JSON from GitHub raw URLs (CORS-friendly).
   - Continue using localStorage caching (`fetchJson()`).

## P0 Requirements (must work)

- **Division-first standings**: Eastlink South first, then Eastlink North.
- **Playoff format**: **Top 4 per division** make playoffs, and the “PLAYOFF LINE” visually appears **between 4th and 5th**.
- **3 Stars uses game stats** (from the last game’s stats), not season totals.
- **Promos only in ticker** (no promo blocks on slides).

## P1 Requirements (must work)

- **League scoring leaders** appear on the League slide from `league_stats.json`.
- **Streaks** + **special teams** (PP/PK) are incorporated (ticker + league context).

## Repo Ground Truth (inspect these first)

Target file:
- `index_mhl_redesign_v3_fixed.html`

Data files:
- `games/amherst-ramblers.json` (Ramblers games; “new format” with `opponent`, `home_game`, `result`, `player_stats`)
- `rosters/amherst-ramblers.json` (player IDs, headshots, season stats)
- `teams.json` (team slug/name/logo)
- `standings_mhl.json` (division standings)
- `league_stats.json` (P1 leaders/streaks/special teams)
- `overrides.json` (ticker override content)

Docs:
- `docs/game-data-structure.md` (canonical shape of `games/amherst-ramblers.json`)
- `docs/P1_UPDATES.md` (statviewfeed JSONP notes + league_stats schema)

Reference code (DO NOT USE DIRECTLY; only port logic inline if needed):
- `display.js` contains working logic for:
  - game-format helpers (`isHomeGame`, `getOpponent`, etc.)
  - computing 3 stars from `player_stats` (`computeThreeStars()`)

## Mandatory Step 0: API Exploration (Playwright)

Before changing parsing, run Playwright exploration and confirm real API shapes:

```bash
node scripts/explore_mhl.mjs
```

Then inspect the local JSON we already generated:

```bash
jq '.games[0] | keys' games/amherst-ramblers.json
jq '.games[0].opponent' games/amherst-ramblers.json
jq '.games[0].result' games/amherst-ramblers.json
jq '.games[0].scoring[0]' games/amherst-ramblers.json
jq '.games[0].player_stats | keys[0:5]' games/amherst-ramblers.json
```

Do not proceed until you understand:
- `games/amherst-ramblers.json` is **NOT** the same shape as `games.json`.
- `scoring[].scorer` is an **object**, not a string.
- `player_stats` is keyed by `player_id` like `mhl-3545`.

## Bug Fix Tasks (with file/line anchors)

All fixes must happen in `index_mhl_redesign_v3_fixed.html`.

### Bug 1: Last 5 games shows “Opponent” instead of team name

**Symptom**
- Slide 1 “Last 5” renders “Opponent”.

**Where**
- `renderNextUp()` (~`index_mhl_redesign_v3_fixed.html:1751`)
- Literal fallback at ~`:1791`:
  - `opp?.name || "Opponent"`

**Root cause**
- `normalizeRamblersGame()` (~`:1523`) does not handle the “new format” from `games/amherst-ramblers.json`.
- As a result, `g.home`/`g.away` are `null`, so opponent names never resolve.

**Ground truth input**
From `games/amherst-ramblers.json` (inside the `.games[]` array):
```json
{
  "date_time": "2026-01-03T19:00:00-04:00",
  "opponent": { "team_name": "Pictou County Weeks Crushers", "team_code": "PCC", "team_id": 7 },
  "home_game": false,
  "venue": "Pictou County Wellness Centre",
  "result": { "ramblers_score": 4, "opponent_score": 6, "won": false, "final_score": "4-6" }
}
```

**Required fix**
- Update `normalizeRamblersGame()` to detect:
  - `g.opponent` exists AND `typeof g.home_game === "boolean"`
- Build `home`/`away` from:
  - `meTeam` = `teamMap.bySlug.get(CONFIG.HOME_TEAM_SLUG)`
  - `oppTeam` = `teamLookup(teamMap, g.opponent.team_name || g.opponent.name || g.opponent.team_code)`
- Set scores based on `home_game`:
  - If `home_game === true`: `home = meTeam`, `away = oppTeam`, `home_score = result.ramblers_score`, `away_score = result.opponent_score`
  - Else: `home = oppTeam`, `away = meTeam`, `home_score = result.opponent_score`, `away_score = result.ramblers_score`

**Acceptance**
- Last 5 shows real opponent names (never “Opponent” unless truly missing).

### Bug 2: Recap shows “Away” / “Home” instead of team names

**Symptom**
- Slide 4 recap scoreboard shows “Away” and “Home”.

**Where**
- `renderRecap()` (~`index_mhl_redesign_v3_fixed.html:2250`)
- Fallback labels at ~`:2287` and `:2294`.

**Root cause**
- Same as Bug 1: the normalized game lacks `home`/`away`.

**Required fix**
- Bug 1 normalization fixes this.

**Acceptance**
- Recap uses real team names and logos.

### Bug 3: 3 Stars shows “[object Object]” and/or wrong stat lines

**Symptom**
- 3 Stars panel shows `[object Object]` or season-like lines (“10 goals”).

**Where**
- `renderRecap()` calls `extractStars()` at ~`index_mhl_redesign_v3_fixed.html:2308`.
- `extractStars()` is at ~`:2388`.

**Root cause**
- The Ramblers games feed does **not** provide `g.stars` in the format `extractStars()` expects.
- The “new format” provides **game stats** in `player_stats`, which should be used for P0.
- Current code also runs `safeStr()` on objects in some fallbacks, producing `[object Object]`.

**Ground truth**
Per `docs/game-data-structure.md`, each game contains:
- `player_stats`: `{ "mhl-3545": { goals, assists, points, ... }, ... }`
- Goalie stats include `saves`, `shots_against`, `goals_against`, `save_percentage`.

**Required fix (P0-correct approach)**
1) Extend `normalizeRamblersGame()` to preserve:
   - `player_stats` (or keep it under `raw` and access `g.raw.player_stats`)
2) Replace/extend `extractStars()` logic to compute 3 stars from **this game’s** `player_stats`:
   - Join `player_stats` keys to roster entries via `player_id` in `rosters/amherst-ramblers.json`.
   - Prefer:
     - skaters with goals/assists (points) > 0
     - goalies with shots_against > 0
   - Output must match what `renderRecap()` expects:
     - `{ name, headshot_url, team, stat }` where `stat` is a string like `2G 1A` or `28 SV • .966`

Port the algorithm from `display.js` into inline code (do not import it):
- `display.js:560` `computeThreeStars()` already scores skaters/goalies correctly from `player_stats`.

**Acceptance**
- 3 Stars never shows `[object Object]`.
- 3 Stars is derived from game stats (not season totals).

### Bug 4: Scoring Summary shows “[object Object]” or missing names

**Symptom**
- Scoring summary is wrong/unreadable (often due to object-to-string coercion).

**Where**
- `extractScoringSummary()` (~`index_mhl_redesign_v3_fixed.html:2482`)

**Root cause**
Scoring events are shaped like:
```json
{
  "period": 1,
  "time": "11:37",
  "team": "opponent",
  "scorer": { "player_id": "mhl-9999", "name": "Alex Christmas" },
  "assists": [{ "player_id": "mhl-2760", "name": "Will Allen" }]
}
```
Current code assumes `scorer` is a string and calls `safeStr()` on an object.

**Required fix**
- Update parsing in `extractScoringSummary()`:
  - `scorerName = typeof ev.scorer === "object" ? ev.scorer?.name : ev.scorer`
  - `assistNames` from `ev.assists?.map(a => a.name)` (array of objects)
  - Optionally append tags: `PP`, `SH`, `EN`, `GWG` from boolean flags.

**Acceptance**
- Scoring summary displays player names correctly, never `[object Object]`.

### Bug 5: Initials overlay covers headshots

**Symptom**
- Initials are drawn on top of real headshot images.

**Where**
- CSS `.hs .ini` (~`index_mhl_redesign_v3_fixed.html:426`)
- `headshotHtml()` (~`:1565`)

**Root cause**
- `.ini` is absolutely positioned over the image; image has no z-index.

**Required fix (prefer CSS-only)**
- Make the image render above initials:
  - `.hs img { position:absolute; inset:0; z-index:1; }`
  - `.hs .ini { z-index:0; }`
- Keep `onerror` behavior so initials show if image fails.

**Acceptance**
- When image loads: initials not visible.
- When image missing: initials visible.

### Bug 6: Faces shows players with 0 stats

**Symptom**
- Faces includes players with 0 GP / 0 points.

**Where**
- `renderFaces()` (~`index_mhl_redesign_v3_fixed.html:2026`)
- `pickRotatingPlayers()` (~`:1976`)

**Required fix**
- Filter eligible players:
  - Skaters: include only those with `gp > 0` (preferred), or `pts > 0`.
  - Goalies: include only those with `gp > 0` or `sv/gaa` present.
- Add fallback: if filtered list is too small, fall back to unfiltered roster to avoid empty slide early season.

**Acceptance**
- Faces slide avoids obvious 0-stat players when enough eligible players exist.

### Bug 7: Playoff line appears after 3rd instead of after 4th (visual off-by-one)

**Symptom**
- “PLAYOFF LINE” label appears between 3rd and 4th.

**Where**
- `.table .r.cut:before { top:-8px; }` (~`index_mhl_redesign_v3_fixed.html:813`)
- `renderStandings()` sets `.cut` on `idx === PLAYOFF_SEEDS - 1` (~`:2554`, `:2582`)

**Root cause**
- The label is drawn above the `.cut` row.
- You’re tagging the 4th team row, so the label appears above it.

**Required fix**
- Tag the **first team out** instead (5th row):
  - Apply `.cut` when `idx === CONFIG.PLAYOFF_SEEDS` (0-based)
  - Only if the division list contains `PLAYOFF_SEEDS + 1` rows

**Acceptance**
- Label visually separates seeds 1–4 from 5+ (between 4th and 5th).

### Bug 8: Ticker promos have no labels, and “Curly’s …” should not show by default

**Symptoms**
- Promo items show without a badge/label.
- Bottom ticker shows a made-up “Curly’s …” sponsor line.

**Where**
- `renderTicker()` promos push `{ b:"", t: p }` (~`index_mhl_redesign_v3_fixed.html:2950`)
- Overrides banner logic is unconditional (~`:2864`)
- `overrides.json` includes: `ticker: "... Curly’s Sports & Supplements: ..."`

**Required fixes**
1) Give promos labels (badge `b`) like:
   - `Promo`, `Tickets`, `Social`, `Home Ice`
2) Gate override ticker text:
   - Only inject override banner when `overrides.active === true` AND not expired by `until`.
3) Ensure made-up sponsor text does not render unless explicitly enabled:
   - Prefer removing it from `overrides.json` or leaving overrides inactive by default.

**Acceptance**
- Promo items show labels.
- “Curly’s …” does not display in normal default run.

### Bug 9: Storylines not generating (OpenAI expectation)

**Symptom**
- User expects OpenAI-generated storylines, but storylines are empty or not “AI-driven”.

**Where**
- Current storylines are deterministic (`buildStandingsStorylines()` ~`index_mhl_redesign_v3_fixed.html:2625`)

**Required fix**
- Ensure deterministic storylines always render when standings exist.
- Add optional AI storylines mode that does not break offline behavior:
  - Preferred: fetch pre-generated `storylines.json` from GitHub (generated by CI using OpenAI).
  - If adding a client-side OpenAI call:
    - Must be opt-in (e.g., query param + key stored in localStorage).
    - Must have short timeout and fallback to deterministic storylines.

**Acceptance**
- Storylines list is populated whenever standings load.
- AI mode is optional and failure-safe.

## Manual Test Plan (must pass before declaring done)

Serve locally:
```bash
python3 -m http.server 8000
```
Open:
- `http://localhost:8000/index_mhl_redesign_v3_fixed.html`

Verify:
1) Last 5 games: real opponent names (no “Opponent”).
2) Recap scoreboard: real team names/logos (no “Away/Home”).
3) 3 Stars: 3 players with game stat lines (no `[object Object]`).
4) Scoring Summary: readable player names (no `[object Object]`).
5) Headshots: initials do not cover real photos.
6) Faces: avoids obvious 0-stat players.
7) Standings: playoff line is between #4 and #5 in each division.
8) Ticker: promos have labels; made-up “Curly’s …” not shown by default.
9) League slide + ticker: P1 leaders/streaks/special teams still present.

## Key v3 code areas (current line numbers)

- `index_mhl_redesign_v3_fixed.html:1523` `normalizeRamblersGame()`
- `index_mhl_redesign_v3_fixed.html:1751` `renderNextUp()` (Last 5)
- `index_mhl_redesign_v3_fixed.html:2250` `renderRecap()`
- `index_mhl_redesign_v3_fixed.html:2388` `extractStars()`
- `index_mhl_redesign_v3_fixed.html:2482` `extractScoringSummary()`
- `index_mhl_redesign_v3_fixed.html:1565` `headshotHtml()` + `.hs` CSS
- `index_mhl_redesign_v3_fixed.html:2026` `renderFaces()` + `pickRotatingPlayers()`
- `index_mhl_redesign_v3_fixed.html:2520` `renderStandings()` + `.cut` CSS
- `index_mhl_redesign_v3_fixed.html:2860` `renderTicker()` (labels + overrides gating)

