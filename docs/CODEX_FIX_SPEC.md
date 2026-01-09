# Codex Technical Specification: Amherst Ramblers Display Bug Fixes

## Overview

The file `index_mhl_redesign_v3_fixed.html` is a self-contained HTML display for the Amherst Ramblers hockey team. It's ~3000 lines with inline CSS and JavaScript. Multiple bugs need fixing.

**Target file:** `/home/clarencehub/amherst-display/index_mhl_redesign_v3_fixed.html`

---

## Bug 1: Last 5 Games Shows "Opponent" Instead of Team Names

### Problem
The Last 5 games panel shows "Opponent" as the team name instead of the actual opponent name (e.g., "Pictou County Weeks Crushers").

### Root Cause
The `normalizeRamblersGame()` function (line 1523-1551) doesn't handle the actual JSON structure from `games/amherst-ramblers.json`.

**Expected by code (line 1527-1528):**
```javascript
const home = teamLookup(teamMap, g.home_slug || g.home_team_slug || ...);
const away = teamLookup(teamMap, g.away_slug || g.away_team_slug || ...);
```

**Actual JSON structure:**
```json
{
  "opponent": { "team_id": 7, "team_name": "Pictou County Weeks Crushers", "team_code": "PCC" },
  "home_game": false,
  "result": { "ramblers_score": 4, "opponent_score": 6 }
}
```

### Fix
Update `normalizeRamblersGame()` to handle this structure. Add logic around line 1527:

```javascript
function normalizeRamblersGame(g, teamMap){
  const dt = toDate(g.start || g.datetime || g.date || g.date_time || g.game_date || g.gameDate || g.played_on || g.playedOn);
  const status = safeStr(g.status || g.state || g.game_status || "");

  // Handle amherst-ramblers.json format with "opponent" + "home_game" boolean
  let home = null;
  let away = null;
  let home_score = null;
  let away_score = null;

  if(g.opponent && typeof g.home_game === 'boolean'){
    // This is the amherst-ramblers.json format
    const oppTeam = {
      name: g.opponent.team_name || g.opponent.name || 'Opponent',
      slug: g.opponent.team_code?.toLowerCase() || '',
      logo_url: g.opponent.logo_url || null
    };
    const ramTeam = {
      name: CONFIG.HOME_TEAM_NAME,
      slug: CONFIG.HOME_TEAM_SLUG,
      logo_url: null
    };

    if(g.home_game){
      home = ramTeam;
      away = oppTeam;
      home_score = safeNum(g.result?.ramblers_score ?? g.ramblers_score, null);
      away_score = safeNum(g.result?.opponent_score ?? g.opponent_score, null);
    } else {
      home = oppTeam;
      away = ramTeam;
      home_score = safeNum(g.result?.opponent_score ?? g.opponent_score, null);
      away_score = safeNum(g.result?.ramblers_score ?? g.ramblers_score, null);
    }
  } else {
    // Standard format with home/away fields
    home = teamLookup(teamMap, g.home_slug || g.home_team_slug || ...);
    away = teamLookup(teamMap, g.away_slug || g.away_team_slug || ...);
    home_score = safeNum(g.home_score ?? g.homeScore ?? ...);
    away_score = safeNum(g.away_score ?? g.awayScore ?? ...);
  }

  const venue = safeStr(g.venue || g.location || g.rink || g.arena || "");
  // ... rest of function unchanged
```

---

## Bug 2: Recap Shows "Away"/"Home" Instead of Team Names

### Problem
In `renderRecap()` (line 2250), the score display shows "Away" and "Home" as fallbacks instead of actual team names.

### Root Cause
Same as Bug 1 - the `home` and `away` objects are `null` because `normalizeRamblersGame()` didn't parse them correctly.

### Fix
Bug 1's fix will resolve this. The code at line 2287-2294 already has appropriate fallbacks. Once `normalizeRamblersGame()` correctly populates `home` and `away`, these will display properly.

---

## Bug 3: 3 Stars Shows "[object Object]"

### Problem
The 3 Stars section shows "[object Object]" with incorrect stats like "10 goals".

### Root Cause
In `extractStars()` (line 2388), when falling back to scoring summary, the `scorer` field contains an object, not a string.

**Actual JSON structure for scoring:**
```json
{
  "scorer": {
    "name": "Alex Christmas",
    "number": 91,
    "position": "C"
  },
  "assists": [{"name": "Will Allen", ...}]
}
```

**Current code (line 2435):**
```javascript
const scorer = safeStr(ev.scorer || ev.player || ev.goal_scorer || ev.name || "");
```

This passes the entire `scorer` object to `safeStr()`, which converts it to "[object Object]".

### Fix
Update line 2435-2437 in `extractStars()`:

```javascript
// Handle scorer being an object or string
const scorerRaw = ev.scorer || ev.player || ev.goal_scorer;
const scorer = typeof scorerRaw === 'object'
  ? safeStr(scorerRaw?.name || "")
  : safeStr(scorerRaw || "");

// Handle assists being an array of objects
const assistsArr = Array.isArray(ev.assists) ? ev.assists : [];
const a1 = assistsArr[0]
  ? (typeof assistsArr[0] === 'object' ? safeStr(assistsArr[0].name || "") : safeStr(assistsArr[0]))
  : safeStr(ev.a1 || ev.assist1 || ev.primary_assist || "");
const a2 = assistsArr[1]
  ? (typeof assistsArr[1] === 'object' ? safeStr(assistsArr[1].name || "") : safeStr(assistsArr[1]))
  : safeStr(ev.a2 || ev.assist2 || ev.secondary_assist || "");
```

Also update `extractScoringSummary()` (line 2482) with the same fix for consistent behavior.

---

## Bug 4: Initials Covering Headshots

### Problem
When a player has a headshot image, the initials overlay still shows on top of it.

### Root Cause
In `headshotHtml()` (line 1565-1575), both the `<img>` and `.ini` div are always rendered. The CSS (line 426-433) positions `.ini` with `position:absolute; inset:0;` so it covers the image.

### Fix
Hide initials when image loads successfully. Update `headshotHtml()`:

```javascript
function headshotHtml(player){
  const url = safeStr(player?.headshot_url || "");
  const ini = initials(player?.name || "P");
  if(url){
    // Image present - hide initials on load, show on error
    return `
      <div class="hs">
        <img src="${url}" alt="${escapeHtml(player.name)}" loading="lazy"
             onload="this.nextElementSibling.style.display='none'"
             onerror="this.style.display='none'"/>
        <div class="ini">${ini}</div>
      </div>
    `;
  }
  // No image URL - just show initials
  return `
    <div class="hs">
      <div class="ini">${ini}</div>
    </div>
  `;
}
```

---

## Bug 5: Faces Panel Shows Players with 0 Stats

### Problem
The "Faces of the Ramblers" panel shows players with 0 points/goals/assists.

### Root Cause
In `renderFaces()` (line 2026), players are sorted but not filtered. The rotating player picker `pickRotatingPlayers()` (around line 2000-2024) doesn't exclude 0-stat players.

### Fix
Update the curated player selection in `renderFaces()` to filter out 0-stat players:

```javascript
function renderFaces(roster, meTeam){
  // Filter out players with no stats
  const skaters = roster
    .filter(p => !String(p.pos).toLowerCase().includes("g"))
    .filter(p => safeNum(p.pts, 0) > 0 || safeNum(p.g, 0) > 0 || safeNum(p.a, 0) > 0);

  const goalies = roster.filter(p => String(p.pos).toLowerCase().includes("g"));

  // Only include top players if they have stats
  const topPts = skaters.length ? [...skaters].sort((a,b) => b.pts - a.pts)[0] : null;
  const topG = skaters.length ? [...skaters].sort((a,b) => b.g - a.g)[0] : null;
  const topA = skaters.length ? [...skaters].sort((a,b) => b.a - a.a)[0] : null;
  // ... etc
```

Also update `pickRotatingPlayers()` to filter:
```javascript
function pickRotatingPlayers(roster, n){
  // Only consider players with some stats
  const eligible = roster.filter(p =>
    (safeNum(p.pts, 0) > 0 || safeNum(p.g, 0) > 0 || safeNum(p.a, 0) > 0 || safeNum(p.gp, 0) > 0)
  );
  // ... rest of function
```

---

## Bug 6: Playoff Line After 3rd Instead of 4th

### Problem
In standings, the playoff cutoff line appears after position 3 instead of after position 4.

### Root Cause
In `renderStandings()` (line 2552-2554), the playoff line check uses:
```javascript
const isPlayoffLine = idx === CONFIG.PLAYOFF_SEEDS - 1;
```

With `CONFIG.PLAYOFF_SEEDS = 4` and 0-indexed `idx`, this means `idx === 3` (the 4th team). The `.cut` class is applied to this row.

### Investigation
The logic appears correct. The issue might be visual - check the `.cut` CSS class styling. The dashed line should appear BELOW (after) the 4th team, not above it.

### Fix
Search for `.cut` in the CSS section. Ensure it uses `border-bottom` not `border-top`:

```css
.r.cut {
  border-bottom: 2px dashed var(--accent);
}
```

If it's currently `border-top`, change to `border-bottom`.

---

## Bug 7: Ticker Items Missing Labels

### Problem
Ticker shows items like "Home games at the amherst stadium" without proper labels.

### Root Cause
In `renderTicker()` at line 2945-2950:
```javascript
const promos = [
  'Season tickets available at ramblershockey.ca',
  'Follow us @AmherstRamblers',
  'Home games at Amherst Stadium',
];
promos.forEach(p => items.push({ b:"", t: p }));  // b:"" means empty label!
```

The `b` (badge/label) is empty string.

### Fix
Add proper labels to promo items:

```javascript
const promos = [
  { b: "Tickets", t: "Season tickets available at ramblershockey.ca" },
  { b: "Social", t: "Follow us @AmherstRamblers" },
  { b: "Home Ice", t: "Home games at Amherst Stadium" },
];
promos.forEach(p => items.push(p));
```

---

## Bug 8: No Storylines Generating (OpenAI Integration)

### Problem
The storylines section shows "No storylines yet" but should be generating dynamic content.

### Root Cause
The `buildStandingsStorylines()` function (line 2625-2685) generates storylines based on standings data, not OpenAI. It should work if standings data is present.

### Investigation
Looking at line 2600-2619, the function returns storylines if:
- There are standings rows
- The Ramblers are found in the South division
- Teams have valid `pts` values

### Debug
Add console.log statements to verify:
```javascript
console.log('standings:', standings);
console.log('south:', south);
console.log('north:', north);
const lines = buildStandingsStorylines(standings, south, north);
console.log('storylines:', lines);
```

### Likely Issue
The standings data might not have the expected structure. The function expects:
```javascript
r.team?.slug === CONFIG.HOME_TEAM_SLUG  // "amherst-ramblers"
r.team?.name  // team name
r.pts         // points
r.w, r.l, r.ot  // record
```

Check `standings_mhl.json` structure and update `buildStandingsStorylines` to match actual data format.

---

## Bug 9: Curly Placeholders at Bottom

### Problem
User sees curly brace placeholders (like `{{something}}`) at the bottom of the page.

### Root Cause
There may be template placeholders in the HTML that weren't replaced, or debug content that should be removed.

### Fix
Search for `{{` or `}}` in the HTML and remove or replace them. Check around line 1100-1140 in the HTML section for any placeholder text.

---

## Testing Checklist

After making fixes, verify:

1. [ ] Last 5 games shows actual team names (e.g., "Pictou County Weeks Crushers")
2. [ ] Recap shows team names, not "Away"/"Home"
3. [ ] 3 Stars shows player names and correct game stats (e.g., "2G 1A"), not "[object Object]"
4. [ ] Headshot images display without initials overlay when loaded
5. [ ] Faces panel only shows players with at least 1 stat
6. [ ] Playoff line appears after 4th place team in each division
7. [ ] Ticker items have labels (e.g., "Tickets:", "Social:")
8. [ ] Storylines generate based on standings data
9. [ ] No curly placeholder text visible

---

## Data Files Reference

| File | Purpose | Key Fields |
|------|---------|------------|
| `games/amherst-ramblers.json` | Team games | `opponent`, `home_game`, `result` |
| `games.json` | League games | `home`, `away`, `home_score`, `away_score` |
| `rosters/amherst-ramblers.json` | Player stats | `name`, `pts`, `g`, `a`, `headshot_url` |
| `standings_mhl.json` | Standings | `team`, `pts`, `w`, `l`, `ot`, `div` |
| `league_stats.json` | Leaders/streaks | `leaders.points`, `streaks`, `special_teams` |
| `teams.json` | Team metadata | `name`, `slug`, `logo_url` |

---

## Key Functions to Modify

| Function | Line | Purpose |
|----------|------|---------|
| `normalizeRamblersGame()` | 1523 | Parse game JSON to standard format |
| `extractStars()` | 2388 | Extract 3 stars from game data |
| `extractScoringSummary()` | 2482 | Parse scoring events |
| `headshotHtml()` | 1565 | Render player headshot/initials |
| `renderFaces()` | 2026 | Display featured players |
| `pickRotatingPlayers()` | ~2000 | Select rotating spotlight players |
| `renderTicker()` | 2860 | Build ticker content |
| `buildStandingsStorylines()` | 2625 | Generate standings storylines |
