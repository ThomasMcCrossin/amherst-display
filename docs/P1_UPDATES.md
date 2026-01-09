# P1 Updates - League-Wide Stats & Display Enhancements

## Overview

P1 adds league-wide statistics scraped from the HockeyTech `statviewfeed` API (different from the `modulekit` API used for team-specific data). This enables showing MHL scoring leaders, player streaks, goalie rankings, and special teams stats.

---

## New Files

### `scripts/league_stats.mjs`

A new scraper that fetches league-wide data from the HockeyTech statviewfeed API.

**API Endpoints Used:**
- `statviewfeed/players` - All skaters/goalies across the league
- `statviewfeed/teams` - Team standings with different contexts (overall, powerplay, penaltykill)
- `statviewfeed/streaks_player` - Player scoring streaks (goal streaks, point streaks)

**Data Extracted:**
| Category | Data |
|----------|------|
| **Scoring Leaders** | Top 100 skaters by points, goals, assists, PPG |
| **Rookie Leaders** | Top rookies by points |
| **Goalie Leaders** | By SV%, GAA, wins, shutouts |
| **Goal Streaks** | Players with 3+ consecutive games scoring |
| **Point Streaks** | Players with 3+ consecutive games with a point |
| **Special Teams** | PP% and PK% rankings for all 12 teams |

**Output:** `league_stats.json`

**Key Functions:**
- `fetchStatView(view, params)` - Fetches from statviewfeed API, handles JSONP unwrapping
- `extractPlayersFromResponse(data)` - Parses nested `sections[].data[].row` structure
- `normalizePlayer(p)` - Normalizes player stat field names
- `normalizeStreak(s)` - Normalizes streak data, calculates if streak is active
- `isRecentDate(dateStr)` - Checks if streak ended within last 7 days (still "hot")

---

### `scripts/explore_mhl.mjs`

A Playwright-based exploration script used to discover what data is available on themhl.ca. This was used during development to understand the API structure.

**Purpose:** Visits various pages on themhl.ca, extracts table headers, filters, and intercepts API calls to understand the available data.

---

## Modified Files

### `scripts/build_all.mjs`

**Changes:**
- Added import: `import { buildLeagueStats } from './league_stats.mjs'`
- Added `buildLeagueStatsWrapper()` function with error handling and fallback
- Added step 6 in `main()`: `await buildLeagueStatsWrapper()`
- Added fallback file creation for `league_stats.json`

---

### `display.js`

**State Changes:**
```javascript
let STATE = {
  // ... existing fields ...
  leagueStats: null, // NEW: League-wide stats, leaders, streaks
};
```

**Data Loading Changes (`loadAllData`):**
- Added `fetchJson('league_stats.json')` to the Promise.allSettled array
- Added `STATE.leagueStats = leagueRes.value` assignment
- Added logging for `leagueLeaders` count

**Enhanced `renderLeague()` Function:**

Previously showed team standings. Now shows:

1. **Scoring Leaders** - Top 5 players in MHL by points
   - Amherst players highlighted with gold background and star icon
   - Shows goals/assists breakdown

2. **Special Teams Section** - Amherst's PP% and PK% with league rankings
   - Only shown if data available

3. **Fallback** - If no player data, falls back to team standings

**Enhanced `renderTicker()` Function:**

New ticker items added:

| Item | Condition |
|------|-----------|
| MHL Points Leader | Always (if data available) |
| Amherst players in top 10 | If any Amherst player is #2-10 in scoring |
| Hot Goal Streak | If any Amherst player has active goal streak |
| Hot Point Streak | If any Amherst player has active point streak (different from goal streak player) |
| Power Play ranking | If Amherst is top 3 in PP% |
| Penalty Kill ranking | If Amherst is top 3 in PK% |

---

### `index.html`

**Changes:**
- Changed "League Leaders" panel title to "Scoring Leaders"
- Changed "Next MHL Games" panel title to "MHL Today"

---

## Data Schema

### `league_stats.json`

```json
{
  "generated_at": "2026-01-08T12:38:08.799Z",
  "season": "2024-25",
  "season_id": 41,
  "league": "MHL",

  "leaders": {
    "points": [
      {
        "player_id": "2799",
        "name": "Anthony Gaudet",
        "number": "11",
        "position": "C",
        "team": "AMH",
        "gp": 32,
        "goals": 14,
        "assists": 39,
        "points": 53,
        "ppg": 4,
        "ppa": 18,
        "shg": 4,
        "pts_per_game": 1.66,
        "rookie": false
      }
      // ... top 20
    ],
    "goals": [...],    // top 20 by goals
    "assists": [...],  // top 20 by assists
    "ppg": [...],      // top 10 by power play goals
    "rookies": [...]   // top 10 rookies by points
  },

  "goalies": {
    "sv_pct": [...],   // top 10 by save percentage
    "gaa": [...],      // top 10 by GAA (ascending)
    "wins": [...],     // top 10 by wins
    "shutouts": [...]  // top 10 by shutouts
  },

  "streaks": {
    "goals": [
      {
        "player_id": "3512",
        "name": "Cole MacLeod",
        "team": "YAR",
        "division": "EastLink South",
        "rank": 1,
        "streak_start": "Nov 15, 2025",
        "streak_end": "Dec 7, 2025",
        "games": 8,
        "streak_length": 9,
        "points": 14,
        "active": false
      }
      // ... top 15 with 3+ game streaks
    ],
    "points": [...]  // same structure
  },

  "standings": [...],  // all 12 teams with full standings data

  "special_teams": {
    "powerplay": [
      {
        "team_id": "10",
        "name": "Summerside Western Capitals",
        "code": "SWC",
        "gp": 34,
        "pp_goals": 52,
        "pp_opportunities": 147,
        "pp_pct": 35.4,
        "pk_pct": 82.0,
        "shg_for": 11,
        "shg_against": 7
      }
      // ... sorted by PP% descending
    ],
    "penaltykill": [...]  // sorted by PK% descending
  }
}
```

---

## Key Findings from Data

As of the last scrape:

| Stat | Value |
|------|-------|
| **MHL Scoring Leader** | Anthony Gaudet (AMH) - 53 PTS |
| **Top Goal Scorer** | Eli Baillargeon (TRU) - 26 G |
| **Amherst PP%** | 30.3% (#2 in MHL) |
| **Best PP%** | Summerside - 35.4% |
| **Best PK%** | Edmundston - 88.7% |

---

## API Notes

### HockeyTech statviewfeed API

**Base URL:** `https://lscluster.hockeytech.com/feed/index.php`

**Required Parameters:**
- `feed=statviewfeed`
- `view=players|teams|streaks_player`
- `key=4a948e7faf5ee58d`
- `client_code=mhl`
- `site_id=2`
- `season=41` (2024-25 season)

**Response Format:** JSONP wrapped in parentheses
```
([{sections: [{data: [{row: {...}, prop: {...}}]}]}])
```

The `row` object contains the actual data fields. The `prop` object contains link metadata.

---

## Build Integration

The league stats scraper runs as step 6 in the build pipeline:

```
1. Rosters
2. Game summaries
3. Standings
4. Schedules
5. CCMHA games
6. League stats  <-- NEW
7. Fallback file creation
```

Run with: `npm run build`

Or standalone: `node scripts/league_stats.mjs`
