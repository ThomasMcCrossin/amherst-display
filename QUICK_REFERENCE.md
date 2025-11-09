# Quick Reference: Amherst Display Data Organization

## File Locations & Purposes

### Root Directory JSON Files
```
/home/user/amherst-display/
├── teams.json              ← Team metadata (MANUALLY MAINTAINED)
├── games.json              ← Generated: Master schedule (ICS + BSHL parsing)
├── next_games.json         ← Generated: Next 3 games per team
├── standings_mhl.json      ← Generated: MHL league standings
├── standings_bshl.json     ← Generated: BSHL league standings
├── ccmha_games.json        ← Generated: Junior hockey games
├── overrides.json          ← Promo messages/tickers (OPTIONAL)
└── index.html              ← Web app (loads all JSON + displays)
```

### Data Sources
```
Input → Processing → Output

data/ramblers.ics ──┐
                    ├──→ scripts/schedules.mjs ──→ games.json
BSHL website ───────┤                              next_games.json
                    └─ parseICS() + web scrape

MHL website ┐
BSHL website ├──→ scripts/standings.mjs ──→ standings_mhl.json
            └─ (web scraping + Puppeteer)    standings_bshl.json

GrayJay API ──→ scripts/ccmha.mjs ──→ ccmha_games.json

teams.json ──→ scripts/build_all.mjs (Orchestrator)
           └─ (slug mapping)
```

### Asset Storage Structure
```
assets/
├── logos/
│   ├── mhl/                   ← 12 MHL team logos (PNG)
│   ├── bshl/                  ← 8 BSHL team logos (PNG)
│   └── ccmha/                 ← Junior hockey logo
├── league/
│   ├── mhl.png                ← League badge
│   └── bshl.png
├── standings/                 ← Screenshots (empty: .gitkeep)
└── bg/
    └── ice-texture.jpg        ← Background image
```

## JSON Structure Cheat Sheet

### teams.json
```json
{
  "league_meta": { "MHL": {...}, "BSHL": {...} },
  "teams": [
    { "slug", "name", "league", "aliases": [...], "logo_url" }
  ]
}
```
**Key:** slug is unique identifier, aliases for parsing

### games.json
```json
{
  "events": [
    {
      "league": "MHL",
      "home_team": "...",
      "away_team": "...",
      "home_slug": "...",
      "away_slug": "...",
      "start": "ISO timestamp",
      "location": "Venue name"
    }
  ]
}
```
**Key:** start is ISO with timezone, location filters "home games"

### standings_*.json
```json
{
  "rows": [
    {
      "team": "...",
      "slug": "...",
      "gp": 17,
      "w": 10,
      "l": 3,
      "otl": 3,
      "sol": 1,
      "pts": 24,
      "pct": "0.706",
      "gf": 75,
      "ga": 62,
      "diff": 13,
      "streak": "0-2-0-0",
      "p10": "5-3-2-0"
    }
  ]
}
```
**Key:** slug for team lookup, gp/w/l/otl/sol for win calculations

### ccmha_games.json
```json
{
  "games": [
    { "start": "ISO", "home_team": "...", "away_team": "...", "league": "..." }
  ]
}
```

## Script Execution Flow

### Daily Build (GitHub Actions)
```
1. build-jsons.yml triggers at 06:30 UTC
   ↓
2. node scripts/build_all.mjs
   ├── Load teams.json (slug mapping)
   ├── buildStandings()
   │  ├── MHL: Puppeteer → themhl.ca → parse tables
   │  └── BSHL: Fetch → parse HTML/regex
   ├── buildSchedules()
   │  ├── Ramblers: data/ramblers.ics → parseICS()
   │  └── Ducks: Fetch BSHL → web scrape
   ├── buildCCMHA()
   │  └── GrayJay API
   └── Validate & write all JSON
   ↓
3. npm run snap (generate standings screenshots)
   ↓
4. git add -A && git commit && git push
   ↓
5. GitHub Pages publishes to thomasmccrossin.github.io
```

## Frontend Data Flow

```
index.html loads:
├── CONFIG (hardcoded URLs)
├── Fetch teams.json ──→ TEAM_DIR (slug → team object)
├── Fetch games.json ──→ Parse & filter for week/home games
├── Fetch overrides.json (optional)
├── Fetch ccmha_games.json (optional)
└── Render display board with:
    ├── Schedule cards (logo from TEAM_DIR)
    ├── Countdown (if Amherst game within 24h)
    ├── Pagination (rotate pages every 20s)
    └── Auto-refresh (every 10 min)

localStorage fallback: Saves last successful games.json
```

## Data Model: Teams

### Team Slug Patterns
```
MHL:  amherst-ramblers, truro-bearcats, pictou-county-weeks-crushers, ...
BSHL: amherst-ducks, belle-baie, elsipogtog, ...
```

### Current Teams (20 total)
**MHL (12 teams):** Amherst Ramblers, Truro Bearcats, Pictou County Weeks Crushers, 
Yarmouth Mariners, Valley Wildcats, West Kent Steamers, Summerside Western Capitals, 
Edmundston Blizzard, Grand Falls Rapids, Miramichi Timberwolves, Campbellton Tigers, 
Chaleur Lightning

**BSHL (8 teams):** Amherst Ducks, Bouctouche, Elsipogtog, Dalhousie, Fredericton, 
Miramichi, Belle-Baie, Péninsule

## Performance Notes

- **Generation time:** ~5-10 minutes (Puppeteer rendering for MHL)
- **File sizes:** games.json ~30KB, standings ~5-6KB each
- **Update frequency:** Daily at 3:30am Atlantic
- **Caching:** 10-minute frontend refresh, localStorage fallback

## To Add Roster/Player Data

**Minimal additions:**
1. Create `/home/user/amherst-display/rosters.json` with structure:
   ```json
   {
     "rosters": [
       {
         "team_slug": "amherst-ramblers",
         "players": [
           { "number": 14, "name": "...", "position": "F", "headshot_url": "..." }
         ]
       }
     ]
   }
   ```

2. Create `/home/user/amherst-display/assets/headshots/amherst-ramblers/` directory

3. Add script: `scripts/rosters.mjs` (fetch from HockeyTech/CSV/manual)

4. Update `build_all.mjs` to call `buildRosters()`

5. Update `index.html` to load rosters.json and display player cards

No database needed - all JSON, all static, all GitHub Pages compatible.
