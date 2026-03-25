# Amherst Stadium GameBoard

A modern, automated sports display system for showcasing Amherst Ramblers (MHL) games at Amherst Stadium. Designed for Yodeck digital signage players.

## Features

### 📅 **Schedule Display**
- Upcoming games for the next 7 days
- Amherst home games highlighted with accent border
- Real-time countdown for game times
- Team logos and venue information
- Minor hockey (CCMHA) games included

### 📊 **Team Statistics**
- Live MHL standings with Amherst Ramblers highlighted
- Team record (Wins-Losses-OT Losses)
- Goals for/against and goal differential
- Home and away record breakdown
- Recent results with win/loss indicators
- Goal scorers for recent games

### 🏆 **Top Scorers**
- Top 5 Amherst Ramblers scorers
- Player headshots served directly from HockeyTech API
- Points, goals, and assists breakdown
- Jersey numbers and positions

### 📈 **Recent Results & Box Scores**
- Last 5 Amherst Ramblers game results
- Score displays with W/L badges
- Goal scorers listed for each game
- Home/away indicators

### ⚙️ **Auto-Updating Data**
- Automated daily updates via GitHub Actions (3:30 AM Atlantic)
- Display refreshes every 10 minutes
- All data served as static JSON files

## Data Sources

| Data Type | Source | Update Frequency |
|-----------|--------|------------------|
| **Ramblers Schedule** | HockeyTech ICS Calendar | Daily |
| **MHL Rosters** | HockeyTech API | Daily |
| **Player Stats** | HockeyTech API | Daily |
| **Game Summaries** | HockeyTech API | Daily |
| **MHL Standings** | MHL Website (Puppeteer) | Daily |
| **Minor Hockey** | GrayJay Leagues API | Daily |

## Quick Start

### For Yodeck Display

1. **Add to Yodeck:**
   - Create new Web Page widget
   - URL: `https://thomasmccrossin.github.io/amherst-display/`
   - Set refresh interval: 10 minutes
   - Configure display duration as needed

2. **Done!** The display will auto-update with fresh data every 10 minutes.

### For Development

1. **Clone Repository:**
   ```bash
   git clone https://github.com/ThomasMcCrossin/amherst-display.git
   cd amherst-display
   ```

2. **Install Dependencies:**
   ```bash
   npm install
   ```

3. **Set Environment Variable (Optional but Recommended):**
   ```bash
   export RAMBLERS_ICS_URL="https://your-hockeytech-calendar-url.ics"
   ```
   Without this, the fallback `data/ramblers.ics` file will be used (which may be outdated).

4. **Build Data:**
   ```bash
   node scripts/build_all.mjs
   ```

5. **Test Locally:**
   ```bash
   # Serve with any static server, e.g.:
   npx http-server -p 8080
   # Visit: http://localhost:8080
   ```

## GitHub Actions Setup

### Required Secret

To ensure fresh schedule data, set the following repository secret:

1. Go to: **Settings → Secrets and variables → Actions → New repository secret**
2. Name: `RAMBLERS_ICS_URL`
3. Value: Your HockeyTech calendar ICS URL (e.g., `https://lscluster.hockeytech.com/...`)

### Workflow Schedule

The automated build runs:
- **Daily at 6:30 AM UTC** (3:30 AM Atlantic / 2:30 AM EDT)
- **On manual trigger** (Actions tab → "Build display JSONs" → Run workflow)
- **On code changes** to scripts or data files (for testing)

### Manual Trigger

To force an immediate update:
1. Go to **Actions** tab
2. Select **"Build display JSONs & standings snapshots"**
3. Click **"Run workflow"** → **"Run workflow"**

## Architecture

### Files Structure

```
amherst-display/
├── index.html                 # Main display application
├── teams.json                 # Team registry (logos, names, slugs)
├── games.json                 # All upcoming games (generated)
├── standings_mhl.json         # MHL standings (generated)
├── rosters/*.json             # Player rosters for all MHL teams (generated)
├── games/amherst-ramblers.json  # Detailed game summaries (generated)
├── ccmha_games.json           # Minor hockey games (generated)
├── assets/
│   ├── logos/                 # Team and league logos
│   ├── headshots/             # Player headshots (NOT for Amherst)
│   └── bg/                    # Background images
├── scripts/
│   ├── build_all.mjs          # Main orchestrator
│   ├── schedules.mjs          # ICS parsing
│   ├── standings.mjs          # MHL standings scraping
│   ├── rosters.mjs            # HockeyTech roster fetching
│   ├── games.mjs              # Game summaries & box scores
│   └── ccmha.mjs              # GrayJay API integration
└── .github/workflows/
    └── build-jsons.yml        # Automated daily build
```

### Data Pipeline

```
Data Sources
    ├── HockeyTech ICS (Ramblers schedule)
    ├── HockeyTech API (rosters, stats, game summaries)
    ├── MHL Website (standings)
    ├── GrayJay API (minor hockey)
    └── teams.json (team metadata)
         ↓
Node.js Scripts (GitHub Actions)
    ├── schedules.mjs  → games.json, next_games.json
    ├── rosters.mjs    → rosters/*.json
    ├── games.mjs      → games/amherst-ramblers.json
    ├── standings.mjs  → standings_*.json
    └── ccmha.mjs      → ccmha_games.json
         ↓
Static JSON Files (GitHub Pages)
         ↓
Yodeck Display (index.html fetches JSON every 10 min)
```

Set `HOCKEYTECH_API_KEY` before running the HockeyTech-backed scripts locally or in CI.

## Key Design Decisions

### Amherst Ramblers Headshots Served from API

**Why:** To avoid storing player photos in the GitHub repository (privacy/licensing concerns).

**How:** The `headshot_url` field points directly to HockeyTech's API:
```json
{
  "player_id": "mhl-3545",
  "name": "Christian White",
  "headshot_url": "https://assets.leaguestat.com/mhl/240x240/3545.jpg"
}
```

**Other Teams:** Headshots are still downloaded and cached in `assets/headshots/` for non-Amherst teams.

### Static JSON + GitHub Pages

All data is pre-generated as static JSON files, making the display:
- ✅ **Fast:** No server-side processing
- ✅ **Reliable:** Works even if APIs are down
- ✅ **Scalable:** Can handle any traffic
- ✅ **Free:** Hosted on GitHub Pages

### Daily Updates Only

The system updates once per day (3:30 AM) to avoid:
- ❌ Hitting rate limits on scraped websites
- ❌ Excessive GitHub Actions usage
- ❌ Unnecessary commits

**Trade-off:** Data may be up to 24 hours old (acceptable for this use case).

## Troubleshooting

### Display Shows "No upcoming games"

**Cause:** Schedule data is outdated or ICS file has old dates.

**Fix:**
1. Check that `RAMBLERS_ICS_URL` secret is set correctly
2. Manually trigger GitHub Actions workflow
3. Verify `games.json` has future dates: `cat games.json | grep start`

### GitHub Actions Not Running

**Cause:** Workflow schedule may be disabled or repo is archived.

**Fix:**
1. Go to **Actions** tab → Check for disabled workflows
2. Enable workflow if needed
3. Manually trigger a test run

### Standings/Rosters Empty

**Cause:** Network failures during scraping, or website structure changed.

**Fix:**
1. Check **Actions** tab → Latest run → View logs
2. Look for errors in standings.mjs or rosters.mjs steps
3. Update selectors in `scripts/standings.mjs` if website changed

### Player Headshots Not Loading

**Cause:** HockeyTech API URLs changed or CORS issues.

**Fix:**
- For Amherst Ramblers: Check `headshot_url` in `rosters/amherst-ramblers.json`
- For other teams: Verify files exist in `assets/headshots/{Team-Name}/`

## Customization

### Change Display Settings

Edit `CONFIG` in `index.html`:

```javascript
const CONFIG = {
  DAYS_AHEAD: 7,                  // Show games for next N days
  HOME_ONLY: true,                // Show only home games
  HOME_VENUES: ["Amherst Stadium"], // Filter by these venues
  REFRESH_MINUTES: 10,            // Auto-refresh interval
};
```

### Add/Remove Teams

Edit `teams.json`:

```json
{
  "slug": "new-team",
  "name": "New Team Name",
  "aliases": ["New Team", "Team Alias"],
  "logo_url": "assets/logos/mhl/new-team.png",
  "league": "MHL"
}
```

### Modify Styling

All CSS is inline in `index.html` for easy customization. Look for the `<style>` section.

## Tech Stack

| Technology | Purpose |
|------------|---------|
| **Vanilla HTML/CSS/JS** | Frontend display (no frameworks) |
| **Node.js 20** | Backend data processing |
| **Puppeteer** | Headless browser for JS-rendered sites |
| **Cheerio** | HTML parsing for scraping |
| **date-fns** | Date/time manipulation |
| **GitHub Actions** | CI/CD automation |
| **GitHub Pages** | Static hosting |

## License

This project is for personal use at Amherst Stadium. Hockey data is sourced from:
- **HockeyTech** (rosters, stats, game summaries)
- **MHL** (standings)
- **GrayJay Leagues** (minor hockey)

Logos and team names are property of their respective organizations.

## Support

For issues, bugs, or feature requests:
- Open an issue on GitHub
- Check the Actions tab for build logs
- Review recent commits for changes

## Changelog

### November 2025 - Enhanced Display
- ✨ Added MHL standings table with Amherst highlighted
- ✨ Added top 5 scorers section with player headshots
- ✨ Added recent results with box scores
- ✨ Added team statistics dashboard
- ✨ Improved layout with 2-column grid design
- ♻️ Changed Amherst Ramblers headshots to serve from API
- 🐛 Fixed display filtering to show future games properly
- 📝 Added comprehensive documentation

### November 2024 - Game Summaries
- ✨ Added detailed game summaries with scoring plays
- ✨ Added penalty tracking
- ✨ Added per-game player statistics

### October 2024 - Initial Release
- 🎉 Initial release with schedules and standings
- 🏒 Support for MHL league
- 🎨 Dark theme optimized for Yodeck displays
