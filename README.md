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
- Static schedule/stats refresh independently every 5 minutes
- Optional Canteen live-game observations refresh every 15 seconds (see below)

## Data Sources

| Data Type | Source | Update Frequency |
|-----------|--------|------------------|
| **Ramblers Schedule / monitor plan** | HockeyTech modulekit schedule (same acquisition) | Daily |
| **MHL Rosters** | HockeyTech API | Daily |
| **Player Stats** | HockeyTech API | Daily |
| **Game Summaries** | HockeyTech API | Daily |
| **MHL Standings** | HockeyTech statviewfeed (snapshot rendered locally) | Daily |
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
   python3 -m venv .venv
   . .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Set Environment Variables:**
   ```bash
   cp .env.example .env
   ```
   Fill in `HOCKEYTECH_API_KEY` and any local Drive/service-account values you want to use. For shared-drive workflows, `scripts/setup_highlight_drive.py` can generate the machine-local env files under `~/.local/state/...`.

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

## Live game observations (opt-in)

The persistent Ramblers game panel consumes the read-only `canteen.live-game.v1`
DTO from Canteen Ops, independently of the daily JSON build and rotating slides.
It never polls HockeyTech. No endpoint is enabled by default: the panel says
`Not configured` and the existing schedule, stats, slides and ticker still work.

The Next Up panel and header use future team fixtures from the daily central
`games.json` schedule (team slugs and offset-qualified start times). Completed
results remain sourced from `games/amherst-ramblers.json`; absence of future
box scores is not evidence of an empty schedule or a completed season.

Configure a **public, credential-free DTO endpoint** explicitly:

```text
http://localhost:8080/?liveFeedUrl=%2Flive-game.json
```

Alternatively set `window.AMHERST_DISPLAY_CONFIG = { liveFeedUrl: "/live-game.json" };`
in an operator-owned script before `display.js`. A `liveFeedUrl` query parameter
overrides that setting; `?liveFeedUrl=` disables it. Use HTTP(S), not a file URL.
Cross-origin hosting requires endpoint CORS permission; HTTPS boards require an
HTTPS endpoint. Do not put export tokens, passwords or provider URLs in this URL,
repository, HTML or browser configuration. Publish/proxy only the safe DTO through
an operator-approved read-only boundary. The browser omits credentials and referrers.

The panel chooses Amherst by MHL team ID `1`, qualifying game identity with client,
season and game IDs. Recent games scheduled within 12 hours take precedence, then
the nearest upcoming game, then the latest past game; provider array order and
team-name matching do not choose the game. Scores, period, clock and explicit
intermission are source observations, not inferred phases. Scheduled status `1`
with `00:00` stays scheduled; status `4` stays final. Null values remain unknown.
The clock is never locally decremented. Source status text is escaped, not HTML. Each game retains its selected source/capture provenance. Camera capture, feed receipt and delayed-video timing are labelled separately; explicit fallback displays Backup source. Clock basis distinguishes remaining, elapsed and as-reported values. A supplied delay estimate is displayed as an estimate, never subtracted or used to synchronize the video. Collection current describes acquisition freshness, not guaranteed physical-clock accuracy.

Observation age updates every second using source sample timestamps, including
when the upstream returns the same JSON or stops responding. The DTO supplies
`stale_after_seconds`. Stale/error observations are marked **Last known, not live**;
transport failures preserve the last useful observation. Requests time out after
10 seconds, and delayed/older responses cannot overwrite newer data. Empty and
unavailable sources are explicit. Static data errors do not block live polling. Empty cached schedules do not prove the season ended: those sections are labelled cached rather than Season Complete, independently of the live panel.
Landscape keeps the slide layout; narrow screens wrap the live panel and allow
scrolling the existing wide slide content.

Deployment is a separate operator-approved cutover. These assets do not configure
the endpoint host, enable a collector, run GitHub Actions, update a Pi, change
`file:///opt/canteen-kiosk/content/index.html`, or control mpv/HLS/video.
Do not replace the current Pi board or stream merely to enable this feature.

## Highlight Pipeline

The highlight workflow is local-first and does not require Drive ingest for normal runs.

- HockeyTech/MHL box-score times are elapsed in period.
- Broadcast OCR scorebug times are remaining in period.
- Goal timing defaults to one rule: the goal event is the first stable scoreboard clock-stop at the official box-score time.
- The default local Flo recording profile is `flo_strip_recording` (Flo's standard MHL strip). The pre-2026-27 top-right banner stays available as `flohockey_recording`.
- The seeded non-standard profile is `yarmouth_recording` for Yarmouth home broadcasts.
- The default automatic reel mode is `goals_only`.
- PP penalty inserts and major-review clips are opt-in reel modes, not part of the default automatic reel.
- Legacy approximate goal fallback is opt-in for broken scorebugs via `--goal-legacy-timing-fallback`; otherwise unverified goal timings stay flagged instead of being silently treated as exact.
- Known scorebug handling lives in `scorebug_profiles.py`, with auto-probe fallback for unknown layouts. Box layouts (period and clock as separate boxes, stitched before OCR) live in `SCOREBUG_BOX_LAYOUTS` in `highlight_extractor/ocr_engine.py`; a new broadcast layout is one entry there, one profile, and a crop in `tests/fixtures/scorebugs/` so `tests/test_scorebug_layouts.py` guards it.
- Shared Drive bootstrap/config now uses generic `HIGHLIGHTS_*` env names with legacy `RAMBLERS_DRIVE_ID` / `DRIVE_*` aliases still supported.

Common local commands:

```bash
# Build a filtered montage of every Amherst goal across multiple recordings
python3 scripts/build_filtered_reel.py \
  --source 2026-03-20=/path/to/game1.mp4 \
  --source 2026-03-22=/path/to/game2.mp4 \
  --event-type goal \
  --team ramblers \
  --output /tmp/ramblers-goals.mp4

# Build a filtered montage of every goal where Gaudet had an assist
python3 scripts/build_filtered_reel.py \
  --source 2026-03-20=/path/to/game1.mp4 \
  --source 2026-03-22=/path/to/game2.mp4 \
  --event-type goal \
  --assist gaudet \
  --output /tmp/gaudet-assists.mp4
```

Notes:

- `scripts/build_series_goal_reel.py` remains as a compatibility wrapper for Amherst goal-only series reels.
- `scripts/build_filtered_reel.py` reuses existing processed game folders when present unless `--force-reprocess` is set.
- `scripts/build_production_highlight_reel.py` now reads `matched_events.json` by default and can skip approved majors with `--skip-major-approved`.
- `scripts/setup_highlight_drive.py` bootstraps the canonical shared-drive tree and writes local env/manifest outputs for future ingest and archive flows.
- The seeded program manifest is `programs/mhl-amherst-ramblers-2025-26.json`.
- For multi-machine setups, keep processing local to each machine and use the Shared Drive tree as the shared archive/review surface after processing completes.
- `highlight_extractor.amherst_integration.find_amherst_display_path()` now prefers `AMHERST_DISPLAY_DIR` and sibling repo layouts before falling back to `~/amherst-display`, so side-by-side clones on WSL or another Ubuntu box work without server-specific paths.
- Windows/WSL-specific conveniences such as mounted-drive source paths or copying review files into Windows `Downloads` are operator-local workflow choices, not committed pipeline requirements. The repo itself stays Linux/env-path driven so pure Ubuntu runs keep using their own local paths.

## GitHub Actions Setup

### Required Secret

The required central metadata credential is passed as an environment input; do not put it in config or logs:

1. Go to: **Settings → Secrets and variables → Actions → New repository secret**
2. Name: `HOCKEYTECH_API_KEY`
3. Value: The MHL public application key. The workflow binds this secret to the Node build.

### Workflow Schedule

The automated build runs:
- **Daily at 6:30 AM UTC** (03:30 ADT / 02:30 AST)
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
│   ├── schedules.mjs          # Shared schedule → display events and monitor plan
│   ├── standings.mjs          # Current-season HockeyTech standings
│   ├── rosters.mjs            # HockeyTech roster fetching
│   ├── games.mjs              # Game summaries & box scores
│   └── ccmha.mjs              # GrayJay API integration
└── .github/workflows/
    └── build-jsons.yml        # Automated daily build
```

### Data Pipeline

```
Data Sources
    ├── HockeyTech modulekit (one schedule acquisition; seasons and rosters)
    ├── HockeyTech statviewfeed (stats, summaries, standings)
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

The only source configuration is `config/hockeytech.json`: MHL client `mhl`, league/team `1`,
site `3`, season `46` (`2026-27`), endpoint and worker monitoring policy. There are no per-script
season overrides or ICS dependencies. Update this config intentionally for season rollover;
the season catalog must agree with the configured identity and label before publication.

The central build uses one cached modulekit schedule for `games.json`, `next_games.json`,
completed-game acquisition and `monitor_plan.json`. The plan is `amherst.monitor-plan.v1`,
with an aware successful-acquisition timestamp, exact source season/game/team IDs, explicit
Final evidence, and every source row accounted for. Unknown/TBD starts are null and
non-monitorable with a reason; postponed/cancelled games are also non-monitorable.
Its source block includes only the nonsecret endpoint/request fields, row counts and SHA-256
hashes of the schedule row arrays, never the key or raw API Parameters.

Required MHL stages and the current-season PNG snapshot run in a disposable local staging
directory. A missing key, wrong season, missing/invalid/empty schedule, incomplete roster or
standings, summary/stats error, or snapshot failure exits nonzero without publishing staged
outputs. Actions uses bash pipefail through tee, uploads the log even on failure, and commits
the JSONs, plan, build receipt and PNG together only after success. `metadata_build.json`
records row coverage, snapshot success and explicit optional warnings. Optional GrayJay and
box-score enrichment failures retain prior data (box scores matched by exact game ID).
Existing archive files are not rewritten. Image-download failures retain existing local files.

The public plan is published at the repository root alongside the display metadata:
`https://raw.githubusercontent.com/ThomasMcCrossin/amherst-display/main/monitor_plan.json`.
The [game-only worker](docs/live-game-worker.md) consumes this plan; it does not independently discover daily schedules.

Node 22.12 or newer is required by the pinned Puppeteer dependency (Actions uses Node 22).
Run the scoped acquisition regressions with `node --test tests/central_metadata.test.mjs`.
For local builds, export `HOCKEYTECH_API_KEY`, run `npm ci` and
`npx puppeteer browsers install chrome`, then `npm run build` (including the PNG gate).

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

**Cause:** The latest required central acquisition failed, so prior published metadata was retained.

**Fix:**
1. Check the `HOCKEYTECH_API_KEY` binding and `config/hockeytech.json` season against the build log
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
3. Fix the reported API schema/coverage or snapshot error; do not publish empty fallback files

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

- Open an issue on GitHub
- Check the Actions tab for build logs
- Review recent commits for changes

## Changelog

### September 2026 - Central MHL Acquisition Repair
- Centralized season/source configuration, replaced stale ICS acquisition with a complete API-derived monitor plan, and gated publication on required metadata and snapshot success.

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
