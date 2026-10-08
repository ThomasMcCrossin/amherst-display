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
- PP penalty inserts, all-penalty clips (`all_penalties`) and major-review clips are opt-in reel modes, not part of the default automatic reel.
- Legacy approximate goal fallback is opt-in for broken scorebugs via `--goal-legacy-timing-fallback`; otherwise unverified goal timings stay flagged instead of being silently treated as exact.
- Known scorebug handling lives in `scorebug_profiles.py`, with auto-probe fallback for unknown layouts. Box layouts (period and clock as separate boxes, stitched before OCR) live in `SCOREBUG_BOX_LAYOUTS` in `highlight_extractor/ocr_engine.py`; a new broadcast layout is one entry there, one profile, and a crop in `tests/fixtures/scorebugs/` so `tests/test_scorebug_layouts.py` guards it. `scorebug_detect.py` picks the layout per recording before the OCR pass: a free OCR vote across known layouts (the winner needs a clear margin, since some crops overlap), then (if `DEEPSEEK_API_KEY` or `SCOREBUG_VISION_API_KEY` is set) a vision check against the reference crops in `assets/scorebugs/`, about 1.7k tokens per call. The generic engine is mirrored to the public [HockeyHighlightExtractor](https://github.com/ThomasMcCrossin/HockeyHighlightExtractor) with `scripts/sync_public_engine.sh <checkout>` after engine changes.
- Flo's bug can freeze (clock and score) for minutes of play. Goals the clock can't time are placed by `goal_locator.py`: it brackets the goal from the readings around the freeze, has the vision model label frames across it, and uses the one group celebration it finds (timing source `vision_celebration`, about 30k prompt tokens per frozen stretch). `GOAL_VISION_LOCATOR = False` in `config.py` disables it; delete `goal_locator.py` and `_locate_goals_by_vision` to remove it.
- `scripts/validate_goal_clips.py --game-dir <game> --video <recording>` checks every HockeyTech goal against the source: the vision model reads the scorebug on both sides of the matched time, and the script marks each goal `confirmed` (score went up for the scoring team, the bug clock agrees, and a frame inside the clip window shows the goal), `score_only` (score and clock agree but no frame inside the clip window shows the goal: the clip may miss it), `visual` (bug frozen, only the picture could confirm), `suspect`, `unclear`, `missed` (covered but no clip) or `not_recorded`. Output: `data/goal_validation.json` (or `--out`), about 3.5k prompt tokens per goal.
- **Vision clip review** (`scripts/review_game.py`, package `clip_review/`, skill `skills/hockey-clip-review/`). Code finds candidates cheaply; cheap vision reviewers look at the frames and may overrule it. See [Vision clip review](#vision-clip-review) below.
- The production reel (`scripts/build_production_highlight_reel.py`) renders overlays with Playwright. On a new host run `npx playwright install chromium-headless-shell` once after `npm install`, or the reel step fails and only `highlights.mp4` is built.
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
- The current program manifest is `programs/mhl-amherst-ramblers-2026-27.json`. Season rollover: update `season_ids`/`season_label` in `config/hockeytech.json`, add `programs/<team>-<season>.json`, then re-run `scripts/setup_highlight_drive.py --program-manifest ... --write-env ...` (see `season.py`).
- For multi-machine setups, keep processing local to each machine and use the Shared Drive tree as the shared archive/review surface after processing completes.
- `highlight_extractor.amherst_integration.find_amherst_display_path()` now prefers `AMHERST_DISPLAY_DIR` and sibling repo layouts before falling back to `~/amherst-display`, so side-by-side clones on WSL or another Ubuntu box work without server-specific paths.
- Windows/WSL-specific conveniences such as mounted-drive source paths or copying review files into Windows `Downloads` are operator-local workflow choices, not committed pipeline requirements. The repo itself stays Linux/env-path driven so pure Ubuntu runs keep using their own local paths.

## Vision clip review

The engine places every game-sheet goal and penalty from the scorebug clock and cuts a fixed
pre/post-roll. `scripts/review_game.py` reviews those clips against the recording:

1. **packet** (code): one directory per incident (goal, or penalties grouped by period+clock)
   with `incident.json` (game-sheet rows, anchor = the engine's placed time, engine window,
   neighbouring events, scorebug-alert flag, authority bounds) and coarse contact sheets
   (goals -75/+40 s at 1 s, -120/+60 s at 1.5 s in scorebug-alert games; minors -60/+25 s;
   majors and fights -120/+120 s).
2. **review**: a reviewer returns a `hockey-clip-review/verdict@1` JSON: `keep`, `adjust`
   (new in/out from the play: for a goal the build-up, from the zone entry / possession change /
   faceoff win that led to it, to the end of the celebration), `relocate` (event more than 30 s from the anchor, with frame evidence),
   `drop` (event not in the recording, with evidence) or `unsure`.
3. **adversary** (`--adversary`, goals, majors and fights): a second model gets the proposed
   final clip as contact sheets and tries to refute it (goal not in clip, cut before the
   puck crosses, starts mid-play, replay included, fight cut off, wrong incident). A dispute
   gets a fresh second review with the objection; still disputed = `held_for_human` (engine
   window kept, listed in the summary).
4. **apply** (`--apply`): `data/review/overrides.json`, reviewed clips in `data/review/clips/`,
   reel manifests `data/review/reel_main.json` (+ `reel_rough_stuff.json`).
   `--build-reel` renders them with `build_production_highlight_reel.py` into
   `output/highlights_reviewed.mp4` (+ `output/highlights_rough_stuff.mp4`). Dropped clips
   leave the reel; overridden windows replace the engine windows.

Authority is enforced in code, not trusted to the model (`check_verdict.py`, used by both the
agents and the pipeline): only game-sheet incidents; relocation needs two evidence frames and
stays within 240 s of the anchor (360 s for majors/fights); goals keep 15-45 s of build-up
before the goal (floor `CLIP_REVIEW_MIN_LEAD_S`, default 15; applied to saved verdicts too) and
8-25 s after the goal (5 s minimum when a replay cuts in), 8-60 s total; minors 9-40 s;
majors up to 90 s; fights from at most 10 s before the gloves drop to at most 10 s after the
players are separated, 75 s cap. A window outside the bounds is clamped; a verdict that still
fails is discarded and the engine window kept. Reel modes (`--reel-mode`): `goals` (default,
unchanged), `with-rough` (fights/majors in the main reel), `separate-rough` (separate
rough-stuff reel).

Backends. `api` (default) is any OpenAI-compatible vision endpoint, two passes (coarse sheets,
then 0.5 s sheets around each boundary): `SCOREBUG_VISION_API_KEY` or `DEEPSEEK_API_KEY`,
`SCOREBUG_VISION_BASE_URL` (default `https://api.deepseek.com`), `SCOREBUG_VISION_MODEL`
(default `deepseek-flash`). `agent` runs any agent harness that can read images and run bash
in the packet directory; it reads `skills/hockey-clip-review/SKILL.md`, pulls more frames itself
with `scripts/frames.py`, writes the verdict and checks it. The command is configuration,
`{prompt}` is replaced by the prompt (without it the prompt goes on stdin) and `{skill}` by the
skill directory. `escalate` runs `api` on every incident and hands off to the agent command only
for a low-confidence, unsure or failed call, a scorebug-alert game, or a fight/major
(`CLIP_REVIEW_ESCALATE_MIN_CONFIDENCE`, default 0.6: the api model reports 0.6 for most ordinary calls, so 0.75 handed off 95% in the bake-off); `summary.json` reports the hand-off rate.

Agents have a budget (SKILL.md: about 20 turns and 12 frame pulls per incident) and a hard
guard in the command: `claude --max-turns N`; pi has no turn flag, so load the skill's
`harness/pi-tool-budget.ts` extension (`-e {skill}/harness/pi-tool-budget.ts`, works with
`--no-extensions`), which blocks frame pulls after `HCR_MAX_TOOL_CALLS` tool calls (default 24)
and stops the run `HCR_TOOL_GRACE` (8) calls later:

```bash
# api reviewer + api adversary, apply, goals-only reel
python3 scripts/review_game.py --game-dir "Games/<game>" --video recording.mp4 --adversary --apply

# agent reviewer (pi + any vision model), cross-model agent adversary, rough stuff in its own reel
export CLIP_REVIEW_AGENT_CMD='env HCR_MAX_TOOL_CALLS=20 pi -p --mode json --no-session --no-context-files --no-skills --no-extensions -e {skill}/harness/pi-tool-budget.ts --tools read,bash --thinking off --model ollama-cloud/deepseek-v4.1-flash {prompt}'
export CLIP_REVIEW_ADVERSARY_CMD='pi -p --mode json --no-session --no-context-files --no-skills --no-extensions -e {skill}/harness/pi-tool-budget.ts --tools read,bash --model ollama-cloud/gemma4:31b {prompt}'
python3 scripts/review_game.py --game-dir "Games/<game>" --video recording.mp4 --backend agent --adversary \
  --apply --reel-mode separate-rough --build-reel

# api first, the lean agent above only where needed
python3 scripts/review_game.py --game-dir "Games/<game>" --video recording.mp4 --backend escalate --apply

# Claude Code as the harness
export CLIP_REVIEW_AGENT_CMD='claude -p --model <model> --output-format json --max-turns 30 --tools Read,Bash --permission-mode bypassPermissions {prompt}'
```

Other flags: `--kinds goal,penalty|wanted`, `--only <id prefix>`, `--workers` (default 4),
`--attempts` (retries on malformed/invalid output, default 3), `--timeout` per agent run,
`--out-dir`. Results are cached per (backend, incident, prompt hash), so a rerun only
pays for what changed. `data/review/summary.json` has per-incident status, disputes, wall time
and tokens (and cost where the harness reports it). Exit code is 0 once the summary is written.
Model comparison: `scripts/clip_review_bakeoff.py` and `docs/2026-10-07-clip-review-bakeoff.md`.

To run the engine itself on a raw recording with no Drive, email or review-monitor side
effects (back catalogue, test corpora): `scripts/run_engine_offline.py --video V --game-id N
--games-json <games/amherst-ramblers.json from that season> --games-root <dir>`.

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
├── ramblers.ics               # Ramblers season calendar, also at data/ramblers.ics (generated)
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
│   ├── ics.mjs                # Season calendar from games.json + results
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
    ├── ics.mjs        → ramblers.ics, data/ramblers.ics (subscribe: https://thomasmccrossin.github.io/amherst-display/ramblers.ics)
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

**Why:** To keep player photos out of the GitHub repository for privacy. The display uses the source URL for Amherst; other teams retain their existing local cache.

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

## Data sources

Hockey data is sourced from:
- **HockeyTech** (rosters, stats, game summaries)
- **MHL** (standings)
- **GrayJay Leagues** (minor hockey)

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
