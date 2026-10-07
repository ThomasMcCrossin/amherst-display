# Board data inventory (HockeyTech, MHL 2026-27)

Owning issue: ThomasMcCrossin/amherst-display#20. Probed 2026-10-07 (8 games into the season; Ramblers 3-4-0-1).
Raw responses were cached under `bakeoff/data/raw/` (gitignored). The API key is never recorded here.

Common params for every call: `client_code=mhl`, `league_id=1`, `fmt=json`, `key=<redacted>`. Endpoint: `https://lscluster.hockeytech.com/feed/index.php`.
IDs: Amherst `team_id=1`, `season_id=46` (2026-27 RS), `41` (2025-26 RS), `45` (2026-27 exhibition), `44` (2026 playoffs), `site_id=3`.
"Freshness" is the HockeyTech update cadence as observed (all feeds are live reads; there is no push).

## Verdict

* The `statviewfeed` and `gc` feeds carry everything the board needs. The `modulekit` views are the thin ones.
* **Official starting goalie: no pre-game source observed.** See "Starting goalie" below.
* Not available at all: per-player shots, hits, blocks, TOI, +/- (all zero league-wide this season), team colors, player bios beyond hometown/birthdate/height/weight, news.
* themhl.ca is behind Cloudflare: plain `curl` gets HTTP 403 and WebFetch got 404 on `/game-center/*` and `/game-summary/*`; `/stats/league-leaders` returned only site navigation (the stat tables are JS widgets that call the same feeds below). Public page content therefore adds nothing beyond these feeds. Do not fetch the website from the board builder.

## Sources that work

### modulekit

| View | Request params | Fields | Fresh | Works | Board use |
|---|---|---|---|---|---|
| `seasons` | none | season_id, name, start/end date, playoff | static | yes | Validate season |
| `schedule` | `season_id=46&team_id=1` | game_id, GameDateISO8601, home/visiting team id+name+code, goal counts, overtime/shootout flags, **attendance**, status, venue name+location, flo ids, tickets_url, game_status | per game | yes (52 games) | Next game, upcoming list, results, attendance, **head-to-head history** (also `season_id=41` for last season) |
| `teamsbyseason` | `season_id=46` | id, name, code, nickname, city, division, `team_logo_url` | static | yes | Team directory and logos (jpg) |
| `roster` | `team_id=1&season_id=46` | name, number, position, birthdate, hometown, shoots, height, weight, rookie, player_image | roster moves | yes | Redundant with statviewtype |
| `statviewtype` `type=skaters` | `team_id=1&season_id=46` | full season line incl. PP/SH/GWG/OT goals, PPG, PIM, age, **birthdate, hometown**, headshot | after each game | yes (24 rows) | Skater table, "local kid" |
| `statviewtype` `type=goalies` | `team_id=1&season_id=46` | GP, W, L, OTL, SOL, saves, shots, GA, SV%, GAA, SO, minutes, catches, hometown | after each game | yes | Goalie table. No GS column |
| `statviewtype` `type=roster` | same | same as roster | | yes | skip |
| `scorebar` | `numberofdaysback=7&numberofdaysahead=7` | league-wide games in the window: scores, clock, period, status, standings-ish W/L, Flo urls | live | yes (36 games) | Other scores tonight; live flag |
| `transactions` | `season_id=46` | type, player, team, date | daily | yes (20 rows league-wide, none for Amherst yet) | Roster-move ticker, low value |
| `gamesummary`, `standings`, `statviewtype type=standings/teams`, `leaders`, `streaks`, `playerstatsbyseason`, `gamebygame`, `teamrecord`, `lastgames`, `players`, `teamdetails`, `bootstrap` | various | `Undefined Tab ...` or empty array | | **no** | Use the statviewfeed equivalents |
| `player`, `statviewtype type=teams` | | invalid (non-JSON) response | | no | |

### statviewfeed (`feed=statviewfeed&site_id=3&season=46`)

| View | Extra params | Fields | Fresh | Works | Board use |
|---|---|---|---|---|---|
| `gameSummary` | `game_id=5033` | Full box score: details (attendance, start/end time, duration), referees, coaches, `mostValuablePlayers` (**three stars**, ordered), per-team shots / PP opps+goals / PIM / hits, per-period goals+shots, goals with scorer, assists, PP/SH/EN/GWG flags, penalties, skaters and goalies lists with **`starting` flag**, goalieLog (period/time in and out) | live during game; final after | yes | Recent results, scorers, stars, shots, goalie of record |
| `gameCenterPreview` | `game_id=5042` | Both teams: record (overall/home/away/L10/streak), GF/GA, PP and PK (overall/home/away), PIM, leadingScorers (5), leadingRookie, leadingPIM, longestStreaks, previousGames (last 5 with scores), `headToHeadRecords` (previousYear/currentYear/previousFiveYears), `previousMeetings`, `lineup.goalies` / `lineup.skaters`, `lineupPairingReport` | updates after each game (stale by one game for GF/GA vs standings) | yes | Pre-game opponent card. `previousMeetings` is `[]` and `lineup` is empty for every game probed (upcoming and finished) |
| `players` | `team=all|<id>&position=skaters|goalies&rookies=0&statsType=standard&rosterstatus=undefined&league_id=1&division=-1&sort=points&order_direction=DESC&limit=100&qualified=all` | Season lines for the whole league or one team, incl. rookie flag and birthdate | after each game | yes | League leaders, opponent goalies. `rookies=1` filters to rookies |
| `player` | `player_id=3756&season_id=46&statsType=standard` | Bio (birthdate, birthplace, height, weight, bio text), **career stats by season** (regular season + playoffs, total row), **`gameByGame`** (every game: date, opponent, G, A, PTS, PIM, PP, SH, GWG; goalies: W/L/OTL/SOL, minutes, GA, SA, saves, SV%), season totals, playerShots (empty), draft info | after each game | yes | Last-5 games, career milestones, **goalie game log** |
| `playerGameByGame` | `player_id=...&season_id=46` | Only the gameByGame block of the above | after each game | yes (cheaper) | Same as above if career is not needed |
| `teams` | `groupTeamsBy=division|conference|league&context=overall|powerplay|penaltykill&division=-1&special=false|true` | Standings (rank, GP, W, L, OTL, SOL, PTS, PCT, RW, OTW, SOW, GF, GA, diff, PIM, streak, past_10); powerplay context adds PP%, PP opps, PK%, SH goals | after each game | yes | Standings, PP/PK ranks. `groupTeamsBy=conference/league` returns one section (no per-division split) |
| `schedule` | `team=1&month=-1&location=homeaway` | Rows with attendance, game report link, game sheet link, summary link, Flo link | per game | yes | Redundant with modulekit schedule |
| `leaders` | none | Rank-1 only for Points, Goals, Save %, Wins, GAA | after each game | yes | Too thin; use `players` |
| `streaks_player` | `stat=goals|points&order_by=&division=-1` | Season-best streaks: GP (length), G, A, PTS, from/to dates, `to="present"` when active | after each game | yes (20 rows) | Hot streaks. Column `goal_streak` is the **G total inside the streak**, not its length (`scripts/league_stats.mjs` reads it as the length) |
| `streaks_team` | none | Longest win / undefeated / losing / winless run with dates | after each game | yes | Team streak trivia |
| `roster` | `team_id=1&season_id=46` | Forwards / Defence / Goalies / Coaches sections with birthdate, hometown, height, weight | | yes | Coaches list is the only unique content |
| `transactions` | none | Same as modulekit | | yes | |
| `standings`, `team`, `goalieGameByGame`, `shots`, `player_gamebygame` | | `InvalidView` | | no | |

### gc (`feed=gc`, the older GameCenter feed)

| View | Params | Fields | Works | Board use |
|---|---|---|---|---|
| `gamesummary` | `game_id=5033&tab=gamesummary` | Same box score in the older shape. `home_team_lineup` / `visitor_team_lineup` have `goalies[]` with **`start` "1"/"0"**, seconds, shots against, plus `skaters[]` with position and flags; `mvps[]`, `shotsByPeriod`, `totalShots`, `goalies` log by period | yes | Alternative to statviewfeed `gameSummary`; same data |
| `preview` | `game_id=5042&tab=preview` | Opponent records and H2H (`HeadToHeadRecord` this season, `last_HeadToHeadRecord` last season, `last5YearsRecord` plus home/visiting splits), `previous_meetings` | yes (`previous_meetings` empty) | H2H win-loss without fetching last season's schedule |
| `lineup` | `game_id=5042&tab=lineup` | `Lineup: []` | yes, empty | Where a pre-game lineup would appear |
| `clock` | `game_id=5033&tab=clock` | period, game_clock, status | yes | Live clock for a live-game mode |
| `plays`, `gamestats` | | `Undefined Tab` | no | |

### Other

| Source | Notes |
|---|---|
| `lscluster.hockeytech.com/game_reports/official-game-report.php?lang_id=1&client_code=mhl&game_id=N` | HTML game sheet, plain GET works. Lists both teams' dressed lineup (goalies shown first) and coaches. Returns "This game is not available." for 5042 (not yet created). Appears only once the game record is built; not a starter flag. |
| `.../text-game-report.php` | Same availability. |
| `assets.leaguestat.com/mhl/240x240/<player_id>.jpg`, `.../mhl/logos/<team_id>.jpg` | Headshots and logos. Headshots exist for roster players. |
| FloHockey link per game (`FloHockeyUrl`) | Stream page only; do not fetch with plain HTTP (see repo AGENTS.md). |

## Starting goalie

**Observed (2026-10-07):**

* Finished games: the goalie who started has `starting: 1` in statviewfeed `gameSummary` (`homeTeam.goalies[]` / `visitingTeam.goalies[]`) and `start: "1"` in gc `gamesummary` lineups. Verified on 5033 and 5019; across all 13 completed box scores cached (Ramblers games plus the opponent's recent games) the flag singled out exactly one goalie per team. The relief goalie has 0, and `goalieLog` records period/time in and out.
* Upcoming game 5042 (puck drop in about 2 days): `gameSummary.homeTeam.goalies = []`, `gameCenterPreview.*.lineup.goalies = []`, gc `lineup` = `[]`, game sheet "not available". No source lists goalies two days out.
* Even finished games return an empty `gameCenterPreview` lineup, so the preview view never carries the lineup.

**Not observed, so unknown:** whether and when the scorekeeper's lineup shows up (and whether `starting` is set) in the hours before puck drop or only after the game starts. I started a read-only watcher (`bakeoff/lineup_watch.mjs`, every 2 minutes, writes `bakeoff/data/raw/lineup_watch.log`) on tonight's MIR vs EDM game 5039 (7:00 pm ADT) and tomorrow's PCC vs YAR 5040. Read that log to get the first-appearance time of `lineup`/`starting` relative to puck drop. Until it is read, treat the answer as "no pre-game source confirmed".

**Guessed (not verified):** HockeyTech fills `home_team_lineup` when the scorekeeper submits the game sheet lineup, typically at or just before puck drop, and `starting` flags the goalie listed in the starting lineup. The board must show "Starter TBD" until a lineup source returns a goalie with the flag set, and `build_board_data.mjs` enforces that: `official_starters` stays `null` unless one of those three fields has the flag.

What the board can show meanwhile (all observed, none a prediction): each goalie's season line, games started per box score, last start date, and for the opponent the goalies who started their last 5 games (`next_game.opponent.goalie_starts_last5`).

## Availability questions

| Question | Answer |
|---|---|
| Head-to-head history | Yes. Schedule rows for this and last season (`season_id=41`), plus record summaries in `gameCenterPreview.headToHeadRecords` and gc `preview` (this season, last season, last 5 years, home/away splits). `previousMeetings` lists are empty. |
| Player game-by-game logs | Yes: statviewfeed `player` / `playerGameByGame` (G, A, PTS, PIM, PP, SH, GWG per game). |
| Goalie game logs | Yes: per game W/L/OTL/SOL, minutes, GA, SA, saves, SV%, GAA. **GS is not a column**; derive it from the box-score `starting` flag (the builder does). Season GP can exceed GS (Lavoie 8 GP, 7 GS: he relieved in one game). |
| Attendance | Yes: schedule `attendance` and `gameSummary.details.attendance`; 2025-26 history available. Ramblers home average so far 759 (3 games). |
| Three stars | Yes: `mostValuablePlayers` (ordered 1-3) in `gameSummary`, `mvps` in gc. Blank for unplayed games. |
| Shot totals | Team level only: per team per game and per period (`gameSummary`). Per-player shots are 0 for everyone. |
| Special teams | Yes at team level: PP/PK %, opportunities, goals, SH goals for/against (`teams` powerplay context, `gameCenterPreview`). Per game PP opps and goals in `gameSummary`. Per-player PP/SH goals and points are in the skater lines. |
| Birthdates / hometowns | Yes for skaters and goalies (`statviewtype`, `roster`, `player`): birthdate, hometown, birthplace. Hometown strings are free text ("Amherst, NS"; some are truncated, e.g. "St-Augustin-de-Desmaures,"). |
| Player milestones | No milestone feed. The builder derives them from career totals in `player` (MHL regular season only; sums across MHL teams). |
| Plus/minus, hits, blocks, TOI, faceoffs | Fields exist, always 0 or null this season. Do not display. |
| Team colors, news | Not in the API. |

## Gaps that would be great but are not available

* Pre-game official starting goalie (unconfirmed, see above).
* Per-player shots and team shot attempts by player.
* TOI, +/-, hits, blocks.
* Line combinations (`lineupPairingReport` is null).
* Injury or scratch status (the `status` field in lineups is empty).
* News feed or social content.

## Gotchas

* `gameCenterPreview` GF/GA lags the standings by one game (22/30 versus 23/31 after game 8); the builder takes totals from `teams` standings.
* `statviewfeed` responses are either an array of `{sections}` or an object; use `statSections()`.
* `teams` standings rank is the division rank; division names come as "EastLink South" and are normalised to "Eastlink South".
* `streaks_player.goal_streak` is goals inside the streak, not its length (see above).
* `gameCenterPreview.longestStreaks` can span seasons (Wheeler shows a 9-game point streak after 8 games).
