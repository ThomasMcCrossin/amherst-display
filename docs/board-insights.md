# Board insights: storylines for the Game TV (amherst-display#20)

`bakeoff/build_insights.mjs` turns `bakeoff/data/board.json` (plus cached raw feeds where noted) into
`bakeoff/data/insights.json`: 15 to 40 short items, deterministic, no LLM, every number traceable through `sources`.
Run `node bakeoff/build_insights.mjs` (`--check` to validate only, `--print` to read every line).
Examples below use the 2026-10-07 snapshot (Ramblers 3-4-0-1, 8 GP; next game Fri Oct 9 vs Chaleur).

Legend: **Built** = emitted today (item id in brackets). **Later** = buildable from data we already have but not worth a slot yet,
or it needs a trivial extra fetch. **Blocked** = needs data the API does not give us.

## Rules the builder enforces

* Goalie rule: no item names, implies or predicts a starter. A starter is stated only from `next_game.official_starters`, with its
  source in the text; otherwise the board gets "Starting goalies: not official yet". Goalie season stats and last-5-starts history are
  plain facts. `validate()` fails the build on `likely / expected / projected / probable / should start / will start / gets the nod /
  in net / next game ...` near a goalie name or goalie word, and on any forecast word anywhere. `selfTest()` runs first and proves the
  checker rejects eight bad sentences and accepts three good ones. An item that names an upcoming opponent's goalie must be a `goalie` item.
* No negativity about individual Ramblers (word filter on every item that names one). Team struggles are stated as numbers.
* No trivial numbers: no 1-game streaks, streak items need 3+ points or 2+ goals, milestone items need a real mark.
* Headline <= 60 characters, detail <= 140, priority 1 to 5, ids and headlines unique, no `undefined/null/NaN` in text.
* Derived numbers say so (`confidence: "derived"`). Pace is labelled as pace and is not emitted today (sample too small).
* `valid_until` is set on anything tied to a game or a birthday so the display can drop stale items.

## 1. Player storylines

| # | Idea | Source field | Status | Example line |
|---|---|---|---|---|
| 1 | Hometown hero, first career goal | `skaters[].hometown`, `career.g == g`, `recent[].scorers` | Built [local-first-goal-3647] | Amherst's own Ethan Fraser has his first MHL goal. Scored with 36 seconds left against Pictou County on Oct 3. |
| 2 | Birthdays this week | `skaters[].birthdate` | Built [birthdays] | Birthday boys: Fraser and Dyke both turn 18 on Thursday |
| 3 | First-goal class (rookies scoring first career goals) | `career.g == g`, `rookie` | Built [first-goal-class] | Five Ramblers have scored their first MHL goals this season: Lyons, Duval, Fraser, Boivin and Lau. |
| 4 | Rookie spotlight with league rookie rank | `skaters[]`, `raw/league_skaters.json` | Built [rookie-3867] | 17-year-old rookie Lyons has 8 points. Tied for 6th among MHL rookies. |
| 5 | Rookie class totals | `skaters[rookie]` | Built [rookie-class] | The rookie class has 20 points and 7 goals |
| 6 | Career milestone within reach (50 goals, 50 points) | `milestones_near` | Built [milestone-2794, milestone-3562] | Miller is 3 goals shy of 50 career MHL goals |
| 7 | Smaller milestones grouped | `milestones_near` | Built [milestones-more] | Christian White: 3 goals from 10. Darien Reynolds: 3 points from 25. |
| 8 | Milestone passed this season (100 points) | `career.pts - pts < 100 <= career.pts` | Built [passed-100-2794] | Miller has gone past 100 career MHL points |
| 9 | Century of MHL games, local kid | `career.gp - gp < 100 <= career.gp`, hometown | Built [local-century-3322] | Amherst's own Sawyer Harvey is past 100 MHL games |
| 10 | Opening-night point streak | `streaks.ramblers[]` | Built [streak-points-3756] | Wheeler opened the year with a point in 5 straight |
| 11 | Active point or goal streak | `skaters[].last5` | Built (code path; nobody qualifies today) [active-points-*, active-goals-*] | (template) Mitchell has a point in 4 straight |
| 12 | Team scoring leader and MHL rank | `skaters[]`, `league_leaders.ramblers_ranks` | Built [team-points-leader] | Wheeler leads the Ramblers with 10 points; 16th in MHL scoring |
| 13 | Everyone scores (distinct scorers) | `skaters[].g` | Built [ten-scorers] | Ten different Ramblers have scored this season |
| 14 | Shared assist lead | `skaters[].a` | Later (cut for the 40-item cap) | Three Ramblers share the assist lead with 5 |
| 15 | Points per game pace over the full schedule | `skaters[].ppg`, schedule length | Later (8 GP is too small; if shown it must read "pace, not a forecast") | Wheeler: 1.25 points a game, about 65 over 52 games |
| 16 | Multi-point game or hat-trick watch | `skaters[].last5` | Later (nobody has a 3-goal game yet) | (template) Wright had 3 points on Oct 3 |
| 17 | Shooting percentage, shots per game | player shots | **Blocked**: per-player shots are 0 league-wide | |
| 18 | Hits, blocks, TOI, +/- | player stats | **Blocked**: not tracked (plus_minus is 0 for everyone) | |
| 19 | Player of the week, three-star count | `recent[].three_stars` | Later (only Wheeler, Lyons, Duval and Lavoie have been named so far; revisit with more games) | (template) Lavoie: 3 three-star nods |
| 20 | Where the roster comes from | `skaters[].hometown` | Built [roster-map] | Home towns: Nova Scotia leads the roster with 7. NS 7, QC 5, NB 4, NL 4, PEI 3, ON 1. |
| 21 | Average age, youngest and oldest | `skaters[].age` | Later (cut: trivia) | Average Ramblers age: 18.6 |
| 22 | Alumni / "played for X before" | transactions, prior teams | **Blocked**: no player history beyond MHL totals | |
| 23 | Injury or scratch news | lineup status | **Blocked**: `status` is empty in every lineup | |

## 2. Team

| # | Idea | Source | Status | Example line |
|---|---|---|---|---|
| 24 | Points from extra time | `recent[].goals` (shootouts detected by missing goal) | Built [extra-time-points] | 5 of the Ramblers' 7 points came in extra time |
| 25 | Third-period scoring | `recent[].goals[].period` | Built [third-period-goals] | Third periods are the Ramblers' best: 10 of 21 goals |
| 26 | Record when trailing after two | goals by period | Built [comebacks-after-two] | Down after two periods? Ramblers still took points in 3 of 6 |
| 27 | Goals in the last two minutes of a period | `goals[].time` | Built [late-period-goals] | Watch the clock: 6 Ramblers goals in a period's last 2:00 |
| 28 | Rally game (erased 2-goal deficits) | running score | Built [rally-2026-09-19] | Twice down two goals, then OT win at Grand Falls. Wheeler tied it with 1:28 left, then scored 21 seconds into overtime. |
| 29 | Penalty kill rank | `standings.pk_pct` | Built inside [pp-vs-pk] | Ramblers kill 84.6% (22 of 26), 2nd in the MHL |
| 30 | Biggest shot night against a hot goalie | `recent[].shots`, three stars | Built [shots-last-game] | 45 shots Oct 3: Pictou County's goalie stopped 44 |
| 31 | Home vs road split | `team_stats.home/away` | Later (1-2-0-0 home reads as negative; show only once it is a talking point) | |
| 32 | Home opener oddity | `recent[].pim` | Built [home-opener-pim] | Home opener vs Grand Falls: 132 penalty minutes |
| 33 | Penalty minutes rank, discipline | `team_stats.pim`, `league_ranks.pim` | Later | 145 PIM, 7th in the MHL |
| 34 | One-goal games record | `recent[]` | Folded into [extra-time-points] data; own item **Later** | Four of eight games were one-goal finishes |
| 35 | Shots for/against per game | `team_stats.shots_*_pg` | Later (43.5 against reads as negative; useful once the goalie story is positive: see #52) | 33.9 for, 43.5 against |
| 36 | Special-teams trend (PP% by game) | `recent[].pp` | Later (3-for-26 is too negative for the TV) | |
| 37 | Score by period before each intermission | goals by period | Later (needs a live game; the data is there) | (template) Ramblers have led after one period in 2 of 8 |
| 38 | Home attendance trend | `recent[].attendance`, `schedule_prev` | Built [crowd-watch] | Season-high home crowd: 793. Can Friday top it? |
| 39 | Line combinations, power-play units | `lineupPairingReport` | **Blocked**: null | |
| 40 | Team shot attempts, faceoff win% | | **Blocked**: not published | |

## 3. Matchup (next game)

| # | Idea | Source | Status | Example line |
|---|---|---|---|---|
| 41 | Standings stakes | `standings` | Built [stakes-win] | Win Friday and the Ramblers climb to 3rd in the South. A win makes 9 points. |
| 42 | Head-to-head last season | `next_game.h2h_last_season` | Built [h2h-last-season] | Last year vs Chaleur: a win and a loss, both by one goal |
| 43 | Head-to-head this season | `next_game.h2h_this_season` | Built (code path; empty before the first meeting) [h2h-this-season] | (template) Season series vs Chaleur: 1-0 |
| 44 | Opponent form and streak | `opponent_form` | Built [opp-win-streak] | Chaleur arrives on a 2-game win streak. 12 goals in two games. |
| 45 | Power play meets penalty kill | `standings.pp_pct/pk_pct` | Built [pp-vs-pk] | Chaleur's power play meets the MHL's No. 2 penalty kill |
| 46 | Opponent points leader to watch | `opponent.leading_scorers` | Built [opp-top-scorer] | Watch Jack Hayne: Chaleur's points leader. 9 points (4 goals, 5 assists). |
| 47 | Back-to-back and travel stretch | `upcoming` | Built [run-of-games] | Three games in four days: Fri, Sat, Mon. Monday is the Thanksgiving matinee. |
| 48 | Division rival twice in a weekend, special teams | `upcoming`, standings | Built [swc-pp] | Summerside's PP is No. 1 in the MHL; we see them twice |
| 49 | Rematch with a team already met this season | `recent`, `upcoming` | Built [rematch-pcc] | Pictou County rematch is Thursday, Oct 15. Ramblers are 0-2 against Pictou County this season. |
| 50 | Last-5-year series record | `h2h_records.last_5_years` | Later (identical to last season for Chaleur today; useful from year two of the data) | |
| 51 | Last season's series vs every upcoming opponent | `raw/schedule_prev.json` | Built for the weekend opponent only [swc-series-last-year]; other opponents Later | Ramblers went 6-2 against Summerside last season |
| 52 | Opponent goalie history (last 5 starts, season SV%) | `opponent.goalie_starts_last5` | Built as plain history [opp-goalie-history] | History only. Last 5 starts: Morgan 3, Cole 2. |
| 53 | Official starters | `official_starters` | Built, fires only when non-null, cites source [starter-official-*]; otherwise [starters-tba] | Starting goalies: not official yet |
| 54 | Opponent birthdays, hometown ties to Amherst | opponent roster (`roster` for team 21) | Later: one extra fetch per opponent | |
| 55 | Opponent injuries | | **Blocked** | |

## 4. League context

| # | Idea | Source | Status | Example line |
|---|---|---|---|---|
| 56 | Division race | `standings` | Built [division-race] | South race: Valley 15, Truro 12, Summerside 8, Amherst 7 |
| 57 | Unbeaten teams | `standings` | Built [unbeaten-tru] | Truro is 6-0, the MHL's only unbeaten team |
| 58 | MHL scoring race | `league_leaders.points` | Built [league-scoring-race] | MHL scoring race: Lutz (Valley) leads with 17 |
| 59 | Longest active point streak | `streaks.point_streaks` | Built [league-hot-streak] | Charles-Olivier Giguère has a point in 7 straight |
| 60 | Goalie save-percentage rank | `league_leaders.goalies` | Built [goalie-svpct-rank] | Lavoie ranks 3rd in the MHL in save percentage |
| 61 | Goalie workload | `recent[].starting_goalie`, `raw/league_goalies.json` | Built [goalie-workload] | Lavoie faced 64 shots at Edmundston. His 286 saves lead the MHL. |
| 62 | Projected finish, playoff odds | standings | **Not built on purpose**: 8 games, would be a forecast. If wanted later, label "pace" and require 20+ GP | |
| 63 | League-wide birthdays | `players` (top 100 only) | **Blocked** for completeness | |
| 64 | Scores around the league tonight | `scorebar` | Later (separate live panel, not an insight) | |

## 5. History and fun

| # | Idea | Source | Status | Example line |
|---|---|---|---|---|
| 65 | Versus last year's start | `raw/schedule_prev.json` | Built [vs-last-year] | 1 point better than last year's start |
| 66 | Biggest crowd last year came against tonight's opponent | `schedule_prev` | Built inside [crowd-watch] | Last year's biggest home crowd, 1,285, came against Chaleur. |
| 67 | "Last time we beat X" | `schedule_prev`, `recent` | Later | Last win over Chaleur: Nov 15, 3-2 in Chaleur |
| 68 | Next home dates | `upcoming` | Later (cut for the 40-item cap; code removed, trivial to restore from `upcoming`) | Next three home games at Amherst Stadium |
| 69 | Single-game records (most goals, most shots) | `recent` | Later (needs a longer season) | |
| 70 | Ramblers all-time vs opponent | | **Blocked**: only 5 seasons of series data | |

## 6. Game night (intermission is the canteen's busiest moment)

| # | Idea | Source | Status |
|---|---|---|---|
| 71 | Third-period scoring (#25), trailing-after-two (#26), late-period goals (#27) are the intermission items: they tell the crowd why staying put through the buzzer pays off. | goal lists | Built |
| 72 | Live period score, shots so far | `gc clock`, `scorebar` | Later: needs a live mode, not an insight file |
| 73 | After-intermission scoring (first 5 minutes of the 2nd and 3rd) | `goals[].time` | Later | (template) Ramblers have scored N goals in the first 5 minutes of a period |

## Data problems found while building

1. `board.json` `recent[]` for 2026-09-16 at Valley says `ot_so: "OT"`; the box score status is **Final SO** (shootout win). Cause: the
   schedule feed's `shootout` field is `"1"` or `"2"` (1 = home side won it, 2 = visiting side won it), and `build_board_data.mjs` tests `=== '1'`.
   Any shootout won by the visitors shows as OT. The insights builder detects shootouts from the goal list instead and prints a data note.
2. `skaters[].hometown` for Nathaniel Noah is truncated ("St-Augustin-de-Desmaures,", no province). The builder has a one-line fix table (`CITY_PROV`).
3. `next_game.opponent.gf` is 33 while the standings row says 34 (preview lags one game). Use the standings row. Same lag for `ga` is absent today.
4. The skater goal sum is 22 against a team GF of 23: the missing goal is the shootout winner (Sep 16), so "22" is the right denominator
   for skater share stats and "23" for team totals.
5. `streaks.*.note` says league top 8 but the lists hold 8 rows each; "longest active run" is only the longest among those rows.
