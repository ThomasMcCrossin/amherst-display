/**
 * Build bakeoff/data/board.json: one self-contained snapshot for the arena TV board.
 *
 * Usage: HOCKEYTECH_API_KEY=... node bakeoff/build_board_data.mjs [--refresh]
 *
 * Reuses scripts/hockeytech.mjs (client, config, statView/statSections helpers).
 * Requests are sequential with a short delay. Raw responses are cached under
 * bakeoff/data/raw/ (gitignored). Final-game box scores are cached forever; everything
 * else for 20 minutes unless --refresh is given. The key is never written anywhere.
 *
 * official_starters is filled ONLY when an official HockeyTech lineup lists a goalie
 * with the start flag set. It is never inferred.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { config, request, moduleRows, statView, statSections } from '../scripts/hockeytech.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const RAW = path.join(HERE, 'data', 'raw');
const OUT = path.join(HERE, 'data', 'board.json');
const REFRESH = process.argv.includes('--refresh');
const TTL_MS = 20 * 60 * 1000;
const DELAY_MS = 350;
const RECENT_N = 8;
const TEAM_ID = config.team_id;            // "1"
const SEASON = config.season_ids[0];       // "46"
const PREV_SEASON = '41';                  // 2025-26 regular season (head-to-head history)
const headshot = id => `https://assets.leaguestat.com/mhl/240x240/${id}.jpg`;
const num = v => { const n = Number(v); return Number.isFinite(n) ? n : null; };
const sleep = ms => new Promise(r => setTimeout(r, ms));
fs.mkdirSync(RAW, { recursive: true });

let live = 0;
/** Cached fetch. ttl: ms, or Infinity for immutable data. */
async function get(name, params, ttl = TTL_MS) {
  const file = path.join(RAW, `${name}.json`);
  if (!REFRESH && fs.existsSync(file) && (ttl === Infinity || Date.now() - fs.statSync(file).mtimeMs < ttl)) {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  }
  if (live++) await sleep(DELAY_MS);
  const data = await request(params);
  fs.writeFileSync(file, JSON.stringify(data));
  return data;
}
const sv = (name, view, params = {}, ttl) => get(name, { feed: 'statviewfeed', view, site_id: config.site_id, season: SEASON, ...params }, ttl);
const mk = (name, view, params = {}, ttl) => get(name, { feed: 'modulekit', view, ...params }, ttl);
const rowsOf = data => statSections(data).flatMap(s => s.data.map(d => d.row));

// ---------------------------------------------------------------- teams
const teamRows = moduleRows(await mk('teamsbyseason', 'teamsbyseason', { season_id: SEASON }), 'Teamsbyseason');
const teams = new Map(teamRows.map(t => [String(t.id), {
  id: Number(t.id), name: t.name, abbr: t.code, nickname: t.nickname, city: t.city,
  division: t.division_long_name.replace(/^EastLink/, 'Eastlink'), logo_url: t.team_logo_url }]));
const teamByCode = new Map([...teams.values()].map(t => [t.abbr, t]));
const me = teams.get(TEAM_ID);
const teamBrief = id => { const t = teams.get(String(id)); return t ? { id: t.id, name: t.name, abbr: t.abbr, logo_url: t.logo_url, division: t.division } : { id: Number(id) }; };

// ---------------------------------------------------------------- schedule
const schedule = moduleRows(await mk('schedule', 'schedule', { season_id: SEASON, team_id: TEAM_ID }), 'Schedule');
const prevSchedule = moduleRows(await mk('schedule_prev', 'schedule', { season_id: PREV_SEASON, team_id: TEAM_ID }, 24 * 3600e3), 'Schedule');
const isFinal = g => g.final === '1';
const byStart = (a, b) => a.GameDateISO8601.localeCompare(b.GameDateISO8601);

function gameView(g) {
  const home = g.home_team === TEAM_ID;
  const oppId = home ? g.visiting_team : g.home_team;
  const mine = num(home ? g.home_goal_count : g.visiting_goal_count);
  const theirs = num(home ? g.visiting_goal_count : g.home_goal_count);
  const ot = g.overtime === '1', so = g.shootout === '1';
  let result = null;
  if (isFinal(g)) result = mine > theirs ? 'W' : (ot || so ? (so ? 'SOL' : 'OTL') : 'L');
  return {
    id: Number(g.game_id), date: g.date_played, start_iso: g.GameDateISO8601, home, opp: teamBrief(oppId),
    venue: g.venue_name, status: g.game_status, final: isFinal(g),
    score: isFinal(g) ? { for: mine, against: theirs } : null, result, ot_so: so ? 'SO' : ot ? 'OT' : null,
    attendance: num(g.attendance) || null,
  };
}
const finals = schedule.filter(isFinal).sort(byStart);
const pending = schedule.filter(g => !isFinal(g)).sort(byStart);
const nextRaw = pending[0] ?? null;

// ---------------------------------------------------------------- box scores (final games cached forever)
const boxes = new Map();
for (const g of finals.slice(-RECENT_N)) boxes.set(g.game_id, await sv(`gameSummary_${g.game_id}`, 'gameSummary', { game_id: g.game_id }, Infinity));

const startedGoalie = side => side?.goalies?.find(x => x.starting === 1) ?? null;
const code = t => t?.info?.abbreviation ?? t?.abbreviation;
function recentView(g) {
  const box = boxes.get(g.game_id);
  const v = gameView(g);
  if (!box) return v;
  const meSide = v.home ? box.homeTeam : box.visitingTeam, oppSide = v.home ? box.visitingTeam : box.homeTeam;
  const goals = box.periods.flatMap(p => p.goals.map(x => ({
    team: String(x.team?.id) === TEAM_ID ? me.abbr : code({ info: x.team }), mine: String(x.team?.id) === TEAM_ID,
    period: p.info.longName, time: x.time,
    scorer: `${x.scoredBy.firstName} ${x.scoredBy.lastName}`, scorer_id: x.scoredBy.id,
    assists: x.assists.map(a => `${a.firstName} ${a.lastName}`),
    pp: x.properties?.isPowerPlay === '1', sh: x.properties?.isShortHanded === '1',
    en: x.properties?.isEmptyNet === '1', gwg: x.properties?.isGameWinningGoal === '1' })));
  const goalieLine = s => { const g0 = startedGoalie(s); return g0 && { id: g0.info.id, name: `${g0.info.firstName} ${g0.info.lastName}`, sa: g0.stats.shotsAgainst, ga: g0.stats.goalsAgainst, sv: g0.stats.saves, toi: g0.stats.timeOnIce }; };
  v.scorers = goals.filter(x => x.mine);
  v.goals = goals;
  v.three_stars = (box.mostValuablePlayers ?? []).map((m, i) => ({ star: i + 1, name: `${m.player.info.firstName} ${m.player.info.lastName}`,
    number: m.player.info.jerseyNumber, team: m.team.abbreviation, id: m.player.info.id,
    line: m.isGoalie ? `${m.player.stats.saves ?? ''} saves` : `${m.player.stats.goals}G ${m.player.stats.assists}A` }));
  v.shots = { for: meSide.stats.shots, against: oppSide.stats.shots };
  v.pp = { for: `${meSide.stats.powerPlayGoals}/${meSide.stats.powerPlayOpportunities}`, against: `${oppSide.stats.powerPlayGoals}/${oppSide.stats.powerPlayOpportunities}` };
  v.pim = { for: meSide.stats.penaltyMinuteCount, against: oppSide.stats.penaltyMinuteCount };
  v.starting_goalie = { for: goalieLine(meSide), against: goalieLine(oppSide) };
  v.attendance = box.details.attendance || v.attendance;
  return v;
}
const recent = finals.slice(-RECENT_N).reverse().map(recentView);

// ---------------------------------------------------------------- standings + team stats
const standingsRaw = statSections(await sv('teams_overall', 'teams', { groupTeamsBy: 'division', context: 'overall', division: -1, special: 'false' }));
const ppRows = rowsOf(await sv('teams_special', 'teams', { groupTeamsBy: 'division', context: 'powerplay', division: -1, special: 'true' }));
const spByName = new Map(ppRows.map(r => [r.name, r]));
const stRank = (rows, key, desc = true) => { const s = [...rows].sort((a, b) => desc ? b[key] - a[key] : a[key] - b[key]); return n => 1 + s.findIndex(r => r.abbr === n); };
const allRows = [];
const divisions = standingsRaw.map(sec => {
  const rows = sec.data.map(({ row: r }) => {
    const t = teamByCode.get(r.team_code) ?? [...teams.values()].find(x => x.name === r.name);
    const sp = spByName.get(r.name) ?? {};
    const row = { rank: r.rank, team: r.name, abbr: t.abbr, team_id: t.id, logo_url: t.logo_url, is_ramblers: String(t.id) === TEAM_ID,
      gp: num(r.games_played), w: num(r.wins), l: num(r.losses), otl: num(r.ot_losses), sol: num(r.shootout_losses), pts: num(r.points), pct: num(r.percentage),
      rw: num(r.regulation_wins), gf: num(r.goals_for), ga: num(r.goals_against), diff: num(r.goals_diff), pim: num(r.penalty_minutes),
      streak: r.streak, last10: r.past_10, pp_pct: num(parseFloat(sp.power_play_pct)), pk_pct: num(parseFloat(sp.penalty_kill_pct)),
      pp: sp.power_play_goals != null ? `${sp.power_play_goals}/${sp.power_plays}` : null, playoff_position: Number(r.rank) <= 4 };
    allRows.push(row);
    return row;
  });
  return { name: (sec.title || rows[0] && teams.get(String(rows[0].team_id))?.division || '').replace(/^EastLink/, 'Eastlink') || teams.get(String(rows[0].team_id)).division, rows };
});
for (const d of divisions) d.name = teams.get(String(d.rows[0].team_id)).division;
const meRow = allRows.find(r => r.is_ramblers);

// ---------------------------------------------------------------- next game
let nextGame = null;
if (nextRaw) {
  const v = gameView(nextRaw);
  const oppId = v.opp.id;
  const gcp = await sv(`gameCenterPreview_${nextRaw.game_id}`, 'gameCenterPreview', { game_id: nextRaw.game_id }, 5 * 60e3);
  const mySide = v.home ? gcp.homeTeam : gcp.visitingTeam, oppSide = v.home ? gcp.visitingTeam : gcp.homeTeam;
  const rec = r => r && { overall: r.overall?.formattedRecord, home: r.home?.formattedRecord, away: r.visiting?.formattedRecord, last10: r.past_10_games?.formattedRecord, streak: r.streak };
  const oppRow = allRows.find(r => r.team_id === oppId);
  const oppForm = (oppSide.previousGames ?? []).map(p => {
    const oppHome = p.home_team === String(oppId);
    const gf = num(oppHome ? p.home_goal_count : p.visiting_goal_count), ga = num(oppHome ? p.visiting_goal_count : p.home_goal_count);
    const other = teams.get(oppHome ? p.visiting_team : p.home_team);
    return { date: p.date_played, home: oppHome, opp: other?.abbr, gf, ga, result: p.winner === String(oppId) ? 'W' : 'L' };
  });
  // Opponent goalies: season stats + who started their recent games (observed from official box scores, NOT a prediction).
  const oppGoalies = rowsOf(await sv(`opp_goalies_${oppId}`, 'players', { team: oppId, position: 'goalies', rookies: 0, statsType: 'standard', rosterstatus: 'undefined', league_id: config.league_id, division: -1, sort: 'games_played', order_direction: 'DESC', limit: 10, qualified: 'all' }))
    .filter(r => r.player_id).map(r => ({ id: num(r.player_id), name: r.name, number: num(r.jersey_number), gp: num(r.games_played), w: num(r.wins), l: num(r.losses), otl: num(r.ot_losses) + num(r.shootout_losses),
      sv_pct: num(r.save_percentage), gaa: num(r.goals_against_average), so: num(r.shutouts), headshot_url: headshot(r.player_id) }));
  const oppSched = moduleRows(await mk(`schedule_opp_${oppId}`, 'schedule', { season_id: SEASON, team_id: oppId }), 'Schedule').filter(isFinal).sort(byStart).slice(-5);
  const oppStarts = [];
  for (const g of oppSched) {
    const box = await sv(`gameSummary_${g.game_id}`, 'gameSummary', { game_id: g.game_id }, Infinity);
    const side = g.home_team === String(oppId) ? box.homeTeam : box.visitingTeam;
    const s = startedGoalie(side);
    if (s) oppStarts.push({ date: g.date_played, game_id: Number(g.game_id), id: s.info.id, name: `${s.info.firstName} ${s.info.lastName}`, sa: s.stats.shotsAgainst, ga: s.stats.goalsAgainst });
  }
  for (const gs of oppGoalies) gs.starts_last5 = oppStarts.filter(s => s.id === gs.id).length;

  // Official starters: only from an official lineup that sets the start flag.
  const official = { home: null, away: null };
  const fromLineup = (side, key, src) => { const g0 = side?.lineup?.goalies?.find(x => x.starting === 1); if (g0 && !official[key]) official[key] = { id: g0.info.id, name: `${g0.info.firstName} ${g0.info.lastName}`, number: g0.info.jerseyNumber, source: src }; };
  fromLineup(gcp.homeTeam, 'home', 'statviewfeed gameCenterPreview lineup.goalies[].starting=1');
  fromLineup(gcp.visitingTeam, 'away', 'statviewfeed gameCenterPreview lineup.goalies[].starting=1');
  const box = await sv(`gameSummary_${nextRaw.game_id}`, 'gameSummary', { game_id: nextRaw.game_id }, 2 * 60e3);
  fromLineup({ lineup: { goalies: box.homeTeam?.goalies } }, 'home', 'statviewfeed gameSummary homeTeam.goalies[].starting=1');
  fromLineup({ lineup: { goalies: box.visitingTeam?.goalies } }, 'away', 'statviewfeed gameSummary visitingTeam.goalies[].starting=1');
  const gcL = (await get(`gc_gamesummary_${nextRaw.game_id}`, { feed: 'gc', view: 'gamesummary', game_id: nextRaw.game_id, tab: 'gamesummary' }, 2 * 60e3)).GC?.Gamesummary;
  for (const [key, lu] of [['home', gcL?.home_team_lineup], ['away', gcL?.visitor_team_lineup]]) {
    const g0 = lu?.goalies?.find(x => x.start === '1');
    if (g0 && !official[key]) official[key] = { id: num(g0.player_id), name: `${g0.first_name} ${g0.last_name}`, number: num(g0.jersey_number), source: 'gc gamesummary *_team_lineup.goalies[].start=1' };
  }

  const prevH2H = prevSchedule.filter(isFinal).filter(g => [g.home_team, g.visiting_team].includes(String(oppId))).sort(byStart).map(g => { const x = gameView(g); return { date: x.date, home: x.home, score: x.score, result: x.result, ot_so: x.ot_so, attendance: x.attendance }; });
  const thisH2H = finals.filter(g => [g.home_team, g.visiting_team].includes(String(oppId))).map(g => { const x = gameView(g); return { id: x.id, date: x.date, home: x.home, score: x.score, result: x.result, ot_so: x.ot_so, attendance: x.attendance }; });
  const gScorers = side => (side.leadingScorers ?? []).map(s => ({ id: s.info.id, name: `${s.info.firstName} ${s.info.lastName}`, number: s.info.jerseyNumber, gp: null, g: s.stats.goals, a: s.stats.assists, pts: s.stats.points, pim: s.stats.penaltyMinutes, headshot_url: headshot(s.info.id) }));
  const pre = (await get(`gc_preview_${nextRaw.game_id}`, { feed: 'gc', view: 'preview', game_id: nextRaw.game_id, tab: 'preview' }, 5 * 60e3)).GC?.Preview;
  const h2hRec = r => r && { w: r.w, l: r.l, otl: r.otl, sl: r.sl, otw: r.otw, sw: r.sw };
  nextGame = {
    id: v.id, start_iso: v.start_iso, home: v.home, venue: v.venue, status: v.status,
    tickets_url: nextRaw.tickets_url || null, flo_url: nextRaw.FloHockeyUrl ?? null,
    opponent: { ...teamBrief(oppId), nickname: teams.get(String(oppId)).nickname, standings: oppRow ?? null,
      record: rec(oppSide.teamRecord), gf: oppSide.goalsFor, ga: oppSide.goalsAgainst,
      pp: oppSide.powerPlayStats?.overall, pk: oppSide.penaltyKillStats?.overall,
      leading_scorers: gScorers(oppSide), goalies: oppGoalies, goalie_starts_last5: oppStarts },
    ramblers: { record: rec(mySide.teamRecord) },
    h2h_this_season: thisH2H,
    h2h_last_season: prevH2H,
    h2h_records: pre ? { this_season: pre.HeadToHeadRecord, last_season: pre.last_HeadToHeadRecord, last_5_years: pre.last5YearsRecord } : null,
    opponent_form: { last5: oppForm, last10: rec(oppSide.teamRecord)?.last10, streak: rec(oppSide.teamRecord)?.streak },
    official_starters: official,
    lineup_note: 'official_starters stays null until HockeyTech publishes a lineup with the start flag; never inferred.',
  };
}

// ---------------------------------------------------------------- Ramblers players
const skRows = moduleRows(await mk('statviewtype_skaters', 'statviewtype', { type: 'skaters', team_id: TEAM_ID, season_id: SEASON }), 'Statviewtype').filter(r => r.team_id === TEAM_ID);
const glRows = moduleRows(await mk('statviewtype_goalies', 'statviewtype', { type: 'goalies', team_id: TEAM_ID, season_id: SEASON }), 'Statviewtype').filter(r => r.team_id === TEAM_ID);

async function profile(id) {
  const p = await sv(`player_${id}`, 'player', { player_id: id, season_id: SEASON, statsType: 'standard' });
  const gbg = (p.gameByGame?.[0]?.sections?.[0]?.data ?? []).map(d => ({ game_id: num(d.prop?.game?.gameLink), ...d.row }));
  const career = p.careerStats?.[0]?.sections?.find(s => s.title === 'Regular Season')?.data.map(d => d.row) ?? [];
  return { p, gbg, careerTotal: career.find(r => r.season_name === 'Total') ?? null };
}
const parseOpp = (s) => { const [vis, hom] = String(s).split(' @ '); const home = hom === me.abbr; return { home, opp: home ? vis : hom }; };

const skaters = [], milestoneSrc = [];
for (const r of skRows) {
  const { gbg, careerTotal } = await profile(r.player_id);
  const gp = num(r.games_played);
  skaters.push({
    id: num(r.player_id), name: r.name, number: num(r.jersey_number), pos: r.position, shoots: r.shoots, age: num(r.age), birthdate: r.birthdate,
    hometown: r.hometown || r.birthtown || null, height: r.height, weight: num(r.weight), rookie: r.rookie === '1', headshot_url: headshot(r.player_id),
    gp, g: num(r.goals), a: num(r.assists), pts: num(r.points), ppg: num(r.points_per_game), pim: num(r.penalty_minutes), plus_minus: num(r.plus_minus),
    pp_goals: num(r.power_play_goals), pp_points: num(r.power_play_points), sh_goals: num(r.short_handed_goals), gwg: num(r.game_winning_goals), ot_goals: num(r.overtime_goals),
    last5: gbg.slice(-5).reverse().map(x => ({ game_id: x.game_id, date: x.date_played, ...parseOpp(x.game), g: num(x.goals), a: num(x.assists), pts: num(x.points), pim: num(x.penalty_minutes) })),
    career: careerTotal && { gp: num(careerTotal.games_played), g: num(careerTotal.goals), a: num(careerTotal.assists), pts: num(careerTotal.points) },
  });
  milestoneSrc.push(skaters.at(-1));
}
skaters.sort((a, b) => b.pts - a.pts || b.g - a.g || a.number - b.number);

// goalie starts observed from official box scores (starting flag), all completed games
const allFinalBoxes = [];
for (const g of finals) {
  const box = boxes.get(g.game_id) ?? await sv(`gameSummary_${g.game_id}`, 'gameSummary', { game_id: g.game_id }, Infinity);
  allFinalBoxes.push({ g, box });
}
const goalies = [];
for (const r of glRows) {
  const { gbg } = await profile(r.player_id);
  const id = num(r.player_id);
  const starts = allFinalBoxes.filter(({ g, box }) => startedGoalie(g.home_team === TEAM_ID ? box.homeTeam : box.visitingTeam)?.info.id === id);
  const gp = num(r.games_played);
  goalies.push({
    id, name: r.name, number: num(r.jersey_number), catches: r.catches, age: num(r.age), birthdate: r.birthdate, hometown: r.hometown || r.birthtown || null,
    height: r.height, weight: num(r.weight), headshot_url: headshot(r.player_id),
    gp, gs: starts.length, gs_source: 'box score starting flag, completed games',
    w: num(r.wins), l: num(r.losses), otl: num(r.ot_losses), sol: num(r.shootout_losses), sv_pct: num(r.save_percentage), gaa: num(r.goals_against_average), so: num(r.shutouts),
    saves: num(r.saves), shots_against: num(r.shots), ga: num(r.goals_against), minutes: num(r.minutes_played),
    last_start_date: starts.length ? starts.map(s => s.g.date_played).sort().at(-1) : null,
    recent_games: gbg.slice(-5).reverse().map(x => ({ game_id: x.game_id, date: x.date_played, ...parseOpp(x.game),
      dec: x.win === '1' ? 'W' : x.loss === '1' ? 'L' : x.ot_loss === '1' ? 'OTL' : x.shootout_loss === '1' ? 'SOL' : null,
      ga: num(x.goals_against), sa: num(x.shots_against), sv: num(x.saves), sv_pct: num(x.svpct), min: x.minutes, started: !!starts.find(s => s.g.game_id === String(x.game_id)) })),
  });
}
goalies.sort((a, b) => b.gp - a.gp);

// ---------------------------------------------------------------- milestones
const MS = { pts: [25, 50, 75, 100, 150, 200, 250], g: [10, 25, 50, 75, 100, 150], gp: [25, 50, 75, 100, 150, 200] };
const WINDOW = { pts: 4, g: 3, gp: 4 };
const label = { pts: 'career MHL points', g: 'career MHL goals', gp: 'career MHL games' };
const milestones_near = [];
for (const s of milestoneSrc) {
  if (!s.career) continue;
  for (const k of ['pts', 'g', 'gp']) {
    const have = s.career[k], target = MS[k].find(t => t > have);
    if (target && target - have <= WINDOW[k]) milestones_near.push({ id: s.id, name: s.name, number: s.number, stat: label[k], current: have, milestone: target, needs: target - have, scope: 'MHL regular season' });
  }
}
milestones_near.sort((a, b) => a.needs - b.needs);

// ---------------------------------------------------------------- league leaders + streaks
const leagueParams = pos => ({ team: 'all', position: pos, rookies: 0, statsType: 'standard', rosterstatus: 'undefined', league_id: config.league_id, division: -1, sort: pos === 'skaters' ? 'points' : 'games_played', order_direction: 'DESC', limit: 100, qualified: 'all' });
const lSk = rowsOf(await sv('league_skaters', 'players', leagueParams('skaters'))).filter(r => r.player_id && num(r.games_played) > 0);
const lGl = rowsOf(await sv('league_goalies', 'players', leagueParams('goalies'))).filter(r => r.player_id && r.team_code && num(r.games_played) >= 3);
const sk = r => ({ id: num(r.player_id), name: r.name, team: r.team_code, pos: r.position, number: num(r.jersey_number), gp: num(r.games_played), g: num(r.goals), a: num(r.assists), pts: num(r.points), ppg: num(r.points_per_game), headshot_url: headshot(r.player_id) });
const gk = r => ({ id: num(r.player_id), name: r.name, team: r.team_code, number: num(r.jersey_number), gp: num(r.games_played), w: num(r.wins), l: num(r.losses), sv_pct: num(r.save_percentage), gaa: num(r.goals_against_average), so: num(r.shutouts), headshot_url: headshot(r.player_id) });
const top = (rows, key, n, desc = true) => [...rows].sort((a, b) => desc ? num(b[key]) - num(a[key]) : num(a[key]) - num(b[key])).slice(0, n);
const league_leaders = {
  points: top(lSk, 'points', 10).map(sk), goals: top(lSk, 'goals', 10).map(sk), assists: top(lSk, 'assists', 10).map(sk),
  goalies: { sv_pct: top(lGl, 'save_percentage', 5).map(gk), gaa: top(lGl, 'goals_against_average', 5, false).map(gk), wins: top(lGl, 'wins', 5).map(gk), min_gp: 3 },
  ramblers_ranks: Object.fromEntries(['points', 'goals', 'assists'].map(k => [k, lSk.filter(r => r.team_code === me.abbr).map(r => ({ id: num(r.player_id), name: r.name, rank: 1 + [...lSk].sort((a, b) => num(b[k]) - num(a[k])).findIndex(x => x.player_id === r.player_id), value: num(r[k]) })).sort((a, b) => a.rank - b.rank).slice(0, 3)])),
};
const strPts = rowsOf(await sv('streaks_player_points', 'streaks_player', { stat: 'points', order_by: '', division: -1 }));
const strGoals = rowsOf(await sv('streaks_player_goals', 'streaks_player', { stat: 'goals', order_by: '', division: -1 }));
const strTeams = rowsOf(await sv('streaks_team', 'streaks_team', {}));
// streaks_player columns: GP = length of the streak in games, G/A/PTS = totals inside it ("goal_streak" is the G column).
// last_game_date === "present" means the streak is still active.
const mkStreak = kind => r => ({ kind, id: num(r.player_id), name: r.name, team: r.team_code, games: num(r.games_played), g: num(r.goal_streak), a: num(r.assists), pts: num(r.points),
  from: r.first_game_date, to: r.last_game_date, active: r.last_game_date === 'present', ramblers: r.team_code === me.abbr });
const streaks = {
  note: 'Season-best consecutive-game streaks (HockeyTech streaks_player). games = streak length; active = feed says to="present".',
  goal_streaks: strGoals.slice(0, 8).map(mkStreak('goals')),
  point_streaks: strPts.slice(0, 8).map(mkStreak('points')),
  ramblers: [...strGoals.map(mkStreak('goals')), ...strPts.map(mkStreak('points'))].filter(s => s.ramblers),
  team_streaks: strTeams.map(r => ({ team: r.name, longest_win: num(r.longest_win), win_from: r.longest_win_start, win_to: r.longest_win_end, longest_losing: num(r.longest_losing), longest_undefeated: num(r.longest_undefeated) })),
  current_team_streak: meRow?.streak,
};

// ---------------------------------------------------------------- team stats
const shotsFor = recent.filter(g => g.shots), nGames = shotsFor.length || 1;
const sumK = (arr, f) => arr.reduce((s, g) => s + (f(g) ?? 0), 0);
const rk = (key, desc) => stRank(allRows, key, desc)(me.abbr);
let gcpMe = null;
if (nextRaw) { const gcp = JSON.parse(fs.readFileSync(path.join(RAW, `gameCenterPreview_${nextRaw.game_id}.json`), 'utf8')); gcpMe = nextRaw.home_team === TEAM_ID ? gcp.homeTeam : gcp.visitingTeam; }
const team_stats = {
  record: meRow && `${meRow.w}-${meRow.l}-${meRow.otl}-${meRow.sol}`, record_note: 'W-L-OTL-SOL', gp: meRow?.gp, pts: meRow?.pts, pct: meRow?.pct, division_rank: meRow && Number(meRow.rank),
  home: gcpMe?.teamRecord?.home?.formattedRecord ?? null, away: gcpMe?.teamRecord?.visiting?.formattedRecord ?? null, last10: meRow?.last10, streak: meRow?.streak,
  gf: meRow?.gf, ga: meRow?.ga, diff: meRow?.diff, gf_pg: meRow && +(meRow.gf / meRow.gp).toFixed(2), ga_pg: meRow && +(meRow.ga / meRow.gp).toFixed(2),
  pp_pct: meRow?.pp_pct, pk_pct: meRow?.pk_pct, pp: meRow?.pp, pim: meRow?.pim, pim_pg: meRow && +(meRow.pim / meRow.gp).toFixed(1),
  shots_for_pg: +(sumK(shotsFor, g => g.shots.for) / nGames).toFixed(1), shots_against_pg: +(sumK(shotsFor, g => g.shots.against) / nGames).toFixed(1),
  shots_sample_games: shotsFor.length,
  league_ranks: { gf: rk('gf', true), ga: rk('ga', false), pp_pct: rk('pp_pct', true), pk_pct: rk('pk_pct', true), pim: rk('pim', false), points_pct: rk('pct', true), of_teams: allRows.length },
  attendance: { home_games: recent.filter(g => g.home && g.attendance).length, avg_home: (() => { const a = finals.filter(g => g.home_team === TEAM_ID && num(g.attendance)).map(g => num(g.attendance)); return a.length ? Math.round(a.reduce((s, x) => s + x, 0) / a.length) : null; })() },
};

// ---------------------------------------------------------------- upcoming
const upcoming = pending.slice(0, 7).map(g => { const v = gameView(g); return { id: v.id, start_iso: v.start_iso, home: v.home, opp: v.opp, venue: v.venue, status: v.status }; });

// ---------------------------------------------------------------- write
const board = {
  generated_at: new Date().toISOString(), season: config.season_label,
  team: { id: me.id, name: me.name, abbr: me.abbr, nickname: me.nickname, city: me.city, division: me.division, logo_url: me.logo_url,
    colors: null, colors_note: 'HockeyTech exposes no team colors; set in the page theme.' },
  next_game: nextGame, upcoming, recent,
  standings: { divisions, playoff_format: 'Top 4 per division qualify', note: 'rank is division rank; otl and sol are separate; pts as published' },
  team_stats, skaters, goalies, league_leaders, milestones_near, streaks,
  sources: ['modulekit teamsbyseason/schedule/statviewtype', 'statviewfeed gameSummary/gameCenterPreview/player/players/teams/streaks_player/streaks_team', 'feed=gc gamesummary/preview'],
  copyright: 'Official statistics provided by Maritime Hockey League. Powered by HockeyTech.com',
};
const json = JSON.stringify(board);
fs.writeFileSync(OUT, JSON.stringify(board, null, 1));
console.log(`board.json written: ${fs.statSync(OUT).size} bytes (compact ${json.length}), ${live} live requests`);
console.log(`next game: ${nextGame ? `${nextGame.id} vs ${nextGame.opponent.name} ${nextGame.start_iso}` : 'none'}; official starters: ${JSON.stringify(nextGame?.official_starters)}`);
