/**
 * Scrape league-wide stats from HockeyTech statviewfeed API
 *
 * This captures data NOT available via the modulekit API:
 * - All players across the league (not just one team)
 * - Player streaks (hot/cold)
 * - Shootout statistics
 * - Special teams (PP/PK) team stats
 *
 * Outputs:
 *   league_leaders.json - Top scorers, goalies across all teams
 *   league_streaks.json - Players on hot scoring streaks
 *   team_special_teams.json - PP% and PK% for all teams
 */

import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(__dirname, '..');

// HockeyTech statviewfeed API (different from modulekit!)
const API_KEY = '4a948e7faf5ee58d';
const CLIENT_CODE = 'mhl';
const SITE_ID = 2;
const SEASON_ID = 41; // 2024-25

const nowISO = () => new Date().toISOString();

/**
 * Fetch from statviewfeed API
 */
async function fetchStatView(view, params = {}) {
  const url = new URL('https://lscluster.hockeytech.com/feed/index.php');
  url.searchParams.set('feed', 'statviewfeed');
  url.searchParams.set('view', view);
  url.searchParams.set('key', API_KEY);
  url.searchParams.set('client_code', CLIENT_CODE);
  url.searchParams.set('site_id', SITE_ID);
  url.searchParams.set('season', SEASON_ID);

  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }

  console.log(`[league] Fetching ${view}...`);

  const response = await fetch(url.toString());
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  let text = await response.text();

  // HockeyTech wraps JSONP responses in parentheses
  if (text.startsWith('(') && text.endsWith(')')) {
    text = text.slice(1, -1);
  }

  return JSON.parse(text);
}

/**
 * Fetch league-wide player stats (all teams)
 */
async function fetchLeaguePlayers(position = 'skaters') {
  const data = await fetchStatView('players', {
    team: 'all',
    position,
    rookies: 0,
    statsType: 'standard',
    rosterstatus: 'undefined',
    league_id: 1,
    division: -1,
    sort: 'points',
    order_direction: 'DESC',
    limit: 100,
    qualified: position === 'goalies' ? 'qualified' : 'all'
  });

  return data;
}

/**
 * Fetch player streaks
 */
async function fetchStreaks(stat = 'goals') {
  const data = await fetchStatView('streaks_player', {
    stat,
    order_by: '',
    division: -1
  });

  return data;
}

/**
 * Fetch team standings with different contexts (PP, PK, etc.)
 */
async function fetchTeamStats(context = 'overall') {
  const data = await fetchStatView('teams', {
    groupTeamsBy: 'division',
    context,
    division: -1,
    special: 'false'
  });

  return data;
}

/**
 * Fetch special teams stats (Power Play)
 */
async function fetchSpecialTeams() {
  // PP stats
  const ppData = await fetchStatView('teams', {
    groupTeamsBy: 'division',
    context: 'powerplay',
    division: -1,
    special: 'true'
  });

  // PK stats
  const pkData = await fetchStatView('teams', {
    groupTeamsBy: 'division',
    context: 'penaltykill',
    division: -1,
    special: 'true'
  });

  return { powerplay: ppData, penaltykill: pkData };
}

/**
 * Normalize player data from statviewfeed format
 */
function normalizePlayer(p) {
  return {
    player_id: p.player_id || p.id,
    name: p.name || `${p.first_name || ''} ${p.last_name || ''}`.trim(),
    first_name: p.first_name,
    last_name: p.last_name,
    number: p.jersey_number || p.tp_jersey_number,
    position: p.position,
    team: p.team_code || p.team_name,
    team_id: p.team_id,
    division: p.division_name || p.division,
    // Stats
    gp: parseInt(p.games_played) || 0,
    goals: parseInt(p.goals) || 0,
    assists: parseInt(p.assists) || 0,
    points: parseInt(p.points) || 0,
    plus_minus: parseInt(p.plus_minus) || 0,
    pim: parseInt(p.penalty_minutes) || 0,
    ppg: parseInt(p.power_play_goals) || 0,
    ppa: parseInt(p.power_play_assists) || 0,
    shg: parseInt(p.short_handed_goals) || 0,
    sha: parseInt(p.short_handed_assists) || 0,
    gwg: parseInt(p.game_winning_goals) || 0,
    // Shootout stats (new!)
    sog: parseInt(p.shootout_goals) || 0,
    soa: parseInt(p.shootout_attempts) || 0,
    sogw: parseInt(p.shootout_game_winners) || 0,
    so_pct: parseFloat(p.shootout_percentage) || 0,
    // Shooting
    shots: parseInt(p.shots) || 0,
    sh_pct: parseFloat(p.shooting_percentage) || 0,
    pts_per_game: parseFloat(p.points_per_game) || 0,
    pim_per_game: parseFloat(p.penalty_minutes_per_game) || 0,
    birthdate: p.birthdate,
    rookie: p.rookie === '1' || p.rookie === true,
  };
}

/**
 * Normalize goalie data
 */
function normalizeGoalie(g) {
  return {
    player_id: g.player_id || g.id,
    name: g.name || `${g.first_name || ''} ${g.last_name || ''}`.trim(),
    first_name: g.first_name,
    last_name: g.last_name,
    number: g.jersey_number || g.tp_jersey_number,
    team: g.team_code || g.team_name,
    team_id: g.team_id,
    division: g.division_name || g.division,
    // Stats
    gp: parseInt(g.games_played) || 0,
    wins: parseInt(g.wins) || 0,
    losses: parseInt(g.losses) || 0,
    otl: parseInt(g.ot_losses) || 0,
    sol: parseInt(g.shootout_losses) || 0,
    minutes: parseInt(g.minutes_played) || 0,
    ga: parseInt(g.goals_against) || 0,
    gaa: parseFloat(g.goals_against_average) || 0,
    saves: parseInt(g.saves) || 0,
    shots_against: parseInt(g.shots) || 0,
    sv_pct: parseFloat(g.save_percentage) || 0,
    shutouts: parseInt(g.shutouts) || 0,
    // Shootout stats (new!)
    so_gp: parseInt(g.shootout_games_played) || 0,
    so_wins: parseInt(g.shootout_wins) || 0,
    so_attempts: parseInt(g.shootout_attempts_against) || 0,
    so_goals_against: parseInt(g.shootout_goals_against) || 0,
    so_sv_pct: parseFloat(g.shootout_save_percentage) || 0,
  };
}

/**
 * Normalize streak data
 * API fields: goal_streak, point_streak, games_played, points, assists
 */
function normalizeStreak(s, streakType = 'goals') {
  return {
    player_id: s.player_id,
    name: s.name || `${s.first_name || ''} ${s.last_name || ''}`.trim(),
    team: s.team_code,
    team_id: s.team_id,
    division: s.division_long_name || s.division_name,
    rank: parseInt(s.rank) || 0,
    // Streak info
    streak_start: s.first_game_date,
    streak_end: s.last_game_date,
    games: parseInt(s.games_played) || 0,
    streak_length: parseInt(s.goal_streak || s.point_streak) || 0,
    assists: parseInt(s.assists) || 0,
    points: parseInt(s.points) || 0,
    // A streak is active if end date is recent (within last week)
    active: isRecentDate(s.last_game_date),
  };
}

/**
 * Check if a date string is within the last 7 days
 */
function isRecentDate(dateStr) {
  if (!dateStr) return false;
  try {
    const date = new Date(dateStr);
    const now = new Date();
    const diffDays = (now - date) / (1000 * 60 * 60 * 24);
    return diffDays <= 7;
  } catch {
    return false;
  }
}

/**
 * Normalize team stats
 */
function normalizeTeam(t) {
  return {
    team_id: t.team_id,
    name: t.name || t.team_name,
    code: t.team_code,
    division: t.division_long_name || t.division_name,
    // Record
    gp: parseInt(t.games_played) || 0,
    wins: parseInt(t.wins) || 0,
    losses: parseInt(t.losses) || 0,
    otl: parseInt(t.ot_losses) || 0,
    sol: parseInt(t.shootout_losses) || 0,
    pts: parseInt(t.points) || 0,
    pct: parseFloat(t.percentage) || 0,
    // Goals
    gf: parseInt(t.goals_for) || 0,
    ga: parseInt(t.goals_against) || 0,
    diff: parseInt(t.goal_differential) || 0,
    // Win types
    rw: parseInt(t.regulation_wins) || 0,
    otw: parseInt(t.ot_wins) || 0,
    sow: parseInt(t.shootout_wins) || 0,
    // Other
    pim: parseInt(t.penalty_minutes) || 0,
    streak: t.streak,
    last_10: t.past_10,
  };
}

/**
 * Extract player data from statviewfeed response
 * Response format: [{ sections: [{ data: [{ row: {...} }] }] }]
 */
function extractPlayersFromResponse(data) {
  if (Array.isArray(data) && data[0]?.sections) {
    return data[0].sections.flatMap(section =>
      (section.data || []).map(item => item.row || item)
    );
  }
  if (data?.sections) {
    return data.sections.flatMap(section =>
      (section.data || []).map(item => item.row || item)
    );
  }
  return data.players || data || [];
}

/**
 * Extract team data from statviewfeed response
 */
function extractTeamsFromResponse(data) {
  if (Array.isArray(data) && data[0]?.sections) {
    return data[0].sections.flatMap(section =>
      (section.data || []).map(item => item.row || item)
    );
  }
  if (data?.sections) {
    return data.sections.flatMap(section =>
      (section.data || []).map(item => item.row || item)
    );
  }
  return data.teams || data || [];
}

/**
 * Extract streaks from response
 */
function extractStreaksFromResponse(data) {
  if (Array.isArray(data) && data[0]?.streaks) {
    return data[0].streaks;
  }
  if (data?.streaks) {
    return data.streaks;
  }
  // Check sections format
  if (Array.isArray(data) && data[0]?.sections) {
    return data[0].sections.flatMap(section =>
      (section.data || []).map(item => item.row || item)
    );
  }
  return [];
}

/**
 * Build all league stats
 */
export async function buildLeagueStats() {
  console.log('[league] Building league-wide statistics...');

  const results = {};

  try {
    // 1. League Leaders (Skaters)
    console.log('\n[league] Fetching skater leaders...');
    const skatersRaw = await fetchLeaguePlayers('skaters');
    const skaters = extractPlayersFromResponse(skatersRaw)
      .map(normalizePlayer)
      .filter(p => p.gp > 0);

    // Top 20 by different categories
    results.leaders = {
      points: [...skaters].sort((a, b) => b.points - a.points).slice(0, 20),
      goals: [...skaters].sort((a, b) => b.goals - a.goals).slice(0, 20),
      assists: [...skaters].sort((a, b) => b.assists - a.assists).slice(0, 20),
      ppg: [...skaters].sort((a, b) => b.ppg - a.ppg).slice(0, 10),
      rookies: skaters.filter(p => p.rookie).sort((a, b) => b.points - a.points).slice(0, 10),
    };
    console.log(`[league] Found ${skaters.length} skaters`);

    // 2. Goalie Leaders
    console.log('\n[league] Fetching goalie leaders...');
    const goaliesRaw = await fetchLeaguePlayers('goalies');
    const goalies = extractPlayersFromResponse(goaliesRaw)
      .map(normalizeGoalie)
      .filter(g => g.gp >= 5); // Min 5 games

    results.goalies = {
      sv_pct: [...goalies].sort((a, b) => b.sv_pct - a.sv_pct).slice(0, 10),
      gaa: [...goalies].sort((a, b) => a.gaa - b.gaa).slice(0, 10),
      wins: [...goalies].sort((a, b) => b.wins - a.wins).slice(0, 10),
      shutouts: [...goalies].sort((a, b) => b.shutouts - a.shutouts).slice(0, 10),
    };
    console.log(`[league] Found ${goalies.length} qualified goalies`);

    // 3. Player Streaks
    console.log('\n[league] Fetching player streaks...');
    const goalsStreaksRaw = await fetchStreaks('goals');
    const pointsStreaksRaw = await fetchStreaks('points');

    results.streaks = {
      goals: extractStreaksFromResponse(goalsStreaksRaw)
        .map(normalizeStreak)
        .filter(s => s.games >= 3)
        .slice(0, 15),
      points: extractStreaksFromResponse(pointsStreaksRaw)
        .map(normalizeStreak)
        .filter(s => s.games >= 3)
        .slice(0, 15),
    };
    console.log(`[league] Found ${results.streaks.goals.length} goal streaks, ${results.streaks.points.length} point streaks`);

    // 4. Team Standings (overall)
    console.log('\n[league] Fetching team standings...');
    const teamsRaw = await fetchTeamStats('overall');
    const teams = extractTeamsFromResponse(teamsRaw)
      .map(normalizeTeam)
      .filter(t => t.gp > 0);

    results.standings = teams;
    console.log(`[league] Found ${teams.length} teams`);

    // 5. Special Teams
    console.log('\n[league] Fetching special teams stats...');
    try {
      const specialRaw = await fetchSpecialTeams();

      // Process PP stats
      const ppTeams = extractTeamsFromResponse(specialRaw.powerplay);

      // Process PK stats
      const pkTeams = extractTeamsFromResponse(specialRaw.penaltykill);

      // Fields from API: power_play_pct, penalty_kill_pct, power_play_goals, power_plays, times_short_handed
      results.special_teams = {
        powerplay: ppTeams.map(t => ({
          team_id: t.team_id,
          name: t.name,
          code: t.team_code,
          gp: parseInt(t.games_played) || 0,
          pp_goals: parseInt(t.power_play_goals) || 0,
          pp_opportunities: parseInt(t.power_plays) || 0,
          pp_pct: parseFloat(t.power_play_pct) || 0, // API returns "35.4%"
          pk_pct: parseFloat(t.penalty_kill_pct) || 0,
          shg_for: parseInt(t.short_handed_goals_for) || 0,
          shg_against: parseInt(t.short_handed_goals_against) || 0,
        })).sort((a, b) => b.pp_pct - a.pp_pct),
        penaltykill: pkTeams.map(t => ({
          team_id: t.team_id,
          name: t.name,
          code: t.team_code,
          gp: parseInt(t.games_played) || 0,
          pk_goals_against: parseInt(t.power_play_goals_against) || 0,
          pk_times_shorthanded: parseInt(t.times_short_handed) || 0,
          pk_pct: parseFloat(t.penalty_kill_pct) || 0,
          pp_pct: parseFloat(t.power_play_pct) || 0,
        })).sort((a, b) => b.pk_pct - a.pk_pct),
      };
      console.log(`[league] Found PP stats for ${results.special_teams.powerplay.length} teams`);
    } catch (e) {
      console.warn('[league] Could not fetch special teams:', e.message);
      results.special_teams = { powerplay: [], penaltykill: [] };
    }

    // Write output file
    const output = {
      generated_at: nowISO(),
      season: '2024-25',
      season_id: SEASON_ID,
      league: 'MHL',
      ...results
    };

    const outputPath = path.join(ROOT_DIR, 'league_stats.json');
    await fs.writeFile(outputPath, JSON.stringify(output, null, 2));
    console.log(`\n[league] Wrote league_stats.json`);

    return output;

  } catch (e) {
    console.error('[league] Error:', e);
    throw e;
  }
}

// CLI entry
if (process.argv[1] && process.argv[1].endsWith('league_stats.mjs')) {
  buildLeagueStats()
    .then(() => console.log('\n[league] Complete!'))
    .catch(e => { console.error(e); process.exit(1); });
}
