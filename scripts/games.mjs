/**
 * Build Amherst Ramblers game summaries with detailed stats
 * - Fetches completed games from schedule
 * - Fetches game summaries with scoring plays, penalties, player stats
 * - Links all data to player IDs from roster
 *
 * Outputs:
 *   games/amherst-ramblers.json - All Ramblers games with detailed data
 */

import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(__dirname, '..');

// HockeyTech API configuration
const HOCKEYTECH_API_KEY = '4a948e7faf5ee58d';
const HOCKEYTECH_CLIENT = 'mhl';
const HOCKEYTECH_BASE_URL = 'https://lscluster.hockeytech.com/feed/';
const SEASON_ID = 41; // 2024-25 season
const AMHERST_TEAM_ID = 1;

const nowISO = () => new Date().toISOString();

const ensureDir = async (fp) => {
  await fs.mkdir(fp, { recursive: true });
};

/**
 * Load roster to map players by jersey number
 */
async function loadRoster() {
  try {
    const rosterPath = path.join(ROOT_DIR, 'rosters', 'amherst-ramblers.json');
    const data = await fs.readFile(rosterPath, 'utf8');
    const roster = JSON.parse(data);

    // Create map: jersey_number -> player_id
    const playerMap = new Map();
    for (const player of roster.players) {
      if (player.number) {
        playerMap.set(String(player.number), player.player_id);
      }
    }

    return playerMap;
  } catch (e) {
    console.warn('[games] Could not load roster:', e.message);
    return new Map();
  }
}

/**
 * Fetch Amherst Ramblers schedule
 */
async function fetchRamblersSchedule() {
  const url = new URL(HOCKEYTECH_BASE_URL);
  url.searchParams.set('feed', 'modulekit');
  url.searchParams.set('view', 'schedule');
  url.searchParams.set('team_id', AMHERST_TEAM_ID);
  url.searchParams.set('season_id', SEASON_ID);
  url.searchParams.set('key', HOCKEYTECH_API_KEY);
  url.searchParams.set('fmt', 'json');
  url.searchParams.set('client_code', HOCKEYTECH_CLIENT);

  console.log(`[games] Fetching Ramblers schedule...`);

  const response = await fetch(url.toString());
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }

  const data = await response.json();
  return data.SiteKit?.Schedule || [];
}

/**
 * Fetch detailed game summary (scoring, penalties, stats)
 */
async function fetchGameSummary(gameId) {
  const url = new URL(HOCKEYTECH_BASE_URL + 'index.php');
  url.searchParams.set('feed', 'statviewfeed');
  url.searchParams.set('view', 'gameSummary');
  url.searchParams.set('game_id', gameId);
  url.searchParams.set('key', HOCKEYTECH_API_KEY);
  url.searchParams.set('client_code', HOCKEYTECH_CLIENT);
  url.searchParams.set('fmt', 'json');

  const response = await fetch(url.toString());
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }

  let text = await response.text();

  // HockeyTech wraps response in parentheses - strip them
  if (text.startsWith('(') && text.endsWith(')')) {
    text = text.slice(1, -1);
  }

  return JSON.parse(text);
}

/**
 * Parse scoring plays from game summary
 */
function parseScoringPlays(gameSummary, playerMap, isHomeGame) {
  const scoringPlays = [];

  if (!gameSummary.periods) return scoringPlays;

  for (const period of gameSummary.periods) {
    if (!period.goals) continue;

    for (const goal of period.goals) {
      // Check if this goal was scored by Amherst
      const isRamblersGoal = isHomeGame ?
        goal.team === 'home' :
        goal.team === 'visiting';

      const scorer = goal.scoredBy || {};
      const scorerNumber = String(scorer.jerseyNumber || '');
      const scorerPlayerId = playerMap.get(scorerNumber);

      const assists = (goal.assists || []).map(assist => ({
        player_id: playerMap.get(String(assist.jerseyNumber || '')),
        name: `${assist.firstName || ''} ${assist.lastName || ''}`.trim(),
        number: assist.jerseyNumber
      }));

      scoringPlays.push({
        period: parseInt(period.info?.id || 0),
        period_name: period.info?.longName || '',
        time: goal.time || '',
        team: isRamblersGoal ? 'amherst-ramblers' : 'opponent',
        scorer: {
          player_id: scorerPlayerId,
          name: `${scorer.firstName || ''} ${scorer.lastName || ''}`.trim(),
          number: scorer.jerseyNumber,
          position: scorer.position
        },
        assists,
        power_play: goal.properties?.isPowerPlay === '1',
        short_handed: goal.properties?.isShortHanded === '1',
        game_winning: goal.properties?.isGameWinningGoal === '1',
        empty_net: goal.properties?.isEmptyNet === '1'
      });
    }
  }

  return scoringPlays;
}

/**
 * Parse penalties from game summary
 */
function parsePenalties(gameSummary, playerMap, isHomeGame) {
  const penalties = [];

  if (!gameSummary.periods) return penalties;

  for (const period of gameSummary.periods) {
    if (!period.penalties) continue;

    for (const penalty of period.penalties) {
      const isRamblersPenalty = isHomeGame ?
        penalty.againstTeam === 'home' :
        penalty.againstTeam === 'visiting';

      if (!isRamblersPenalty) continue; // Only track Ramblers penalties

      const player = penalty.takenBy || {};
      const playerNumber = String(player.jerseyNumber || '');
      const playerId = playerMap.get(playerNumber);

      penalties.push({
        period: parseInt(period.info?.id || 0),
        period_name: period.info?.longName || '',
        time: penalty.time || '',
        player: {
          player_id: playerId,
          name: `${player.firstName || ''} ${player.lastName || ''}`.trim(),
          number: player.jerseyNumber,
          position: player.position
        },
        infraction: penalty.description || '',
        duration: parseInt(penalty.minutes || 0),
        is_bench: penalty.isBench === true
      });
    }
  }

  return penalties;
}

/**
 * Parse player stats from game summary
 */
function parsePlayerStats(gameSummary, playerMap, isHomeGame) {
  const stats = {};

  const team = isHomeGame ? gameSummary.homeTeam : gameSummary.visitingTeam;
  if (!team) return stats;

  // Skater stats
  if (team.skaters) {
    for (const skater of team.skaters) {
      const number = String(skater.info?.jerseyNumber || '');
      const playerId = playerMap.get(number);

      if (!playerId) continue;

      stats[playerId] = {
        position: skater.info?.position || '',
        goals: parseInt(skater.stats?.goals || 0),
        assists: parseInt(skater.stats?.assists || 0),
        points: parseInt(skater.stats?.points || 0),
        penalty_minutes: parseInt(skater.stats?.penaltyMinutes || 0),
        shots: parseInt(skater.stats?.shots || 0),
        plus_minus: skater.stats?.plusMinus || '0',
        hits: parseInt(skater.stats?.hits || 0),
        blocked_shots: parseInt(skater.stats?.blockedShots || 0),
        faceoff_wins: parseInt(skater.stats?.faceoffWins || 0),
        faceoff_losses: parseInt(skater.stats?.faceoffLosses || 0)
      };
    }
  }

  // Goalie stats
  if (team.goalies) {
    for (const goalie of team.goalies) {
      const number = String(goalie.info?.jerseyNumber || '');
      const playerId = playerMap.get(number);

      if (!playerId) continue;

      stats[playerId] = {
        position: 'G',
        saves: parseInt(goalie.stats?.saves || 0),
        shots_against: parseInt(goalie.stats?.shotsAgainst || 0),
        goals_against: parseInt(goalie.stats?.goalsAgainst || 0),
        save_percentage: goalie.stats?.savePct || '0.000',
        time_on_ice: goalie.stats?.timeOnIce || '00:00'
      };
    }
  }

  return stats;
}

/**
 * Process games and fetch detailed summaries
 */
async function processGames(schedule, playerMap) {
  const games = [];

  // Filter completed games
  const completedGames = schedule.filter(game =>
    game.final === '1' && game.status === '4'
  );

  console.log(`[games] Processing ${completedGames.length} completed games...`);

  for (const game of completedGames) {
    const isHomeGame = game.home_team === String(AMHERST_TEAM_ID);
    const ramblersScore = isHomeGame ? parseInt(game.home_goal_count) : parseInt(game.visiting_goal_count);
    const opponentScore = isHomeGame ? parseInt(game.visiting_goal_count) : parseInt(game.home_goal_count);

    const won = ramblersScore > opponentScore;
    const overtime = game.overtime === '1';
    const shootout = game.shootout === '1';

    // Fetch detailed game summary
    let gameSummary = null;
    let scoring = [];
    let penalties = [];
    let player_stats = {};

    try {
      gameSummary = await fetchGameSummary(game.game_id);
      scoring = parseScoringPlays(gameSummary, playerMap, isHomeGame);
      penalties = parsePenalties(gameSummary, playerMap, isHomeGame);
      player_stats = parsePlayerStats(gameSummary, playerMap, isHomeGame);
      console.log(`[games] Fetched summary for game ${game.game_id} (${scoring.length} goals, ${penalties.length} penalties)`);
    } catch (e) {
      console.warn(`[games] Could not fetch summary for game ${game.game_id}:`, e.message);
    }

    const gameData = {
      game_id: game.game_id,
      date: game.date_played,
      date_time: game.GameDateISO8601,
      opponent: {
        team_id: isHomeGame ? parseInt(game.visiting_team) : parseInt(game.home_team),
        team_name: isHomeGame ? game.visiting_team_name : game.home_team_name,
        team_code: isHomeGame ? game.visiting_team_code : game.home_team_code
      },
      home_game: isHomeGame,
      venue: game.venue_name,
      result: {
        won,
        ramblers_score: ramblersScore,
        opponent_score: opponentScore,
        overtime,
        shootout,
        final_score: `${ramblersScore}-${opponentScore}${overtime ? ' (OT)' : ''}${shootout ? ' (SO)' : ''}`
      },
      attendance: game.attendance ? parseInt(game.attendance) : null,
      scoring,
      penalties,
      player_stats
    };

    games.push(gameData);

    // Small delay to avoid hammering API
    await new Promise(resolve => setTimeout(resolve, 200));
  }

  // Sort by date (most recent first)
  games.sort((a, b) => new Date(b.date) - new Date(a.date));

  return games;
}

/**
 * Calculate season summary stats
 */
function calculateSeasonSummary(games) {
  const summary = {
    games_played: games.length,
    wins: 0,
    losses: 0,
    ot_losses: 0,
    shootout_losses: 0,
    points: 0,
    goals_for: 0,
    goals_against: 0,
    home_record: { wins: 0, losses: 0, ot_losses: 0 },
    away_record: { wins: 0, losses: 0, ot_losses: 0 }
  };

  for (const game of games) {
    const { won, ramblers_score, opponent_score, overtime, shootout } = game.result;
    const { home_game } = game;

    summary.goals_for += ramblers_score;
    summary.goals_against += opponent_score;

    if (won) {
      summary.wins++;
      summary.points += 2;
      if (home_game) summary.home_record.wins++;
      else summary.away_record.wins++;
    } else {
      if (overtime || shootout) {
        summary.ot_losses++;
        if (shootout) summary.shootout_losses++;
        summary.points += 1;
        if (home_game) summary.home_record.ot_losses++;
        else summary.away_record.ot_losses++;
      } else {
        summary.losses++;
        if (home_game) summary.home_record.losses++;
        else summary.away_record.losses++;
      }
    }
  }

  summary.goal_differential = summary.goals_for - summary.goals_against;
  summary.record = `${summary.wins}-${summary.losses}-${summary.ot_losses}`;

  return summary;
}

/**
 * Build Ramblers games
 */
export async function buildRamblersGames() {
  console.log('[games] Starting Ramblers games build...');

  const gamesDir = path.join(ROOT_DIR, 'games');
  await ensureDir(gamesDir);

  try {
    const playerMap = await loadRoster();
    console.log(`[games] Loaded roster with ${playerMap.size} players`);

    const schedule = await fetchRamblersSchedule();
    const games = await processGames(schedule, playerMap);
    const summary = calculateSeasonSummary(games);

    const output = {
      team_slug: 'amherst-ramblers',
      team_name: 'Amherst Ramblers',
      team_id: AMHERST_TEAM_ID,
      season: '2024-25',
      season_id: SEASON_ID,
      updated_at: nowISO(),
      summary,
      games
    };

    // Write games file
    const outputPath = path.join(gamesDir, 'amherst-ramblers.json');
    await fs.writeFile(outputPath, JSON.stringify(output, null, 2));
    console.log(`[games] Wrote games/amherst-ramblers.json (${games.length} completed games)`);

    return output;
  } catch (e) {
    console.error('[games] Error:', e.message);
    throw e;
  }
}

// ------------- CLI entry (optional local run) -------------
if (process.argv[1] && process.argv[1].endsWith('games.mjs')) {
  buildRamblersGames()
    .then(() => {
      console.log('[games] Complete!');
      process.exit(0);
    })
    .catch(e => {
      console.error('[games] Fatal error:', e);
      process.exit(1);
    });
}
