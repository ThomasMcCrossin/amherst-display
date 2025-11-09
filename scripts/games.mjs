/**
 * Build Amherst Ramblers game summaries
 * - Fetches completed games from schedule
 * - Stores game results with basic info
 * - Linked to player IDs for future stat expansion
 *
 * Outputs:
 *   games/amherst-ramblers.json - All Ramblers games this season
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
 * Process games and create summaries
 */
function processGames(schedule) {
  const games = [];

  for (const game of schedule) {
    // Only include completed games (status 4 = Final)
    if (game.final !== '1' || game.status !== '4') {
      continue;
    }

    const isHomeGame = game.home_team === String(AMHERST_TEAM_ID);
    const ramblersScore = isHomeGame ? parseInt(game.home_goal_count) : parseInt(game.visiting_goal_count);
    const opponentScore = isHomeGame ? parseInt(game.visiting_goal_count) : parseInt(game.home_goal_count);

    const won = ramblersScore > opponentScore;
    const overtime = game.overtime === '1';
    const shootout = game.shootout === '1';

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
      // Placeholder for future player stats
      player_stats: {
        // Will be populated later with individual player stats by player_id
        // Format: { player_id: { goals, assists, points, ... } }
      }
    };

    games.push(gameData);
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
    const schedule = await fetchRamblersSchedule();
    const games = processGames(schedule);
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
