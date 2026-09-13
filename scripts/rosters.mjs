/**
 * Build rosters JSON with player details and headshots.
 * - Uses HockeyTech API to fetch roster data directly (no scraping needed!)
 * - Downloads player headshots
 * - Supports multiple MHL teams
 *
 * Outputs:
 *   rosters.json - Player roster data for all teams
 *   assets/headshots/{Team-Name}/{player-id}.jpg - Player headshots
 */

import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = process.env.METADATA_OUTPUT_DIR || path.resolve(__dirname, '..');

import { config as sourceConfig, request, moduleRows } from './hockeytech.mjs';
const SEASON_ID = Number(sourceConfig.season_ids[0]);

// Stable roster consumer identities (different from display aliases for two teams).
const TEAMS = {
  'amherst-ramblers': { name: 'Amherst-Ramblers', team_id: 1, league: 'MHL' },
  'edmundston-blizzard': { name: 'Edmundston-Blizzard', team_id: 2, league: 'MHL' },
  'truro-bearcats': { name: 'Truro-Bearcats', team_id: 3, league: 'MHL' },
  'valley-wildcats': { name: 'Valley-Wildcats', team_id: 4, league: 'MHL' },
  'yarmouth-mariners': { name: 'Yarmouth-Mariners', team_id: 5, league: 'MHL' },
  'west-kent-steamers': { name: 'West-Kent-Steamers', team_id: 6, league: 'MHL' },
  'pictou-county-crushers': { name: 'Pictou-County-Crushers', team_id: 7, league: 'MHL' },
  'campbellton-tigers': { name: 'Campbellton-Tigers', team_id: 8, league: 'MHL' },
  'miramichi-timberwolves': { name: 'Miramichi-Timberwolves', team_id: 9, league: 'MHL' },
  'summerside-capitals': { name: 'Summerside-Capitals', team_id: 10, league: 'MHL' },
  'grand-falls-rapids': { name: 'Grand-Falls-Rapids', team_id: 12, league: 'MHL' },
  'chaleur-lightning': { name: 'Chaleur-Lightning', team_id: 21, league: 'MHL' }
};

const nowISO = () => new Date().toISOString();

const ensureDir = async (fp) => {
  await fs.mkdir(fp, { recursive: true });
};

/**
 * Fetch player stats from HockeyTech API
 */
async function fetchPlayerStats(teamId) {
  const params = { feed: 'modulekit', view: 'statviewtype', team_id: teamId, season_id: SEASON_ID };
  console.log(`[rosters] Fetching stats from API: team_id=${teamId}`);

  try {
    const [skatersData, goaliesData] = await Promise.all([
      request({ ...params, type: 'topscorers' }), request({ ...params, type: 'goalies' })
    ]);
    moduleRows(skatersData, 'Statviewtype');
    moduleRows(goaliesData, 'Statviewtype');

    const statsMap = new Map();

    // Process skater stats
    for (const player of (skatersData.SiteKit?.Statviewtype || [])) {
      statsMap.set(player.player_id, {
        games_played: parseInt(player.games_played) || 0,
        goals: parseInt(player.goals) || 0,
        assists: parseInt(player.assists) || 0,
        points: parseInt(player.points) || 0,
        plus_minus: parseInt(player.plus_minus) || 0,
        penalty_minutes: parseInt(player.penalty_minutes) || 0,
        power_play_goals: parseInt(player.power_play_goals) || 0,
        power_play_assists: parseInt(player.power_play_assists) || 0,
        power_play_points: parseInt(player.power_play_points) || 0,
        short_handed_goals: parseInt(player.short_handed_goals) || 0,
        short_handed_assists: parseInt(player.short_handed_assists) || 0,
        short_handed_points: parseInt(player.short_handed_points) || 0,
        game_winning_goals: parseInt(player.game_winning_goals) || 0,
        overtime_goals: parseInt(player.overtime_goals) || 0,
        points_per_game: parseFloat(player.points_per_game) || 0
      });
    }

    // Process goalie stats
    for (const player of (goaliesData.SiteKit?.Statviewtype || [])) {
      statsMap.set(player.player_id, {
        games_played: parseInt(player.games_played) || 0,
        wins: parseInt(player.wins) || 0,
        losses: parseInt(player.losses) || 0,
        ot_losses: parseInt(player.ot_losses) || 0,
        minutes_played: parseInt(player.minutes_played) || 0,
        saves: parseInt(player.saves) || 0,
        shots: parseInt(player.shots) || 0,
        goals_against: parseInt(player.goals_against) || 0,
        shutouts: parseInt(player.shutouts) || 0,
        save_percentage: parseFloat(player.save_percentage) || 0,
        goals_against_average: parseFloat(player.goals_against_average) || 0
      });
    }

    return statsMap;
  } catch (e) {
    throw e;
  }
}

/**
 * Fetch roster data from HockeyTech API
 */
export function rosterPlayers(rows) {
  const players = [];
  for (const record of rows) {
    if (Array.isArray(record)) {
      if (record.some(staff => !staff?.role_id || !staff.id)) throw new Error('Invalid roster staff section');
      continue; // HockeyTech appends a nested staff section; staff are not player records.
    }
    if (!record || !/^\d+$/.test(record.id) || !(record.name || record.first_name)) throw new Error('Invalid roster player');
    players.push(record);
  }
  if (!players.length) throw new Error('Empty required player roster');
  return players;
}

async function fetchRosterFromAPI(teamId) {
  const data = await request({ feed: 'modulekit', view: 'roster', team_id: teamId, season_id: SEASON_ID });
  return rosterPlayers(moduleRows(data, 'Roster'));
}

/**
 * Download image from URL and save to local path
 */
async function downloadImage(url, outputPath) {
  try {
    // Skip placeholder images
    if (url.includes('nophoto.png')) {
      console.log(`[rosters] Skipping placeholder image`);
      return false;
    }

    const response = await fetch(url, { signal: AbortSignal.timeout(30000) });
    if (!response.ok) {
      console.warn(`[rosters] Failed to download image: ${url} (${response.status})`);
      return false;
    }
    const buffer = await response.arrayBuffer();
    await ensureDir(path.dirname(outputPath));
    await fs.writeFile(outputPath, Buffer.from(buffer));
    console.log(`[rosters] Downloaded: ${path.basename(outputPath)}`);
    return true;
  } catch (e) {
    console.warn(`[rosters] Error downloading ${url}:`, e.message);
    return false;
  }
}

/**
 * Normalize player name to create a safe file/reference ID
 */
function normalizeNameForId(name) {
  if (!name) return 'unknown';
  return String(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

/**
 * Generate unique player ID
 * Format: mhl-{player_id} or use team_player combination for uniqueness
 */
function generatePlayerId(playerData) {
  // Use HockeyTech's player ID as the unique identifier
  return `mhl-${playerData.id}`;
}

/**
 * Parse height string (e.g., "6'03", "5'11") to inches
 */
function parseHeight(heightStr) {
  if (!heightStr) return null;
  const match = heightStr.match(/(\d+)'(\d+)/);
  if (!match) return null;
  const feet = parseInt(match[1]);
  const inches = parseInt(match[2]);
  return feet * 12 + inches;
}

/**
 * Parse weight string to number
 */
function parseWeight(weightStr) {
  if (!weightStr) return null;
  const num = parseInt(weightStr);
  return isNaN(num) ? null : num;
}

/**
 * Process roster: normalize data and download headshots
 * Note: Amherst Ramblers headshots are served directly from API (not downloaded)
 */
async function processRoster(teamSlug, teamName, rawPlayers, statsMap) {
  const players = [];
  // Never download headshots for Amherst Ramblers - serve from API to avoid storing player photos in GitHub
  const downloadHeadshots = teamSlug !== 'amherst-ramblers';

  if (downloadHeadshots) {
    const headshotDir = path.join(ROOT_DIR, 'assets', 'headshots', teamName);
    await ensureDir(headshotDir);
  }

  console.log(`[rosters/${teamSlug}] Processing ${rawPlayers.length} players${downloadHeadshots ? ' (downloading headshots)' : ' (using API URLs for headshots)'}`);

  for (const rawPlayer of rawPlayers) {
    const playerId = generatePlayerId(rawPlayer);
    const jerseyNum = rawPlayer.tp_jersey_number || rawPlayer.jersey || '';
    const playerName = rawPlayer.name || `${rawPlayer.first_name || ''} ${rawPlayer.last_name || ''}`.trim() || 'Unknown Player';
    const fileId = `${jerseyNum}-${normalizeNameForId(playerName)}`;

    // Download headshots for other teams, but use API URLs for Amherst Ramblers
    let headshotPath = null;
    if (rawPlayer.player_image && downloadHeadshots) {
      const ext = rawPlayer.player_image.match(/\.(jpg|jpeg|png|gif)(\?|$)/i)?.[1] || 'jpg';
      const filename = `${fileId}.${ext}`;
      const headshotDir = path.join(ROOT_DIR, 'assets', 'headshots', teamName);
      const localPath = path.join(headshotDir, filename);
      const success = await downloadImage(rawPlayer.player_image, localPath);
      const priorExists = await fs.access(localPath).then(() => true, () => false);
      if (success || priorExists) {
        headshotPath = `assets/headshots/${teamName}/${filename}`;
      }
    }

    // Get stats for this player
    const stats = statsMap.get(rawPlayer.id) || {};

    // Create normalized player object
    const player = {
      player_id: playerId, // Unique ID across all teams/seasons
      number: jerseyNum,
      first_name: rawPlayer.first_name,
      last_name: rawPlayer.last_name,
      name: playerName,
      position: rawPlayer.position,
      height: rawPlayer.height,
      height_inches: parseHeight(rawPlayer.height),
      weight: parseWeight(rawPlayer.weight),
      shoots: rawPlayer.shoots || rawPlayer.catches, // catches for goalies
      birthdate: rawPlayer.rawbirthdate || rawPlayer.birthdate,
      birth_year: rawPlayer.birthdate_year,
      age: rawPlayer.birthdate ? new Date().getFullYear() - new Date(rawPlayer.rawbirthdate).getFullYear() : null,
      hometown: rawPlayer.hometown || rawPlayer.homeplace,
      birthplace: rawPlayer.birthplace,
      rookie: rawPlayer.rookie === '1' || rawPlayer.isRookie === '*',
      veteran: rawPlayer.veteran_status === '1',
      headshot: headshotPath,
      headshot_url: rawPlayer.player_image?.includes('nophoto') ? null : rawPlayer.player_image,
      // Season stats
      stats,
      // Additional metadata
      hockeytech_id: rawPlayer.id,
      person_id: rawPlayer.person_id,
      flags: rawPlayer.flags || []
    };

    // Remove null/undefined values for cleaner JSON
    Object.keys(player).forEach(key => {
      if (player[key] === null || player[key] === undefined || player[key] === '') {
        delete player[key];
      }
    });

    players.push(player);
  }

  // Sort by jersey number
  players.sort((a, b) => {
    const numA = parseInt(a.number) || 999;
    const numB = parseInt(b.number) || 999;
    return numA - numB;
  });

  return players;
}

/**
 * Build all rosters
 */
export async function buildRosters() {
  console.log('[rosters] Starting roster build for all MHL teams...');
  const sourceTeams = moduleRows(await request({ feed: 'modulekit', view: 'teamsbyseason', season_id: SEASON_ID }), 'Teamsbyseason');
  const expectedIds = new Set(Object.values(TEAMS).map(team => String(team.team_id)));
  if (sourceTeams.length !== expectedIds.size || sourceTeams.some(team => !expectedIds.delete(String(team.id))) || expectedIds.size) {
    throw new Error('MHL roster consumer directory does not cover source season teams');
  }

  const rostersDir = path.join(ROOT_DIR, 'rosters');
  await ensureDir(rostersDir);

  const teamIndex = [];

  for (const [teamSlug, config] of Object.entries(TEAMS)) {
    try {
      console.log(`[rosters/${teamSlug}] Fetching roster for ${config.name}...`);
      const [rawPlayers, statsMap] = await Promise.all([
        fetchRosterFromAPI(config.team_id),
        fetchPlayerStats(config.team_id)
      ]);
      const players = await processRoster(teamSlug, config.name, rawPlayers, statsMap);

      const rosterData = {
        team_slug: teamSlug,
        team_name: config.name,
        league: config.league,
        season: sourceConfig.season_label,
        season_id: SEASON_ID,
        team_id: config.team_id,
        updated_at: nowISO(),
        player_count: players.length,
        players
      };

      // Write individual team roster file
      const teamFilePath = path.join(rostersDir, `${teamSlug}.json`);
      await fs.writeFile(teamFilePath, JSON.stringify(rosterData, null, 2));
      console.log(`[rosters/${teamSlug}] Wrote rosters/${teamSlug}.json (${players.length} players)`);

      // Add to index
      teamIndex.push({
        team_slug: teamSlug,
        team_name: config.name,
        league: config.league,
        team_id: config.team_id,
        player_count: players.length,
        file: `rosters/${teamSlug}.json`
      });

    } catch (e) {
      throw new Error('Required roster failed for ' + teamSlug + ': ' + e.message);
    }
  }

  // Write index file
  const indexPath = path.join(rostersDir, 'index.json');
  const indexData = {
    generated_at: nowISO(),
    season: sourceConfig.season_label,
    season_id: SEASON_ID,
    api_source: 'HockeyTech',
    league: 'MHL',
    team_count: teamIndex.length,
    teams: teamIndex
  };

  await fs.writeFile(indexPath, JSON.stringify(indexData, null, 2));
  console.log(`[rosters] Wrote rosters/index.json with ${teamIndex.length} team(s)`);

  return indexData;
}

// ------------- CLI entry (optional local run) -------------
if (process.argv[1] && process.argv[1].endsWith('rosters.mjs')) {
  buildRosters()
    .then(() => {
      console.log('[rosters] Complete!');
      process.exit(0);
    })
    .catch(e => {
      console.error('[rosters] Fatal error:', e);
      process.exit(1);
    });
}
