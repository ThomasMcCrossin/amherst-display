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
const ROOT_DIR = path.resolve(__dirname, '..');

// HockeyTech API configuration
const HOCKEYTECH_API_KEY = '4a948e7faf5ee58d';
const HOCKEYTECH_CLIENT = 'mhl';
const HOCKEYTECH_BASE_URL = 'https://lscluster.hockeytech.com/feed/';
const SEASON_ID = 41; // 2024-25 season

// Team configuration: { slug: { name, team_id } }
const TEAMS = {
  'amherst-ramblers': { name: 'Amherst-Ramblers', team_id: 1, league: 'MHL' },
  // Uncomment to add more teams:
  // 'truro-bearcats': { name: 'Truro-Bearcats', team_id: 3, league: 'MHL' },
  // 'valley-wildcats': { name: 'Valley-Wildcats', team_id: 4, league: 'MHL' },
  // etc.
};

const nowISO = () => new Date().toISOString();

const ensureDir = async (fp) => {
  await fs.mkdir(fp, { recursive: true });
};

/**
 * Fetch roster data from HockeyTech API
 */
async function fetchRosterFromAPI(teamId) {
  const url = new URL(HOCKEYTECH_BASE_URL);
  url.searchParams.set('feed', 'modulekit');
  url.searchParams.set('view', 'roster');
  url.searchParams.set('team_id', teamId);
  url.searchParams.set('season_id', SEASON_ID);
  url.searchParams.set('key', HOCKEYTECH_API_KEY);
  url.searchParams.set('fmt', 'json');
  url.searchParams.set('client_code', HOCKEYTECH_CLIENT);

  console.log(`[rosters] Fetching from API: team_id=${teamId}`);

  const response = await fetch(url.toString());
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }

  const data = await response.json();
  return data.SiteKit?.Roster || [];
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

    const response = await fetch(url);
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
 */
async function processRoster(teamSlug, teamName, rawPlayers) {
  const players = [];
  const headshotDir = path.join(ROOT_DIR, 'assets', 'headshots', teamName);
  await ensureDir(headshotDir);

  console.log(`[rosters/${teamSlug}] Processing ${rawPlayers.length} players`);

  for (const rawPlayer of rawPlayers) {
    const playerId = generatePlayerId(rawPlayer);
    const jerseyNum = rawPlayer.tp_jersey_number || rawPlayer.jersey || '';
    const playerName = rawPlayer.name || `${rawPlayer.first_name || ''} ${rawPlayer.last_name || ''}`.trim() || 'Unknown Player';
    const fileId = `${jerseyNum}-${normalizeNameForId(playerName)}`;

    // Download headshot if available
    let headshotPath = null;
    if (rawPlayer.player_image) {
      const ext = rawPlayer.player_image.match(/\.(jpg|jpeg|png|gif)(\?|$)/i)?.[1] || 'jpg';
      const filename = `${fileId}.${ext}`;
      const localPath = path.join(headshotDir, filename);
      const success = await downloadImage(rawPlayer.player_image, localPath);
      if (success) {
        headshotPath = `assets/headshots/${teamName}/${filename}`;
      }
    }

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
  console.log('[rosters] Starting roster build...');

  const rosters = {};

  for (const [teamSlug, config] of Object.entries(TEAMS)) {
    try {
      console.log(`[rosters/${teamSlug}] Fetching roster for ${config.name}...`);
      const rawPlayers = await fetchRosterFromAPI(config.team_id);
      const players = await processRoster(teamSlug, config.name, rawPlayers);

      rosters[teamSlug] = {
        team_slug: teamSlug,
        team_name: config.name,
        league: config.league,
        season: '2024-25',
        season_id: SEASON_ID,
        team_id: config.team_id,
        updated_at: nowISO(),
        player_count: players.length,
        players
      };

      console.log(`[rosters/${teamSlug}] Processed ${players.length} players`);
    } catch (e) {
      console.error(`[rosters/${teamSlug}] Error:`, e.message);
      rosters[teamSlug] = {
        team_slug: teamSlug,
        team_name: config.name,
        league: config.league,
        season: '2024-25',
        season_id: SEASON_ID,
        team_id: config.team_id,
        updated_at: nowISO(),
        player_count: 0,
        players: [],
        error: e.message
      };
    }
  }

  // Write rosters.json
  const outputPath = path.join(ROOT_DIR, 'rosters.json');
  const output = {
    generated_at: nowISO(),
    season: '2024-25',
    season_id: SEASON_ID,
    api_source: 'HockeyTech',
    teams: rosters
  };

  await fs.writeFile(outputPath, JSON.stringify(output, null, 2));
  console.log(`[rosters] Wrote rosters.json with ${Object.keys(rosters).length} team(s)`);

  return output;
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
