/**
 * Build rosters JSON with player details and headshots.
 * - Uses Puppeteer to scrape MHL roster pages (JS-rendered)
 * - Downloads player headshots
 * - Supports multiple teams (focused on Amherst Ramblers)
 *
 * Outputs:
 *   rosters.json - Player roster data for all teams
 *   assets/headshots/{team-slug}/{player-id}.jpg - Player headshots
 *
 * Requires in package.json: "puppeteer"
 * Workflow must install Chrome: npx puppeteer browsers install chrome
 */

import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import puppeteer from 'puppeteer';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(__dirname, '..');

// MHL roster URLs - format: /stats/roster/{season_id}/{team_id}
const ROSTER_URLS = {
  'amherst-ramblers': 'https://www.themhl.ca/stats/roster/1/41'
  // Can add more teams later:
  // 'truro-bearcats': 'https://www.themhl.ca/stats/roster/1/42',
  // etc.
};

const nowISO = () => new Date().toISOString();

const ensureDir = async (fp) => {
  await fs.mkdir(fp, { recursive: true });
};

/**
 * Download image from URL and save to local path
 */
async function downloadImage(url, outputPath) {
  try {
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
 * Normalize player name to create a safe file ID
 */
function normalizePlayerId(name, number) {
  const normalized = name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return `${number}-${normalized}`;
}

/**
 * Parse height string (e.g., "6'2\"", "5'11\"") to inches
 */
function parseHeight(heightStr) {
  if (!heightStr) return null;
  const match = heightStr.match(/(\d+)'(\d+)"/);
  if (!match) return null;
  const feet = parseInt(match[1]);
  const inches = parseInt(match[2]);
  return feet * 12 + inches;
}

/**
 * Parse weight string (e.g., "185 lbs") to number
 */
function parseWeight(weightStr) {
  if (!weightStr) return null;
  const match = weightStr.match(/(\d+)/);
  return match ? parseInt(match[1]) : null;
}

/**
 * Scrape roster for a single team using Puppeteer
 */
async function scrapeTeamRoster(browser, teamSlug, url) {
  console.log(`[rosters/${teamSlug}] Fetching ${url}`);

  const page = await browser.newPage();
  await page.setViewport({ width: 1600, height: 1200, deviceScaleFactor: 1 });

  try {
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });

    // Wait for roster table to load
    try {
      await page.waitForSelector('table, .roster-table, .player-row', { timeout: 10000 });
      console.log(`[rosters/${teamSlug}] Page loaded, extracting data...`);
    } catch (e) {
      console.warn(`[rosters/${teamSlug}] Timeout waiting for roster table`);
    }

    // Give extra time for JS rendering
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Extract roster data from the page
    const rosterData = await page.evaluate(() => {
      const players = [];

      // Try to find roster table
      const tables = Array.from(document.querySelectorAll('table'));
      let rosterTable = null;

      // Find table with player data (headers like Number, Name, Position)
      for (const table of tables) {
        const headerText = table.querySelector('thead, tr')?.textContent || '';
        if (/number|name|position|player/i.test(headerText)) {
          rosterTable = table;
          break;
        }
      }

      if (!rosterTable && tables.length > 0) {
        // Fallback to largest table
        rosterTable = tables.sort((a, b) => {
          const ra = a.getBoundingClientRect(), rb = b.getBoundingClientRect();
          return (rb.width * rb.height) - (ra.width * ra.height);
        })[0];
      }

      if (!rosterTable) {
        return { players: [], error: 'No roster table found' };
      }

      // Extract headers
      const headerRow = rosterTable.querySelector('thead tr, tr:first-child');
      const headers = Array.from(headerRow?.querySelectorAll('th, td') || [])
        .map(cell => cell.textContent.trim().toLowerCase());

      // Map header indices
      const getIndex = (keywords) => {
        for (const keyword of keywords) {
          const idx = headers.findIndex(h => h.includes(keyword));
          if (idx >= 0) return idx;
        }
        return -1;
      };

      const indices = {
        number: getIndex(['#', 'number', 'no', 'num']),
        name: getIndex(['name', 'player']),
        position: getIndex(['position', 'pos']),
        height: getIndex(['height', 'ht']),
        weight: getIndex(['weight', 'wt']),
        shoots: getIndex(['shoots', 'shot', 's/c']),
        birthdate: getIndex(['birthdate', 'dob', 'born']),
        hometown: getIndex(['hometown', 'home town', 'city']),
        // Look for image column
        image: getIndex(['photo', 'image', 'picture', 'headshot'])
      };

      // Extract player rows
      const rows = Array.from(rosterTable.querySelectorAll('tbody tr, tr'));

      for (const row of rows) {
        const cells = Array.from(row.querySelectorAll('td'));
        if (cells.length < 2) continue; // Skip header/empty rows

        const getText = (idx) => idx >= 0 && cells[idx] ? cells[idx].textContent.trim() : '';

        const number = getText(indices.number);
        const name = getText(indices.name);

        // Skip if no name or it's a header row
        if (!name || /^name$/i.test(name) || /^player$/i.test(name)) continue;

        // Look for image in the row
        let imageUrl = null;
        const imgElement = row.querySelector('img');
        if (imgElement) {
          imageUrl = imgElement.src || imgElement.getAttribute('data-src');
          // Convert relative URLs to absolute
          if (imageUrl && imageUrl.startsWith('/')) {
            imageUrl = new URL(imageUrl, window.location.origin).href;
          }
        }

        const player = {
          number,
          name,
          position: getText(indices.position),
          height: getText(indices.height),
          weight: getText(indices.weight),
          shoots: getText(indices.shoots),
          birthdate: getText(indices.birthdate),
          hometown: getText(indices.hometown),
          image_url: imageUrl
        };

        // Clean up empty fields
        Object.keys(player).forEach(key => {
          if (player[key] === '' || player[key] === '-' || player[key] === 'N/A') {
            player[key] = null;
          }
        });

        players.push(player);
      }

      return {
        players,
        tableInfo: {
          headers,
          rowCount: rows.length,
          playerCount: players.length
        }
      };
    });

    console.log(`[rosters/${teamSlug}] Found ${rosterData.players?.length || 0} players`);

    if (rosterData.error) {
      console.warn(`[rosters/${teamSlug}] ${rosterData.error}`);
    }

    return rosterData.players || [];

  } catch (e) {
    console.error(`[rosters/${teamSlug}] Error:`, e.message);
    return [];
  } finally {
    await page.close();
  }
}

/**
 * Process roster: normalize data and download headshots
 */
async function processRoster(teamSlug, rawPlayers) {
  const players = [];
  const headshotDir = path.join(ROOT_DIR, 'assets', 'headshots', teamSlug);
  await ensureDir(headshotDir);

  for (const rawPlayer of rawPlayers) {
    const playerId = normalizePlayerId(rawPlayer.name, rawPlayer.number);

    // Download headshot if available
    let headshotPath = null;
    if (rawPlayer.image_url) {
      const ext = rawPlayer.image_url.match(/\.(jpg|jpeg|png|gif)(\?|$)/i)?.[1] || 'jpg';
      const filename = `${playerId}.${ext}`;
      const localPath = path.join(headshotDir, filename);
      const success = await downloadImage(rawPlayer.image_url, localPath);
      if (success) {
        headshotPath = `assets/headshots/${teamSlug}/${filename}`;
      }
    }

    // Create normalized player object
    const player = {
      id: playerId,
      number: rawPlayer.number,
      name: rawPlayer.name,
      position: rawPlayer.position,
      height: rawPlayer.height,
      height_inches: parseHeight(rawPlayer.height),
      weight: rawPlayer.weight,
      weight_lbs: parseWeight(rawPlayer.weight),
      shoots: rawPlayer.shoots,
      birthdate: rawPlayer.birthdate,
      hometown: rawPlayer.hometown,
      headshot: headshotPath,
      headshot_url: rawPlayer.image_url
    };

    // Remove null values for cleaner JSON
    Object.keys(player).forEach(key => {
      if (player[key] === null) delete player[key];
    });

    players.push(player);
  }

  return players;
}

/**
 * Build all rosters
 */
export async function buildRosters() {
  console.log('[rosters] Starting roster build...');

  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  const rosters = {};

  try {
    for (const [teamSlug, url] of Object.entries(ROSTER_URLS)) {
      const rawPlayers = await scrapeTeamRoster(browser, teamSlug, url);
      const players = await processRoster(teamSlug, rawPlayers);

      rosters[teamSlug] = {
        team_slug: teamSlug,
        team_name: teamSlug.split('-').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' '),
        league: 'MHL',
        season: '2024-25',
        updated_at: nowISO(),
        player_count: players.length,
        players
      };

      console.log(`[rosters/${teamSlug}] Processed ${players.length} players`);
    }
  } finally {
    await browser.close();
  }

  // Write rosters.json
  const outputPath = path.join(ROOT_DIR, 'rosters.json');
  const output = {
    generated_at: nowISO(),
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
