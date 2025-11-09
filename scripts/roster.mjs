/**
 * Build roster JSON with player data and headshots.
 * - MHL: Headless Chrome (HockeyTech widget) → parse player table → download headshots
 *
 * Outputs:
 *   roster.json
 *   assets/headshots/[team-slug]/[player-id].jpg
 *
 * Requires in package.json: "puppeteer"
 * Workflow must install Chrome: npx puppeteer browsers install chrome
 */

import fs from 'fs/promises';
import { existsSync } from 'fs';
import path from 'path';
import { createWriteStream } from 'fs';
import https from 'https';
import http from 'http';
import puppeteer from 'puppeteer';
import crypto from 'crypto';

const MHL_ROSTER_BASE = 'https://www.themhl.ca/stats/roster';
const AMHERST_RAMBLERS_TEAM_ID = 1;
const SEASON_ID = 41; // 2024-25 season

const nowISO = () => new Date().toISOString();

const ensureDir = async (fp) => {
  const dir = path.dirname(fp);
  await fs.mkdir(dir, { recursive: true });
};

/**
 * Generate a unique player ID from player data
 * Uses name + number to create a stable, unique identifier
 */
function generatePlayerId(playerName, jerseyNumber, teamSlug) {
  const normalized = `${teamSlug}-${playerName}-${jerseyNumber}`.toLowerCase().replace(/[^a-z0-9-]/g, '-');
  const hash = crypto.createHash('md5').update(normalized).digest('hex').substring(0, 8);
  return `${teamSlug}-${hash}`;
}

/**
 * Download image from URL to local file
 */
async function downloadImage(url, filepath) {
  await ensureDir(filepath);

  return new Promise((resolve, reject) => {
    const protocol = url.startsWith('https') ? https : http;
    const file = createWriteStream(filepath);

    protocol.get(url, (response) => {
      if (response.statusCode === 200) {
        response.pipe(file);
        file.on('finish', () => {
          file.close();
          resolve(filepath);
        });
      } else {
        file.close();
        fs.unlink(filepath).catch(() => {});
        reject(new Error(`Failed to download: ${response.statusCode}`));
      }
    }).on('error', (err) => {
      file.close();
      fs.unlink(filepath).catch(() => {});
      reject(err);
    });
  });
}

/**
 * Scrape Amherst Ramblers roster from MHL website
 */
export async function scrapeAmherstRamblersRoster() {
  const teamSlug = 'amherst-ramblers';
  const teamName = 'Amherst Ramblers';
  const league = 'MHL';
  const season = '2024-25';

  console.log('[roster/MHL] Starting Amherst Ramblers roster scrape...');

  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1600, height: 1200, deviceScaleFactor: 1 });

    const rosterUrl = `${MHL_ROSTER_BASE}/${AMHERST_RAMBLERS_TEAM_ID}/${SEASON_ID}`;
    console.log(`[roster/MHL] Navigating to ${rosterUrl}`);

    await page.goto(rosterUrl, { waitUntil: 'networkidle2', timeout: 60000 });

    // Wait for HockeyTech widget table to load
    try {
      await page.waitForSelector('.hockeytech_widget table, .ht-table, table', { timeout: 15000 });
      console.log('[roster/MHL] Table found, waiting for data to load...');
      await new Promise(resolve => setTimeout(resolve, 2000)); // Extra time for dynamic content
    } catch (e) {
      console.warn('[roster/MHL] Table selector timeout, trying anyway');
    }

    // Extract player data from the page
    const players = await page.evaluate(() => {
      const results = [];

      // Try to find the roster table - look for common patterns
      const tables = Array.from(document.querySelectorAll('table'));
      let rosterTable = null;

      // Find table with player data (look for headers like "Name", "Position", "#", etc.)
      for (const table of tables) {
        const headerText = table.textContent.toLowerCase();
        if (headerText.includes('name') && (headerText.includes('position') || headerText.includes('pos'))) {
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
        console.warn('No roster table found');
        return [];
      }

      // Extract headers to map column positions
      const headerRow = rosterTable.querySelector('thead tr, tr');
      const headers = headerRow ? Array.from(headerRow.querySelectorAll('th, td')).map(cell =>
        cell.textContent.trim().toLowerCase()
      ) : [];

      console.log('Headers found:', headers);

      // Find column indices
      const colIndex = (terms) => {
        for (const term of terms) {
          const idx = headers.findIndex(h => h.includes(term));
          if (idx >= 0) return idx;
        }
        return -1;
      };

      const numberCol = colIndex(['#', 'no', 'num', 'jersey']);
      const nameCol = colIndex(['name', 'player']);
      const posCol = colIndex(['pos', 'position']);
      const heightCol = colIndex(['ht', 'height']);
      const weightCol = colIndex(['wt', 'weight', 'lbs']);
      const hometownCol = colIndex(['hometown', 'home town', 'city']);
      const birthCol = colIndex(['dob', 'birth', 'born']);

      // Extract rows
      const rows = rosterTable.querySelectorAll('tbody tr, tr');

      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];

        // Skip header rows
        if (row.querySelector('th')) continue;

        const cells = Array.from(row.querySelectorAll('td'));
        if (cells.length < 2) continue;

        const number = numberCol >= 0 ? cells[numberCol]?.textContent.trim() : '';
        const name = nameCol >= 0 ? cells[nameCol]?.textContent.trim() : cells[0]?.textContent.trim();
        const position = posCol >= 0 ? cells[posCol]?.textContent.trim() : '';
        const height = heightCol >= 0 ? cells[heightCol]?.textContent.trim() : '';
        const weight = weightCol >= 0 ? cells[weightCol]?.textContent.trim() : '';
        const hometown = hometownCol >= 0 ? cells[hometownCol]?.textContent.trim() : '';
        const birthdate = birthCol >= 0 ? cells[birthCol]?.textContent.trim() : '';

        // Look for headshot image in the row
        const img = row.querySelector('img');
        const headshotUrl = img?.src || img?.getAttribute('data-src') || '';

        // Look for player profile link to get potential player ID
        const link = row.querySelector('a[href*="/player/"], a[href*="player"]');
        const profileUrl = link?.href || '';

        // Extract player ID from URL if available
        const playerIdMatch = profileUrl.match(/player[\/=](\d+)/i);
        const externalPlayerId = playerIdMatch ? playerIdMatch[1] : '';

        if (name && name.length > 1) {
          results.push({
            name,
            number,
            position,
            height,
            weight,
            hometown,
            birthdate,
            headshotUrl,
            profileUrl,
            externalPlayerId
          });
        }
      }

      return results;
    });

    console.log(`[roster/MHL] Found ${players.length} players`);

    // Process players: generate IDs and download headshots
    const processedPlayers = [];
    const headshotsDir = `assets/headshots/${teamSlug}`;
    await ensureDir(`${headshotsDir}/.gitkeep`);

    for (const player of players) {
      const playerId = generatePlayerId(player.name, player.number, teamSlug);

      // Download headshot if available
      let headshotPath = '';
      if (player.headshotUrl && player.headshotUrl.startsWith('http')) {
        try {
          const ext = player.headshotUrl.includes('.png') ? 'png' : 'jpg';
          headshotPath = `${headshotsDir}/${playerId}.${ext}`;
          await downloadImage(player.headshotUrl, headshotPath);
          console.log(`[roster/MHL] Downloaded headshot for ${player.name} (${playerId})`);
          headshotPath = headshotPath.replace(/^assets\//, ''); // Store relative path without 'assets/'
        } catch (err) {
          console.warn(`[roster/MHL] Failed to download headshot for ${player.name}: ${err.message}`);
        }
      }

      processedPlayers.push({
        player_id: playerId,
        name: player.name,
        number: player.number || '',
        position: player.position || '',
        height: player.height || '',
        weight: player.weight || '',
        hometown: player.hometown || '',
        birthdate: player.birthdate || '',
        headshot: headshotPath || '',
        external_player_id: player.externalPlayerId || ''
      });
    }

    await browser.close();

    return {
      team_slug: teamSlug,
      team_name: teamName,
      league,
      season,
      updated_at: nowISO(),
      player_count: processedPlayers.length,
      players: processedPlayers
    };

  } catch (err) {
    await browser.close();
    throw err;
  }
}

/**
 * Build complete roster JSON for all teams
 */
export async function buildRosterData() {
  console.log('[roster] Building roster data...');

  const teams = {};

  try {
    const amherstData = await scrapeAmherstRamblersRoster();
    teams['amherst-ramblers'] = amherstData;
  } catch (err) {
    console.error('[roster] Failed to scrape Amherst Ramblers:', err.message);
    // Provide fallback empty roster
    teams['amherst-ramblers'] = {
      team_slug: 'amherst-ramblers',
      team_name: 'Amherst Ramblers',
      league: 'MHL',
      season: '2024-25',
      updated_at: nowISO(),
      player_count: 0,
      players: []
    };
  }

  return {
    generated_at: nowISO(),
    teams
  };
}

// Allow running standalone for testing
if (import.meta.url === `file://${process.argv[1]}`) {
  buildRosterData()
    .then(data => {
      const output = JSON.stringify(data, null, 2);
      console.log('\n--- ROSTER DATA ---');
      console.log(output);
      return fs.writeFile('roster.json', output);
    })
    .then(() => console.log('\n✓ roster.json written'))
    .catch(err => {
      console.error('Error:', err);
      process.exit(1);
    });
}
