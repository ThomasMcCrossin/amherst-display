/**
 * Playwright-based box score scraper for Amherst Ramblers games
 * Scrapes additional data from themhl.ca that isn't available via API:
 * - Shots by period
 * - Power play summary
 * - Officials (referees, linesmen)
 * - Game timing (start, end, duration)
 * - Goaltender details
 * - Shootout attempts
 * - Three stars (when available)
 *
 * Outputs:
 *   games/amherst-ramblers-boxscores.json - Enhanced box score data
 */

import { chromium } from 'playwright';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(__dirname, '..');

const MHL_BASE_URL = 'https://www.themhl.ca/stats/game-summary/';
const AMHERST_TEAM_ID = 1;

const nowISO = () => new Date().toISOString();

/**
 * Parse shots by period from the shots table
 */
function parseShotsTable(tableData) {
  if (!tableData || tableData.length < 2) return null;

  const result = { ramblers: null, opponent: null };

  for (const row of tableData) {
    const teamName = row[0]?.toLowerCase() || '';
    const isAmherst = teamName.includes('amherst') || teamName.includes('ramblers');
    const target = isAmherst ? 'ramblers' : 'opponent';

    const shots = {
      period1: parseInt(row[1]) || 0,
      period2: parseInt(row[2]) || 0,
      period3: parseInt(row[3]) || 0,
      total: parseInt(row[4]) || 0
    };

    // Handle overtime if present (more than 5 columns)
    if (row.length > 5) {
      shots.overtime = parseInt(row[4]) || 0;
      shots.total = parseInt(row[5]) || 0;
    }

    result[target] = shots;
  }

  return result;
}

/**
 * Parse power play summary
 */
function parsePowerPlaySummary(tableData) {
  if (!tableData || tableData.length < 2) return null;

  const result = {};

  for (const row of tableData) {
    const teamName = row[0]?.toLowerCase() || '';
    const isAmherst = teamName.includes('amherst') || teamName.includes('ramblers');
    const target = isAmherst ? 'ramblers' : 'opponent';

    // Parse PP like "0 / 4" or "2/5"
    const ppText = row[1] || '';
    const ppMatch = ppText.match(/(\d+)\s*\/\s*(\d+)/);

    // Parse PIM like "10 min / 5 inf"
    const pimText = row[2] || '';
    const pimMatch = pimText.match(/(\d+)\s*min/i);
    const infMatch = pimText.match(/(\d+)\s*inf/i);

    result[target] = {
      power_play_goals: ppMatch ? parseInt(ppMatch[1]) : 0,
      power_play_opportunities: ppMatch ? parseInt(ppMatch[2]) : 0,
      penalty_minutes: pimMatch ? parseInt(pimMatch[1]) : 0,
      penalties: infMatch ? parseInt(infMatch[1]) : 0
    };
  }

  return result;
}

/**
 * Parse officials from the arena/officials table
 */
function parseOfficials(tableData) {
  if (!tableData || tableData.length < 1) return null;

  const row = tableData[0];
  const arenaCell = row[0] || '';
  const officialsCell = row[1] || '';

  const result = {
    arena: null,
    attendance: null,
    start_time: null,
    end_time: null,
    duration: null,
    referees: [],
    linesmen: []
  };

  // Parse arena info - extract just the venue name before Attendance
  const arenaMatch = arenaCell.match(/^([^A]+?)(?:\s*Attendance|$)/i);
  if (arenaMatch) {
    result.arena = arenaMatch[1].trim();
  } else {
    // Fallback - take first line
    const firstLine = arenaCell.split(/\n/)[0];
    result.arena = firstLine?.trim() || null;
  }

  const attendanceMatch = arenaCell.match(/Attendance:\s*(\d+)/i);
  if (attendanceMatch) result.attendance = parseInt(attendanceMatch[1]);

  const startMatch = arenaCell.match(/Start:\s*([\d:]+\s*[ap]m\s*\w*)/i);
  if (startMatch) result.start_time = startMatch[1].trim();

  const endMatch = arenaCell.match(/End:\s*([\d:]+\s*[ap]m\s*\w*)/i);
  if (endMatch) result.end_time = endMatch[1].trim();

  const lengthMatch = arenaCell.match(/Length:\s*([\d:]+)/i);
  if (lengthMatch) result.duration = lengthMatch[1].trim();

  // Parse officials
  const refMatches = officialsCell.matchAll(/Referee\s*\d*:\s*([^(]+)\s*\((\d+)\)/gi);
  for (const match of refMatches) {
    result.referees.push({
      name: match[1].trim(),
      number: parseInt(match[2])
    });
  }

  const linesmanMatches = officialsCell.matchAll(/Linesman\s*\d*:\s*([^(]+)\s*\((\d+)\)/gi);
  for (const match of linesmanMatches) {
    result.linesmen.push({
      name: match[1].trim(),
      number: parseInt(match[2])
    });
  }

  return result;
}

/**
 * Parse goaltender stats
 * Table structure: Each team has its own section with a header row followed by goalie rows
 * The thead header (first team) may be stripped, so data rows may appear before any "TOI" header
 * Row format: [Goalie Name (W/L) | time | saves/shots | on_time | off_time]
 * Section header: [TEAM_NAME | TOI | SV | On | Off]
 */
function parseGoaltenderStats(tableData, isHomeGame) {
  if (!tableData || tableData.length < 1) return null;

  const result = { ramblers: [], opponent: [] };

  // Assume first section is away team (which is at the top of the table)
  // Second section starts when we see a row with "TOI" in the second column
  let sectionIndex = 1; // Start at 1 (first section = away team)

  for (const row of tableData) {
    const firstCell = (row[0] || '').trim();
    const secondCell = (row[1] || '').trim();

    // Detect team header row (team name in all caps, "TOI" in second column)
    if (secondCell === 'TOI') {
      sectionIndex = 2; // Now in second section (home team)
      continue;
    }

    // Parse goalie data row - must have a name with time format in second column
    if (firstCell && secondCell && /^\d+:\d+$/.test(secondCell)) {
      const rawName = firstCell;
      const name = rawName.replace(/\s*\([WL]\)\s*/gi, '').trim();

      // Skip if this looks like a header or empty
      if (!name || (name.toUpperCase() === name && !name.includes(' '))) continue;

      const savesMatch = (row[2] || '').match(/(\d+)\/(\d+)/);

      const goalieData = {
        name,
        time_on_ice: secondCell || '',
        saves: savesMatch ? parseInt(savesMatch[1]) : 0,
        shots_against: savesMatch ? parseInt(savesMatch[2]) : 0,
        on_ice: row[3]?.trim() || '',
        off_ice: row[4]?.trim() || '',
        decision: rawName.includes('(W)') ? 'W' : rawName.includes('(L)') ? 'L' : null
      };

      // Section 1 = away team, Section 2 = home team
      // Determine target based on isHomeGame
      let target;
      if (sectionIndex === 1) {
        // First section is away team
        target = !isHomeGame ? 'ramblers' : 'opponent';
      } else {
        // Second section is home team
        target = isHomeGame ? 'ramblers' : 'opponent';
      }

      result[target].push(goalieData);
    }
  }

  return result;
}

/**
 * Parse shootout attempts
 */
function parseShootout(tableData, isHomeGame) {
  if (!tableData || tableData.length < 1) return null;

  const result = { ramblers: [], opponent: [] };

  for (const row of tableData) {
    if (!row[1]) continue; // Skip empty rows

    const attempt = {
      shooter_number: parseInt(row[0]) || 0,
      shooter_name: row[1]?.trim() || '',
      goalie_number: parseInt(row[2]) || 0,
      goalie_name: row[3]?.trim() || '',
      period: row[4]?.trim() || '',
      time: row[5]?.trim() || '',
      goal: (row[6] || '').toLowerCase().includes('yes') || row[6] === '✓'
    };

    // Determine which team based on context (would need more info)
    // For now, store all attempts
    result.attempts = result.attempts || [];
    result.attempts.push(attempt);
  }

  return result.attempts?.length > 0 ? result : null;
}

/**
 * Parse three stars if available
 */
function parseThreeStars(pageContent) {
  const stars = [];

  // Look for star patterns in text
  const starPatterns = [
    /1st\s*Star[:\s]*([^2]+)/i,
    /2nd\s*Star[:\s]*([^3]+)/i,
    /3rd\s*Star[:\s]*(.+)/i
  ];

  for (let i = 0; i < starPatterns.length; i++) {
    const match = pageContent.match(starPatterns[i]);
    if (match) {
      stars.push({
        position: i + 1,
        player: match[1].trim().substring(0, 50)
      });
    }
  }

  return stars.length > 0 ? stars : null;
}

/**
 * Scrape box score data for a single game
 */
async function scrapeGameBoxScore(page, gameId, isHomeGame) {
  const url = MHL_BASE_URL + gameId;

  try {
    await page.goto(url, { waitUntil: 'networkidle', timeout: 45000 });
    await page.waitForTimeout(1500);

    const boxScore = await page.evaluate(() => {
      const result = {
        tables: {}
      };

      // Helper to extract table data
      const extractTable = (table) => {
        const rows = [];
        table.querySelectorAll('tbody tr, tr').forEach((tr, idx) => {
          if (idx === 0 && tr.querySelector('th')) return; // Skip header row
          const cells = [];
          tr.querySelectorAll('td, th').forEach(cell => {
            cells.push(cell.textContent.trim().replace(/\s+/g, ' '));
          });
          if (cells.some(c => c)) rows.push(cells);
        });
        return rows;
      };

      // Get all tables
      const tables = document.querySelectorAll('table');

      tables.forEach((table, idx) => {
        const headerRow = table.querySelector('thead tr, tr:first-child');
        const headers = headerRow ?
          Array.from(headerRow.querySelectorAll('th, td')).map(c => c.textContent.trim().toLowerCase()) : [];

        const headerStr = headers.join('|');

        // Identify table type by headers
        if (headerStr.includes('shots') && headerStr.includes('1') && headerStr.includes('total')) {
          result.tables.shots = extractTable(table);
        } else if (headerStr.includes('pp') && headerStr.includes('pim')) {
          result.tables.powerplay = extractTable(table);
        } else if (headerStr.includes('arena') && headerStr.includes('officials')) {
          result.tables.officials = extractTable(table);
        } else if (headerStr.includes('toi') && headerStr.includes('sv')) {
          result.tables.goalies = extractTable(table);
        } else if (headerStr.includes('shooter') && headerStr.includes('goaltender')) {
          result.tables.shootout = result.tables.shootout || [];
          result.tables.shootout.push(...extractTable(table));
        } else if (headerStr.includes('scoring') && headerStr.includes('total')) {
          result.tables.scoring = extractTable(table);
        }
      });

      // Get page text for three stars
      result.pageText = document.body.textContent.substring(0, 5000);

      return result;
    });

    // Parse the extracted tables
    const parsed = {
      game_id: gameId,
      shots_by_period: parseShotsTable(boxScore.tables.shots),
      power_play_summary: parsePowerPlaySummary(boxScore.tables.powerplay),
      game_info: parseOfficials(boxScore.tables.officials),
      goaltenders: parseGoaltenderStats(boxScore.tables.goalies, isHomeGame),
      shootout: parseShootout(boxScore.tables.shootout, isHomeGame),
      three_stars: parseThreeStars(boxScore.pageText)
    };

    // Also extract period-by-period scoring if available
    if (boxScore.tables.scoring) {
      parsed.scoring_by_period = {};
      for (const row of boxScore.tables.scoring) {
        const teamName = row[0]?.toLowerCase() || '';
        const isAmherst = teamName.includes('amherst') || teamName.includes('ramblers');
        const target = isAmherst ? 'ramblers' : 'opponent';

        parsed.scoring_by_period[target] = {
          period1: parseInt(row[1]) || 0,
          period2: parseInt(row[2]) || 0,
          period3: parseInt(row[3]) || 0,
          total: parseInt(row[4]) || 0
        };
      }
    }

    return parsed;
  } catch (e) {
    console.error(`[boxscores] Error scraping game ${gameId}:`, e.message);
    return { game_id: gameId, error: e.message };
  }
}

/**
 * Load existing games data to get game IDs
 */
async function loadGamesData() {
  try {
    const gamesPath = path.join(ROOT_DIR, 'games', 'amherst-ramblers.json');
    const data = await fs.readFile(gamesPath, 'utf8');
    return JSON.parse(data);
  } catch (e) {
    console.error('[boxscores] Could not load games data:', e.message);
    return null;
  }
}

/**
 * Main function to scrape all Ramblers game box scores
 */
export async function scrapeRamblersBoxScores(options = {}) {
  const { limit = 0, gameIds = null } = options;

  console.log('[boxscores] Starting Playwright box score scraper...');

  const gamesData = await loadGamesData();
  if (!gamesData || !gamesData.games) {
    throw new Error('No games data available');
  }

  // Get list of game IDs to scrape
  let gamesToScrape = gamesData.games.map(g => ({
    game_id: g.game_id,
    is_home_game: g.home_game
  }));

  if (gameIds) {
    gamesToScrape = gamesToScrape.filter(g => gameIds.includes(g.game_id));
  }

  if (limit > 0) {
    gamesToScrape = gamesToScrape.slice(0, limit);
  }

  console.log(`[boxscores] Scraping ${gamesToScrape.length} games...`);

  const browser = await chromium.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  const context = await browser.newContext({
    viewport: { width: 1920, height: 1080 }
  });

  const page = await context.newPage();
  const boxScores = [];

  for (const game of gamesToScrape) {
    console.log(`[boxscores] Scraping game ${game.game_id}...`);
    const boxScore = await scrapeGameBoxScore(page, game.game_id, game.is_home_game);
    boxScores.push(boxScore);

    // Rate limiting
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  await browser.close();

  // Merge box score data with existing games data
  const enhancedGames = gamesData.games.map(game => {
    const boxScore = boxScores.find(bs => bs.game_id === game.game_id);
    if (boxScore && !boxScore.error) {
      return {
        ...game,
        box_score: {
          shots_by_period: boxScore.shots_by_period,
          scoring_by_period: boxScore.scoring_by_period,
          power_play_summary: boxScore.power_play_summary,
          goaltenders: boxScore.goaltenders,
          shootout: boxScore.shootout,
          three_stars: boxScore.three_stars
        },
        game_info: boxScore.game_info
      };
    }
    return game;
  });

  const output = {
    ...gamesData,
    updated_at: nowISO(),
    games: enhancedGames
  };

  // Write enhanced games file
  const outputPath = path.join(ROOT_DIR, 'games', 'amherst-ramblers.json');
  await fs.writeFile(outputPath, JSON.stringify(output, null, 2));
  console.log(`[boxscores] Updated games/amherst-ramblers.json with box score data`);

  return output;
}

// ------------- CLI entry -------------
if (process.argv[1] && process.argv[1].endsWith('boxscores.mjs')) {
  const args = process.argv.slice(2);
  const limitArg = args.find(a => a.startsWith('--limit='));
  const limit = limitArg ? parseInt(limitArg.split('=')[1]) : 0;

  const gameIdArg = args.find(a => a.startsWith('--game='));
  const gameIds = gameIdArg ? [gameIdArg.split('=')[1]] : null;

  scrapeRamblersBoxScores({ limit, gameIds })
    .then(() => {
      console.log('[boxscores] Complete!');
      process.exit(0);
    })
    .catch(e => {
      console.error('[boxscores] Fatal error:', e);
      process.exit(1);
    });
}
