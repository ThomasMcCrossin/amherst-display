/**
 * Build standings JSONs with robust parsing + helpful logs.
 * - MHL: Headless Chrome (JS-rendered) → parse both division tables
 *
 * Outputs:
 *   standings_mhl.json (includes division info for division-first playoffs)
 *
 * MHL Divisions (2024-25):
 *   Eastlink South: Amherst Ramblers, Truro Bearcats, Pictou County Weeks Crushers,
 *                   Yarmouth Mariners, Valley Wildcats, South Shore Lumberjacks
 *   Eastlink North: Grand Falls Rapids, West Kent Steamers, Chaleur Lightning,
 *                   Edmundston Blizzard, Campbellton Tigers, Miramichi Timberwolves,
 *                   Summerside Western Capitals
 *
 * Requires in package.json: "puppeteer", "cheerio"
 * Workflow must install Chrome:  npx puppeteer browsers install chrome
 */

import fs from 'fs/promises';
import path from 'path';
import * as cheerio from 'cheerio';
import puppeteer from 'puppeteer';

const URL_MHL  = 'https://www.themhl.ca/stats/standings';

const nowISO = () => new Date().toISOString();

const ensureDir = async (fp) => {
  const dir = path.dirname(fp);
  await fs.mkdir(dir, { recursive: true });
};

// Division mapping for MHL teams (slug -> division)
const MHL_DIVISIONS = {
  'amherst-ramblers': 'Eastlink South',
  'truro-bearcats': 'Eastlink South',
  'pictou-county-weeks-crushers': 'Eastlink South',
  'yarmouth-mariners': 'Eastlink South',
  'valley-wildcats': 'Eastlink South',
  'south-shore-lumberjacks': 'Eastlink South',
  'grand-falls-rapids': 'Eastlink North',
  'west-kent-steamers': 'Eastlink North',
  'chaleur-lightning': 'Eastlink North',
  'edmundston-blizzard': 'Eastlink North',
  'campbellton-tigers': 'Eastlink North',
  'miramichi-timberwolves': 'Eastlink North',
  'summerside-western-capitals': 'Eastlink North',
};

// Team name to division mapping (for cases where slug isn't available yet)
const TEAM_NAME_TO_DIVISION = {
  'amherst ramblers': 'Eastlink South',
  'truro bearcats': 'Eastlink South',
  'pictou county weeks crushers': 'Eastlink South',
  'pictou county crushers': 'Eastlink South',
  'yarmouth mariners': 'Eastlink South',
  'valley wildcats': 'Eastlink South',
  'south shore lumberjacks': 'Eastlink South',
  'grand falls rapids': 'Eastlink North',
  'west kent steamers': 'Eastlink North',
  'chaleur lightning': 'Eastlink North',
  'edmundston blizzard': 'Eastlink North',
  'campbellton tigers': 'Eastlink North',
  'miramichi timberwolves': 'Eastlink North',
  'summerside western capitals': 'Eastlink North',
};

// ------------ slug mapping helpers (use map passed in from build_all) -----------
const norm = s => (s||'').toLowerCase().replace(/\s+/g,' ').trim();
function slugFor(name, nameToSlug){
  if (!name || !nameToSlug) return null;
  const n = norm(name);
  return nameToSlug.get(n) || null;
}
function attachSlugAndDivision(row, nameToSlug){
  const slug = slugFor(row.team, nameToSlug);
  // Get division from slug or team name
  let division = null;
  if (slug && MHL_DIVISIONS[slug]) {
    division = MHL_DIVISIONS[slug];
  } else {
    const teamLower = norm(row.team);
    division = TEAM_NAME_TO_DIVISION[teamLower] || null;
  }
  return { ...row, slug, division };
}

// ------------ MHL ------------
export async function buildMHLStandings({ nameToSlug }){
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox','--disable-setuid-sandbox']
  });

  try{
    const page = await browser.newPage();
    await page.setViewport({ width: 1600, height: 1200, deviceScaleFactor: 1 });
    await page.goto(URL_MHL, { waitUntil: 'networkidle2', timeout: 60000 });

    // Wait for table to appear with data rows
    try {
      await page.waitForSelector('table tbody tr', { timeout: 10000 });
      console.log('[standings/MHL] table rows found');
    } catch (e) {
      console.warn('[standings/MHL] table selector timeout, trying anyway');
    }
    await new Promise(resolve => setTimeout(resolve, 1000));

    const info = await page.evaluate(() => {
      const tables = Array.from(document.querySelectorAll('table'));

      // Get ONLY the text content of the header cells, not any nested elements
      const getHeaderText = (cell) => {
        // Get direct text content, ignoring nested spans/divs
        const text = cell.textContent || '';
        return text.replace(/\s+/g,' ').trim().toLowerCase();
      };

      const headerWords = t => {
        const thead = t.querySelector('thead');
        const headerRow = thead ? thead.querySelector('tr') : t.querySelector('tr');
        if (!headerRow) return [];
        const cells = Array.from(headerRow.querySelectorAll('th,td'));
        return cells.map(getHeaderText);
      };

      // Find all standings tables (there may be 2 for divisions)
      const standingsTables = [];
      for (const t of tables) {
        const words = headerWords(t);
        const hasTeam = words.some(w => w.includes('team'));
        const hasGP = words.some(w => w === 'gp' || w.includes('games'));
        const hasPts = words.some(w => w === 'pts' || w === 'points');
        if (hasTeam && hasGP && hasPts) {
          standingsTables.push(t);
        }
      }

      // If no matching tables, fall back to largest
      if (!standingsTables.length && tables.length) {
        standingsTables.push(tables.sort((a,b) => {
          const ra=a.getBoundingClientRect(), rb=b.getBoundingClientRect();
          return (rb.width*rb.height) - (ra.width*ra.height);
        })[0]);
      }

      if (!standingsTables.length) return { count: tables.length, headers: [], rows: [], divisions: [] };

      // Parse all tables, detecting division headers
      const allRows = [];
      let currentDivision = null;

      for (const table of standingsTables) {
        // Check for division header before or within the table
        const prevEl = table.previousElementSibling;
        if (prevEl) {
          const prevText = (prevEl.textContent || '').toLowerCase();
          if (prevText.includes('south')) currentDivision = 'Eastlink South';
          else if (prevText.includes('north')) currentDivision = 'Eastlink North';
        }

        // Also check table caption or first header
        const caption = table.querySelector('caption');
        if (caption) {
          const capText = (caption.textContent || '').toLowerCase();
          if (capText.includes('south')) currentDivision = 'Eastlink South';
          else if (capText.includes('north')) currentDivision = 'Eastlink North';
        }

        const headers = headerWords(table);
        const bodyRows = table.querySelector('tbody')
          ? Array.from(table.querySelectorAll('tbody tr'))
          : Array.from(table.querySelectorAll('tr')).slice(1);

        for (const tr of bodyRows) {
          // Check if this row is a division header row
          const rowText = (tr.textContent || '').toLowerCase();
          if (rowText.includes('eastlink south') || rowText.includes('south division')) {
            currentDivision = 'Eastlink South';
            continue;
          }
          if (rowText.includes('eastlink north') || rowText.includes('north division')) {
            currentDivision = 'Eastlink North';
            continue;
          }

          const tds = Array.from(tr.querySelectorAll('td'));
          if (!tds.length) continue;
          const cells = tds.map(td => td.textContent.replace(/\s+/g,' ').trim());
          // Skip empty rows
          if (cells.every(c => !c)) continue;

          allRows.push({ cells, division: currentDivision, headers });
        }
      }

      return {
        count: tables.length,
        standingsTableCount: standingsTables.length,
        rows: allRows
      };
    });

    console.log(`[standings/MHL] tables=${info.count} standings_tables=${info.standingsTableCount || 1} rows=${info.rows.length}`);

    if (!info.rows.length) {
      return { generated_at: nowISO(), league:'MHL', season:'', rows: [], divisions: ['Eastlink South', 'Eastlink North'] };
    }

    // Map headers to canonical keys - handle duplicated/concatenated names
    const mapHeader = (h) => {
      const x = (h||'').toLowerCase().replace(/[^a-z0-9]+/g,'');
      // Handle concatenated duplicates (e.g., "gpgp", "ptspts")
      if (/^team/.test(x) || x.includes('team')) return 'team';
      if (x === 'gp' || x === 'gpgp' || x === 'games') return 'gp';
      if (x === 'w' || x === 'ww' || x === 'wins')  return 'w';
      if (x === 'l' || x === 'll' || x === 'losses')  return 'l';
      if (x === 'otl' || x === 'otlotl' || x === 'ol' || x === 'otlosses') return 'otl';
      if (x === 'sol' || x === 'solsol' || x === 'so' || x === 'shootout') return 'sol';
      if (x === 'pts' || x === 'ptspts' || x === 'points') return 'pts';
      if (x === 'pct' || x === 'pctpct' || x === 'percent') return 'pct';
      if (x === 'gf' || x === 'gfgf' || x === 'goalsfor') return 'gf';
      if (x === 'ga' || x === 'gaga' || x === 'goalsagainst') return 'ga';
      if (x === 'diff' || x === 'diffdiff' || x === 'plusminus' || x === 'gd' || x === '+-') return 'diff';
      if (x === 'pim' || x === 'pimpim' || x === 'penaltyminutes') return 'pim';
      if (x === 'streak' || x === 'stk' || x === 'stkstk' || x === 'streakstreak') return 'streak';
      if (x === 'p10' || x === 'p10p10' || x === 'last10' || x === 'l10') return 'p10';
      if (x === 'rw' || x === 'rwrw' || x === 'regwins') return 'rw';
      if (x === 'otw' || x === 'otwotw' || x === 'otwins') return 'otw';
      if (x === 'sow' || x === 'sowsow' || x === 'sowins') return 'sow';
      // Skip unnamed/rank columns
      if (!x || x.match(/^\d+$/) || x === 'col') return null;
      return null; // Skip unknown headers to avoid garbage keys
    };

    // Build normalized rows
    const rows = [];
    for (const { cells: arr, division, headers } of info.rows) {
      if (!arr.length) continue;
      const obj = { division_detected: division };
      const headerKeys = (headers || []).map(mapHeader);

      for (let i=0; i<arr.length; i++){
        const key = headerKeys[i];
        if (key) {
          obj[key] = arr[i];
        }
      }

      // Find team name: look for first non-numeric cell with length > 2
      if (!obj.team || /^\d+$/.test(obj.team)) {
        obj.team = arr.find(cell => cell && cell.length > 2 && !/^[\d.]+$/.test(cell)) || arr[1] || arr[0] || '';
      }

      // Clean up team name (remove any rank prefix like "1 ")
      if (obj.team) {
        obj.team = obj.team.replace(/^\d+\s+/, '').trim();
      }

      // numeric coercion where obvious
      const toNum = (v) => (/^-?\d+(\.\d+)?$/.test(String(v||''))) ? Number(v) : v;
      ['gp','w','l','otl','sol','pts','rw','otw','sow','gf','ga','diff','pim'].forEach(k=>{
        if (obj[k] != null) obj[k] = toNum(obj[k]);
      });
      if (obj.diff == null && typeof obj.gf === 'number' && typeof obj.ga === 'number') obj.diff = obj.gf - obj.ga;

      rows.push(obj);
    }

    // Attach slug and division
    const withSlugAndDivision = rows
      .filter(r => r.team && (r.gp!=null || r.pts!=null)) // drop empty/footer rows
      .map(r => {
        const enhanced = attachSlugAndDivision(r, nameToSlug);
        // Use detected division if available, otherwise use our mapping
        enhanced.division = enhanced.division || r.division_detected;
        delete enhanced.division_detected;
        return enhanced;
      });

    // Sort by points within each division
    withSlugAndDivision.sort((a, b) => {
      // First by division (South first for Ramblers focus)
      if (a.division !== b.division) {
        if (a.division === 'Eastlink South') return -1;
        if (b.division === 'Eastlink South') return 1;
        return 0;
      }
      // Then by points, then goal diff
      const ptsDiff = (b.pts || 0) - (a.pts || 0);
      if (ptsDiff !== 0) return ptsDiff;
      return (b.diff || 0) - (a.diff || 0);
    });

    // Add division rank
    let southRank = 0, northRank = 0;
    for (const row of withSlugAndDivision) {
      if (row.division === 'Eastlink South') {
        southRank++;
        row.division_rank = southRank;
      } else if (row.division === 'Eastlink North') {
        northRank++;
        row.division_rank = northRank;
      }
    }

    console.log(withSlugAndDivision[0] ? `[standings/MHL] example=${JSON.stringify(withSlugAndDivision[0])}` : '[standings/MHL] example=none');
    console.log(`[standings/MHL] South teams: ${withSlugAndDivision.filter(r => r.division === 'Eastlink South').length}, North teams: ${withSlugAndDivision.filter(r => r.division === 'Eastlink North').length}`);

    return {
      generated_at: nowISO(),
      season: '2024-25',
      league: 'MHL',
      divisions: ['Eastlink South', 'Eastlink North'],
      playoff_format: 'Top 4 per division qualify',
      rows: withSlugAndDivision
    };
  }catch(e){
    console.warn('[standings/MHL] failed:', e.message);
    return { generated_at: nowISO(), league: 'MHL', season: '', rows: [], divisions: ['Eastlink South', 'Eastlink North'] };
  }finally{
    await browser.close();
  }
}

// ------------- CLI entry (optional local run) -------------
if (process.argv[1] && process.argv[1].endsWith('standings.mjs')) {
  (async ()=>{
    // In local ad-hoc run, we won't have nameToSlug; parse without slugs.
    const nameToSlug = new Map(); // provide one if you want local mapping
    const mhl = await buildMHLStandings({ nameToSlug });
    await ensureDir('standings_mhl.json');
    await fs.writeFile('standings_mhl.json', JSON.stringify(mhl, null, 2));
    console.log(`[standings] wrote: MHL=${mhl.rows.length}`);
  })().catch(e=>{ console.error(e); process.exit(1); });
}
