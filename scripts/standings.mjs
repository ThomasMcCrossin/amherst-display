/**
 * Build standings JSONs with rich logging.
 * - MHL: Headless Chrome → parse largest table → rows[]
 * - BSHL: Static HTML → regex lines → rows[]
 *
 * Outputs:
 *   standings_mhl.json
 *   standings_bshl.json
 *
 * Requires:
 *   - puppeteer ^23 (workflow must install chrome: `npx puppeteer browsers install chrome`)
 *   - cheerio ^1
 */

import fs from 'fs/promises';
import path from 'path';
import * as cheerio from 'cheerio';
import puppeteer from 'puppeteer';

const TZ = 'America/Halifax';
const OUT_MHL  = 'standings_mhl.json';
const OUT_BSHL = 'standings_bshl.json';

const URL_MHL  = 'https://www.themhl.ca/stats/standings';
const URL_BSHL = 'https://www.beausejourseniorhockeyleague.ca/standings.php';

// ---------- helpers ----------
const nowISO = () => new Date().toISOString();

const readTeams = async () => {
  let teamsRaw = null;
  try {
    teamsRaw = JSON.parse(await fs.readFile('teams.json', 'utf8'));
  } catch {
    console.warn('[teams] teams.json not found — slug mapping will be empty.');
    return { byName: new Map(), bySlug: new Map() };
  }
  const teams = Array.isArray(teamsRaw.teams) ? teamsRaw.teams : [];
  const bySlug = new Map(teams.map(t => [t.slug, t]));
  const byName = new Map();
  const norm = s => (s||'').toLowerCase().replace(/\s+/g, ' ').trim();

  for (const t of teams) {
    if (t.name) byName.set(norm(t.name), t.slug);
    for (const a of (t.aliases || [])) byName.set(norm(a), t.slug);
  }
  return { byName, bySlug };
};

const nameToSlug = (name, byName) => {
  if (!name) return null;
  const norm = name.toLowerCase().replace(/\s+/g, ' ').trim();
  return byName.get(norm) || null;
};

const ensureDir = async (fp) => {
  const dir = path.dirname(fp);
  await fs.mkdir(dir, { recursive: true });
};

// ---------- BSHL (static text parse) ----------
async function buildBSHL(byName) {
  console.log(`[bshl] fetch ${URL_BSHL}`);
  const res = await fetch(URL_BSHL);
  if (!res.ok) throw new Error(`[bshl] HTTP ${res.status}`);
  const html = await res.text();
  const $ = cheerio.load(html);

  // Grab the visible text; their page lists a header and then rows of text with numbers.
  const bodyText = $('body').text().split('\n').map(s => s.trim()).filter(Boolean);

  // Keep only lines that look like standings rows.
  // Example: "Amherst Ducks 0 0 0 0 0-0-0 0-0-0 0 0 0 0"
  const rowRe = /^([A-Za-zÀ-ÿ'’\-\s]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+-\d+-\d+)\s+(\d+-\d+-\d+)\s+(\d+)\s+(\d+)\s+([+-]?\d+)\s+(\d+)$/;

  const rows = [];
  for (const line of bodyText) {
    if (/^Team\s+GP\b/i.test(line)) continue; // skip header
    const m = line.match(rowRe);
    if (!m) continue;
    const [, team, gp, w, l, otl, homeRec, roadRec, gf, ga, diff, pts] = m;
    const slug = nameToSlug(team, byName);
    rows.push({
      team, slug,
      gp: +gp, w: +w, l: +l, otl: +otl,
      home: homeRec, road: roadRec,
      gf: +gf, ga: +ga, diff: +diff, pts: +pts
    });
  }

  console.log(`[bshl] scanned=${bodyText.length} matched=${rows.length}${rows[0] ? ` example=${JSON.stringify(rows[0])}` : ''}`);
  return {
    generated_at: nowISO(),
    league: 'BSHL',
    season: '',
    rows
  };
}

// ---------- MHL (JS-rendered → headless table scrape) ----------
async function buildMHL(byName) {
  console.log(`[mhl] launch headless → ${URL_MHL}`);
  const browser = await puppeteer.launch({
    channel: 'chrome',
    headless: 'new',
    args: ['--no-sandbox','--disable-setuid-sandbox']
  });

  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1600, height: 1200, deviceScaleFactor: 1 });
    await page.goto(URL_MHL, { waitUntil: 'domcontentloaded', timeout: 60000 });

    // Wait for client JS to draw; then see if any tables exist.
    await page.waitForTimeout(3500);

    // Find the largest table on the page (what worked best in practice)
    const tableInfo = await page.evaluate(() => {
      function rectArea(r){ return r.width * r.height; }
      const tables = Array.from(document.querySelectorAll('table'));
      if (!tables.length) return null;

      let best = null, bestArea = 0;
      for (const t of tables) {
        const r = t.getBoundingClientRect();
        const area = rectArea(r);
        if (area > bestArea) { best = t; bestArea = area; }
      }
      if (!best) return null;

      const headers = [];
      const thead = best.querySelector('thead');
      if (thead) {
        const ths = Array.from(thead.querySelectorAll('th'));
        for (const th of ths) headers.push(th.textContent.trim());
      } else {
        // Sometimes headers are the first row of tbody
        const firstRow = best.querySelector('tr');
        if (firstRow) {
          const cells = Array.from(firstRow.querySelectorAll('th,td'));
          if (cells.length > 3) headers.push(...cells.map(c => c.textContent.trim()));
        }
      }

      const rows = [];
      const trs = Array.from(best.querySelectorAll('tbody tr')).length
        ? Array.from(best.querySelectorAll('tbody tr'))
        : Array.from(best.querySelectorAll('tr')).slice(1); // skip first if used as header

      for (const tr of trs) {
        const tds = Array.from(tr.querySelectorAll('td'));
        if (!tds.length) continue;
        rows.push(tds.map(td => td.textContent.replace(/\s+/g,' ').trim()));
      }

      return {
        size: best.getBoundingClientRect(),
        headers,
        rows
      };
    });

    if (!tableInfo) {
      console.warn('[mhl] No <table> elements found on page.');
      return { generated_at: nowISO(), league: 'MHL', season: '', rows: [] };
    }

    const { size, headers, rows: grid } = tableInfo;
    console.log(`[mhl] table ${Math.round(size.width)}x${Math.round(size.height)} headers=${JSON.stringify(headers)} rows=${grid.length}`);

    // Normalize header → key mapping
    const mapHeader = (h) => {
      const x = (h||'').toLowerCase().replace(/[^a-z0-9]+/g,'');
      if (/^team/.test(x)) return 'team';
      if (x === 'gp') return 'gp';
      if (x === 'w')  return 'w';
      if (x === 'l')  return 'l';
      if (x === 'otl' || x === 'ol') return 'otl';
      if (x === 'sol' || x === 'so') return 'sol';
      if (x === 'pts' || x === 'points') return 'pts';
      if (x === 'pct' || x === 'winpct') return 'pct';
      if (x === 'rw') return 'rw';
      if (x === 'otw') return 'otw';
      if (x === 'sow') return 'sow';
      if (x === 'gf') return 'gf';
      if (x === 'ga') return 'ga';
      if (x === 'diff' || x === '+-') return 'diff';
      if (x === 'pim') return 'pim';
      if (x === 'stk' || x === 'streak') return 'streak';
      if (x === 'p10' || x === 'last10') return 'p10';
      return h || x || 'col';
    };
    const headerKeys = headers.map(mapHeader);

    // Build row objects
    const rows = [];
    for (const row of grid) {
      const obj = {};
      for (let i=0; i<row.length; i++) {
        const key = headerKeys[i] || `col${i}`;
        obj[key] = row[i];
      }
      // Team name is usually in column 0
      obj.team = obj.team || row[0] || '';
      // Attach slug if we can
      const slug = nameToSlug(obj.team, byName);
      if (slug) obj.slug = slug;

      // Parse numerics where obvious
      ['gp','w','l','otl','sol','pts','rw','otw','sow','gf','ga','diff','pim'].forEach(k=>{
        if (obj[k] != null && /^[+-]?\d+(\.\d+)?$/.test(String(obj[k]))) obj[k] = Number(obj[k]);
      });

      rows.push(obj);
    }

    console.log(`[mhl] parsed rows=${rows.length}${rows[0] ? ` example=${JSON.stringify(rows[0])}` : ''}`);
    return {
      generated_at: nowISO(),
      league: 'MHL',
      season: '',
      rows
    };
  } finally {
    await browser.close();
  }
}

// ---------- main ----------
(async () => {
  const { byName } = await readTeams();

  // Build BSHL first (fast), then MHL
  let bshl = null, mhl = null;
  try {
    bshl = await buildBSHL(byName);
  } catch (e) {
    console.warn('[bshl] failed:', e.message);
    bshl = { generated_at: nowISO(), league: 'BSHL', season: '', rows: [] };
  }

  try {
    mhl = await buildMHL(byName);
  } catch (e) {
    console.warn('[mhl] failed:', e.message);
    mhl = { generated_at: nowISO(), league: 'MHL', season: '', rows: [] };
  }

  await ensureDir(OUT_BSHL);
  await fs.writeFile(OUT_BSHL, JSON.stringify(bshl, null, 2));
  await ensureDir(OUT_MHL);
  await fs.writeFile(OUT_MHL, JSON.stringify(mhl, null, 2));

  console.log(`[standings] done → ${OUT_MHL} rows=${mhl.rows.length} | ${OUT_BSHL} rows=${bshl.rows.length}`);
})().catch(err => {
  console.error('[standings] fatal:', err);
  process.exit(1);
});
