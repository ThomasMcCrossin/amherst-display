/**
 * Build standings JSONs with robust parsing + helpful logs.
 * - MHL: Headless Chrome (JS-rendered) → choose table by headers → parse rows
 * - BSHL: Try real <table> first; fallback to text-line regex
 *
 * Outputs:
 *   standings_mhl.json
 *   standings_bshl.json
 *
 * Requires in package.json: "puppeteer", "cheerio"
 * Workflow must install Chrome:  npx puppeteer browsers install chrome
 */

import fs from 'fs/promises';
import path from 'path';
import * as cheerio from 'cheerio';
import puppeteer from 'puppeteer';

const URL_MHL  = 'https://www.themhl.ca/stats/standings';
const URL_BSHL = 'https://www.beausejourseniorhockeyleague.ca/standings.php';

const nowISO = () => new Date().toISOString();

const ensureDir = async (fp) => {
  const dir = path.dirname(fp);
  await fs.mkdir(dir, { recursive: true });
};

// ------------ slug mapping helpers (use map passed in from build_all) -----------
const norm = s => (s||'').toLowerCase().replace(/\s+/g,' ').trim();
function slugFor(name, nameToSlug){
  if (!name || !nameToSlug) return null;
  const n = norm(name);
  return nameToSlug.get(n) || null;
}
function attachSlug(row, nameToSlug){
  const slug = slugFor(row.team, nameToSlug);
  return { ...row, slug };
}

// ------------ BSHL ------------
async function fetchText(url){
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.text();
}

function parseBSHLTable($, nameToSlug){
  const tables = $('table');
  if (!tables.length) return [];

  // Parse ALL tables and combine rows (multiple divisions)
  const allRows = [];

  tables.each((tableIdx, table) => {
    const rows = parseTableRows($, $(table), nameToSlug);
    allRows.push(...rows);
  });

  return allRows;
}

function parseTableRows($, table, nameToSlug){
  let headers = table.find('thead th, tr:first-child th, tr:first-child td').map((i,el)=>$(el).text().trim().toLowerCase()).get();
  // Filter empty headers caused by malformed HTML
  headers = headers.filter(h => h !== '');

  const hidx = (label) => {
    const i = headers.findIndex(h => h === label || h.includes(label));
    return i >= 0 ? i : -1;
  };

  const teamIdx = Math.max(0, hidx('team'));
  const idx = {
    gp: hidx('gp'), w: hidx('w'), l: hidx('l'),
    otl: hidx('otl') >=0 ? hidx('otl') : (hidx('ol')>=0 ? hidx('ol') : -1),
    sol: hidx('sol'), pts: hidx('pts') >=0 ? hidx('pts') : hidx('points'),
    gf: hidx('gf'), ga: hidx('ga'), diff: (hidx('diff')>=0 ? hidx('diff') : hidx('+') )
  };

  const out = [];
  table.find('tbody tr, tr').each((i,tr)=>{
    const tds = $(tr).find('td');
    if (!tds.length) return;
    let cells = tds.map((j,td)=>$(td).text().replace(/\s+/g,' ').trim()).get();
    // Filter out empty cells caused by malformed HTML
    cells = cells.filter(c => c !== '');
    const team = cells[teamIdx] || cells[0] || '';
    if (!team) return;

    // Skip header rows
    if (/^team$/i.test(team)) return;

    const num = (i) => {
      if (i < 0 || !cells[i]) return 0;
      const val = cells[i].replace(/\s+/g,'').trim();
      return /^-?\d+(\.\d+)?$/.test(val) ? Number(val) : 0;
    };
    const row = {
      team,
      gp:  num(idx.gp), w: num(idx.w), l: num(idx.l),
      otl: num(idx.otl), sol: num(idx.sol),
      pts: num(idx.pts),
      gf:  num(idx.gf), ga: num(idx.ga),
      diff: num(idx.diff)
    };

    out.push(attachSlug(row, nameToSlug));
  });

  return out.filter(r => r.team); // basic sanity
}

function parseBSHLText($, nameToSlug){
  // Example line:
  // Amherst Ducks 0 0 0 0 0-0-0 0-0-0 0 0 0 0
  const lines = $('body').text().replace(/\u00A0/g,' ').split('\n').map(s=>s.trim()).filter(Boolean);
  const rowRe = /^([A-Za-zÀ-ÿ'’\-\s]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+-\d+-\d+)\s+(\d+-\d+-\d+)\s+(\d+)\s+(\d+)\s+([+-]?\d+)\s+(\d+)$/;

  const out = [];
  let matched = 0;
  for (const line of lines) {
    if (/^team\s+gp\b/i.test(line)) continue;
    const m = line.match(rowRe);
    if (!m) continue;
    matched++;
    const [, team, gp, w, l, otl, home, road, gf, ga, diff, pts] = m;
    out.push(attachSlug({
      team,
      gp:+gp, w:+w, l:+l, otl:+otl, pts:+pts,
      gf:+gf, ga:+ga, diff:+diff,
      home, road
    }, nameToSlug));
  }
  console.log(`[standings/BSHL] text parse matched=${matched} lines`);
  return out;
}

export async function buildBSHLStandings({ nameToSlug }){
  try{
    let html = await fetchText(URL_BSHL);
    // Fix malformed HTML where <td> appears instead of </td>
    // Pattern: </td><td>X<td> becomes </td><td>X</td>
    html = html.replace(/(<\/td>)<td>([^<]+)<td>/gi, '$1<td>$2</td><td>');
    // Pattern at start: <td>X<td> becomes <td>X</td><td>
    html = html.replace(/([^d])<td>([^<]+)<td>/gi, '$1<td>$2</td><td>');
    const $ = cheerio.load(html);

    let rows = parseBSHLTable($, nameToSlug);
    console.log(`[standings/BSHL] table rows=${rows.length}`);
    if (rows.length < 2) {
      rows = parseBSHLText($, nameToSlug);
    }

    // compute diff if missing
    rows = rows.map(r => ({ ...r, diff: (r.diff || (typeof r.gf === 'number' && typeof r.ga === 'number') ? (r.gf - r.ga) : 0) }));

    console.log(rows[0] ? `[standings/BSHL] example=${JSON.stringify(rows[0])}` : '[standings/BSHL] example=none');

    return {
      generated_at: nowISO(),
      season: '',
      league: 'BSHL',
      rows
    };
  }catch(e){
    console.warn('[standings/BSHL] failed:', e.message);
    return { generated_at: nowISO(), league: 'BSHL', season: '', rows: [] };
  }
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
      const headerWords = t => {
        const h = t.querySelector('thead') || t.querySelector('tr');
        if (!h) return [];
        const cells = Array.from(h.querySelectorAll('th,td')).map(c => {
          return (c.textContent||'').replace(/\s+/g,' ').trim().toLowerCase();
        });
        return cells;
      };

      // Choose by header (TEAM / GP / PTS or Points). Fallback: largest.
      let chosen = null;
      for (const t of tables) {
        const words = headerWords(t);
        const ok = words.includes('team') && words.includes('gp') && (words.includes('pts') || words.includes('points'));
        if (ok) { chosen = t; break; }
      }
      if (!chosen && tables.length){
        chosen = tables.sort((a,b) => {
          const ra=a.getBoundingClientRect(), rb=b.getBoundingClientRect();
          return (rb.width*rb.height) - (ra.width*ra.height);
        })[0];
      }
      if (!chosen) return { count: tables.length, headers: [], rows: [], size:{w:0,h:0} };

      const headers = headerWords(chosen);
      const bodyRows = chosen.querySelector('tbody') ? Array.from(chosen.querySelectorAll('tbody tr')) : Array.from(chosen.querySelectorAll('tr')).slice(1);

      const rows = [];
      for (const tr of bodyRows) {
        const tds = Array.from(tr.querySelectorAll('td'));
        if (!tds.length) continue;
        const cells = tds.map(td => td.textContent.replace(/\s+/g,' ').trim());
        // Skip empty rows
        if (cells.every(c => !c)) continue;
        rows.push(cells);
      }
      const r = chosen.getBoundingClientRect();
      return {
        count: tables.length,
        headers,
        rows,
        size: {w:r.width, h:r.height},
        tbodyRows: bodyRows.length,
        firstRowSample: rows[0] || null
      };
    });

    console.log(`[standings/MHL] tables=${info.count} chosen=${Math.round(info.size.w)}x${Math.round(info.size.h)} rows=${info.rows.length}`);

    if (!info.rows.length) {
      return { generated_at: nowISO(), league:'MHL', season:'', rows: [] };
    }

    // Map headers to canonical keys
    const mapHeader = (h) => {
      const x = (h||'').toLowerCase().replace(/[^a-z0-9]+/g,'');
      if (/^team/.test(x)) return 'team';
      if (x === 'gp' || x === 'gpgp') return 'gp';
      if (x === 'w' || x === 'ww')  return 'w';
      if (x === 'l' || x === 'll')  return 'l';
      if (x === 'otl' || x === 'otlotl' || x === 'ol') return 'otl';
      if (x === 'sol' || x === 'solsol' || x === 'so') return 'sol';
      if (x === 'pts' || x === 'ptspts' || x === 'points') return 'pts';
      if (x === 'gf' || x === 'gfgf') return 'gf';
      if (x === 'ga' || x === 'gaga') return 'ga';
      if (x === 'diff' || x === 'diffdiff' || x === 'plusminus' || x === 'gd' || x === '+-') return 'diff';
      if (x === 'streak' || x === 'stk' || x === 'stkstk') return 'streak';
      if (x === 'p10' || x === 'p10p10' || x === 'last10') return 'p10';
      if (x === 'rw' || x === 'rwrw') return 'rw';
      if (x === 'otw' || x === 'otwotw') return 'otw';
      if (x === 'sow' || x === 'sowsow') return 'sow';
      return h || x || 'col';
    };
    const headerKeys = info.headers.map(mapHeader);

    // Build normalized rows
    const rows = [];
    for (const arr of info.rows) {
      if (!arr.length) continue;
      const obj = {};
      for (let i=0;i<arr.length;i++){
        const key = headerKeys[i] || `col${i}`;
        obj[key] = arr[i];
      }
      // Find team name: look for first non-numeric cell with length > 2
      if (!obj.team || /^\d+$/.test(obj.team)) {
        obj.team = arr.find(cell => cell && cell.length > 2 && !/^[\d.]+$/.test(cell)) || arr[1] || arr[0] || '';
      }
      // numeric coercion where obvious
      const toNum = (v) => (/^-?\d+(\.\d+)?$/.test(String(v||''))) ? Number(v) : v;
      ['gp','w','l','otl','sol','pts','rw','otw','sow','gf','ga','diff'].forEach(k=>{
        if (obj[k] != null) obj[k] = toNum(obj[k]);
      });
      if (obj.diff == null && typeof obj.gf === 'number' && typeof obj.ga === 'number') obj.diff = obj.gf - obj.ga;

      rows.push(obj);
    }

    // Attach slug
    const withSlug = rows
      .filter(r => r.team && (r.gp!=null || r.pts!=null)) // drop empty/footer rows
      .map(r => attachSlug(r, nameToSlug));

    console.log(withSlug[0] ? `[standings/MHL] example=${JSON.stringify(withSlug[0])}` : '[standings/MHL] example=none');

    return {
      generated_at: nowISO(),
      season: '',
      league: 'MHL',
      rows: withSlug
    };
  }catch(e){
    console.warn('[standings/MHL] failed:', e.message);
    return { generated_at: nowISO(), league: 'MHL', season: '', rows: [] };
  }finally{
    await browser.close();
  }
}

// ------------- CLI entry (optional local run) -------------
if (process.argv[1] && process.argv[1].endsWith('standings.mjs')) {
  (async ()=>{
    // In local ad-hoc run, we won’t have nameToSlug; parse without slugs.
    const nameToSlug = new Map(); // provide one if you want local mapping
    const mhl = await buildMHLStandings({ nameToSlug });
    const bshl = await buildBSHLStandings({ nameToSlug });
    await ensureDir('standings_mhl.json');
    await fs.writeFile('standings_mhl.json', JSON.stringify(mhl, null, 2));
    await ensureDir('standings_bshl.json');
    await fs.writeFile('standings_bshl.json', JSON.stringify(bshl, null, 2));
    console.log(`[standings] wrote: MHL=${mhl.rows.length} BSHL=${bshl.rows.length}`);
  })().catch(e=>{ console.error(e); process.exit(1); });
}
