// Render the successfully acquired season, never an independently scraped page default.
import fs from 'node:fs/promises';
import puppeteer from 'puppeteer';
import { pathToFileURL } from 'node:url';
import { config } from './hockeytech.mjs';

const escape = value => String(value ?? '').replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
export function standingsHTML(data) {
  if (data.season !== config.season_label || !Array.isArray(data.rows) || !data.rows.length || data.rows.some(r => !r.team || !r.division)) {
    throw new Error('Snapshot requires validated current-season standings');
  }
  const columns = [['team', 'Team'], ['gp', 'GP'], ['w', 'W'], ['l', 'L'], ['otl', 'OTL'], ['sol', 'SOL'], ['pts', 'PTS'], ['gf', 'GF'], ['ga', 'GA'], ['diff', '+/-']];
  const divisions = [...new Set(data.rows.map(row => row.division))];
  return `<!doctype html><html><head><meta charset="utf-8"><style>
    html,body { margin:0; background:#0b0c10; color:#f5f7fb; font:18px Arial,sans-serif; }
    main { width:1100px; padding:16px; } h1 { font-size:24px; margin:0 0 16px; }
    table { width:100%; border-collapse:collapse; background:#11151e; }
    th,td { border:1px solid #2a2f37; padding:10px; text-align:center; }
    th:first-child,td:first-child { text-align:left; } th { background:#171b23; }
    .division th { color:#c9b7ff; padding-top:16px; }
  </style></head><body><main><h1>MHL Standings — ${escape(data.season)}</h1><table>
  ${divisions.map(division => `<tbody><tr class="division"><th colspan="10">${escape(division)}</th></tr>
    <tr>${columns.map(([, label]) => `<th>${label}</th>`).join('')}</tr>
    ${data.rows.filter(row => row.division === division).map(row => `<tr>${columns.map(([key]) => `<td>${escape(row[key])}</td>`).join('')}</tr>`).join('')}</tbody>`).join('')}
  </table></main></body></html>`;
}
export async function snapshotStandings() {
  const data = JSON.parse(await fs.readFile('standings_mhl.json', 'utf8'));
  const html = standingsHTML(data);
  const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] });
  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1200, height: 1200, deviceScaleFactor: 2 });
    await page.setContent(html, { waitUntil: 'load' });
    await page.waitForSelector('table tbody tr td', { visible: true, timeout: 10000 });
    await page.evaluate(() => document.fonts.ready);
    const surface = await page.$('main');
    const rect = await surface.boundingBox();
    if (!rect || rect.width < 200 || rect.height < 100) throw new Error('No suitable standings snapshot surface');
    await fs.mkdir('assets/standings', { recursive: true });
    const output = 'assets/standings/standings_mhl.png';
    await surface.screenshot({ path: output + '.tmp', type: 'png' });
    await fs.rename(output + '.tmp', output);
    console.log(`[snap] Saved ${output} (${data.rows.length} teams, ${data.season})`);
  } finally { await browser.close(); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  snapshotStandings().catch(error => { console.error('[snap]', error.message); process.exitCode = 1; });
}
