// Snap just the standings table into PNGs for clean display (no page chrome).
// Saves: assets/standings/standings_mhl.png and standings_bshl.png

import fs from 'fs/promises';
import path from 'path';
import puppeteer from 'puppeteer';

const OUT_DIR = 'assets/standings';
await fs.mkdir(OUT_DIR, { recursive: true });

const TARGETS = [
  {
    league: 'mhl',
    url: 'https://www.themhl.ca/stats/standings',
    selector: 'table',
    darkCSS: `
      html,body { background:#0b0c10 !important; }
      header,nav,footer,.cookie,.cookies,.ad,[role="banner"],[role="contentinfo"] { display:none !important; }
      table { background:#11151e !important; color:#f5f7fb !important; border-collapse:collapse; }
      th,td { border:1px solid #2a2f37 !important; padding:6px 10px !important; font-size:14px !important; }
      thead th { background:#171b23 !important; font-weight:800 !important; }
    `
  },
  {
    league: 'bshl',
    url: 'https://www.beausejourseniorhockeyleague.ca/standings.php',
    selector: 'table',
    darkCSS: `
      html,body { background:#0b0c10 !important; }
      header,nav,footer,.ad,.ads,.cookie,.cookies,[role="banner"],[role="contentinfo"] { display:none !important; }
      table { background:#11151e !important; color:#f5f7fb !important; border-collapse:collapse; }
      th,td { border:1px solid #2a2f37 !important; padding:6px 10px !important; font-size:14px !important; }
      thead th { background:#171b23 !important; font-weight:800 !important; }
    `
  }
];

function pickLargestTableRect() {
  const tables = Array.from(document.querySelectorAll('table'));
  let best = null, bestArea = 0;
  for (const t of tables) {
    const r = t.getBoundingClientRect();
    const area = r.width * r.height;
    if (area > bestArea) { best = r; bestArea = area; }
  }
  return best;
}

async function snapOne(browser, { league, url, selector, darkCSS }) {
  const page = await browser.newPage();
  await page.setViewport({ width: 1920, height: 1080, deviceScaleFactor: 2 });
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });

  // JS-rendered pages (MHL) need a moment
  await page.waitForTimeout(3500);
  await page.addStyleTag({ content: darkCSS });

  const hasTable = await page.$(selector);
  if (!hasTable) await page.waitForSelector(selector, { timeout: 10000 }).catch(()=>{});

  const rect = await page.evaluate(pickLargestTableRect);
  if (!rect || rect.width < 200 || rect.height < 100) {
    console.warn(`[snap] No suitable table on ${url} — skipping ${league}`);
    await page.close();
    return false;
  }

  const pad = 16;
  const clip = {
    x: Math.max(0, rect.x - pad),
    y: Math.max(0, rect.y - pad),
    width: rect.width + pad * 2,
    height: rect.height + pad * 2
  };

  const outPath = path.join(OUT_DIR, `standings_${league}.png`);
  await page.screenshot({ path: outPath, clip, type: 'png' });
  await page.close();
  console.log(`[snap] Saved ${outPath} (${Math.round(clip.width)}x${Math.round(clip.height)})`);
  return true;
}

(async () => {
  const browser = await puppeteer.launch({
    channel: 'chrome',                         // use the Chrome we install in the workflow
    headless: 'new',
    args: ['--no-sandbox','--disable-setuid-sandbox']
  });

  for (const tgt of TARGETS) {
    try { await snapOne(browser, tgt); }
    catch (e) { console.warn(`[snap] ${tgt.league} failed: ${e.message}`); }
  }

  await browser.close();
})();
