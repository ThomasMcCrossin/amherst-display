import fetch from 'node-fetch';
import * as cheerio from 'cheerio';

// simple Atlantic timestamp (no tz lib)
function generatedNowAtlantic(){
  const now = new Date();
  // use UTC now -> attach Atlantic offset (good enough for a stamp)
  const pad = n => String(n).padStart(2,'0');
  const y = now.getUTCFullYear(), mo = pad(now.getUTCMonth()+1), da = pad(now.getUTCDate());
  const hh = pad(now.getUTCHours()), mi = pad(now.getUTCMinutes()), ss = pad(now.getUTCSeconds());
  // we don't need perfect locals here; keep UTC 'Z' to avoid ICU entirely:
  return `${y}-${mo}-${da}T${hh}:${mi}:${ss}Z`;
}

const norm = s => (s||'').replace(/\s+/g,' ').trim();
const lo = s => norm(s).toLowerCase();

function findTableByHeader($, tokens) {
  const tks = tokens.map(t => t.toLowerCase());
  let best = null;
  $('table').each((_, tbl) => {
    const headerText = lo($(tbl).find('thead').text() || $(tbl).find('tr').first().text());
    const ok = tks.every(t => headerText.includes(t));
    if (ok && !best) best = tbl;
  });
  return best;
}

function parseStandingsTable($, tableEl, nameToSlug) {
  const $tbl = $(tableEl);
  const $headRow = $tbl.find('thead tr').first().length ? $tbl.find('thead tr').first() : $tbl.find('tr').first();
  const headers = [];
  $headRow.find('th,td').each((i, th) => headers.push(lo($(th).text())));

  const idx = (names) => {
    for (const n of names) {
      const k = headers.findIndex(h => h.includes(n));
      if (k !== -1) return k;
    }
    return -1;
  };
  const iTeam = idx(['team']);
  const iGP   = idx(['gp','games']);
  const iW    = idx(['w','wins']);
  const iL    = idx(['l','losses']);
  const iOTL  = idx(['otl','ol','sol','o/sol','ot/sol']);
  const iPTS  = idx(['pts','points']);

  const rows = [];
  const $bodyRows = $tbl.find('tbody tr').length ? $tbl.find('tbody tr') : $tbl.find('tr').slice(1);
  $bodyRows.each((_, tr) => {
    const tds = $(tr).find('td');
    if (!tds.length) return;
    const teamName = norm($(tds.get(iTeam >= 0 ? iTeam : 0)).text());
    if (!teamName) return;
    const slug = nameToSlug.get(lo(teamName));
    if (!slug) return; // unmapped team; add alias if you want it rendered
    const num = (i) => i>=0 ? parseInt($(tds.get(i)).text(),10)||0 : 0;
    rows.push({ team_slug: slug, gp: num(iGP), w: num(iW), l: num(iL), otl: num(iOTL), pts: num(iPTS) });
  });
  return rows;
}

export async function buildBSHLStandings({ nameToSlug, standingsUrl = 'https://www.beausejourseniorhockeyleague.ca/standings.php' }) {
  const res = await fetch(standingsUrl);
  if (!res.ok) throw new Error(`BSHL standings HTTP ${res.status}`);
  const html = await res.text();
  const $ = cheerio.load(html);
  const table = findTableByHeader($, ['team','gp','pts']) || $('table').first();
  const rows = parseStandingsTable($, table, nameToSlug);
  return { generated_at: generatedNowAtlantic(), season: '', league: 'BSHL', rows };
}

export async function buildMHLStandings({ nameToSlug, standingsUrl = 'https://www.themhl.ca/stats/standings' }) {
  const res = await fetch(standingsUrl);
  if (!res.ok) throw new Error(`MHL standings HTTP ${res.status}`);
  const html = await res.text();
  const $ = cheerio.load(html);

  const rows = [];
  $('table').each((_, tbl) => {
    const headerTxt = lo($(tbl).find('thead').text() || $(tbl).find('tr').first().text());
    if (!headerTxt) return;
    if (headerTxt.includes('team') && (headerTxt.includes('gp') || headerTxt.includes('games'))) {
      rows.push(...parseStandingsTable($, tbl, nameToSlug));
    }
  });

  return { generated_at: generatedNowAtlantic(), season: '', league: 'MHL', rows };
}
