import fetch from 'node-fetch';
import cheerio from 'cheerio';
import { formatInTimeZone } from 'date-fns-tz';
import { writeFileSync, readFileSync } from 'fs';

const TZ = 'America/Halifax';
const nowISO = () => formatInTimeZone(new Date(), TZ, "yyyy-MM-dd'T'HH:mm:ssXXX");

// Load team directory (slug + aliases)
const TEAM_DIR = JSON.parse(readFileSync('teams.json','utf8'));
const SLUG_BY_NAME = (() => {
  const map = new Map();
  for (const t of TEAM_DIR.teams) {
    map.set(t.name.toLowerCase(), t.slug);
    for (const a of (t.aliases || [])) map.set(a.toLowerCase(), t.slug);
  }
  return map;
})();
const nameToSlug = (name) => {
  if (!name) return null;
  const key = name.replace(/\s+/g,' ').trim().toLowerCase();
  return SLUG_BY_NAME.get(key) || null;
};

// ----- SOURCE URLS (plug correct ones once; after that it's set-and-forget) -----
const SOURCES = {
  mhl: {
    standings: 'https://themhla.ca/standings',               // TODO: confirm
    ramblersSchedule: 'https://themhla.ca/stats/schedule/123'// TODO: Ramblers schedule page
  },
  bshl: {
    standings: 'https://bshlhockey.com/standings',           // TODO: confirm
    ducksSchedule: 'https://bshlhockey.com/schedule/amherst-ducks' // TODO: Ducks schedule page
  }
};

// Date helper: combine league date+time strings into Halifax ISO
function parseDateTimeLocal(dateStr, timeStr) {
  const d = new Date(`${dateStr} ${timeStr}`); // relies on site strings like "Sat, Oct 12, 2025 7:00 PM"
  return formatInTimeZone(d, TZ, "yyyy-MM-dd'T'HH:mm:ssXXX");
}

// ----- BUILDERS -----
async function buildMHLStandings() {
  const html = await (await fetch(SOURCES.mhl.standings)).text();
  const $ = cheerio.load(html);
  const rows = [];
  // TODO: adjust selector to the right standings table
  $('table tbody tr').each((_, tr) => {
    const tds = $(tr).find('td');
    if (tds.length < 6) return;
    const teamName = $(tds[0]).text().trim();
    const gp = parseInt($(tds[1]).text(), 10) || 0;
    const w  = parseInt($(tds[2]).text(), 10) || 0;
    const l  = parseInt($(tds[3]).text(), 10) || 0;
    const ot = parseInt($(tds[4]).text(), 10) || 0; // OTL or OTL+SOL depending on site
    const pts= parseInt($(tds[5]).text(), 10) || 0;
    const slug = nameToSlug(teamName);
    if (!slug) { console.warn('[MHL] Unmapped team name:', teamName); return; }
    rows.push({ team_slug: slug, gp, w, l, otl: ot, pts });
  });
  writeFileSync('standings_mhl.json', JSON.stringify({ generated_at: nowISO(), season: '', league: 'MHL', rows }, null, 2));
}

async function buildBSHLStandings() {
  const html = await (await fetch(SOURCES.bshl.standings)).text();
  const $ = cheerio.load(html);
  const rows = [];
  $('table tbody tr').each((_, tr) => {
    const tds = $(tr).find('td');
    if (tds.length < 6) return;
    const teamName = $(tds[0]).text().trim();
    const gp = parseInt($(tds[1]).text(), 10) || 0;
    const w  = parseInt($(tds[2]).text(), 10) || 0;
    const l  = parseInt($(tds[3]).text(), 10) || 0;
    const ot = parseInt($(tds[4]).text(), 10) || 0;
    const pts= parseInt($(tds[5]).text(), 10) || 0;
    const slug = nameToSlug(teamName);
    if (!slug) { console.warn('[BSHL] Unmapped team name:', teamName); return; }
    rows.push({ team_slug: slug, gp, w, l, otl: ot, pts });
  });
  writeFileSync('standings_bshl.json', JSON.stringify({ generated_at: nowISO(), season: '', league: 'BSHL', rows }, null, 2));
}

async function buildSchedules() {
  const events = [];

  // ---- Ramblers (MHL) schedule ----
  try {
    const html = await (await fetch(SOURCES.mhl.ramblersSchedule)).text();
    const $ = cheerio.load(html);
    // TODO: adjust selectors for the schedule rows
    $('table tbody tr').each((_, tr) => {
      const tds = $(tr).find('td');
      if (tds.length < 5) return;
      const dateStr  = $(tds[0]).text().trim();
      const timeStr  = $(tds[1]).text().trim();
      const homeName = $(tds[2]).text().trim();
      const awayName = $(tds[3]).text().trim();
      const venue    = $(tds[4]).text().trim();
      const homeSlug = nameToSlug(homeName);
      const awaySlug = nameToSlug(awayName);
      if (!homeSlug || !awaySlug) return;
      events.push({
        league: 'MHL',
        home_team: homeName,
        away_team: awayName,
        home_slug: homeSlug,
        away_slug: awaySlug,
        start: parseDateTimeLocal(dateStr, timeStr),
        location: venue
      });
    });
  } catch (e) { console.warn('Ramblers schedule error:', e.message); }

  // ---- Ducks (BSHL) schedule ----
  try {
    const html = await (await fetch(SOURCES.bshl.ducksSchedule)).text();
    const $ = cheerio.load(html);
    $('table tbody tr').each((_, tr) => {
      const tds = $(tr).find('td');
      if (tds.length < 5) return;
      const dateStr  = $(tds[0]).text().trim();
      const timeStr  = $(tds[1]).text().trim();
      const homeName = $(tds[2]).text().trim();
      const awayName = $(tds[3]).text().trim();
      const venue    = $(tds[4]).text().trim();
      const homeSlug = nameToSlug(homeName);
      const awaySlug = nameToSlug(awayName);
      if (!homeSlug || !awaySlug) return;
      events.push({
        league: 'BSHL',
        home_team: homeName,
        away_team: awayName,
        home_slug: homeSlug,
        away_slug: awaySlug,
        start: parseDateTimeLocal(dateStr, timeStr),
        location: venue
      });
    });
  } catch (e) { console.warn('Ducks schedule error:', e.message); }

  // Sort & write master events
  events.sort((a,b) => new Date(a.start) - new Date(b.start));
  writeFileSync('games.json', JSON.stringify({ generated_at: nowISO(), timezone: TZ, events }, null, 2));

  // Derive ROAD games (next 2–3) for Amherst teams
  const road = (slugTeam) => events
    .filter(e => e.away_slug === slugTeam)
    .slice(0, 3)
    .map(e => ({ opponent_slug: e.home_slug, start: e.start, venue: e.location || '', city: '' }));

  writeFileSync('road_games_ramblers.json', JSON.stringify({ team_slug: 'amherst-ramblers', games: road('amherst-ramblers') }, null, 2));
  writeFileSync('road_games_ducks.json',     JSON.stringify({ team_slug: 'amherst-ducks',     games: road('amherst-ducks') }, null, 2));
}

async function main() {
  await Promise.all([
    buildMHLStandings().catch(e => console.error('standings MHL', e)),
    buildBSHLStandings().catch(e => console.error('standings BSHL', e)),
  ]);
  await buildSchedules();
}
main().catch(e => { console.error(e); process.exit(1); });


