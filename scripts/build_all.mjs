/**
 * Amherst Display Builder
 * - Reads Amherst Ramblers schedule from ICS (URL or local file)
 * - Scrapes Ducks (BSHL) schedule from league site
 * - Writes:
 *    - games.json        (master feed for GameBoard)
 *    - next_games.json   (Next 3 — Ramblers & Ducks)
 *    - standings_mhl.json / standings_bshl.json (stubs here; wire later if you want)
 *
 * Requirements in package.json:
 *   "node-fetch", "cheerio", "date-fns-tz"
 */

import fetch from 'node-fetch';
import cheerio from 'cheerio';
import { existsSync, readFileSync, writeFileSync } from 'fs';
import { formatInTimeZone } from 'date-fns-tz';

// --------------------------- Config ---------------------------
const TZ = 'America/Halifax';
const nowISO = () => formatInTimeZone(new Date(), TZ, "yyyy-MM-dd'T'HH:mm:ssXXX");

// 1) RAMBLERS ICS
// Option A: set a public URL in env RAMBLERS_ICS_URL
// Option B: drop the file into your repo at data/ramblers.ics
const RAMBLERS_ICS_URL = process.env.RAMBLERS_ICS_URL || null;
const RAMBLERS_ICS_LOCAL = 'data/ramblers.ics';

// 2) DUCKS (BSHL) schedule page (server-rendered, whole season)
const BSHL_SCHEDULE_URL = 'https://www.beausejourseniorhockeyleague.ca/schedule.php';

// 3) League labels (for your cards)
const LEAGUE_MHL = 'MHL';
const LEAGUE_BSHL = 'BSHL';

// ---------------------- helpers / utilities -------------------
const normalize = (s) => (s || '').replace(/\s+/g, ' ').trim();
const lower = (s) => normalize(s).toLowerCase();

/** Load teams.json and build a name->slug map using aliases */
function loadTeamDirectory() {
  const raw = JSON.parse(readFileSync('teams.json', 'utf8'));
  const teams = raw.teams || [];
  const bySlug = Object.fromEntries(teams.map(t => [t.slug, t]));
  const nameToSlug = new Map();
  for (const t of teams) {
    if (t.name) nameToSlug.set(lower(t.name), t.slug);
    for (const a of (t.aliases || [])) nameToSlug.set(lower(a), t.slug);
  }
  return { teams, bySlug, nameToSlug };
}

/** Convert a JS Date (UTC or local) to Atlantic ISO */
function toAtlanticISO(d) {
  return formatInTimeZone(d, TZ, "yyyy-MM-dd'T'HH:mm:ssXXX");
}

/** Parse a SUMMARY like "Amherst Ramblers @ Truro Bearcats" / "Home vs Away" -> {homeName, awayName} */
function parseTeamsFromSummary(summary) {
  const s = normalize(summary);
  if (!s) return { homeName: null, awayName: null };
  // common hockey formats:
  // Away @ Home
  let m = s.match(/^(.*?)\s*@\s*(.*?)$/);
  if (m) return { awayName: normalize(m[1]), homeName: normalize(m[2]) };
  // Home vs Away
  m = s.match(/^(.*?)\s*(?:vs\.?|v)\s*(.*?)$/i);
  if (m) return { homeName: normalize(m[1]), awayName: normalize(m[2]) };
  // fallback: try split on dash or en-dash — this is guessy
  m = s.split(/[-–—]/);
  if (m.length === 2) return { homeName: normalize(m[0]), awayName: normalize(m[1]) };
  return { homeName: null, awayName: null };
}

/** Given display names and alias map -> slugs (or null) */
function namesToSlugs({ homeName, awayName }, nameToSlug) {
  const home = homeName ? nameToSlug.get(lower(homeName)) : null;
  const away = awayName ? nameToSlug.get(lower(awayName)) : null;
  return { homeSlug: home || null, awaySlug: away || null };
}

// --------------------------- ICS (Ramblers) ---------------------------
async function fetchRamblersICS() {
  let icsText = '';
  if (RAMBLERS_ICS_URL) {
    const res = await fetch(RAMBLERS_ICS_URL);
    if (!res.ok) throw new Error(`ICS HTTP ${res.status}`);
    icsText = await res.text();
  } else if (existsSync(RAMBLERS_ICS_LOCAL)) {
    icsText = readFileSync(RAMBLERS_ICS_LOCAL, 'utf8');
  } else {
    console.warn('[ICS] No ICS source set (env RAMBLERS_ICS_URL or data/ramblers.ics). Returning [].');
    return [];
  }
  return parseICS(icsText);
}

/** Minimal ICS parser for the fields we need (VEVENT w/ DTSTART/DTEND/SUMMARY/LOCATION) */
function parseICS(text) {
  const lines = text.split(/\r?\n/);
  const events = [];
  let inEvent = false;
  let cur = null;

  for (let line of lines) {
    line = line.trim();
    if (line === 'BEGIN:VEVENT') {
      inEvent = true;
      cur = { SUMMARY: '', LOCATION: '', DTSTART: '', DTEND: '' };
    } else if (line === 'END:VEVENT') {
      if (cur && cur.DTSTART && cur.SUMMARY) {
        events.push({ ...cur });
      }
      inEvent = false;
      cur = null;
    } else if (inEvent && cur) {
      // Unfolded ICS may continue lines starting with space; we keep it simple since the feed is compact.
      const idx = line.indexOf(':');
      if (idx > -1) {
        const key = line.slice(0, idx);
        const val = line.slice(idx + 1);
        const k = key.split(';')[0]; // strip TZID= etc.
        if (k === 'DTSTART') cur.DTSTART = val;     // e.g., 20250927T220000Z (UTC)
        else if (k === 'DTEND') cur.DTEND = val;
        else if (k === 'SUMMARY') cur.SUMMARY = val;
        else if (k === 'LOCATION') cur.LOCATION = val;
        // STATUS/UID not required for display
      }
    }
  }
  return events;
}

function icsToEvents(icsEvents, nameToSlug) {
  const out = [];
  for (const ev of icsEvents) {
    const { SUMMARY, LOCATION, DTSTART, DTEND } = ev;
    const { homeName, awayName } = parseTeamsFromSummary(SUMMARY);
    if (!homeName || !awayName) continue;

    const { homeSlug, awaySlug } = namesToSlugs({ homeName, awayName }, nameToSlug);
    if (!homeSlug || !awaySlug) {
      console.warn('[ICS] Unmapped team(s):', homeName, awayName);
      continue;
    }

    // DTSTART/DTEND like 20250927T220000Z => Date in UTC
    const start = new Date(DTSTART.replace('Z', 'Z')); // ensure UTC
    const end = DTEND ? new Date(DTEND.replace('Z', 'Z')) : null;

    out.push({
      league: LEAGUE_MHL,
      home_team: homeName,
      away_team: awayName,
      home_slug: homeSlug,
      away_slug: awaySlug,
      start: toAtlanticISO(start),
      end: end ? toAtlanticISO(end) : undefined,
      location: LOCATION || ''
    });
  }
  // sort by start
  out.sort((a,b)=> new Date(a.start) - new Date(b.start));
  return out;
}

// ---------------------- BSHL (Ducks) scraper -----------------------
async function fetchDucksSchedule(nameToSlug) {
  const res = await fetch(BSHL_SCHEDULE_URL);
  if (!res.ok) { console.warn('[BSHL] HTTP', res.status); return []; }
  const html = await res.text();
  const $ = cheerio.load(html);

  // The schedule page contains lines with “ -- ” separators: Away -- Home -- Time Venue
  const text = $('body').text();
  const lines = text.split('\n').map(s => s.trim()).filter(Boolean);

  const events = [];
  const dateRe = /(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),\s+([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,\s+(\d{4})/i;

  for (const line of lines) {
    if (!dateRe.test(line) || !line.includes('--')) continue;
    try {
      const [datePart, restRaw] = line.split(/(?<=\d{4})\s+/); // split after the year
      const m = datePart.match(dateRe);
      if (!m) continue;
      const [, , mon, day, year] = m;
      const dateISO = `${mon} ${day}, ${year}`;

      const parts = restRaw.split('--').map(s => s.trim());
      if (parts.length < 3) continue;
      // The page uses "Away  -- Home  --  8:15 PM Venue"
      const awayN = parts[0];
      const homeN = parts[1];
      const timeArena = parts[2];

      const timeMatch = timeArena.match(/(\d{1,2}:\d{2}\s*[AP]M)/i);
      const timeStr = timeMatch ? timeMatch[1] : '7:00 PM';
      const venue = timeMatch ? timeArena.replace(timeMatch[1], '').trim() : timeArena.trim();

      const startLocal = new Date(`${dateISO} ${timeStr}`); // local parse; we convert to Atlantic ISO next
      const startISO = toAtlanticISO(startLocal);

      const homeSlug = nameToSlug.get(lower(homeN));
      const awaySlug = nameToSlug.get(lower(awayN));
      if (!homeSlug || !awaySlug) continue;

      events.push({
        league: LEAGUE_BSHL,
        home_team: homeN,
        away_team: awayN,
        home_slug: homeSlug,
        away_slug: awaySlug,
        start: startISO,
        location: venue
      });
    } catch {}
  }

  events.sort((a,b)=> new Date(a.start) - new Date(b.start));
  return events;
}

// ------------------------------ Build ------------------------------
async function main() {
  const { nameToSlug } = loadTeamDirectory();

  // 1) Ramblers from ICS
  const icsRaw = await fetchRamblersICS();                   // raw VEVENTs
  const ramblers = icsToEvents(icsRaw, nameToSlug);          // normalized events

  // 2) Ducks from BSHL page
  const ducks = await fetchDucksSchedule(nameToSlug);

  // 3) Merge & write games.json
  const events = [...ramblers, ...ducks].sort((a,b)=> new Date(a.start) - new Date(b.start));

  writeFileSync('games.json', JSON.stringify({
    generated_at: nowISO(),
    timezone: TZ,
    events
  }, null, 2));

  // 4) Write next_games.json (Next 3 for Ramblers & Ducks, home OR away)
  const nextN = (slug, n=3) => events
    .filter(e => e.home_slug === slug || e.away_slug === slug)
    .slice(0, n)
    .map(e => ({
      opponent_slug: e.home_slug === slug ? e.away_slug : e.home_slug,
      home: e.home_slug === slug,
      start: e.start,
      venue: e.location || '',
      city: ''
    }));

  const nextPayload = {
    generated_at: nowISO(),
    timezone: TZ,
    teams: [
      { team_slug: 'amherst-ramblers', games: nextN('amherst-ramblers', 3) },
      { team_slug: 'amherst-ducks',    games: nextN('amherst-ducks', 3) }
    ]
  };
  writeFileSync('next_games.json', JSON.stringify(nextPayload, null, 2));

  // 5) Leave standings as empty scaffolds (wire later)
  if (!existsSync('standings_mhl.json')) writeFileSync('standings_mhl.json', JSON.stringify({ generated_at: nowISO(), season: '', league: LEAGUE_MHL, rows: [] }, null, 2));
  if (!existsSync('standings_bshl.json')) writeFileSync('standings_bshl.json', JSON.stringify({ generated_at: nowISO(), season: '', league: LEAGUE_BSHL, rows: [] }, null, 2));

  console.log(`[OK] events=${events.length}  ramblers=${ramblers.length}  ducks=${ducks.length}`);
}

main().catch(err => { console.error(err); process.exit(1); });
