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
      end: end ? toAtlanti
