import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { existsSync, readFileSync } from 'fs';
import { formatInTimeZone } from 'date-fns-tz';

const TZ = 'America/Halifax';
const toISO = (d) => formatInTimeZone(d, TZ, "yyyy-MM-dd'T'HH:mm:ssXXX");
const norm = s => (s||'').replace(/\s+/g,' ').trim();
const lo = s => norm(s).toLowerCase();

// ---- Ramblers from ICS ----
const RAMBLERS_ICS_URL = process.env.RAMBLERS_ICS_URL || null;
const RAMBLERS_ICS_LOCAL = 'data/ramblers.ics';

async function fetchRamblersICSRaw() {
  if (RAMBLERS_ICS_URL) {
    const r = await fetch(RAMBLERS_ICS_URL);
    if (!r.ok) throw new Error(`ICS HTTP ${r.status}`);
    return await r.text();
  }
  if (existsSync(RAMBLERS_ICS_LOCAL)) return readFileSync(RAMBLERS_ICS_LOCAL, 'utf8');
  return '';
}

function parseICS(text) {
  const lines = text.split(/\r?\n/);
  const out = [];
  let inEvent = false, cur = null;
  for (let line of lines) {
    line = line.trim();
    if (line === 'BEGIN:VEVENT') { inEvent = true; cur = { SUMMARY:'', LOCATION:'', DTSTART:'', DTEND:'' }; continue; }
    if (line === 'END:VEVENT')   { if (cur?.DTSTART && cur?.SUMMARY) out.push(cur); inEvent = false; cur = null; continue; }
    if (!inEvent || !cur) continue;
    const i = line.indexOf(':'); if (i < 0) continue;
    const key = line.slice(0,i).split(';')[0];
    const val = line.slice(i+1);
    if (key === 'SUMMARY') cur.SUMMARY = val;
    if (key === 'LOCATION') cur.LOCATION = val;
    if (key === 'DTSTART') cur.DTSTART = val;
    if (key === 'DTEND')   cur.DTEND   = val;
  }
  return out;
}

function parseTeamsFromSummary(summary) {
  const s = norm(summary);
  let m = s.match(/^(.*?)\s*@\s*(.*?)$/);         // Away @ Home
  if (m) return { homeName: norm(m[2]), awayName: norm(m[1]) };
  m = s.match(/^(.*?)\s*(?:vs\.?|v)\s*(.*?)$/i);  // Home vs Away
  if (m) return { homeName: norm(m[1]), awayName: norm(m[2]) };
  return { homeName: null, awayName: null };
}

export async function fetchRamblersFromICS({ nameToSlug }) {
  const raw = await fetchRamblersICSRaw();
  if (!raw) return [];
  const ics = parseICS(raw);
  const events = [];

  for (const ev of ics) {
    const { SUMMARY, LOCATION, DTSTART, DTEND } = ev;
    const { homeName, awayName } = parseTeamsFromSummary(SUMMARY);
    if (!homeName || !awayName) continue;

    const homeSlug = nameToSlug.get(lo(homeName));
    const awaySlug = nameToSlug.get(lo(awayName));
    if (!homeSlug || !awaySlug) continue;

    const start = new Date(DTSTART.replace('Z','Z'));
    const end = DTEND ? new Date(DTEND.replace('Z','Z')) : null;

    events.push({
      league: 'MHL',
      home_team: homeName, away_team: awayName,
      home_slug: homeSlug, away_slug: awaySlug,
      start: toISO(start),
      end: end ? toISO(end) : undefined,
      location: LOCATION || ''
    });
  }
  events.sort((a,b)=> new Date(a.start) - new Date(b.start));
  return events;
}

// ---- Ducks from BSHL league page ----
const BSHL_SCHEDULE_URL = 'https://www.beausejourseniorhockeyleague.ca/schedule.php';

export async function fetchDucksFromBSHL({ nameToSlug }) {
  const res = await fetch(BSHL_SCHEDULE_URL);
  if (!res.ok) return [];
  const html = await res.text();
  const $ = cheerio.load(html);

  const text = $('body').text();
  const lines = text.split('\n').map(s => s.trim()).filter(Boolean);
  const events = [];
  const dateRe = /(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),\s+([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,\s+(\d{4})/i;

  for (const line of lines) {
    if (!dateRe.test(line) || !line.includes('--')) continue;
    try {
      const [datePart, rest] = line.split(/(?<=\d{4})\s+/);
      const m = datePart.match(dateRe);
      if (!m) continue;
      const [, , mon, day, year] = m;
      const dateISO = `${mon} ${day}, ${year}`;

      const parts = rest.split('--').map(s => s.trim());
      if (parts.length < 3) continue;
      const awayN = parts[0];
      const homeN = parts[1];
      const timeArena = parts[2];

      const timeMatch = timeArena.match(/(\d{1,2}:\d{2}\s*[AP]M)/i);
      const timeStr = timeMatch ? timeMatch[1] : '7:00 PM';
      const venue = timeMatch ? timeArena.replace(timeMatch[1], '').trim() : timeArena.trim();

      const startLocal = new Date(`${dateISO} ${timeStr}`);
      const homeSlug = nameToSlug.get(lo(homeN));
      const awaySlug = nameToSlug.get(lo(awayN));
      if (!homeSlug || !awaySlug) continue;

      events.push({
        league: 'BSHL',
        home_team: homeN, away_team: awayN,
        home_slug: homeSlug, away_slug: awaySlug,
        start: toISO(startLocal),
        location: venue
      });
    } catch {/* ignore bad line */}
  }

  events.sort((a,b)=> new Date(a.start) - new Date(b.start));
  return events;
}
