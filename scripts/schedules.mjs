import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { existsSync, readFileSync } from 'fs';

/* ---- Atlantic time helpers (no Intl tz needed) ---- */
// DST in Atlantic Canada: second Sunday in March to first Sunday in November.
function dstStart(year){ // 2nd Sunday in March
  const d = new Date(Date.UTC(year, 2, 1)); // Mar 1 UTC
  const day = d.getUTCDay();                // 0 Sun .. 6 Sat
  const firstSun = 7 - day;                 // days to first Sunday (1..7)
  const secondSun = firstSun + 7;           // 2nd Sunday date (8..14)
  return new Date(Date.UTC(year, 2, secondSun, 6, 0, 0)); // 3:00 local becomes 6:00 UTC-ish; safe pivot
}
function dstEnd(year){ // 1st Sunday in November
  const d = new Date(Date.UTC(year, 10, 1)); // Nov 1 UTC
  const day = d.getUTCDay();
  const firstSun = (7 - day) % 7 || 7;       // 1..7
  return new Date(Date.UTC(year, 10, firstSun, 5, 0, 0)); // 2:00 local ~ 5:00 UTC pivot
}
function atlanticOffsetMinutes(utcDate){
  const y = utcDate.getUTCFullYear();
  const start = dstStart(y), end = dstEnd(y);
  // in DST => ADT = UTC-3 (-180); else AST = UTC-4 (-240)
  return (utcDate >= start && utcDate < end) ? -180 : -240;
}
function atlanticISOFromUTC(utcDate){
  const offMin = atlanticOffsetMinutes(utcDate);
  const localMs = utcDate.getTime() + offMin*60000;
  const d = new Date(localMs);
  const pad = n => String(n).padStart(2,'0');
  const sign = offMin <= 0 ? '-' : '+';
  const off = Math.abs(offMin);
  const hh = pad(Math.floor(off/60)), mm = pad(off%60);
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())}`+
         `T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}${sign}${hh}:${mm}`;
}
/* If you already have a local Atlantic wall-time (e.g., parsed from site), make an ISO with the
   *correct zone suffix* for that calendar date. */
function atlanticISOFromLocalParts(year, monIdx, day, hour, min, sec=0){
  // pretend the supplied time is Atlantic local; find its UTC by subtracting offset
  // First guess: assume DST; we’ll compute offset using a temporary UTC guess
  const guessUTC = new Date(Date.UTC(year, monIdx, day, hour, min, sec));
  const offMin = atlanticOffsetMinutes(guessUTC);
  const utcMs = Date.UTC(year, monIdx, day, hour, min, sec) - offMin*60000;
  return atlanticISOFromUTC(new Date(utcMs));
}

// ---------------- Ramblers from ICS ----------------
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
    if (key === 'DTSTART') cur.DTSTART = val; // e.g. 20250927T220000Z (UTC)
    if (key === 'DTEND')   cur.DTEND   = val;
  }
  return out;
}

const norm = s => (s||'').replace(/\s+/g,' ').trim();
const lo = s => norm(s).toLowerCase();

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

    // ICS times are UTC (…Z). Convert to Atlantic ISO with correct DST suffix.
    const startUTC = new Date(DTSTART.replace('Z','Z'));
    const endUTC   = DTEND ? new Date(DTEND.replace('Z','Z')) : null;

    events.push({
      league: 'MHL',
      home_team: homeName, away_team: awayName,
      home_slug: homeSlug, away_slug: awaySlug,
      start: atlanticISOFromUTC(startUTC),
      end: endUTC ? atlanticISOFromUTC(endUTC) : undefined,
      location: LOCATION || ''
    });
  }
  events.sort((a,b)=> new Date(a.start) - new Date(b.start));
  return events;
}

// ---------------- Ducks from BSHL schedule page ----------------
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
  const monIdx = m => ({
    january:0,february:1,march:2,april:3,may:4,june:5,
    july:6,august:7,september:8,october:9,november:10,december:11
  })[m.toLowerCase()] ?? null;

  for (const line of lines) {
    if (!dateRe.test(line) || !line.includes('--')) continue;
    try {
      const [datePart, rest] = line.split(/(?<=\d{4})\s+/); // split after YYYY
      const m = datePart.match(dateRe);
      if (!m) continue;
      const [, , monName, dayStr, yearStr] = m;
      const mi = monIdx(monName); if (mi == null) continue;
      const day = parseInt(dayStr,10), year = parseInt(yearStr,10);

      const parts = rest.split('--').map(s => s.trim());
      if (parts.length < 3) continue;
      const awayN = parts[0];
      const homeN = parts[1];
      const timeArena = parts[2];

      const timeMatch = timeArena.match(/(\d{1,2}):(\d{2})\s*([AP]M)/i);
      const hh = timeMatch ? (parseInt(timeMatch[1],10)%12 + (/p/i.test(timeMatch[3])?12:0)) : 19;
      const mm = timeMatch ? parseInt(timeMatch[2],10) : 0;
      const venue = timeMatch ? timeArena.replace(timeMatch[0], '').trim() : timeArena.trim();

      const homeSlug = nameToSlug.get(lo(homeN));
      const awaySlug = nameToSlug.get(lo(awayN));
      if (!homeSlug || !awaySlug) continue;

      // Build Atlantic local ISO with correct DST suffix from local calendar parts:
      const startISO = atlanticISOFromLocalParts(year, mi, day, hh, mm, 0);

      events.push({
        league: 'BSHL',
        home_team: homeN, away_team: awayN,
        home_slug: homeSlug, away_slug: awaySlug,
        start: startISO,
        location: venue
      });
    } catch {/* ignore bad line */}
  }

  events.sort((a,b)=> new Date(a.start) - new Date(b.start));
  return events;
}
