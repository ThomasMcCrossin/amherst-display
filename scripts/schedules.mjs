// scripts/schedules.mjs
// Builds schedule events from:
//  - Amherst Ramblers ICS (URL via env RAMBLERS_ICS_URL or local data/ramblers.ics)
//  - BSHL schedule page (table-based parsing)
// Emits normalized events used by build_all.mjs

import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { existsSync, readFileSync } from 'fs';

/* ========= Atlantic Time Helpers (no Intl tz needed on Actions) =========
   DST in Atlantic Canada: second Sunday in March to first Sunday in November. */
function dstStart(year){ // 2nd Sunday in March, at ~local 3am (use UTC pivot)
  const d=new Date(Date.UTC(year,2,1)); const day=d.getUTCDay(); const firstSun=7-day; const secondSun=firstSun+7;
  return new Date(Date.UTC(year,2,secondSun,6,0,0));
}
function dstEnd(year){ // 1st Sunday in November, at ~local 2am (use UTC pivot)
  const d=new Date(Date.UTC(year,10,1)); const day=d.getUTCDay(); const firstSun=(7-day)%7||7;
  return new Date(Date.UTC(year,10,firstSun,5,0,0));
}
function atlanticOffsetMinutes(utcDate){
  const y=utcDate.getUTCFullYear(); const s=dstStart(y), e=dstEnd(y);
  // ADT = UTC-3 (-180) during DST, otherwise AST = UTC-4 (-240)
  return (utcDate>=s && utcDate<e)? -180 : -240;
}
function atlanticISOFromUTC(utcDate){
  const offMin=atlanticOffsetMinutes(utcDate);
  const localMs=utcDate.getTime()+offMin*60000;
  const d=new Date(localMs);
  const pad=n=>String(n).padStart(2,'0');
  const sign=offMin<=0?'-':'+'; const off=Math.abs(offMin);
  const hh=pad(Math.floor(off/60)), mm=pad(off%60);
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())}`+
         `T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}${sign}${hh}:${mm}`;
}
/** Make an ISO for a known Atlantic **local** wall-time with correct DST suffix. */
function atlanticISOFromLocalParts(year, monIdx, day, hour, min, sec=0){
  // Use a UTC "guess" on the same date to decide DST, then offset
  const guessUTC=new Date(Date.UTC(year,monIdx,day,hour,min,sec));
  const off=atlanticOffsetMinutes(guessUTC); // minutes
  const utcMs=Date.UTC(year,monIdx,day,hour,min,sec)-off*60000;
  return atlanticISOFromUTC(new Date(utcMs));
}

/* ========================= Small utils ========================= */
const norm=s=>(s||'').replace(/\s+/g,' ').trim();
const lo=s=>norm(s).toLowerCase();

async function safeFetchText(url, label){
  try{
    const r = await fetch(url, {redirect:'follow'});
    if(!r.ok){ console.warn(`[${label}] HTTP ${r.status} for ${url}`); return null; }
    return await r.text();
  }catch(e){
    console.warn(`[${label}] fetch error: ${e.message}`);
    return null;
  }
}

/* ========================= ICS (Ramblers) ========================= */
const RAMBLERS_ICS_URL   = process.env.RAMBLERS_ICS_URL || null;
const RAMBLERS_ICS_LOCAL = 'data/ramblers.ics';

async function fetchRamblersICSRaw() {
  if (RAMBLERS_ICS_URL) {
    const t = await safeFetchText(RAMBLERS_ICS_URL, 'ICS');
    if (t) return t;
  }
  if (existsSync(RAMBLERS_ICS_LOCAL)) return readFileSync(RAMBLERS_ICS_LOCAL,'utf8');
  console.warn('[ICS] No ICS provided (env RAMBLERS_ICS_URL or data/ramblers.ics).');
  return '';
}

/** Minimal VEVENT parser, preserving TZID if present. */
function parseICS(text) {
  if(!text) return [];
  const lines = text.split(/\r?\n/);
  const out = [];
  let inEvent=false, cur=null;

  for (let raw of lines) {
    const line = raw.trim();
    if (line === 'BEGIN:VEVENT') { inEvent=true; cur={ SUMMARY:'', LOCATION:'', DTSTART:null, DTEND:null, DTSTART_TZID:null, DTEND_TZID:null }; continue; }
    if (line === 'END:VEVENT')   { if (cur?.DTSTART && cur?.SUMMARY) out.push(cur); inEvent=false; cur=null; continue; }
    if (!inEvent || !cur) continue;

    const i = line.indexOf(':'); if (i<0) continue;
    const keyFull = line.slice(0,i), val = line.slice(i+1);
    const baseKey = keyFull.split(';')[0]; // DTSTART / DTEND / SUMMARY / LOCATION
    const params  = Object.fromEntries(keyFull.split(';').slice(1).map(p=>{
      const j=p.indexOf('='); return j>0 ? [p.slice(0,j).toUpperCase(), p.slice(j+1)] : [p.toUpperCase(),''];
    }));

    if (baseKey==='SUMMARY')  cur.SUMMARY  = val;
    else if (baseKey==='LOCATION') cur.LOCATION = val;
    else if (baseKey==='DTSTART') { cur.DTSTART = val; cur.DTSTART_TZID = params.TZID || null; }
    else if (baseKey==='DTEND')   { cur.DTEND   = val; cur.DTEND_TZID   = params.TZID || null; }
  }
  return out;
}

/** Parse local ICS datetime (no zone) "YYYYMMDDTHHmm[ss]" → parts */
function parseICSLocalParts(v){
  const m = v.match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})?$/);
  if(!m) return null;
  const year=+m[1], monIdx=(+m[2])-1, day=+m[3], hour=+m[4], min=+m[5], sec=m[6]? +m[6] : 0;
  return {year, monIdx, day, hour, min, sec};
}

/** Parse basic UTC "YYYYMMDDTHHmm[ss]Z" → Date (UTC). */
function parseBasicUTC(v){
  const m = v.match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})?Z$/);
  if (!m) return null;
  const [ , y, mo, d, hh, mm, ss ] = m;
  return new Date(Date.UTC(+y, +mo-1, +d, +hh, +mm, ss ? +ss : 0));
}

/** Convert ICS DTSTART/DTEND + optional TZID → ISO string in Atlantic time. */
function icsToAtlanticISO(value, tzid){
  if (!value) return null;

  // Case 1: UTC with trailing Z (may be basic or extended). Try native parse; fallback to basic parser.
  if (/Z$/.test(value)) {
    let d = new Date(value);
    if (isNaN(d)) d = parseBasicUTC(value);
    if (!d || isNaN(d)) return null;
    return atlanticISOFromUTC(d);
  }

  // Case 2: Local wall-time (with or without TZID). Treat as Atlantic local.
  const parts = parseICSLocalParts(value);
  if (!parts) return null;
  // If TZID is present and not Halifax, we still render as Halifax local (scoreboard is local).
  return atlanticISOFromLocalParts(parts.year, parts.monIdx, parts.day, parts.hour, parts.min, parts.sec);
}

function parseTeamsFromSummary(summary){
  const s=norm(summary);
  let m=s.match(/^(.*?)\s*@\s*(.*?)$/); if(m) return {homeName: norm(m[2]), awayName: norm(m[1])}; // Away @ Home
  m=s.match(/^(.*?)\s*(?:vs\.?|v)\s*(.*?)$/i); if(m) return {homeName: norm(m[1]), awayName: norm(m[2])}; // Home vs Away
  return {homeName:null, awayName:null};
}

export async function fetchRamblersFromICS({ nameToSlug }){
  try{
    const raw = await fetchRamblersICSRaw();
    const ics = parseICS(raw);
    const events = [];

    for (const ev of ics) {
      const { SUMMARY, LOCATION, DTSTART, DTEND, DTSTART_TZID, DTEND_TZID } = ev;
      const { homeName, awayName } = parseTeamsFromSummary(SUMMARY);
      if (!homeName || !awayName) continue;

      const homeSlug = nameToSlug.get(lo(homeName));
      const awaySlug = nameToSlug.get(lo(awayName));
      if (!homeSlug || !awaySlug) { console.warn('[ICS] unmapped teams:', homeName,'/',awayName); continue; }

      const startISO = icsToAtlanticISO(DTSTART, DTSTART_TZID);
      const endISO   = DTEND ? icsToAtlanticISO(DTEND, DTEND_TZID) : null;
      if (!startISO) { console.warn('[ICS] bad DTSTART:', DTSTART, 'TZID=', DTSTART_TZID); continue; }

      events.push({
        league: 'MHL',
        home_team: homeName, away_team: awayName,
        home_slug: homeSlug, away_slug: awaySlug,
        start: startISO,
        end: endISO || undefined,
        location: LOCATION || ''
      });
    }

    events.sort((a,b)=> new Date(a.start) - new Date(b.start));
    console.log(`[schedules/ICS] parsed=${events.length}`);
    return events;
  }catch(e){
    console.warn('[ICS] fatal:', e.message);
    return [];
  }
}

/* ========================= BSHL (Ducks) =========================
   Table-driven parser to avoid stray weekday/month words leaking into venue. */
const BSHL_SCHEDULE_URL = 'https://www.beausejourseniorhockeyleague.ca/schedule.php';

export async function fetchDucksFromBSHL({ nameToSlug }){
  try{
    const html = await safeFetchText(BSHL_SCHEDULE_URL, 'BSHL');
    if(!html){ console.warn('[BSHL] empty HTML'); return []; }
    const $ = cheerio.load(html);

    // Find a schedule-looking table (has Away/Home/Time headers)
    let target = null;
    $('table').each((_, tbl)=>{
      const head = ($(tbl).find('thead').text() || $(tbl).find('tr').first().text() || '').toLowerCase();
      if(head.includes('away') && head.includes('home') && (head.includes('time') || head.includes('venue') || head.includes('rink') || head.includes('arena'))){
        target = tbl; return false;
      }
    });
    if(!target){ console.warn('[BSHL] schedule table not found'); return []; }

    const headerRow = $(target).find('thead tr').first().length ? $(target).find('thead tr').first() : $(target).find('tr').first();
    const headers = [];
    headerRow.find('th,td').each((i, th) => headers.push(lo($(th).text())));

    const idx = keys => {
      for(const k of keys){ const i = headers.findIndex(h => h.includes(k)); if(i !== -1) return i; }
      return -1;
    };
    const iDate  = idx(['date']);
    const iAway  = idx(['away','visitor']);
    const iHome  = idx(['home']);
    const iTime  = idx(['time','start']);
    const iVenue = idx(['venue','rink','arena','location']);

    const monthIdx = m => ({january:0,february:1,march:2,april:3,may:4,june:5,july:6,august:7,september:8,october:9,november:10,december:11})[lo(m)] ?? null;
    const timeRe = /(\d{1,2}):(\d{2})\s*([AP]M)/i;

    const rows = $(target).find('tbody tr').length ? $(target).find('tbody tr') : $(target).find('tr').slice(1);
    const events = [];

    rows.each((_, tr)=>{
      const $td = $(tr).find('td');
      if(!$td.length) return;

      const awayN = norm($td.eq(iAway >= 0 ? iAway : 0).text());
      const homeN = norm($td.eq(iHome >= 0 ? iHome : 1).text());
      let   timeT = norm($td.eq(iTime >= 0 ? iTime : 2).text());
      let   venue = iVenue >= 0 ? norm($td.eq(iVenue).text()) : '';

      if(!awayN || !homeN) return;

      // Date
      let year, mi, day;
      if(iDate >= 0){
        const dtxt = norm($td.eq(iDate).text());               // e.g., "Saturday, October 12, 2025"
        const m = dtxt.match(/([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})/);
        if(m){ mi = monthIdx(m[1]); day = +m[2]; year = +m[3]; }
      }
      if(mi == null){ // fallback: parse from row text if no Date column
        const rowtxt = norm($(tr).text());
        const m = rowtxt.match(/([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,\s*(\d{4})/);
        if(m){ mi = monthIdx(m[1]); day = +m[2]; year = +m[3]; }
      }
      if(mi == null || !year) return; // can't schedule without a date

      // Time (default 7:00 PM if absent)
      const tm = timeT.match(timeRe);
      const HH = tm ? ((parseInt(tm[1],10) % 12) + (/p/i.test(tm[3]) ? 12 : 0)) : 19;
      const MM = tm ? parseInt(tm[2],10) : 0;

      // Clean venue: strip any lingering weekday junk
      venue = venue.replace(/\b(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday)\b.*$/i, '').trim();

      // Map team names → slugs
      const homeSlug = nameToSlug.get(lo(homeN));
      const awaySlug = nameToSlug.get(lo(awayN));
      if(!homeSlug || !awaySlug) return;

      // Build Atlantic-local ISO
      const startISO = atlanticISOFromLocalParts(year, mi, day, HH, MM, 0);

      events.push({
        league:'BSHL',
        home_team: homeN, away_team: awayN,
        home_slug: homeSlug, away_slug: awaySlug,
        start: startISO,
        location: venue
      });
    });

    events.sort((a,b)=> new Date(a.start)-new Date(b.start));
    console.log(`[schedules/BSHL] parsed=${events.length}`);
    return events;
  }catch(e){
    console.warn('[BSHL] fatal:', e.message);
    return [];
  }
}
