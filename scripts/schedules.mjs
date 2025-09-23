import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { existsSync, readFileSync } from 'fs';

/* ---- Atlantic time helpers (no Intl tz needed on Actions) ---- */
function dstStart(year){ const d=new Date(Date.UTC(year,2,1)); const day=d.getUTCDay(); const firstSun=7-day; const secondSun=firstSun+7; return new Date(Date.UTC(year,2,secondSun,6,0,0)); }
function dstEnd(year){ const d=new Date(Date.UTC(year,10,1)); const day=d.getUTCDay(); const firstSun=(7-day)%7||7; return new Date(Date.UTC(year,10,firstSun,5,0,0)); }
function atlanticOffsetMinutes(utcDate){ const y=utcDate.getUTCFullYear(); const s=dstStart(y), e=dstEnd(y); return (utcDate>=s && utcDate<e)? -180 : -240; }
function atlanticISOFromUTC(utcDate){
  const offMin=atlanticOffsetMinutes(utcDate), localMs=utcDate.getTime()+offMin*60000, d=new Date(localMs);
  const pad=n=>String(n).padStart(2,'0'); const sign=offMin<=0?'-':'+'; const off=Math.abs(offMin);
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())}T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}${sign}${String(Math.floor(off/60)).padStart(2,'0')}:${String(off%60).padStart(2,'0')}`;
}
function atlanticISOFromLocalParts(year, monIdx, day, hour, min, sec=0){
  // Interpret given parts as Atlantic wall-time; compute correct DST offset for that date.
  const guessUTC = new Date(Date.UTC(year, monIdx, day, hour, min, sec));
  const offMin = atlanticOffsetMinutes(guessUTC);
  const utcMs = Date.UTC(year, monIdx, day, hour, min, sec) - offMin*60000;
  return atlanticISOFromUTC(new Date(utcMs));
}

/* ---------- small helpers ---------- */
const norm=s=>(s||'').replace(/\s+/g,' ').trim();
const lo=s=>norm(s).toLowerCase();

/* ---------- robust fetch ---------- */
async function safeFetchText(url, label){
  try{
    const r = await fetch(url, {redirect:'follow'});
    if(!r.ok){ console.warn(`[${label}] HTTP ${r.status}`); return null; }
    return await r.text();
  }catch(e){
    console.warn(`[${label}] fetch error: ${e.message}`);
    return null;
  }
}

/* ================= ICS (Ramblers) ================= */
const RAMBLERS_ICS_URL = process.env.RAMBLERS_ICS_URL || null;
const RAMBLERS_ICS_LOCAL = 'data/ramblers.ics';

async function fetchRamblersICSRaw() {
  if (RAMBLERS_ICS_URL) return await safeFetchText(RAMBLERS_ICS_URL, 'ICS') || '';
  if (existsSync(RAMBLERS_ICS_LOCAL)) return readFileSync(RAMBLERS_ICS_LOCAL,'utf8');
  console.warn('[ICS] No ICS provided');
  return '';
}

/** Parse ICS VEVENTs and keep TZID when present */
function parseICS(text) {
  if(!text) return [];
  const lines=text.split(/\r?\n/);
  const out=[]; let inEvent=false, cur=null;
  for (let raw of lines){
    const line = raw.trim();
    if(line==='BEGIN:VEVENT'){ inEvent=true; cur={ SUMMARY:'', LOCATION:'', DTSTART:null, DTEND:null, DTSTART_TZID:null, DTEND_TZID:null }; continue; }
    if(line==='END:VEVENT'){ if(cur?.DTSTART && cur?.SUMMARY) out.push(cur); inEvent=false; cur=null; continue; }
    if(!inEvent || !cur) continue;

    const i=line.indexOf(':'); if(i<0) continue;
    const keyFull=line.slice(0,i); const val=line.slice(i+1);
    const baseKey = keyFull.split(';')[0]; // DTSTART / DTEND / SUMMARY / LOCATION
    const params = Object.fromEntries(keyFull.split(';').slice(1).map(p => {
      const j=p.indexOf('='); return j>0 ? [p.slice(0,j).toUpperCase(), p.slice(j+1)] : [p.toUpperCase(), ''];
    }));

    if(baseKey==='SUMMARY') cur.SUMMARY = val;
    else if(baseKey==='LOCATION') cur.LOCATION = val;
    else if(baseKey==='DTSTART'){ cur.DTSTART = val; cur.DTSTART_TZID = params.TZID || null; }
    else if(baseKey==='DTEND'){   cur.DTEND   = val; cur.DTEND_TZID   = params.TZID || null; }
  }
  return out;
}

/** Parse an ICS local datetime string like 20251018T190000 (or 20251018T1900) into parts */
function parseICSLocalParts(v){
  const m = v.match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})?$/);
  if(!m) return null;
  const year = +m[1], monIdx = (+m[2])-1, day = +m[3], hour = +m[4], min = +m[5], sec = m[6]? +m[6] : 0;
  return {year, monIdx, day, hour, min, sec};
}

/** Convert ICS DTSTART/DTEND + optional TZID → ISO string in Atlantic time */
function icsToAtlanticISO(value, tzid){
  if(!value) return null;
  if(/Z$/.test(value)){ // already UTC
    const d = new Date(value); if(isNaN(d)) return null;
    return atlanticISOFromUTC(d);
  }
  // Local wall time, maybe with TZID=America/Halifax (or none). We treat any/unknown TZID as Halifax local.
  const parts = parseICSLocalParts(value);
  if(!parts) return null;
  return atlanticISOFromLocalParts(parts.year, parts.monIdx, parts.day, parts.hour, parts.min, parts.sec);
}

function parseTeamsFromSummary(summary){
  const s=norm(summary);
  let m=s.match(/^(.*?)\s*@\s*(.*?)$/); if(m) return {homeName:norm(m[2]), awayName:norm(m[1])};
  m=s.match(/^(.*?)\s*(?:vs\.?|v)\s*(.*?)$/i); if(m) return {homeName:norm(m[1]), awayName:norm(m[2])};
  return {homeName:null, awayName:null};
}

export async function fetchRamblersFromICS({ nameToSlug }){
  try{
    const raw=await fetchRamblersICSRaw();
    const ics=parseICS(raw);
    const events=[];
    for(const ev of ics){
      const {SUMMARY,LOCATION,DTSTART,DTEND,DTSTART_TZID,DTEND_TZID}=ev;
      const {homeName,awayName}=parseTeamsFromSummary(SUMMARY);
      if(!homeName||!awayName) continue;

      const homeSlug=nameToSlug.get(lo(homeName));
      const awaySlug=nameToSlug.get(lo(awayName));
      if(!homeSlug||!awaySlug){ console.warn('[ICS] unmapped teams:', homeName,'/',awayName); continue; }

      const startISO = icsToAtlanticISO(DTSTART, DTSTART_TZID);
      const endISO   = DTEND ? icsToAtlanticISO(DTEND, DTEND_TZID) : null;
      if(!startISO){ console.warn('[ICS] bad DTSTART:', DTSTART, 'TZID=', DTSTART_TZID); continue; }

      events.push({
        league:'MHL',
        home_team:homeName, away_team:awayName,
        home_slug:homeSlug, away_slug:awaySlug,
        start:startISO,
        end:endISO || undefined,
        location:LOCATION||''
      });
    }
    events.sort((a,b)=> new Date(a.start)-new Date(b.start));
    console.log(`[schedules/ICS] parsed=${events.length}`);
    return events;
  }catch(e){
    console.warn('[ICS] fatal:', e.message);
    return [];
  }
}

/* ================= BSHL (Ducks) ================= */
const BSHL_SCHEDULE_URL = 'https://www.beausejourseniorhockeyleague.ca/schedule.php';

export async function fetchDucksFromBSHL({ nameToSlug }){
  try{
    const html = await safeFetchText(BSHL_SCHEDULE_URL, 'BSHL');
    if(!html){ console.warn('[BSHL] empty HTML'); return []; }
    const $ = cheerio.load(html);

    const bodyText = $('body').text() || '';
    const lines = bodyText.split('\n').map(s=>s.trim()).filter(Boolean);

    const events=[];
    const dateRe=/(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),\s+([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,\s+(\d{4})/i;
    const monthIdx = m => ({january:0,february:1,march:2,april:3,may:4,june:5,july:6,august:7,september:8,october:9,november:10,december:11})[m.toLowerCase()];

    for(const line of lines){
      if(!dateRe.test(line) || !line.includes('--')) continue;
      try{
        const [datePart, restRaw] = line.split(/(?<=\d{4})\s+/);
        const m = datePart.match(dateRe);
        if(!m) continue;
        const [, , monName, dayStr, yearStr] = m;
        const mi = monthIdx(monName); if(mi==null) continue;
        const day = parseInt(dayStr,10), year=parseInt(yearStr,10);

        const parts = restRaw.split('--').map(s=>s.trim());
        if(parts.length<3) continue;
        const awayN = parts[0] || '';
        const homeN = parts[1] || '';
        const timeArena = parts[2] || '';

        const timeMatch = timeArena.match(/(\d{1,2}):(\d{2})\s*([AP]M)/i);
        const hh = timeMatch ? ((parseInt(timeMatch[1],10)%12) + (/p/i.test(timeMatch[3])?12:0)) : 19;
        const mm = timeMatch ? parseInt(timeMatch[2],10) : 0;
        const venue = timeMatch ? timeArena.replace(timeMatch[0],'').trim() : timeArena.trim();

        const homeSlug = nameToSlug.get(lo(homeN));
        const awaySlug = nameToSlug.get(lo(awayN));
        if(!homeSlug || !awaySlug) continue;

        const startISO = atlanticISOFromLocalParts(year, mi, day, hh, mm, 0);

        events.push({
          league:'BSHL',
          home_team:homeN, away_team:awayN,
          home_slug:homeSlug, away_slug:awaySlug,
          start:startISO,
          location:venue
        });
      }catch{/* skip noisy line */}
    }

    events.sort((a,b)=> new Date(a.start)-new Date(b.start));
    console.log(`[schedules/BSHL] parsed=${events.length}`);
    return events;
  }catch(e){
    console.warn('[BSHL] fatal:', e.message);
    return [];
  }
}
