import { writeFileSync, readFileSync } from 'fs';
import { formatInTimeZone } from 'date-fns-tz';
import { buildMHLStandings, buildBSHLStandings } from './standings.mjs';
import { fetchRamblersSchedule, fetchDucksSchedule } from './schedules.mjs';
import { readFileSync, writeFileSync, existsSync } from 'fs';
import { formatInTimeZone } from 'date-fns-tz';

const TZ = 'America/Halifax';
const nowISO = () => formatInTimeZone(new Date(), TZ, "yyyy-MM-dd'T'HH:mm:ssXXX");

function loadJSON(path, fallback){ 
  try{ return JSON.parse(readFileSync(path,'utf8')); } 
  catch{ return fallback; }
}

function sameTeams(a,b){ return a.home_slug===b.home_slug && a.away_slug===b.away_slug; }
function nearSameDay(a,b){
  const A = new Date(a.start), B = new Date(b.start);
  const diff = Math.abs(A - B) / 86400000;
  return diff <= 2; // within ±2 days
}

// Merge: manual overrides win
function mergeEvents(scraped, manual){
  const out = [...scraped];

  // Apply/insert manual
  for(const m of manual){
    const idx = out.findIndex(e => sameTeams(e,m) && nearSameDay(e,m));
    const status = (m.status||'scheduled').toLowerCase();

    if(status === 'cancelled'){
      if(idx>=0) out.splice(idx,1);
      continue;
    }
    if(status === 'postponed'){
      if(idx>=0) out.splice(idx,1);
      continue;
    }

    if(idx>=0){
      // Update existing
      const e = out[idx];
      e.start = m.revised_start || m.start || e.start;
      e.end   = m.end || e.end;
      e.location = m.location || e.location;
      e.league   = m.league || e.league;
    }else{
      // Insert new
      out.push({...m});
    }
  }

  // sort by start
  out.sort((a,b)=> new Date(a.start) - new Date(b.start));
  return out;
}

// Load team directory once (for aliases)
const TEAM_DIR = JSON.parse(readFileSync('teams.json','utf8'));
const NAME_TO_SLUG = (()=>{
  const m = new Map();
  for(const t of TEAM_DIR.teams){
    m.set(t.name.toLowerCase(), t.slug);
    for(const a of (t.aliases||[])) m.set(a.toLowerCase(), t.slug);
  }
  return m;
})();

// ---- Source URLs (plug these once) ----
const SOURCES = {
  mhlStandings: 'https://themhla.ca/standings',                   // TODO: confirm final URL
  bshlStandings: 'https://bshlhockey.com/standings',              // TODO
  ramblersSchedule: 'https://themhla.ca/stats/schedule/123',      // TODO
  ducksSchedule:     'https://bshlhockey.com/schedule/amherst-ducks' // TODO
};

async function buildStandings(){
  const mhl = await buildMHLStandings({ nameToSlug: NAME_TO_SLUG, standingsUrl: SOURCES.mhlStandings });
  const bshl= await buildBSHLStandings({ nameToSlug: NAME_TO_SLUG, standingsUrl: SOURCES.bshlStandings });
  writeFileSync('standings_mhl.json', JSON.stringify(mhl,  null, 2));
  writeFileSync('standings_bshl.json', JSON.stringify(bshl, null, 2));
}

async function buildSchedules(){
  const ev1 = await fetchRamblersSchedule({ scheduleUrl: SOURCES.ramblersSchedule, nameToSlug: NAME_TO_SLUG });
  const ev2 = await fetchDucksSchedule({     scheduleUrl: SOURCES.ducksSchedule,     nameToSlug: NAME_TO_SLUG });
  const events = [...ev1, ...ev2].sort((a,b)=> new Date(a.start) - new Date(b.start));

  // Master events (weekly board)
  writeFileSync('games.json', JSON.stringify({ generated_at: nowISO(), timezone: TZ, events }, null, 2));

  // Helper: next N games for a team (home OR away)
  const nextN = (teamSlug, n=3) => events
    .filter(e => e.home_slug===teamSlug || e.away_slug===teamSlug)
    .slice(0,n)
    .map(e => ({
      opponent_slug: e.home_slug===teamSlug ? e.away_slug : e.home_slug,
      home: e.home_slug===teamSlug,
      start: e.start,
      venue: e.location||'',
      city: ''
    }));

  // Combined next-games payload for the Next 3 slide
  const nextPayload = {
    generated_at: nowISO(),
    timezone: TZ,
    teams: [
      { team_slug: 'amherst-ramblers', games: nextN('amherst-ramblers', 3) },
      { team_slug: 'amherst-ducks',    games: nextN('amherst-ducks', 3) }
    ]
  };
  writeFileSync('next_games.json', JSON.stringify(nextPayload, null, 2));

  // Optional road files
  const road = (slugTeam) => events
    .filter(e=> e.away_slug === slugTeam)
    .slice(0,3)
    .map(e=> ({ opponent_slug: e.home_slug, start: e.start, venue: e.location||'', city: '' }));
  writeFileSync('road_games_ramblers.json', JSON.stringify({ team_slug: 'amherst-ramblers', games: road('amherst-ramblers') }, null, 2));
  writeFileSync('road_games_ducks.json',     JSON.stringify({ team_slug: 'amherst-ducks',    games: road('amherst-ducks') }, null, 2));
}

async function main(){
  await buildStandings();
  await buildSchedules();
}
main().catch(e=>{ console.error(e); process.exit(1); });
