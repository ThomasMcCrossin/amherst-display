import { readFileSync, writeFileSync, existsSync } from 'fs';
import { fetchRamblersFromICS, fetchDucksFromBSHL } from './schedules.mjs';
import { buildMHLStandings, buildBSHLStandings } from './standings.mjs';

// simple generated_at (UTC stamp, avoids ICU)
function nowISO(){ const d=new Date();
  const pad=n=>String(n).padStart(2,'0');
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())}T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}Z`;
}

function loadTeamDirectory() {
  const raw = JSON.parse(readFileSync('teams.json','utf8'));
  const m = new Map();
  for (const t of (raw.teams||[])) {
    if (t.name) m.set(t.name.toLowerCase().trim(), t.slug);
    for (const a of (t.aliases||[])) m.set(String(a).toLowerCase().trim(), t.slug);
  }
  return m;
}

async function buildSchedules() {
  const nameToSlug = loadTeamDirectory();
  const ramblers = await fetchRamblersFromICS({ nameToSlug });
  const ducks    = await fetchDucksFromBSHL({ nameToSlug });
  const events = [...ramblers, ...ducks].sort((a,b)=> new Date(a.start) - new Date(b.start));

  writeFileSync('games.json', JSON.stringify({ generated_at: nowISO(), timezone: 'America/Halifax', events }, null, 2));

  const nextN = (slug, n=3) => events
    .filter(e => e.home_slug===slug || e.away_slug===slug)
    .slice(0, n)
    .map(e => ({
      opponent_slug: e.home_slug===slug ? e.away_slug : e.home_slug,
      home: e.home_slug===slug,
      start: e.start,
      venue: e.location || '',
      city: ''
    }));
  const nextPayload = {
    generated_at: nowISO(),
    timezone: 'America/Halifax',
    teams: [
      { team_slug: 'amherst-ramblers', games: nextN('amherst-ramblers', 3) },
      { team_slug: 'amherst-ducks',    games: nextN('amherst-ducks', 3) }
    ]
  };
  writeFileSync('next_games.json', JSON.stringify(nextPayload, null, 2));

  console.log(`[schedules] events=${events.length} ramblers=${ramblers.length} ducks=${ducks.length}`);
}

async function buildStandings() {
  const nameToSlug = loadTeamDirectory();
  const mhl  = await buildMHLStandings({ nameToSlug });
  const bshl = await buildBSHLStandings({ nameToSlug });
  writeFileSync('standings_mhl.json',  JSON.stringify(mhl,  null, 2));
  writeFileSync('standings_bshl.json', JSON.stringify(bshl, null, 2));
  console.log(`[standings] mhlRows=${mhl.rows?.length||0} bshlRows=${bshl.rows?.length||0}`);
}

async function main(){
  await buildStandings();
  await buildSchedules();

  // ensure files exist even on first run
  if (!existsSync('games.json'))          writeFileSync('games.json',          JSON.stringify({generated_at: nowISO(), timezone: 'America/Halifax', events: []}, null, 2));
  if (!existsSync('next_games.json'))     writeFileSync('next_games.json',     JSON.stringify({generated_at: nowISO(), timezone: 'America/Halifax', teams: []}, null, 2));
  if (!existsSync('standings_mhl.json'))  writeFileSync('standings_mhl.json',  JSON.stringify({generated_at: nowISO(), season: '', league: 'MHL', rows: []}, null, 2));
  if (!existsSync('standings_bshl.json')) writeFileSync('standings_bshl.json', JSON.stringify({generated_at: nowISO(), season: '', league: 'BSHL', rows: []}, null, 2));
}
main().catch(e => { console.error(e); process.exit(1); });
