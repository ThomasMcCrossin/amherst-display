/**
 * Amherst Display - Build Orchestrator
 * - Combines schedules + standings and emits JSONs for the apps
 *
 * Requires:
 *   scripts/schedules.mjs  -> fetchRamblersFromICS, fetchDucksFromBSHL
 *   scripts/standings.mjs  -> buildMHLStandings, buildBSHLStandings
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { formatInTimeZone } from 'date-fns-tz';
import { fetchRamblersFromICS, fetchDucksFromBSHL } from './schedules.mjs';
import { buildMHLStandings, buildBSHLStandings } from './standings.mjs';

const TZ = 'America/Halifax';
const nowISO = () => formatInTimeZone(new Date(), TZ, "yyyy-MM-dd'T'HH:mm:ssXXX");

// ---------- team directory ----------
function loadTeamDirectory() {
  const raw = JSON.parse(readFileSync('teams.json','utf8'));
  const m = new Map();
  for (const t of (raw.teams||[])) {
    if (t.name) m.set(t.name.toLowerCase().trim(), t.slug);
    for (const a of (t.aliases||[])) m.set(String(a).toLowerCase().trim(), t.slug);
  }
  return m;
}

// ---------- build schedules (games.json + next_games.json) ----------
async function buildSchedules() {
  const nameToSlug = loadTeamDirectory();

  // Ramblers via ICS (URL env RAMBLERS_ICS_URL or data/ramblers.ics)
  const ramblers = await fetchRamblersFromICS({ nameToSlug });

  // Ducks via BSHL league page
  const ducks = await fetchDucksFromBSHL({ nameToSlug });

  const events = [...ramblers, ...ducks].sort((a,b)=> new Date(a.start) - new Date(b.start));

  // games.json for the Weekly GameBoard
  writeFileSync('games.json', JSON.stringify({
    generated_at: nowISO(),
    timezone: TZ,
    events
  }, null, 2));

  // next_games.json for the Next-3 slide
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
    timezone: TZ,
    teams: [
      { team_slug: 'amherst-ramblers', games: nextN('amherst-ramblers', 3) },
      { team_slug: 'amherst-ducks',    games: nextN('amherst-ducks', 3) }
    ]
  };
  writeFileSync('next_games.json', JSON.stringify(nextPayload, null, 2));

  console.log(`[schedules] events=${events.length} ramblers=${ramblers.length} ducks=${ducks.length}`);
}

// ---------- build standings ----------
async function buildStandings() {
  const nameToSlug = loadTeamDirectory();
  const mhl  = await buildMHLStandings({ nameToSlug, standingsUrl: 'https://www.themhl.ca/stats/standings' });
  const bshl = await buildBSHLStandings({ nameToSlug, standingsUrl: 'https://www.beausejourseniorhockeyleague.ca/standings.php' });

  writeFileSync('standings_mhl.json',  JSON.stringify(mhl,  null, 2));
  writeFileSync('standings_bshl.json', JSON.stringify(bshl, null, 2));

  console.log(`[standings] mhlRows=${mhl.rows?.length||0} bshlRows=${bshl.rows?.length||0}`);
}

// ---------- entry ----------
async function main(){
  await buildStandings();
  await buildSchedules();

  // ensure files exist even on first run
  if (!existsSync('games.json'))          writeFileSync('games.json',          JSON.stringify({generated_at: nowISO(), timezone: TZ, events: []}, null, 2));
  if (!existsSync('next_games.json'))     writeFileSync('next_games.json',     JSON.stringify({generated_at: nowISO(), timezone: TZ, teams: []}, null, 2));
  if (!existsSync('standings_mhl.json'))  writeFileSync('standings_mhl.json',  JSON.stringify({generated_at: nowISO(), season: '', league: 'MHL', rows: []}, null, 2));
  if (!existsSync('standings_bshl.json')) writeFileSync('standings_bshl.json', JSON.stringify({generated_at: nowISO(), season: '', league: 'BSHL', rows: []}, null, 2));
}

main().catch(e => { console.error(e); process.exit(1); });
