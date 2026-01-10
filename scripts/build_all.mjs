// scripts/build_all.mjs
// Orchestrates: standings → schedules → JSON outputs
// - Reads teams.json (aliases -> slug map)
// - Builds MHL standings via ./standings.mjs
// - Builds games.json + next_games.json via ./schedules.mjs
// - Writes sane fallbacks if any stage fails

import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'fs';
import { fetchRamblersFromICS } from './schedules.mjs';
import { buildMHLStandings } from './standings.mjs';
import { fetchCCMHAGames } from './ccmha.mjs';
import { buildRosters } from './rosters.mjs';
import { buildRamblersGames } from './games.mjs';
import { buildLeagueStats } from './league_stats.mjs';
import { scrapeRamblersBoxScores } from './boxscores.mjs';

const TZ = 'America/Halifax';

// UTC timestamp (no ICU needed)
function nowISO(){
  const d = new Date();
  const pad = n => String(n).padStart(2,'0');
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())}T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}Z`;
}

// teams.json → name/alias → slug map (case-insensitive)
function loadTeamDirectory() {
  try{
    const raw = JSON.parse(readFileSync('teams.json','utf8'));
    const m = new Map();
    for (const t of (raw.teams||[])) {
      if (t.name) m.set(t.name.toLowerCase().trim(), t.slug);
      for (const a of (t.aliases||[])) m.set(String(a).toLowerCase().trim(), t.slug);
    }
    return m;
  }catch(e){
    console.warn('[teams] teams.json not found or invalid; continuing without alias mapping');
    return new Map();
  }
}

function writeJson(file, obj){
  writeFileSync(file, JSON.stringify(obj, null, 2));
  return file;
}

// Helpful validation + de-dup for events
function sanitizeEvents(list){
  const out = [];
  const seen = new Set();
  let bad = 0, dup = 0;

  for(const e of list){
    if(!e || !e.start || !e.home_slug || !e.away_slug){
      bad++; continue;
    }
    const t = new Date(e.start);
    if (isNaN(t)) { bad++; continue; }

    const key = `${e.league||''}|${e.home_slug}|${e.away_slug}|${new Date(e.start).toISOString()}`;
    if (seen.has(key)) { dup++; continue; }
    seen.add(key);
    out.push(e);
  }

  if (bad || dup) {
    console.warn(`[schedules] dropped ${bad} invalid and ${dup} duplicate event(s)`);
  }
  // sort ascending by start
  out.sort((a,b)=> new Date(a.start) - new Date(b.start));
  return out;
}

async function buildSchedules() {
  const nameToSlug = loadTeamDirectory();
  let ramblers = [];

  try{
    ramblers = await fetchRamblersFromICS({ nameToSlug });
  }catch(e){
    console.warn('[schedules/ICS] failed:', e.message);
  }

  let events = sanitizeEvents([ ...ramblers ]);

  writeJson('games.json', {
    generated_at: nowISO(),
    timezone: TZ,
    events
  });

  // Helper: next N FUTURE games for a team (home OR away)
  const nextN = (teamSlug, n=3) => {
    const now = new Date();
    return events
      .filter(e => (e.home_slug===teamSlug || e.away_slug===teamSlug) && new Date(e.start) >= now)
      .sort((a,b)=> new Date(a.start)-new Date(b.start))
      .slice(0,n)
      .map(e => ({
        opponent_slug: e.home_slug===teamSlug ? e.away_slug : e.home_slug,
        home: e.home_slug===teamSlug,
        start: e.start,
        venue: e.location || '',
        city: ''
      }));
  };

  writeJson('next_games.json', {
    generated_at: nowISO(),
    timezone: TZ,
    teams: [
      { team_slug: 'amherst-ramblers', games: nextN('amherst-ramblers', 3) }
    ]
  });

  console.log(`[schedules] events=${events.length}  ramblers=${ramblers.length}`);
}

async function buildStandings() {
  const nameToSlug = loadTeamDirectory();

  let mhl = { generated_at: nowISO(), league: 'MHL', season: '', rows: [] };

  try{
    const res = await buildMHLStandings({ nameToSlug });
    if (res && Array.isArray(res.rows)) mhl = res;
  }catch(e){
    console.warn('[standings/MHL] failed:', e.message);
  }

  writeJson('standings_mhl.json',  mhl);

  console.log(`[standings] mhlRows=${mhl.rows?.length||0}`);
}

async function buildCCMHA() {
  let ccmhaGames = [];

  try {
    ccmhaGames = await fetchCCMHAGames({ daysAhead: 7 });
  } catch(e) {
    console.warn('[ccmha] failed:', e.message);
  }

  writeJson('ccmha_games.json', {
    generated_at: nowISO(),
    timezone: TZ,
    games: ccmhaGames
  });

  console.log(`[ccmha] games=${ccmhaGames.length}`);
}

async function buildRostersWrapper() {
  try {
    const index = await buildRosters();
    console.log(`[rosters] Complete! Teams=${index.team_count}`);
  } catch(e) {
    console.warn('[rosters] failed:', e.message);
    // Write empty index as fallback
    const rostersDir = 'rosters';
    try {
      mkdirSync(rostersDir, { recursive: true });
    } catch {}
    writeJson('rosters/index.json', {
      generated_at: nowISO(),
      league: 'MHL',
      team_count: 0,
      teams: []
    });
  }
}

async function buildGamesWrapper() {
  try {
    const gamesData = await buildRamblersGames();
    console.log(`[games] Complete! Games=${gamesData.games.length}`);
  } catch(e) {
    console.warn('[games] failed:', e.message);
    // Write empty games file as fallback
    const gamesDir = 'games';
    try {
      mkdirSync(gamesDir, { recursive: true });
    } catch {}
    writeJson('games/amherst-ramblers.json', {
      generated_at: nowISO(),
      team_slug: 'amherst-ramblers',
      team_name: 'Amherst Ramblers',
      season: '2024-25',
      summary: {},
      games: []
    });
  }
}

async function buildLeagueStatsWrapper() {
  try {
    const stats = await buildLeagueStats();
    console.log(`[league] Complete! Leaders=${stats.leaders?.points?.length || 0}`);
  } catch(e) {
    console.warn('[league] failed:', e.message);
    // Write empty league stats as fallback
    writeJson('league_stats.json', {
      generated_at: nowISO(),
      season: '2024-25',
      league: 'MHL',
      leaders: { points: [], goals: [], assists: [], ppg: [], rookies: [] },
      goalies: { sv_pct: [], gaa: [], wins: [], shutouts: [] },
      streaks: { goals: [], points: [] },
      standings: [],
      special_teams: { powerplay: [], penaltykill: [] }
    });
  }
}

async function buildBoxScoresWrapper() {
  try {
    // BOXSCORE_LIMIT env var controls how many games to scrape (0 = all)
    const limit = parseInt(process.env.BOXSCORE_LIMIT || '0') || 0;
    const data = await scrapeRamblersBoxScores({ limit });
    const gamesWithBoxScore = data.games.filter(g => g.box_score).length;
    console.log(`[boxscores] Complete! Enhanced ${gamesWithBoxScore} games with box score data`);
  } catch(e) {
    console.warn('[boxscores] failed:', e.message);
    // Box scores are optional - games.json still has base data
  }
}

async function main(){
  // 1) Rosters (can take time, but good to do early)
  await buildRostersWrapper();

  // 2) Ramblers game summaries (from API)
  await buildGamesWrapper();

  // 3) Enhanced box scores (from Playwright scraping)
  await buildBoxScoresWrapper();

  // 4) Standings (fast feedback if selectors change)
  await buildStandings();

  // 5) Schedules
  await buildSchedules();

  // 6) CCMHA minor hockey games
  await buildCCMHA();

  // 7) League-wide stats (leaders, streaks, special teams)
  await buildLeagueStatsWrapper();

  // 8) Ensure base files exist (first run safety)
  if (!existsSync('rosters/index.json'))          writeJson('rosters/index.json',          { generated_at: nowISO(), league: 'MHL', team_count: 0, teams: [] });
  if (!existsSync('games/amherst-ramblers.json')) writeJson('games/amherst-ramblers.json', { generated_at: nowISO(), team_slug: 'amherst-ramblers', season: '2024-25', summary: {}, games: [] });
  if (!existsSync('games.json'))                  writeJson('games.json',                  { generated_at: nowISO(), timezone: TZ, events: [] });
  if (!existsSync('next_games.json'))             writeJson('next_games.json',             { generated_at: nowISO(), timezone: TZ, teams: [] });
  if (!existsSync('standings_mhl.json'))          writeJson('standings_mhl.json',          { generated_at: nowISO(), season: '', league: 'MHL',  rows: [] });
  if (!existsSync('ccmha_games.json'))            writeJson('ccmha_games.json',            { generated_at: nowISO(), timezone: TZ, games: [] });
  if (!existsSync('league_stats.json'))           writeJson('league_stats.json',           { generated_at: nowISO(), season: '2024-25', league: 'MHL', leaders: {}, goalies: {}, streaks: {}, standings: [], special_teams: {} });
}

main().catch(e => { console.error(e); process.exit(1); });
