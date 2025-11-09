// scripts/ccmha.mjs
// Fetches CCMHA (Cumberland County Minor Hockey) games from GrayJay API
// Filters for games only (not practices) at Amherst Stadium

import fetch from 'node-fetch';

const CCMHA_API_BASE = 'https://ccmha.grayjayleagues.com';
const VENUE_FILTER = 'Amherst Stadium';

// Schedule type mapping from GrayJay API
const SCHEDULE_TYPES = {
  1: 'Practice',
  2: 'Off-Ice Training',
  3: 'Team Meeting',
  4: 'Tournament Game',
  5: 'Other',
  6: 'Evaluation',
  7: 'Game'
};

/* ========= Atlantic Time Helpers (copied from schedules.mjs) ========= */
function dstStart(year){
  const d=new Date(Date.UTC(year,2,1)); const day=d.getUTCDay(); const firstSun=7-day; const secondSun=firstSun+7;
  return new Date(Date.UTC(year,2,secondSun,6,0,0));
}
function dstEnd(year){
  const d=new Date(Date.UTC(year,10,1)); const day=d.getUTCDay(); const firstSun=(7-day)%7||7;
  return new Date(Date.UTC(year,10,firstSun,5,0,0));
}
function atlanticOffsetMinutes(utcDate){
  const y=utcDate.getUTCFullYear(); const s=dstStart(y), e=dstEnd(y);
  return (utcDate>=s && utcDate<e)? -180 : -240;
}
function atlanticISOFromLocalParts(year, monIdx, day, hour, min, sec=0){
  const guessUTC=new Date(Date.UTC(year,monIdx,day,hour,min,sec));
  const off=atlanticOffsetMinutes(guessUTC);
  const utcMs=Date.UTC(year,monIdx,day,hour,min,sec)-off*60000;
  const d=new Date(utcMs+off*60000);
  const pad=n=>String(n).padStart(2,'0');
  const sign=off<=0?'-':'+'; const absOff=Math.abs(off);
  const hh=pad(Math.floor(absOff/60)), mm=pad(absOff%60);
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())}`+
         `T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}${sign}${hh}:${mm}`;
}

/**
 * Parse date/time from CCMHA API format and convert to Atlantic timezone ISO string
 */
function parseDateTime(dateStr, timeStr) {
  if (!dateStr || !timeStr) return null;

  try {
    // dateStr format: "YYYY-MM-DD"
    // timeStr format: "HH:MM:SS"
    const [year, month, day] = dateStr.split('-').map(Number);
    const [hour, min, sec] = timeStr.split(':').map(Number);

    return atlanticISOFromLocalParts(year, month - 1, day, hour, min, sec);
  } catch (e) {
    console.warn(`[CCMHA] Could not parse date/time: ${dateStr} ${timeStr}`);
    return null;
  }
}

/**
 * Fetch CCMHA schedule from GrayJay API
 */
export async function fetchCCMHAGames({ daysAhead = 7 } = {}) {
  console.log(`[CCMHA] Fetching games for next ${daysAhead} days`);

  try {
    // Fetch from master schedule API - type 7 = games only
    const url = `${CCMHA_API_BASE}/api/teams/frontendMasterSchedule/?true=1&team_id=0&league_id=0&schedule_types=7&season_id=0&show_past=0`;

    const response = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; AmherstDisplay/1.0)'
      },
      timeout: 30000
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();

    if (data.status !== 'success') {
      throw new Error(`API returned status: ${data.status}`);
    }

    const items = data.data || [];
    console.log(`[CCMHA] API returned ${items.length} total schedule items`);

    // Filter by date range and venue
    const now = new Date();
    // Start of today (midnight) - to include all games happening today
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const endDate = new Date(today.getTime() + daysAhead * 24 * 60 * 60 * 1000);

    const games = [];

    for (const item of items) {
      // Only process games (should already be filtered by API, but double-check)
      if (!item.game_id) continue;

      // Check venue
      const venueName = item.venue_name || '';
      if (!venueName.toLowerCase().includes(VENUE_FILTER.toLowerCase())) {
        continue;
      }

      // Parse date
      const gameDate = item.game_date;
      if (!gameDate) continue;

      try {
        const itemDate = new Date(gameDate + 'T00:00:00');
        if (itemDate < today || itemDate > endDate) {
          continue;
        }
      } catch (e) {
        console.warn(`[CCMHA] Could not parse date: ${gameDate}`);
        continue;
      }

      // Parse start/end times
      const startISO = parseDateTime(item.game_date, item.game_start_time);
      const endISO = parseDateTime(item.game_date, item.game_end_time);

      if (!startISO) {
        console.warn(`[CCMHA] Missing start time for game: ${item.game_id}`);
        continue;
      }

      // Extract team names and league
      const teamA = (item.team_a_name || '').trim();
      const teamB = (item.team_b_name || '').trim();
      const league = (item.league_name || 'CCMHA').trim();

      if (!teamA || !teamB) {
        console.warn(`[CCMHA] Missing team names for game: ${item.game_id}`);
        continue;
      }

      games.push({
        league: league,
        home_team: teamA,
        away_team: teamB,
        start: startISO,
        end: endISO,
        location: venueName,
        source: 'ccmha'
      });
    }

    console.log(`[CCMHA] Found ${games.length} games at ${VENUE_FILTER}`);
    return games;

  } catch (error) {
    console.error(`[CCMHA] Failed to fetch schedule: ${error.message}`);
    return [];
  }
}
