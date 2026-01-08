/**
 * Amherst Ramblers MHL Display - JavaScript Logic
 * P0 Requirements:
 * - Robust data loading with cache fallback
 * - Division-first standings (top 4 per division make playoffs)
 * - 3 Stars use GAME stats, not season totals
 * - Promos only in ticker, not in slides
 */

(function() {
  'use strict';

  // ===========================================================================
  // CONFIG
  // ===========================================================================
  const CONFIG = {
    team: 'amherst-ramblers',
    teamName: 'Amherst Ramblers',
    division: 'Eastlink South',
    // GitHub raw URLs
    baseUrl: 'https://raw.githubusercontent.com/ThomasMcCrossin/amherst-display/main',
    // Cache TTL in ms
    cacheTTL: 5 * 60 * 1000, // 5 minutes
    staleOK: 30 * 60 * 1000, // 30 minutes - still show stale data
    // Slide timing (can be overridden by data-seconds)
    defaultSlideTime: 18000,
    // Refresh interval
    refreshInterval: 5 * 60 * 1000, // 5 min
  };

  // ===========================================================================
  // STATE
  // ===========================================================================
  let STATE = {
    games: [],
    roster: [],
    standings: null,
    schedule: [],
    currentSlide: 0,
    slideTimer: null,
    dataError: false,
    lastUpdate: null,
  };

  // ===========================================================================
  // UTILITIES
  // ===========================================================================

  // Escape HTML to prevent XSS - all dynamic content goes through this
  function esc(str) {
    if (str == null) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  // Safe number conversion
  function safeNum(v, fallback = 0) {
    const n = parseFloat(v);
    return isNaN(n) ? fallback : n;
  }

  // Format date like "Jan 8"
  function fmtDate(d) {
    if (!d) return '';
    const date = new Date(d);
    if (isNaN(date)) return '';
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  }

  // Format time like "7:00 PM"
  function fmtTime(d) {
    if (!d) return '';
    const date = new Date(d);
    if (isNaN(date)) return '';
    return date.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });
  }

  // Format date and time
  function fmtDateTime(d) {
    if (!d) return '';
    return fmtDate(d) + ' • ' + fmtTime(d);
  }

  // Days until a date
  function daysUntil(d) {
    if (!d) return null;
    const now = new Date();
    const target = new Date(d);
    const diff = target - now;
    return Math.ceil(diff / (1000 * 60 * 60 * 24));
  }

  // Player initials
  function initials(name) {
    if (!name) return '??';
    return name.split(' ').map(w => w[0]).join('').slice(0, 2).toUpperCase();
  }

  // Team short name
  function shortName(name) {
    if (!name) return '';
    // Handle "Amherst Ramblers" -> "Ramblers"
    const parts = name.split(' ');
    if (parts.length > 1) return parts.slice(1).join(' ');
    return name;
  }

  // Determine if game is home for Ramblers
  function isHomeGame(game) {
    if (!game) return false;
    // New format: game.home_game is a boolean
    if (typeof game.home_game === 'boolean') return game.home_game;
    // Legacy format
    const homeSlug = game.home_team_slug || game.homeTeam?.slug || '';
    return homeSlug === CONFIG.team;
  }

  // Get opponent from game
  function getOpponent(game) {
    if (!game) return { name: 'TBD', slug: '' };
    // New format: opponent is an object
    if (game.opponent) {
      const code = game.opponent.team_code || '';
      return {
        name: game.opponent.team_name || 'TBD',
        slug: code ? code.toLowerCase() : '',
        code,
      };
    }
    // Legacy format
    if (isHomeGame(game)) {
      return {
        name: game.away_team || game.awayTeam?.name || 'TBD',
        slug: game.away_team_slug || game.awayTeam?.slug || '',
      };
    }
    return {
      name: game.home_team || game.homeTeam?.name || 'TBD',
      slug: game.home_team_slug || game.homeTeam?.slug || '',
    };
  }

  // Get our score from game
  function getOurScore(game) {
    if (!game) return null;
    // New format: result.ramblers_score
    if (game.result) return game.result.ramblers_score ?? null;
    // Legacy format
    if (isHomeGame(game)) {
      return game.home_score ?? game.homeScore ?? null;
    }
    return game.away_score ?? game.awayScore ?? null;
  }

  // Get their score
  function getTheirScore(game) {
    if (!game) return null;
    // New format: result.opponent_score
    if (game.result) return game.result.opponent_score ?? null;
    // Legacy format
    if (isHomeGame(game)) {
      return game.away_score ?? game.awayScore ?? null;
    }
    return game.home_score ?? game.homeScore ?? null;
  }

  // Did we win?
  function didWeWin(game) {
    // New format: result.won
    if (game?.result?.won !== undefined) return game.result.won;
    // Legacy format
    const ours = getOurScore(game);
    const theirs = getTheirScore(game);
    if (ours == null || theirs == null) return null;
    return ours > theirs;
  }

  // Check if game has been played (has a result)
  function isCompleted(game) {
    if (!game) return false;
    return game.result != null || game.home_score != null || game.homeScore != null;
  }

  // Logo URL for team
  function logoUrl(slug) {
    if (!slug) return '';
    return `${CONFIG.baseUrl}/logos/${slug}.png`;
  }

  // Headshot URL for player
  function headshotUrl(playerId) {
    if (!playerId) return '';
    return `${CONFIG.baseUrl}/headshots/${playerId}.png`;
  }

  // ===========================================================================
  // DATA LOADING
  // ===========================================================================

  async function fetchJson(path) {
    const url = `${CONFIG.baseUrl}/${path}`;
    const cacheKey = 'cache_' + path.replace(/[^a-z0-9]/gi, '_');

    try {
      const resp = await fetch(url, { cache: 'no-store' });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();

      // Cache successful response
      try {
        localStorage.setItem(cacheKey, JSON.stringify({
          ts: Date.now(),
          data,
        }));
      } catch (e) { /* localStorage might be full */ }

      return data;
    } catch (err) {
      console.warn(`[fetch] ${path} failed:`, err.message);

      // Try cache fallback
      try {
        const cached = localStorage.getItem(cacheKey);
        if (cached) {
          const { ts, data } = JSON.parse(cached);
          const age = Date.now() - ts;
          if (age < CONFIG.staleOK) {
            console.log(`[fetch] Using cached data for ${path} (age: ${Math.round(age / 1000)}s)`);
            return data;
          }
        }
      } catch (e) { /* cache parse failed */ }

      throw err;
    }
  }

  async function loadAllData() {
    const results = await Promise.allSettled([
      fetchJson(`games/${CONFIG.team}.json`),
      fetchJson(`rosters/${CONFIG.team}.json`),
      fetchJson('standings_mhl.json'),
      fetchJson('schedule/schedule_mhl.json'),
    ]);

    const [gamesRes, rosterRes, standingsRes, scheduleRes] = results;

    if (gamesRes.status === 'fulfilled') {
      STATE.games = gamesRes.value?.games || gamesRes.value || [];
    }
    if (rosterRes.status === 'fulfilled') {
      STATE.roster = rosterRes.value?.roster || rosterRes.value?.players || rosterRes.value || [];
    }
    if (standingsRes.status === 'fulfilled') {
      STATE.standings = standingsRes.value;
    }
    if (scheduleRes.status === 'fulfilled') {
      STATE.schedule = scheduleRes.value?.games || scheduleRes.value || [];
    }

    STATE.lastUpdate = new Date();
    STATE.dataError = results.every(r => r.status === 'rejected');

    console.log('[data] Loaded:', {
      games: STATE.games.length,
      roster: STATE.roster.length,
      standings: STATE.standings?.rows?.length || 0,
      schedule: STATE.schedule.length,
    });
  }

  // ===========================================================================
  // CLOCK
  // ===========================================================================

  function updateClock() {
    const now = new Date();
    const clock = document.getElementById('clock');
    const date = document.getElementById('date');
    if (clock) {
      clock.textContent = now.toLocaleTimeString('en-US', {
        hour: 'numeric',
        minute: '2-digit',
      });
    }
    if (date) {
      date.textContent = now.toLocaleDateString('en-US', {
        weekday: 'short',
        month: 'short',
        day: 'numeric',
      });
    }
  }

  // ===========================================================================
  // STATUS BAR
  // ===========================================================================

  function updateStatus() {
    const dot = document.getElementById('statusDot');
    const text = document.getElementById('statusText');

    if (STATE.dataError) {
      if (dot) dot.style.background = 'var(--bad)';
      if (text) text.innerHTML = 'Data unavailable - using cached data';
      return;
    }

    // Find next game (not yet completed)
    const now = new Date();
    const upcoming = STATE.games
      .filter(g => !isCompleted(g))
      .sort((a, b) => new Date(a.date || a.date_time || a.game_date) - new Date(b.date || b.date_time || b.game_date));

    if (upcoming.length === 0) {
      if (dot) dot.style.background = 'var(--muted)';
      if (text) text.textContent = 'Season complete';
      return;
    }

    const next = upcoming[0];
    const opponent = getOpponent(next);
    const days = daysUntil(next.date || next.game_date);

    if (dot) dot.style.background = 'var(--good)';
    if (text) {
      if (days === 0) {
        text.innerHTML = `<b>Game Day!</b> vs ${esc(shortName(opponent.name))} • ${esc(fmtTime(next.date || next.game_date))}`;
      } else if (days === 1) {
        text.innerHTML = `<b>Tomorrow</b> vs ${esc(shortName(opponent.name))} • ${esc(fmtTime(next.date || next.game_date))}`;
      } else {
        text.innerHTML = `<b>Next</b> vs ${esc(shortName(opponent.name))} • ${esc(fmtDate(next.date || next.game_date))}`;
      }
    }
  }

  // ===========================================================================
  // DIVISION META
  // ===========================================================================

  function updateDivisionMeta() {
    const divMeta = document.getElementById('divisionMeta');
    const ptsMeta = document.getElementById('pointsMeta');

    if (!STATE.standings?.rows) {
      if (divMeta) divMeta.textContent = '--';
      if (ptsMeta) ptsMeta.textContent = '--';
      return;
    }

    // Find Ramblers in standings
    const ramblers = STATE.standings.rows.find(r =>
      r.slug === CONFIG.team || (r.team || '').toLowerCase().includes('amherst')
    );

    if (!ramblers) {
      if (divMeta) divMeta.textContent = '--';
      if (ptsMeta) ptsMeta.textContent = '--';
      return;
    }

    // Get division rank (from our South division only)
    const southTeams = STATE.standings.rows
      .filter(r => r.division === CONFIG.division || (r.team || '').toLowerCase().includes('amherst'))
      .sort((a, b) => (b.pts || 0) - (a.pts || 0));

    const divRank = southTeams.findIndex(t =>
      t.slug === CONFIG.team || (t.team || '').toLowerCase().includes('amherst')
    ) + 1;

    if (divMeta) {
      divMeta.textContent = divRank > 0 ? `#${divRank} South` : '--';
    }
    if (ptsMeta) {
      ptsMeta.textContent = `${ramblers.pts || 0} PTS`;
    }
  }

  // ===========================================================================
  // SLIDE: NEXT UP
  // ===========================================================================

  function renderNextUp() {
    const hero = document.getElementById('nextHero');
    const lastFive = document.getElementById('lastFive');
    const stadiumNext = document.getElementById('stadiumNext');
    const nextSub = document.getElementById('nextSub');

    if (!hero || !lastFive || !stadiumNext) return;

    const now = new Date();
    const games = STATE.games || [];

    // Past games (completed games, most recent first)
    const past = games
      .filter(g => isCompleted(g))
      .sort((a, b) => new Date(b.date || b.date_time || b.game_date) - new Date(a.date || a.date_time || a.game_date));

    // Future games (not completed)
    const future = games
      .filter(g => !isCompleted(g))
      .sort((a, b) => new Date(a.date || a.date_time || a.game_date) - new Date(b.date || b.date_time || b.game_date));

    // Home games upcoming
    const homeGames = future.filter(g => isHomeGame(g)).slice(0, 3);

    // Next game hero
    if (future.length > 0) {
      const next = future[0];
      const opponent = getOpponent(next);
      const gameDate = new Date(next.date || next.game_date);
      const days = daysUntil(next.date || next.game_date);

      let countdown = '';
      if (days === 0) countdown = 'Today';
      else if (days === 1) countdown = 'Tomorrow';
      else if (days < 7) countdown = `${days} days`;
      else countdown = fmtDate(next.date || next.game_date);

      hero.innerHTML = `
        <div class="matchTop">
          <div>
            <div class="bigTitle">${esc(isHomeGame(next) ? 'vs' : '@')} ${esc(shortName(opponent.name))}</div>
            <div class="bigSub">${esc(fmtDateTime(next.date || next.game_date))}</div>
          </div>
          <div class="countdown">
            <div class="t">${esc(countdown)}</div>
            <div class="l">${days === 0 ? 'Game Day' : 'Until puck drop'}</div>
          </div>
        </div>
        <div class="matchMid">
          <div class="bigTeam">
            <div class="logo"><img src="${esc(logoUrl(CONFIG.team))}" alt="" onerror="this.style.display='none'"></div>
            <div><div class="nm">Ramblers</div><div class="sub">${esc(isHomeGame(next) ? 'HOME' : 'AWAY')}</div></div>
          </div>
          <div class="vs">VS</div>
          <div class="bigTeam" style="flex-direction:row-reverse;text-align:right">
            <div class="logo"><img src="${esc(logoUrl(opponent.slug))}" alt="" onerror="this.style.display='none'"></div>
            <div><div class="nm">${esc(shortName(opponent.name))}</div><div class="sub">${esc(isHomeGame(next) ? 'AWAY' : 'HOME')}</div></div>
          </div>
        </div>
        <div class="matchBottom">
          <div class="miniMetrics">
            <div class="pill"><span class="ic">🏟</span>${esc(next.venue || (isHomeGame(next) ? 'Amherst Stadium' : 'Away'))}</div>
          </div>
        </div>
      `;
    } else {
      hero.innerHTML = '<div class="bigTitle">Season Complete</div>';
    }

    // Last 5 games
    lastFive.innerHTML = past.slice(0, 5).map(g => {
      const opponent = getOpponent(g);
      const won = didWeWin(g);
      const ourScore = getOurScore(g);
      const theirScore = getTheirScore(g);
      return `
        <div class="row">
          <div class="team">
            <div class="logo"><img src="${esc(logoUrl(opponent.slug))}" alt="" onerror="this.style.display='none'"></div>
            <div>
              <div class="nm">${esc(shortName(opponent.name))}</div>
              <div class="sub">${esc(fmtDate(g.date || g.game_date))} • ${esc(isHomeGame(g) ? 'H' : 'A')}</div>
            </div>
          </div>
          <div class="badge ${won ? 'good' : 'bad'}">${won ? 'W' : 'L'} ${esc(ourScore)}-${esc(theirScore)}</div>
        </div>
      `;
    }).join('') || '<div class="row">No recent games</div>';

    // Next home games
    stadiumNext.innerHTML = homeGames.map(g => {
      const opponent = getOpponent(g);
      return `
        <div class="row">
          <div class="team">
            <div class="logo"><img src="${esc(logoUrl(opponent.slug))}" alt="" onerror="this.style.display='none'"></div>
            <div>
              <div class="nm">vs ${esc(shortName(opponent.name))}</div>
              <div class="sub">${esc(fmtDateTime(g.date || g.game_date))}</div>
            </div>
          </div>
        </div>
      `;
    }).join('') || '<div class="row">No upcoming home games</div>';

    // Subtitle
    if (nextSub) {
      const record = past.length > 0 ?
        `${past.filter(g => didWeWin(g)).length}-${past.filter(g => didWeWin(g) === false).length}` :
        '--';
      nextSub.textContent = `Season Record: ${record}`;
    }
  }

  // ===========================================================================
  // SLIDE: FACES (Top Performers)
  // ===========================================================================

  // Helper to get skater stats (nested under .stats in roster data)
  function skaterStats(p) {
    return {
      goals: safeNum(p.stats?.goals || p.goals, 0),
      assists: safeNum(p.stats?.assists || p.assists, 0),
      points: safeNum(p.stats?.points || p.points, 0) || (safeNum(p.stats?.goals || p.goals, 0) + safeNum(p.stats?.assists || p.assists, 0)),
      gp: safeNum(p.stats?.games_played || p.gp || p.games_played, 0),
    };
  }

  function renderFaces() {
    const grid = document.getElementById('facesGrid');
    if (!grid) return;

    // Get skaters sorted by points (stats are nested under .stats)
    const skaters = (STATE.roster || [])
      .filter(p => (p.position || '').toUpperCase() !== 'G')
      .map(p => ({
        ...p,
        ...skaterStats(p),
      }))
      .sort((a, b) => b.points - a.points)
      .slice(0, 9);

    grid.innerHTML = skaters.map(p => `
      <div class="faceCard">
        <div class="hs">
          <img src="${esc(headshotUrl(p.player_id))}" alt="" onerror="this.nextElementSibling.style.display='flex';this.style.display='none'">
          <div class="ini" style="display:none">${esc(initials(p.name))}</div>
        </div>
        <div class="who">
          <div class="playerName">${esc(p.name)}</div>
          <div class="playerSub">#${esc(p.number || '?')} • ${esc(p.position || '?')}</div>
        </div>
        <div class="statBox">
          <div class="big">${p.points}</div>
          <div class="lbl">PTS</div>
        </div>
      </div>
    `).join('') || '<div class="faceCard">No roster data</div>';
  }

  // ===========================================================================
  // SLIDE: RECAP (Last Game)
  // - P0: 3 Stars must use GAME stats, not season totals
  // ===========================================================================

  function computeThreeStars(game, roster) {
    // Build player lookup from roster
    const playerById = new Map();
    (roster || []).forEach(p => {
      const id = String(p.id || p.player_id || '');
      if (id) playerById.set(id, p);
    });

    // Get player stats from THIS GAME (not season stats!)
    const playerStats = game.player_stats || game.playerStats || {};
    const candidates = [];

    for (const [playerId, stats] of Object.entries(playerStats)) {
      const player = playerById.get(String(playerId));
      const name = player?.name || player?.player_name || stats.name || `Player ${playerId}`;
      const position = stats.position || player?.position || '';
      const isGoalie = position.toUpperCase() === 'G';

      if (isGoalie) {
        // Goalie stats from this game
        const saves = safeNum(stats.saves, 0);
        const shotsAgainst = safeNum(stats.shots_against || stats.shotsAgainst, 0);
        const ga = safeNum(stats.goals_against || stats.goalsAgainst, 0);

        if (shotsAgainst > 0) {
          const svPct = saves / shotsAgainst;
          candidates.push({
            name,
            jersey: player?.number || player?.jersey || player?.jersey_number || '',
            position: 'G',
            statLine: `${saves} SV`,
            statLabel: `${(svPct * 100).toFixed(1)}% • ${ga} GA`,
            score: saves * 2 + (ga === 0 ? 50 : 0) + (svPct * 100),
            isGoalie: true,
          });
        }
      } else {
        // Skater stats from this game
        const goals = safeNum(stats.goals, 0);
        const assists = safeNum(stats.assists, 0);
        const points = goals + assists;

        if (points > 0 || goals > 0) {
          let statLine = '';
          if (goals > 0) statLine += `${goals}G`;
          if (assists > 0) statLine += (statLine ? ' ' : '') + `${assists}A`;

          candidates.push({
            name,
            jersey: player?.number || player?.jersey || player?.jersey_number || '',
            position: position || '?',
            statLine: statLine || '--',
            statLabel: 'GAME',
            score: goals * 3 + assists * 2,
            isGoalie: false,
          });
        }
      }
    }

    // Sort by score
    candidates.sort((a, b) => b.score - a.score);
    return candidates.slice(0, 3);
  }

  function renderRecap() {
    const recapScore = document.getElementById('recapScore');
    const recapStory = document.getElementById('recapStory');
    const threeStars = document.getElementById('threeStars');
    const scoringSummary = document.getElementById('scoringSummary');
    const recapSub = document.getElementById('recapSub');
    const starsLabel = document.getElementById('starsLabel');

    if (!recapScore) return;

    const games = STATE.games || [];

    // Find most recent completed game
    const past = games
      .filter(g => isCompleted(g))
      .sort((a, b) => new Date(b.date || b.date_time || b.game_date) - new Date(a.date || a.date_time || a.game_date));

    if (past.length === 0) {
      recapScore.innerHTML = '<div class="bigTitle">No recent games</div>';
      if (threeStars) threeStars.innerHTML = '';
      if (scoringSummary) scoringSummary.innerHTML = '';
      return;
    }

    const lastGame = past[0];
    const opponent = getOpponent(lastGame);
    const won = didWeWin(lastGame);
    const ourScore = getOurScore(lastGame);
    const theirScore = getTheirScore(lastGame);

    // Score display
    recapScore.innerHTML = `
      <div class="teams">
        <div class="team">
          <div class="logo"><img src="${esc(logoUrl(CONFIG.team))}" alt="" onerror="this.style.display='none'"></div>
          <div><div class="nm">Ramblers</div><div class="sub">${esc(isHomeGame(lastGame) ? 'HOME' : 'AWAY')}</div></div>
        </div>
        <div class="team">
          <div class="logo"><img src="${esc(logoUrl(opponent.slug))}" alt="" onerror="this.style.display='none'"></div>
          <div><div class="nm">${esc(shortName(opponent.name))}</div><div class="sub">${esc(isHomeGame(lastGame) ? 'AWAY' : 'HOME')}</div></div>
        </div>
      </div>
      <div>
        <div class="scoreBig">${esc(ourScore)}<span class="sep">-</span>${esc(theirScore)}</div>
        <div class="badge ${won ? 'good' : 'bad'}" style="margin-top:10px">${won ? 'WIN' : 'LOSS'}</div>
      </div>
    `;

    // Story/recap text
    if (recapStory) {
      const story = lastGame.recap || lastGame.story || lastGame.summary;
      if (story) {
        recapStory.innerHTML = `<p>${esc(story)}</p>`;
      } else {
        const desc = won
          ? `The Ramblers defeated the ${shortName(opponent.name)} ${ourScore}-${theirScore}.`
          : `The Ramblers fell to the ${shortName(opponent.name)} ${theirScore}-${ourScore}.`;
        recapStory.innerHTML = `<p>${esc(desc)}</p><p class="faint">${esc(fmtDate(lastGame.date || lastGame.game_date))} • ${esc(lastGame.venue || 'MHL')}</p>`;
      }
    }

    // 3 Stars - USE GAME STATS (P0 requirement!)
    if (threeStars) {
      // Check if game has official 3 stars
      const officialStars = lastGame.three_stars || lastGame.threeStars || [];

      if (officialStars.length >= 3) {
        // Use official stars
        if (starsLabel) starsLabel.textContent = '3 Stars (Official)';
        threeStars.innerHTML = officialStars.slice(0, 3).map((star, i) => `
          <div class="leaderRow">
            <div class="rank">${i + 1}</div>
            <div class="hs">
              <img src="${esc(headshotUrl(star.player_id || star.id))}" alt="" onerror="this.nextElementSibling.style.display='flex';this.style.display='none'">
              <div class="ini" style="display:none">${esc(initials(star.name || star.player_name))}</div>
            </div>
            <div class="who">
              <div class="playerName">${esc(star.name || star.player_name)}</div>
              <div class="playerSub">${esc(star.stat_line || star.statLine || '')}</div>
            </div>
          </div>
        `).join('');
      } else {
        // Compute from GAME stats
        if (starsLabel) starsLabel.textContent = 'Top Performers (Game)';
        const computed = computeThreeStars(lastGame, STATE.roster);

        if (computed.length > 0) {
          threeStars.innerHTML = computed.map((star, i) => `
            <div class="leaderRow">
              <div class="rank">${i + 1}</div>
              <div class="who">
                <div class="playerName">${esc(star.name)}</div>
                <div class="playerSub">#${esc(star.jersey)} • ${esc(star.position)}</div>
              </div>
              <div class="statBox">
                <div class="big">${esc(star.statLine)}</div>
                <div class="lbl">${esc(star.statLabel)}</div>
              </div>
            </div>
          `).join('');
        } else {
          threeStars.innerHTML = '<div class="leaderRow">No game stats available</div>';
        }
      }
    }

    // Period-by-period scoring
    if (scoringSummary) {
      const scoring = lastGame.scoring || lastGame.period_scoring || [];
      const periods = { '1': [], '2': [], '3': [], 'OT': [] };

      scoring.forEach(g => {
        const period = String(g.period || g.per || '1');
        const key = period === '4' || period.toLowerCase() === 'ot' ? 'OT' : period;
        if (periods[key]) periods[key].push(g);
      });

      const periodSummary = ['1', '2', '3', 'OT']
        .filter(p => periods[p].length > 0 || p !== 'OT')
        .map(p => {
          const goals = periods[p];
          // New format: team is "ramblers" or "opponent"
          // Legacy: team_slug or team name
          const ourGoals = goals.filter(g => {
            const team = (g.team || '').toLowerCase();
            return team === 'ramblers' ||
              team === 'amherst' ||
              g.team_slug === CONFIG.team ||
              team.includes('amherst') ||
              team.includes('ramblers');
          }).length;
          const theirGoals = goals.length - ourGoals;
          return { period: p, our: ourGoals, their: theirGoals };
        });

      scoringSummary.innerHTML = periodSummary.map(p => `
        <div class="row">
          <div><b>${p.period === 'OT' ? 'OT' : `P${p.period}`}</b></div>
          <div class="meta">
            <span>AMH: <b>${p.our}</b></span>
            <span>OPP: <b>${p.their}</b></span>
          </div>
        </div>
      `).join('') || '<div class="row">No scoring data</div>';
    }

    // Subtitle
    if (recapSub) {
      recapSub.textContent = fmtDate(lastGame.date || lastGame.game_date);
    }
  }

  // ===========================================================================
  // SLIDE: STANDINGS (Division-first - P0 requirement!)
  // ===========================================================================

  function renderStandings() {
    const southTable = document.getElementById('southTable');
    const northTable = document.getElementById('northTable');

    if (!southTable || !northTable) return;

    const rows = STATE.standings?.rows || [];

    // Determine division for each team
    const southTeams = [];
    const northTeams = [];

    rows.forEach(team => {
      // Use explicit division field, or detect from team name
      const div = team.division ||
        (SOUTH_TEAMS.some(t => (team.team || '').toLowerCase().includes(t)) ? 'Eastlink South' : 'Eastlink North');

      if (div === 'Eastlink South') {
        southTeams.push(team);
      } else {
        northTeams.push(team);
      }
    });

    // Sort by points
    southTeams.sort((a, b) => (b.pts || 0) - (a.pts || 0));
    northTeams.sort((a, b) => (b.pts || 0) - (a.pts || 0));

    // Render each division table
    const renderDivision = (teams, container, isOurDivision) => {
      const header = `
        <div class="hdr">
          <div>#</div>
          <div>Team</div>
          <div>GP</div>
          <div>W</div>
          <div>L</div>
          <div>PTS</div>
          <div>DIFF</div>
        </div>
      `;

      const rowsHtml = teams.map((team, i) => {
        const isUs = team.slug === CONFIG.team ||
          (team.team || '').toLowerCase().includes('amherst');
        const rank = i + 1;
        // Top 4 make playoffs - show cutline after 4th
        const isCutline = rank === 4;

        return `
          <div class="r ${isUs ? 'me' : ''} ${isCutline ? 'cutline' : ''}">
            <div class="pos">${rank}</div>
            <div class="tn">
              <div class="logo"><img src="${esc(logoUrl(team.slug))}" alt="" onerror="this.style.display='none'"></div>
              <div class="nm">${esc(shortName(team.team))}</div>
            </div>
            <div class="num">${team.gp || 0}</div>
            <div class="num">${team.w || 0}</div>
            <div class="num">${team.l || 0}</div>
            <div class="num">${team.pts || 0}</div>
            <div class="num">${team.diff > 0 ? '+' : ''}${team.diff || 0}</div>
          </div>
        `;
      }).join('');

      container.innerHTML = header + rowsHtml;
    };

    renderDivision(southTeams, southTable, true);
    renderDivision(northTeams, northTable, false);
  }

  // South division teams for detection if division field is missing
  const SOUTH_TEAMS = [
    'amherst', 'ramblers', 'truro', 'bearcats', 'pictou', 'crushers',
    'yarmouth', 'mariners', 'valley', 'wildcats', 'south shore', 'lumberjacks'
  ];

  // ===========================================================================
  // SLIDE: GOALIES
  // ===========================================================================

  // Helper to get goalie stats (nested under .stats in roster data)
  function goalieStats(g) {
    return {
      gp: safeNum(g.stats?.games_played || g.gp || g.games_played, 0),
      wins: safeNum(g.stats?.wins || g.w || g.wins, 0),
      losses: safeNum(g.stats?.losses || g.l || g.losses, 0),
      svPct: safeNum(g.stats?.save_percentage || g.sv_pct || g.save_percentage, 0),
      gaa: safeNum(g.stats?.goals_against_average || g.gaa || g.goals_against_avg, 0),
    };
  }

  function renderGoalies() {
    const matchup = document.getElementById('goalieMatchup');
    const list = document.getElementById('goalieList');
    const story = document.getElementById('goalieStory');

    if (!matchup || !list) return;

    // Get goalies from roster, sort by games played
    const goalies = (STATE.roster || [])
      .filter(p => (p.position || '').toUpperCase() === 'G')
      .sort((a, b) => goalieStats(b).gp - goalieStats(a).gp);

    if (goalies.length === 0) {
      matchup.innerHTML = '<div>No goalie data</div>';
      list.innerHTML = '';
      return;
    }

    // Main goalie matchup (next game)
    const future = (STATE.games || [])
      .filter(g => !isCompleted(g))
      .sort((a, b) => new Date(a.date || a.date_time || a.game_date) - new Date(b.date || b.date_time || b.game_date));

    const nextGame = future[0];
    const ourGoalie = goalies[0];
    const ourStats = goalieStats(ourGoalie);

    if (nextGame) {
      const opponent = getOpponent(nextGame);
      matchup.innerHTML = `
        <div class="goalieDuel">
          <div class="goalieCard">
            <div class="hs">
              <img src="${esc(headshotUrl(ourGoalie.player_id))}" alt="" onerror="this.nextElementSibling.style.display='flex';this.style.display='none'">
              <div class="ini" style="display:none">${esc(initials(ourGoalie.name))}</div>
            </div>
            <div class="who">
              <div class="playerName">${esc(ourGoalie.name)}</div>
              <div class="playerSub">#${esc(ourGoalie.number || '?')}</div>
              <div class="goalieMini">
                <div class="gmini"><div class="k">SV%</div><div class="v">${ourStats.svPct.toFixed(3).slice(1)}</div></div>
                <div class="gmini"><div class="k">GAA</div><div class="v">${ourStats.gaa.toFixed(2)}</div></div>
              </div>
            </div>
          </div>
          <div class="vs">VS</div>
          <div class="goalieCard" style="flex-direction:row-reverse;text-align:right">
            <div class="hs">
              <div class="ini">${esc(shortName(opponent.name).slice(0, 2).toUpperCase())}</div>
            </div>
            <div class="who">
              <div class="playerName">${esc(shortName(opponent.name))}</div>
              <div class="playerSub">${esc(fmtDate(nextGame.date || nextGame.game_date))}</div>
            </div>
          </div>
        </div>
      `;
    } else {
      matchup.innerHTML = `
        <div class="goalieCard">
          <div class="hs">
            <img src="${esc(headshotUrl(ourGoalie.player_id))}" alt="" onerror="this.nextElementSibling.style.display='flex';this.style.display='none'">
            <div class="ini" style="display:none">${esc(initials(ourGoalie.name))}</div>
          </div>
          <div class="who">
            <div class="playerName">${esc(ourGoalie.name)}</div>
            <div class="playerSub">#${esc(ourGoalie.number || '?')}</div>
          </div>
        </div>
      `;
    }

    // Goalie list
    list.innerHTML = goalies.map(g => {
      const stats = goalieStats(g);
      return `
        <div class="row">
          <div class="playerLine">
            <div class="hs" style="width:44px;height:44px">
              <img src="${esc(headshotUrl(g.player_id))}" alt="" onerror="this.nextElementSibling.style.display='flex';this.style.display='none'">
              <div class="ini" style="display:none">${esc(initials(g.name))}</div>
            </div>
            <div>
              <div class="playerName">${esc(g.name)}</div>
              <div class="playerSub">#${esc(g.number || '?')} • ${stats.gp} GP</div>
            </div>
          </div>
          <div class="statBox">
            <div class="big">${stats.svPct.toFixed(3).slice(1)}</div>
            <div class="lbl">SV%</div>
          </div>
        </div>
      `;
    }).join('') || '<div class="row">No goalies</div>';

    // Story
    if (story && ourGoalie) {
      story.innerHTML = `
        <p>${esc(ourGoalie.name)} has started ${ourStats.gp} games this season with a record of ${ourStats.wins}-${ourStats.losses}.</p>
        <p class="faint">Save percentage: ${ourStats.svPct.toFixed(3)}</p>
      `;
    }
  }

  // ===========================================================================
  // SLIDE: LEAGUE
  // ===========================================================================

  function renderLeague() {
    const leaders = document.getElementById('leagueLeaders');
    const nextGames = document.getElementById('leagueNextGames');

    if (!leaders || !nextGames) return;

    // League leaders - get from standings sorted by points
    const standings = STATE.standings?.rows || [];
    const topTeams = [...standings].sort((a, b) => (b.pts || 0) - (a.pts || 0)).slice(0, 5);

    leaders.innerHTML = topTeams.map((team, i) => {
      const isUs = team.slug === CONFIG.team || (team.team || '').toLowerCase().includes('amherst');
      return `
        <div class="leaderRow" ${isUs ? 'style="background:rgba(var(--accent-rgb),.18)"' : ''}>
          <div class="rank">${i + 1}</div>
          <div class="logo"><img src="${esc(logoUrl(team.slug))}" alt="" onerror="this.style.display='none'"></div>
          <div class="who">
            <div class="playerName">${esc(team.team)}</div>
            <div class="playerSub">${team.w || 0}-${team.l || 0}-${team.otl || 0}</div>
          </div>
          <div class="statBox">
            <div class="big">${team.pts || 0}</div>
            <div class="lbl">PTS</div>
          </div>
        </div>
      `;
    }).join('') || '<div class="leaderRow">No standings data</div>';

    // Next MHL games (from schedule)
    const now = new Date();
    const upcoming = (STATE.schedule || [])
      .filter(g => new Date(g.date || g.game_date) > now)
      .sort((a, b) => new Date(a.date || a.game_date) - new Date(b.date || b.game_date))
      .slice(0, 5);

    nextGames.innerHTML = upcoming.map(g => `
      <div class="row">
        <div class="meta">
          <span>${esc(g.away_team || g.awayTeam?.name || 'TBD')}</span>
          <span>@</span>
          <span>${esc(g.home_team || g.homeTeam?.name || 'TBD')}</span>
        </div>
        <div class="pill">${esc(fmtDateTime(g.date || g.game_date))}</div>
      </div>
    `).join('') || '<div class="row">No upcoming games</div>';
  }

  // ===========================================================================
  // TICKER (Promos go here - P0 requirement!)
  // ===========================================================================

  function renderTicker() {
    const track = document.getElementById('tickerTrack');
    if (!track) return;

    const items = [];

    // Add standings info
    if (STATE.standings?.rows) {
      const ramblers = STATE.standings.rows.find(r =>
        r.slug === CONFIG.team || (r.team || '').toLowerCase().includes('amherst')
      );
      if (ramblers) {
        items.push(`<span class="b">Ramblers</span> ${ramblers.pts || 0} PTS • ${ramblers.w || 0}-${ramblers.l || 0}-${ramblers.otl || 0}`);
      }
    }

    // Add next game
    const future = (STATE.games || [])
      .filter(g => !isCompleted(g))
      .sort((a, b) => new Date(a.date || a.date_time || a.game_date) - new Date(b.date || b.date_time || b.game_date));

    if (future.length > 0) {
      const next = future[0];
      const opponent = getOpponent(next);
      items.push(`<span class="b">Next Game</span> vs ${shortName(opponent.name)} • ${fmtDateTime(next.date || next.date_time || next.game_date)}`);
    }

    // Add last result
    const past = (STATE.games || [])
      .filter(g => isCompleted(g))
      .sort((a, b) => new Date(b.date || b.date_time || b.game_date) - new Date(a.date || a.date_time || a.game_date));

    if (past.length > 0) {
      const last = past[0];
      const opponent = getOpponent(last);
      const won = didWeWin(last);
      const ourScore = getOurScore(last);
      const theirScore = getTheirScore(last);
      items.push(`<span class="b">Last Game</span> ${won ? 'W' : 'L'} ${ourScore}-${theirScore} vs ${shortName(opponent.name)}`);
    }

    // Add promos/announcements HERE (not in slides!)
    const promos = [
      'Season tickets available at ramblershockey.ca',
      'Follow us @AmherstRamblers',
      'Home games at Amherst Stadium',
    ];
    promos.forEach(p => items.push(p));

    // Add division standings teaser
    if (STATE.standings?.rows) {
      const southTeams = STATE.standings.rows
        .filter(r => r.division === 'Eastlink South' || SOUTH_TEAMS.some(t => (r.team || '').toLowerCase().includes(t)))
        .sort((a, b) => (b.pts || 0) - (a.pts || 0));

      const leader = southTeams[0];
      if (leader) {
        items.push(`<span class="b">Eastlink South Leader</span> ${leader.team} • ${leader.pts} PTS`);
      }
    }

    // Duplicate for seamless scroll
    const allItems = [...items, ...items];

    track.innerHTML = allItems.map(item =>
      `<div class="tick-item">${item}<span class="sep">•</span></div>`
    ).join('');
  }

  // ===========================================================================
  // SLIDE ROTATION
  // ===========================================================================

  function startSlideshow() {
    const slides = document.querySelectorAll('.slide');
    if (slides.length === 0) return;

    function showSlide(index) {
      slides.forEach((s, i) => {
        s.classList.toggle('active', i === index);
      });

      // Schedule next slide
      const current = slides[index];
      const seconds = parseInt(current.dataset.seconds, 10) || (CONFIG.defaultSlideTime / 1000);

      if (STATE.slideTimer) clearTimeout(STATE.slideTimer);
      STATE.slideTimer = setTimeout(() => {
        STATE.currentSlide = (STATE.currentSlide + 1) % slides.length;
        showSlide(STATE.currentSlide);
      }, seconds * 1000);
    }

    // Start with first slide
    STATE.currentSlide = 0;
    showSlide(0);
  }

  // ===========================================================================
  // WATERMARK
  // ===========================================================================

  function setWatermark() {
    const watermark = document.getElementById('watermark');
    if (watermark) {
      watermark.style.backgroundImage = `url(${logoUrl(CONFIG.team)})`;
    }
  }

  // ===========================================================================
  // BRAND
  // ===========================================================================

  function setBrand() {
    const logo = document.getElementById('brandLogo');
    if (logo) {
      const img = document.createElement('img');
      img.src = logoUrl(CONFIG.team);
      img.alt = '';
      img.onerror = () => img.style.display = 'none';
      // Keep fallback visible if image fails
      const fallback = logo.querySelector('.fallback');
      if (fallback) {
        img.onload = () => fallback.style.display = 'none';
      }
      logo.insertBefore(img, logo.firstChild);
    }
  }

  // ===========================================================================
  // INIT
  // ===========================================================================

  async function init() {
    console.log('[display] Starting...');

    // Static setup
    setBrand();
    setWatermark();
    updateClock();
    setInterval(updateClock, 1000);

    // Load data
    await loadAllData();

    // Render all slides
    renderAll();

    // Start slideshow
    startSlideshow();

    // Periodic refresh
    setInterval(async () => {
      await loadAllData();
      renderAll();
    }, CONFIG.refreshInterval);
  }

  function renderAll() {
    updateStatus();
    updateDivisionMeta();
    renderNextUp();
    renderFaces();
    renderRecap();
    renderStandings();
    renderGoalies();
    renderLeague();
    renderTicker();
  }

  // Start when DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
