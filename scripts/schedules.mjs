// Display schedules and the worker authorization plan share one validated acquisition.
import { config } from './hockeytech.mjs';

export function buildMonitorPlan(acquisition, settings = config) {
  const games = acquisition.rows.map(row => {
    const iso = row.GameDateISO8601;
    const knownTime = String(row.time_tbd) === '0' && String(row.date_tbd) === '0' &&
      typeof iso === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})$/.test(iso) && Number.isFinite(Date.parse(iso));
    const unavailable = /postponed|cancelled|canceled/i.test(row.game_status || '');
    const reason = !knownTime ? 'unknown_or_tbd_start' : unavailable ? 'postponed_or_cancelled' : null;
    return {
      game_id: String(row.game_id), season_id: String(row.season_id),
      starts_at: knownTime ? iso : null,
      home_team_id: String(row.home_team), away_team_id: String(row.visiting_team),
      final: String(row.final) === '1', monitorable: !reason,
      ...(reason ? { reason } : {})
    };
  });
  return {
    schema_version: 'amherst.monitor-plan.v1', generated_at: acquisition.generated_at, complete: true,
    client_code: settings.client_code, league_id: settings.league_id, team_id: settings.team_id,
    season_ids: settings.season_ids, monitoring: settings.monitoring, games, source: acquisition.source
  };
}

export function buildScheduleOutputs(acquisition, nameToSlug, now = new Date()) {
  const plan = buildMonitorPlan(acquisition);
  const events = [];
  for (let i = 0; i < acquisition.rows.length; i++) {
    const row = acquisition.rows[i], game = plan.games[i];
    const homeSlug = nameToSlug.get(row.home_team_name?.toLowerCase().trim());
    const awaySlug = nameToSlug.get(row.visiting_team_name?.toLowerCase().trim());
    if (!homeSlug || !awaySlug) throw new Error(`Unmapped display team in game ${game.game_id}`);
    if (!game.monitorable) continue; // Accounted for explicitly in the complete plan, never guessed midnight.
    events.push({ league: 'MHL', home_team: row.home_team_name, away_team: row.visiting_team_name,
      home_slug: homeSlug, away_slug: awaySlug, start: game.starts_at, location: row.venue_name || '',
      game_id: game.game_id, season_id: game.season_id });
  }
  events.sort((a, b) => Date.parse(a.start) - Date.parse(b.start));
  const teamSlug = 'amherst-ramblers';
  const finals = new Set(plan.games.filter(g => g.final).map(g => g.game_id));
  const next = events.filter(e => !finals.has(e.game_id) && Date.parse(e.start) >= now.getTime()).slice(0, 3).map(e => ({
    opponent_slug: e.home_slug === teamSlug ? e.away_slug : e.home_slug,
    home: e.home_slug === teamSlug, start: e.start, venue: e.location, city: ''
  }));
  const common = { generated_at: acquisition.generated_at, timezone: 'America/Halifax' };
  return { plan, games: { ...common, events }, next: { ...common, teams: [{ team_slug: teamSlug, games: next }] } };
}
