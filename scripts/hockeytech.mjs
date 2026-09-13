import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';

export const config = JSON.parse(readFileSync(new URL('../config/hockeytech.json', import.meta.url), 'utf8'));
export function validateConfig(value) {
  for (const field of ['client_code', 'league_id', 'team_id', 'site_id', 'season_label']) {
    if (typeof value[field] !== 'string' || !value[field]) throw new Error(`Invalid HockeyTech config: ${field}`);
  }
  if (!/^\d{4}-\d{2}$/.test(value.season_label) || !Array.isArray(value.season_ids) || !value.season_ids.length ||
      value.season_ids.some(id => typeof id !== 'string' || !/^\d+$/.test(id)) || new Set(value.season_ids).size !== value.season_ids.length) {
    throw new Error('Invalid HockeyTech season configuration');
  }
  const endpoint = new URL(value.base_feed_url);
  if (endpoint.protocol !== 'https:' || endpoint.search || endpoint.username || endpoint.password) throw new Error('Invalid HockeyTech endpoint');
  for (const field of ['active_seconds', 'before_minutes', 'after_hours', 'plan_refresh_seconds', 'plan_max_age_seconds']) {
    if (!Number.isFinite(value.monitoring?.[field]) || value.monitoring[field] <= 0) throw new Error(`Invalid monitoring configuration: ${field}`);
  }
  return value;
}
validateConfig(config);

export function createClient({ settings = config, fetchImpl = globalThis.fetch, apiKey = process.env.HOCKEYTECH_API_KEY } = {}) {
  validateConfig(settings);
  const cache = new Map();
  return async function request(params) {
    if (!apiKey?.trim()) throw new Error('HOCKEYTECH_API_KEY is required');
    const safe = { client_code: settings.client_code, league_id: settings.league_id, fmt: 'json', ...params };
    const cacheKey = JSON.stringify(Object.entries(safe).sort());
    if (!cache.has(cacheKey)) cache.set(cacheKey, (async () => {
      const url = new URL(settings.base_feed_url);
      for (const [key, value] of Object.entries(safe)) url.searchParams.set(key, value);
      url.searchParams.set('key', apiKey.trim());
      let response;
      try { response = await fetchImpl(url, { signal: AbortSignal.timeout(30000) }); }
      catch { throw new Error(`HockeyTech ${params.view}: network request failed`); }
      if (!response.ok) throw new Error(`HockeyTech ${params.view}: HTTP ${response.status}`);
      let text = (await response.text()).trim();
      if (text.startsWith('(') && /\);?$/.test(text)) text = text.replace(/^\(/, '').replace(/\);?$/, '');
      let data;
      try { data = JSON.parse(text); } catch { throw new Error(`HockeyTech ${params.view}: invalid JSON`); }
      if (!data || typeof data !== 'object') throw new Error(`HockeyTech ${params.view}: invalid response`);
      return data;
    })());
    return cache.get(cacheKey);
  };
}
export const request = createClient();
export function moduleRows(data, name) {
  const rows = data?.SiteKit?.[name];
  if (!Array.isArray(rows)) throw new Error(`HockeyTech ${name}: missing row array`);
  return rows;
}
export function statSections(data) {
  const sections = Array.isArray(data) ? data.flatMap(group => {
    if (!Array.isArray(group?.sections)) throw new Error('HockeyTech statviewfeed: missing sections');
    return group.sections;
  }) : data?.sections;
  if (!Array.isArray(sections) || !sections.length || sections.some(section => !Array.isArray(section.data))) {
    throw new Error('HockeyTech statviewfeed: incomplete sections');
  }
  if (sections.some(section => section.data.some(item => !item?.row || typeof item.row !== 'object' || Array.isArray(item.row)))) {
    throw new Error('HockeyTech statviewfeed: invalid row');
  }
  return sections;
}
export function statView(view, params = {}) {
  return request({ feed: 'statviewfeed', view, site_id: config.site_id, season: config.season_ids[0], ...params });
}
export function validateSeasons(rows, settings = config) {
  for (const id of settings.season_ids) {
    const season = rows.find(row => String(row.season_id) === id);
    if (!season || !String(season.start_date).startsWith(settings.season_label.slice(0, 4)) ||
        !String(season.season_name).includes(settings.season_label)) throw new Error(`HockeyTech season ${id} does not match ${settings.season_label}`);
  }
}
export function validateSchedule(rows, seasonId, settings = config) {
  if (!rows.length) throw new Error(`HockeyTech schedule ${seasonId}: empty acquisition is not publishable`);
  const seen = new Set();
  for (const row of rows) {
    if (!/^\d+$/.test(row.game_id) || String(row.season_id) !== seasonId || seen.has(row.game_id) ||
        !/^\d+$/.test(row.home_team) || !/^\d+$/.test(row.visiting_team) ||
        ![String(row.home_team), String(row.visiting_team)].includes(settings.team_id) ||
        !['0', '1'].includes(String(row.final))) throw new Error(`HockeyTech schedule ${seasonId}: invalid, duplicate or foreign row`);
    seen.add(row.game_id);
  }
  return rows;
}
export async function acquireSchedule({ client = request, settings = config } = {}) {
  validateSeasons(moduleRows(await client({ feed: 'modulekit', view: 'seasons' }), 'Seasons'), settings);
  const rows = [], sources = [];
  const seen = new Set();
  for (const seasonId of settings.season_ids) {
    const params = { feed: 'modulekit', view: 'schedule', season_id: seasonId, team_id: settings.team_id };
    const seasonRows = validateSchedule(moduleRows(await client(params), 'Schedule'), seasonId, settings);
    for (const row of seasonRows) {
      if (seen.has(String(row.game_id))) throw new Error('HockeyTech schedule: duplicate game across seasons');
      seen.add(String(row.game_id));
      rows.push(row);
    }
    sources.push({ ...params, row_count: seasonRows.length, sha256: createHash('sha256').update(JSON.stringify(seasonRows)).digest('hex') });
  }
  return { rows, generated_at: new Date().toISOString(), source: { endpoint: settings.base_feed_url, client_code: settings.client_code, league_id: settings.league_id, requests: sources } };
}
