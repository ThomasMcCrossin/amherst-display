import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { acquireSchedule, config, createClient, validateSchedule, validateSeasons } from '../scripts/hockeytech.mjs';
import { buildScheduleOutputs } from '../scripts/schedules.mjs';
import { rosterPlayers } from '../scripts/rosters.mjs';

const root = new URL('../', import.meta.url);
const row = { game_id: '4996', season_id: '46', home_team: '1', visiting_team: '12', final: '0', status: '1',
  home_team_name: 'Amherst Ramblers', visiting_team_name: 'Grand Falls Rapids',
  date_tbd: '0', time_tbd: '0', GameDateISO8601: '2026-09-12T19:00:00-03:00', home_goal_count: '0', visiting_goal_count: '0' };
const seasons = [{ season_id: '46', season_name: '2026-27 MHL Regular Season', start_date: '2026-09-10' }];
const names = new Map([['amherst ramblers', 'amherst-ramblers'], ['grand falls rapids', 'grand-falls-rapids']]);
const fixture = rows => async params => ({ SiteKit: params.view === 'seasons' ? { Seasons: seasons } : { Schedule: rows } });

test('nested source staff sections never become phantom roster players', () => {
  const player = { id: '3545', first_name: 'Christian' };
  assert.deepEqual(rosterPlayers([player, [{ id: '17969', role_id: '7', name: 'Coach' }]]), [player]);
  assert.throws(() => rosterPlayers([player, [{ id: 'broken-without-role' }]]), /staff section/);
  assert.throws(() => rosterPlayers([{}]), /Invalid roster player/);
  assert.throws(() => rosterPlayers([[{ id: '17969', role_id: '7' }]]), /Empty required player roster/);
});

test('plan accounts for every row, preserves aware starts and never guesses TBD or Final', async () => {
  const acquisition = await acquireSchedule({ client: fixture([
    row, { ...row, game_id: '4997', time_tbd: '1' },
    { ...row, game_id: '4998', GameDateISO8601: '2026-09-14T19:00:00', final: '1' },
    { ...row, game_id: '4999', game_status: 'Postponed' }
  ]) });
  const { plan, games, next } = buildScheduleOutputs(acquisition, names, new Date('2026-09-12T12:00:00Z'));
  assert.equal(plan.complete, true);
  assert.deepEqual(plan.games.map(g => [g.game_id, g.starts_at, g.monitorable, g.final]), [
    ['4996', row.GameDateISO8601, true, false], ['4997', null, false, false],
    ['4998', null, false, true], ['4999', row.GameDateISO8601, false, false]
  ]);
  assert.equal(plan.games[1].reason, 'unknown_or_tbd_start');
  assert.deepEqual(games.events.map(e => e.game_id), ['4996']);
  assert.equal(next.teams[0].games[0].opponent_slug, 'grand-falls-rapids');
  assert.equal(plan.source.requests[0].row_count, 4);
  assert.match(plan.source.requests[0].sha256, /^[a-f0-9]{64}$/);
});

test('missing, empty, duplicate, wrong-season and foreign-team acquisitions fail closed', async () => {
  await assert.rejects(acquireSchedule({ client: async () => ({ SiteKit: {} }) }), /missing row array/);
  for (const rows of [[], [row, row], [{ ...row, season_id: '41' }], [{ ...row, home_team: '3' }], [{ ...row, game_id: '' }]]) {
    assert.throws(() => validateSchedule(rows, '46'));
  }
  assert.throws(() => validateSeasons([{ ...seasons[0], season_name: '2025-26 Regular Season' }]), /does not match/);
  assert.throws(() => validateSeasons(seasons, { ...config, season_ids: ['41'] }), /does not match/);
  const acquisition = await acquireSchedule({ client: fixture([row]) });
  assert.throws(() => buildScheduleOutputs(acquisition, new Map()), /Unmapped display team/);
});

test('request cache shares a single acquisition and rejects HTTP, malformed JSON and missing key without disclosure', async () => {
  let calls = 0;
  const client = createClient({ apiKey: 'private-test-key', fetchImpl: async () => {
    calls++; return new Response(JSON.stringify({ SiteKit: { Schedule: [row] } }));
  } });
  const params = { feed: 'modulekit', view: 'schedule', season_id: '46', team_id: '1' };
  await Promise.all([client(params), client({ ...params })]);
  assert.equal(calls, 1);
  for (const fetchImpl of [async () => new Response('denied', { status: 403 }), async () => new Response('invalid'), async () => { throw new Error('private-test-key'); }]) {
    await assert.rejects(createClient({ apiKey: 'private-test-key', fetchImpl })(params), error => !error.message.includes('private-test-key'));
  }
  await assert.rejects(createClient({ apiKey: '', fetchImpl: () => { throw new Error('unexpected fetch'); } })(params), /HOCKEYTECH_API_KEY is required/);
});

test('required failure after schedule acquisition preserves all previously published data and removes staging', async () => {
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'central-metadata-'));
  try {
    await fs.mkdir(path.join(tmp, 'scripts'));
    for (const name of ['build_all.mjs', 'hockeytech.mjs', 'schedules.mjs', 'standings.mjs', 'rosters.mjs', 'games.mjs', 'league_stats.mjs', 'ccmha.mjs', 'snap_standings.mjs']) {
      await fs.copyFile(new URL('scripts/' + name, root), path.join(tmp, 'scripts', name));
    }
    await fs.cp(new URL('config/', root), path.join(tmp, 'config'), { recursive: true });
    await fs.copyFile(new URL('teams.json', root), path.join(tmp, 'teams.json'));
    await fs.symlink(new URL('node_modules/', root).pathname, path.join(tmp, 'node_modules'), 'dir');
    const prior = { 'monitor_plan.json': '{"old_plan":true}', 'games.json': '{"old_schedule":true}', 'standings_mhl.json': '{"old_standings":true}' };
    for (const [name, content] of Object.entries(prior)) await fs.writeFile(path.join(tmp, name), content);
    await fs.writeFile(path.join(tmp, 'inject.mjs'), `globalThis.fetch = async url => {
      const view = new URL(url).searchParams.get('view');
      if (view === 'seasons') return new Response(JSON.stringify({SiteKit:{Seasons:${JSON.stringify(seasons)}}}));
      if (view === 'schedule') return new Response(JSON.stringify({SiteKit:{Schedule:[${JSON.stringify(row)}]}}));
      return new Response('unavailable', {status:503});
    };`);
    const result = spawnSync(process.execPath, ['--import', './inject.mjs', 'scripts/build_all.mjs'], {
      cwd: tmp, env: { ...process.env, HOCKEYTECH_API_KEY: 'private-test-key' }, encoding: 'utf8', timeout: 20000
    });
    assert.equal(result.status, 1, result.stderr);
    assert.match(result.stderr, /Required stage failed/);
    assert.ok(!result.stderr.includes('private-test-key'));
    for (const [name, content] of Object.entries(prior)) assert.equal(await fs.readFile(path.join(tmp, name), 'utf8'), content);
    assert.equal((await fs.readdir(tmp)).filter(name => name.startsWith('.metadata-stage-')).length, 0);
    await assert.rejects(fs.stat(path.join(tmp, 'metadata_build.json')), { code: 'ENOENT' });
  } finally { await fs.rm(tmp, { recursive: true, force: true }); }
});

test('snapshot process rejects stale season without replacing prior image', async () => {
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'central-snapshot-'));
  try {
    await fs.mkdir(path.join(tmp, 'assets/standings'), { recursive: true });
    await fs.writeFile(path.join(tmp, 'assets/standings/standings_mhl.png'), 'previous-image');
    await fs.writeFile(path.join(tmp, 'standings_mhl.json'), JSON.stringify({ season: '2024-25', rows: [{ team: 'Amherst', division: 'South' }] }));
    const result = spawnSync(process.execPath, [new URL('scripts/snap_standings.mjs', root).pathname], { cwd: tmp, encoding: 'utf8', timeout: 20000 });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /current-season standings/);
    assert.equal(await fs.readFile(path.join(tmp, 'assets/standings/standings_mhl.png'), 'utf8'), 'previous-image');
  } finally { await fs.rm(tmp, { recursive: true, force: true }); }
});
