// Required acquisition and snapshot generation happen in a disposable staging directory.
// No output (especially no fresh plan) is published after a failed required stage.
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { acquireSchedule, config } from './hockeytech.mjs';
import { buildScheduleOutputs } from './schedules.mjs';
import { buildMHLStandings } from './standings.mjs';

const ROOT = fileURLToPath(new URL('../', import.meta.url));
const OUTPUTS = ['games.json', 'next_games.json', 'standings_mhl.json', 'ccmha_games.json', 'league_stats.json',
  'games', 'rosters', 'assets/headshots', 'assets/standings', 'metadata_build.json', 'monitor_plan.json'];
const writeJSON = (file, data) => fs.writeFile(file, JSON.stringify(data, null, 2) + '\n');
async function copyIfPresent(from, to) {
  try { await fs.cp(from, to, { recursive: true }); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
}
export async function optionalStage(name, run, warnings) {
  try { return await run(); }
  catch (error) {
    const warning = { stage: name, status: 'failed_preserved_prior_data', message: error.message };
    warnings.push(warning);
    console.warn(`[optional/${name}] ${error.message}; prior data retained`);
  }
}
export async function buildAll() {
  // Check key, season identity, full source row coverage and display aliases before any staging writes.
  const acquisition = await acquireSchedule();
  const directory = JSON.parse(await fs.readFile(path.join(ROOT, 'teams.json'), 'utf8'));
  const nameToSlug = new Map();
  for (const team of directory.teams) {
    for (const name of [team.name, ...(team.aliases || [])]) nameToSlug.set(name.toLowerCase().trim(), team.slug);
  }
  const schedule = buildScheduleOutputs(acquisition, nameToSlug);
  const stage = await fs.mkdtemp(path.join(ROOT, '.metadata-stage-'));
  const previousCwd = process.cwd(), previousOutput = process.env.METADATA_OUTPUT_DIR;
  try {
    for (const file of OUTPUTS) await copyIfPresent(path.join(ROOT, file), path.join(stage, file));
    process.env.METADATA_OUTPUT_DIR = stage;
    process.chdir(stage);
    const [{ buildRosters }, { buildRamblersGames }, { buildLeagueStats }, { fetchCCMHAGames }, { snapshotStandings }] = await Promise.all([
      import('./rosters.mjs'), import('./games.mjs'), import('./league_stats.mjs'), import('./ccmha.mjs'), import('./snap_standings.mjs')
    ]);
    const warnings = [];
    await buildRosters();
    let priorGames = [];
    try { priorGames = JSON.parse(await fs.readFile('games/amherst-ramblers.json', 'utf8')).games; }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    const gameData = await buildRamblersGames(acquisition);
    // Keep optional enrichments by exact source game ID if their independent source is unavailable.
    const priorById = new Map(priorGames.map(game => [String(game.game_id), game]));
    for (const game of gameData.games) {
      const previous = priorById.get(String(game.game_id));
      if (previous?.box_score) game.box_score = previous.box_score;
      if (previous?.game_info) game.game_info = previous.game_info;
    }
    await writeJSON('games/amherst-ramblers.json', gameData);
    await optionalStage('boxscores', async () => {
      const { scrapeRamblersBoxScores } = await import('./boxscores.mjs');
      await scrapeRamblersBoxScores();
    }, warnings);
    await writeJSON('standings_mhl.json', await buildMHLStandings({ nameToSlug }));
    await buildLeagueStats();
    await optionalStage('ccmha', async () => {
      const games = await fetchCCMHAGames({ daysAhead: 7 });
      await writeJSON('ccmha_games.json', { generated_at: new Date().toISOString(), timezone: 'America/Halifax', games });
    }, warnings);
    await writeJSON('games.json', schedule.games);
    await writeJSON('next_games.json', schedule.next);
    await snapshotStandings();
    await writeJSON('monitor_plan.json', schedule.plan);
    await writeJSON('metadata_build.json', { generated_at: acquisition.generated_at, completed_at: new Date().toISOString(),
      complete: true, season_ids: config.season_ids, schedule_rows: acquisition.rows.length,
      plan_games: schedule.plan.games.length, snapshot: 'success', optional_warnings: warnings });
    // GitHub publishes these together in one commit only after this command exits successfully.
    for (const file of OUTPUTS) await copyIfPresent(path.join(stage, file), path.join(ROOT, file));
    console.log(`[build] Complete: ${schedule.plan.games.length} source games, season ${config.season_label}; optional warnings=${warnings.length}`);
    return schedule.plan;
  } finally {
    process.chdir(previousCwd);
    if (previousOutput === undefined) delete process.env.METADATA_OUTPUT_DIR;
    else process.env.METADATA_OUTPUT_DIR = previousOutput;
    await fs.rm(stage, { recursive: true, force: true });
  }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  buildAll().catch(error => { console.error('[build] Required stage failed:', error.message); process.exitCode = 1; });
}
