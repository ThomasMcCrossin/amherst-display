// Observes when official lineup/starting goalie data first appears for upcoming games.
// Usage: HOCKEYTECH_API_KEY=... node bakeoff/lineup_watch.mjs <untilISO> <everySec> <gameId...>
// Appends one JSON line per game per tick to bakeoff/data/raw/lineup_watch.log (gitignored; contains no key).
import fs from 'node:fs'; import { request } from '../scripts/hockeytech.mjs';
const [until, every, ...games] = process.argv.slice(2);
const log = 'bakeoff/data/raw/lineup_watch.log';
const sleep = ms => new Promise(r => setTimeout(r, ms));
while (Date.now() < Date.parse(until)) {
  for (const id of games) {
    const rec = { t: new Date().toISOString(), game: id };
    try {
      const gc = await request({ feed: 'gc', view: 'gamesummary', game_id: id, tab: 'gamesummary' });
      const m = gc.GC.Gamesummary;
      const side = s => (m[s + '_team_lineup']?.goalies || []).map(g => `${g.first_name} ${g.last_name}:start=${g.start}`);
      rec.gc = { started: m.meta.started, final: m.meta.final, home_goalies: side('home'), visitor_goalies: side('visitor'),
        home_skaters: (m.home_team_lineup?.skaters || []).length, visitor_skaters: (m.visitor_team_lineup?.skaters || []).length };
      await sleep(400);
      const sv = await request({ feed: 'statviewfeed', view: 'gameSummary', game_id: id });
      rec.sv = { home_goalies: (sv.homeTeam?.goalies || []).map(g => `${g.info.lastName}:starting=${g.starting}`),
        visitor_goalies: (sv.visitingTeam?.goalies || []).map(g => `${g.info.lastName}:starting=${g.starting}`) };
      await sleep(400);
      const gp = await request({ feed: 'statviewfeed', view: 'gameCenterPreview', game_id: id, site_id: 3 });
      rec.gcp = { home: (gp.homeTeam?.lineup?.goalies || []).length, visitor: (gp.visitingTeam?.lineup?.goalies || []).length };
    } catch (e) { rec.err = e.message; }
    fs.appendFileSync(log, JSON.stringify(rec) + '\n');
    await sleep(500);
  }
  await sleep(+every * 1000);
}
