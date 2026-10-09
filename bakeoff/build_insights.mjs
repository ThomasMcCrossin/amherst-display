#!/usr/bin/env node
// Insights engine for the Game TV board (amherst-display#20).
// Reads bakeoff/data/board.json (plus optional cached raw feeds), writes bakeoff/data/insights.json.
// Deterministic: no network, no LLM, no clock except the board snapshot time. Every number comes from the data.
//
//   node bakeoff/build_insights.mjs            build + validate + write
//   node bakeoff/build_insights.mjs --check    build + validate, do not write
//
// Goalie rule (Tom): nothing here names, implies or predicts a starting goalie for an upcoming game.
// A starter is only stated when next_game.official_starters is non-null, with its source. validate() fails the
// build if goalie text carries a forecast phrase (see GOALIE_BANNED); selfTest() proves the checker bites.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const DATA = path.join(HERE, 'data');
const RAW = path.join(DATA, 'raw');

// ------------------------------------------------------------------ helpers
const readJson = f => JSON.parse(fs.readFileSync(f, 'utf8'));
const readRaw = name => { try { return readJson(path.join(RAW, name)); } catch { return null; } };
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const DAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
const dayOf = ymd => DAYS[new Date(`${ymd}T12:00:00Z`).getUTCDay()];
const mdOf = ymd => `${MONTHS[+ymd.slice(5, 7) - 1]} ${+ymd.slice(8, 10)}`;
const addDays = (ymd, n) => new Date(new Date(`${ymd}T12:00:00Z`).getTime() + n * 864e5).toISOString().slice(0, 10);
const clock12 = iso => { let h = +iso.slice(11, 13); const m = iso.slice(14, 16); const ap = h >= 12 ? 'PM' : 'AM'; h = h % 12 || 12; return `${h}:${m} ${ap}`; };
const ord = n => { const s = ['th', 'st', 'nd', 'rd'], v = n % 100; return n + (s[(v - 20) % 10] || s[v] || s[0]); };
const plural = (n, one, many = one + 's') => (n === 1 ? one : many);
const WORDS = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten', 'eleven', 'twelve'];
const word = n => WORDS[n] ?? String(n);
const cap = s => s[0].toUpperCase() + s.slice(1);
const surname = full => full.trim().split(/\s+/).slice(1).join(' ') || full;
const secs = t => { const [m, s] = t.split(':').map(Number); return m * 60 + s; };
const list = a => (a.length <= 1 ? a.join('') : a.length === 2 ? a.join(' and ') : `${a.slice(0, -1).join(', ')} and ${a[a.length - 1]}`);
// Static team labels (the API gives full names like "Summerside Western Capitals"; the TV wants the town).
const TOWN = { AMH: 'Amherst', CAM: 'Campbellton', CHA: 'Chaleur', EDM: 'Edmundston', GFR: 'Grand Falls', MIR: 'Miramichi',
  PCC: 'Pictou County', SWC: 'Summerside', TRU: 'Truro', VAL: 'Valley', WKS: 'West Kent', YAR: 'Yarmouth' };
const town = abbr => TOWN[abbr] ?? abbr;
// Hometown strings are free text; one is truncated in the feed ("St-Augustin-de-Desmaures,"). Known-city fixes only.
const CITY_PROV = { 'St-Augustin-de-Desmaures': 'QC' };
const provOf = h => {
  if (!h) return null;
  const [city, prov] = h.split(',').map(x => x.trim());
  return prov || CITY_PROV[city] || null;
};
const parseBirth = s => { // "Oct  8, 2008"
  const m = /^([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})$/.exec(s ?? '');
  return m ? { mon: MONTHS.indexOf(m[1]) + 1, day: +m[2], year: +m[3] } : null;
};
const halifaxDate = iso => new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Halifax' }).format(new Date(iso));
const mdOfFeed = t => { const m = /^([A-Za-z]{3})\s+(\d{1,2}),/.exec(t ?? ''); return m ? `${m[1]} ${+m[2]}` : t; };
const rank1 = (rows, key, who, desc = true) => 1 + rows.filter(r => (desc ? r[key] > who[key] : r[key] < who[key])).length;
// Notability gate (Tom, #20): a rank is printed only when it is worth bragging about; a weaker rank is dropped, never softened.
const NOTABLE_MAX = { player: 10, rookie: 5, team: 3, division: 2 };
const notable = (scope, rank) => Number.isFinite(rank) && rank >= 1 && rank <= NOTABLE_MAX[scope];

// ------------------------------------------------------------------ goalie rule checker
const GOALIE_BANNED = /\b(likely|expected|projected|probable|should start|will start|to start|set to start|going to start|gets? the nod|slated|in net|between the pipes|in goal|starts? (tonight|friday|saturday|sunday|monday|tuesday|wednesday|thursday)|next game)\b/i;
const GLOBAL_BANNED = /\b(likely|expected|projected|probable)\b/i; // never needed anywhere on this board
const GOALIE_WORDS = /\b(goalies|goalie|goaltender|goaltending|netminder|goalkeeper|starter|starters|starting)\b/i;
export function goalieViolations(items, goalieNames) {
  const bad = [];
  const nameRe = goalieNames.length ? new RegExp(`\\b(${goalieNames.map(n => n.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|')})\\b`, 'i') : null;
  for (const it of items) {
    const text = `${it.headline} ${it.detail}`;
    if (GLOBAL_BANNED.test(text)) bad.push(`${it.id}: forecast word in "${text}"`);
    const mentionsGoalie = GOALIE_WORDS.test(text) || (nameRe && nameRe.test(text));
    if (mentionsGoalie && GOALIE_BANNED.test(text)) bad.push(`${it.id}: forecast phrase near a goalie in "${text}"`);
  }
  return bad;
}
function selfTest() {
  const probe = (headline, detail = '') => goalieViolations([{ id: 't', headline, detail }], ['Lavoie', 'Morgan', 'Will Cole', 'Cole']);
  const mustFail = [['Lavoie likely in net Friday'], ['Chaleur goalie expected to start'], ['Morgan gets the nod'], ['Starter projected: Cole'],
    ['Goalie news', 'Lavoie should start against Chaleur'], ['Will Cole will start Friday'], ['Fun fact', 'Likely a big night'], ['Netminder set to start tonight']];
  for (const [h, d] of mustFail) if (!probe(h, d).length) throw new Error(`goalie checker missed: ${h} ${d ?? ''}`);
  const mustPass = [['Lavoie ranks 3rd in the MHL', '.923 save percentage'], ['Last 5 starts: Morgan 3, Cole 2', 'History only'], ['Starting goalies: not official yet']];
  for (const [h, d] of mustPass) if (probe(h, d).length) throw new Error(`goalie checker false positive: ${h}`);
}

// ------------------------------------------------------------------ the engine
export function buildItems(board, opts = {}) {
  const raw = opts.raw ?? {};
  const items = [];
  const notes = []; // data warnings for the operator, printed by the CLI
  const today = halifaxDate(board.generated_at);
  const ng = board.next_game;
  const me = board.team;
  const skaters = board.skaters;
  const byId = new Map(skaters.map(s => [s.id, s]));
  const gById = new Map(board.goalies.map(g => [g.id, g]));
  const rows = board.standings.divisions.flatMap(d => d.rows.map(r => ({ ...r, division: d.name })));
  const ams = rows.find(r => r.is_ramblers);
  const mySouth = board.standings.divisions.find(d => d.rows.some(r => r.is_ramblers));
  const ts = board.team_stats;
  const recentOld = [...board.recent].sort((a, b) => a.date.localeCompare(b.date)); // oldest first
  const dateOfGame = ng ? ng.start_iso.slice(0, 10) : null;
  const gameEndIso = ng ? new Date(new Date(ng.start_iso).getTime() + 3 * 3600e3).toISOString() : null;
  const gameDay = ng ? dayOf(dateOfGame) : null;
  const endOfDay = ymd => `${ymd}T23:59:59-03:00`;

  const add = it => {
    items.push({ id: it.id, kind: it.kind, priority: it.priority, headline: it.headline, detail: it.detail, stat: it.stat ?? null,
      player_id: it.player_id ?? null, team_id: it.team_id ?? null, headshot_url: it.headshot_url ?? null,
      valid_until: it.valid_until ?? null, confidence: it.confidence ?? 'fact', sources: it.sources });
  };
  const playerItem = (s, rest) => add({ ...rest, player_id: s.id, team_id: me.id, headshot_url: s.headshot_url });

  // ---- game-level derivations from box-score goal lists (oldest first)
  const PERIOD = { '1st': 0, '2nd': 1, '3rd': 2 };
  const analyze = g => {
    const goals = g.goals ?? [];
    const mine = goals.filter(x => x.mine).length, theirs = goals.length - mine;
    const missing = (g.score.for + g.score.against) - (mine + theirs);
    const shootout = missing === 1; // a shootout decides a tied game with exactly one goal that is not in the goal list
    const hasOT = goals.some(x => /OT/.test(x.period));
    const byp = [0, 1, 2].map(i => ({ f: goals.filter(x => x.mine && PERIOD[x.period] === i).length, a: goals.filter(x => !x.mine && PERIOD[x.period] === i).length }));
    let d = 0, minD = 0, episodes2 = 0; // episodes of falling two or more behind
    for (const x of goals) { const before = d; d += x.mine ? 1 : -1; minD = Math.min(minD, d); if (before > -2 && d <= -2) episodes2++; }
    const after2 = byp[0].f + byp[1].f - byp[0].a - byp[1].a;
    const decided = shootout ? 'SO' : hasOT ? 'OT' : null;
    return { goals, byp, after2, maxDeficit: -minD, episodes2, decided, shootout, hasOT };
  };
  const A = new Map(recentOld.map(g => [g.id, analyze(g)]));
  for (const g of recentOld) { // flag disagreements with the builder's ot_so label
    const dec = A.get(g.id).decided;
    if ((g.ot_so ?? null) !== dec) notes.push(`board.json recent ${g.date} vs ${g.opp.abbr}: ot_so=${g.ot_so} but box score shows ${dec}`);
  }
  const ptsOf = g => (g.result === 'W' ? 2 : g.result === 'L' ? 0 : 1);
  const gp = recentOld.length;
  const regGoals = { f: 0, a: 0 }, periodGoals = { f: [0, 0, 0], a: [0, 0, 0] };
  for (const g of recentOld) for (const x of A.get(g.id).goals) {
    const i = PERIOD[x.period]; if (i === undefined) continue;
    periodGoals[x.mine ? 'f' : 'a'][i]++; regGoals[x.mine ? 'f' : 'a']++;
  }

  // sanity: skater goals vs team goals
  const skaterGoals = skaters.reduce((n, s) => n + (s.g ?? 0), 0);
  if (ams && skaterGoals !== ams.gf) notes.push(`skater goals sum ${skaterGoals} vs standings GF ${ams.gf} (difference is shootout winners)`);

  // ================================================================== MATCHUP: next game
  if (ng && ams) {
    const opp = ng.opponent, oRow = rows.find(r => r.team_id === opp.id), oTown = town(opp.abbr);
    const where = ng.home ? 'at Amherst Stadium' : `at ${ng.venue}`;
    const srcNg = 'board.json:next_game';

    // standings stakes
    const winPts = ams.pts + 2;
    const others = mySouth.rows.filter(r => !r.is_ramblers);
    const above = others.filter(r => r.pts > winPts).length, tied = others.filter(r => r.pts === winPts);
    const newRank = above + 1;
    const behind = others.filter(r => r.pts < winPts && r.pts >= ams.pts - 2).sort((a, b) => b.pts - a.pts);
    const next3 = others.find(r => r.rank === ams.rank - 1);
    if (!tied.length) {
      add({ id: 'stakes-win', kind: 'matchup', priority: 1, team_id: me.id, valid_until: gameEndIso, confidence: 'derived',
        headline: newRank < ams.rank ? `Win ${gameDay} and the Ramblers climb to ${ord(newRank)} in the South` : `Win ${gameDay} and the Ramblers reach ${winPts} points`,
        detail: `A win makes ${winPts} points. Now ${ord(ams.rank)} with ${ams.pts}${next3 ? `; ${town(next3.abbr)} has ${next3.pts} (${next3.gp} GP)` : ''}. Top 4 make the playoffs.`,
        stat: { value: String(winPts), label: 'points with a win' }, sources: ['board.json:standings', srcNg] });
    }

    // head to head
    const hl = ng.h2h_last_season ?? [];
    const sorted = [...hl].sort((a, b) => b.date.localeCompare(a.date));
    if (sorted.length >= 2) {
      const w = sorted.filter(g => g.result === 'W').length, l = sorted.length - w;
      const oneGoal = sorted.every(g => Math.abs(g.score.for - g.score.against) === 1);
      const crowd = sorted.find(g => g.home && g.attendance);
      const lines = sorted.map(g => `${g.result === 'W' ? 'Won' : g.result === 'L' ? 'Lost' : 'Lost in ' + g.ot_so} ${g.score.for}-${g.score.against} ${g.home ? 'at home' : 'on the road'} ${mdOf(g.date)}${g === crowd ? ` in front of ${g.attendance.toLocaleString('en-CA')}` : ''}`);
      add({ id: 'h2h-last-season', kind: 'matchup', priority: 2, team_id: opp.id, valid_until: gameEndIso,
        headline: oneGoal ? `Last year vs ${oTown}: ${word(w)} win, ${word(l)} loss, both by one goal`.replace('one win, one loss', 'a win and a loss') : `Last year vs ${oTown}: ${w}-${l}`,
        detail: `${lines.join('. ')}.`,
        stat: { value: `${w}-${l}`, label: `last season vs ${opp.abbr}` }, sources: [`${srcNg}.h2h_last_season`] });
    }
    if ((ng.h2h_this_season ?? []).length) {
      const t = ng.h2h_this_season, w = t.filter(g => g.result === 'W').length;
      add({ id: 'h2h-this-season', kind: 'matchup', priority: 2, team_id: opp.id, valid_until: gameEndIso,
        headline: `Season series vs ${oTown}: ${w}-${t.length - w}`, detail: t.map(g => `${g.result} ${g.score.for}-${g.score.against} ${mdOf(g.date)}`).join(', ') + '.',
        stat: { value: `${w}-${t.length - w}`, label: 'this season' }, sources: [`${srcNg}.h2h_this_season`] });
    }

    // opponent form
    const f5 = ng.opponent_form?.last5 ?? [];
    const winStreak = /^(\d+)-0-0-0$/.exec(ng.opponent_form?.streak ?? '');
    if (winStreak && +winStreak[1] >= 2) {
      const n = +winStreak[1], run = f5.slice(0, n);
      add({ id: 'opp-win-streak', kind: 'matchup', priority: 2, team_id: opp.id, valid_until: gameEndIso,
        headline: `${oTown} arrives on a ${n}-game win streak`,
        detail: `${run.map(g => `${g.gf}-${g.ga} ${g.home ? 'vs' : 'at'} ${town(g.opp)} (${mdOf(g.date)})`).join(', ')}: ${run.reduce((s, g) => s + g.gf, 0)} goals in ${word(n)} games.`,
        stat: { value: String(n), label: `${opp.abbr} win streak` }, sources: [`${srcNg}.opponent_form`] });
    }

    // power play vs penalty kill
    if (oRow) {
      const ppRank = rank1(rows, 'pp_pct', oRow), pkRank = rank1(rows, 'pk_pct', ams);
      const against = recentOld.reduce((a, g) => { const [gl, op] = g.pp.against.split('/').map(Number); return { gl: a.gl + gl, op: a.op + op }; }, { gl: 0, op: 0 });
      const killed = against.op - against.gl;
      const killOk = against.op > 0 && Math.round(1000 * killed / against.op) / 10 === ams.pk_pct;
      add({ id: 'pp-vs-pk', kind: 'matchup', priority: 2, team_id: opp.id, valid_until: gameEndIso, confidence: 'derived',
        headline: (notable('team', pkRank) ? `${oTown}'s power play meets the MHL's No. ${pkRank} penalty kill` : `${oTown}'s power play meets the Ramblers' penalty kill`).slice(0, 60),
        detail: `${oTown} scores on ${oRow.pp_pct}% of power plays (${oRow.pp.replace('/', ' for ')}${notable('team', ppRank) ? `, ${ord(ppRank)}` : ''}). Ramblers kill ${ams.pk_pct}%${killOk ? ` (${killed} of ${against.op})` : ''}.`,
        stat: { value: `${ams.pk_pct}%`, label: notable('team', pkRank) ? `Ramblers PK, ${ord(pkRank)} in MHL` : 'Ramblers PK' }, sources: ['board.json:standings', 'board.json:recent[].pp'] });
    }

    // opponent leader
    const lead = (opp.leading_scorers ?? [])[0];
    if (lead && lead.pts >= 4) {
      const peers = opp.leading_scorers.slice(1).filter(s => s.pts === opp.leading_scorers[1]?.pts && s.pts >= lead.pts - 2 && s.pts > 0);
      add({ id: 'opp-top-scorer', kind: 'matchup', priority: 3, team_id: opp.id, headshot_url: lead.headshot_url, valid_until: gameEndIso,
        headline: `Watch ${lead.name}: ${oTown}'s points leader`.slice(0, 60),
        detail: `#${lead.number} has ${lead.pts} points (${lead.g} goals, ${lead.a} assists)${peers.length ? `. ${list(peers.map(p => surname(p.name)))} ${peers.length > 1 ? 'are' : 'is'} at ${peers[0].pts}` : ''}.`,
        stat: { value: String(lead.pts), label: `${surname(lead.name)} points` }, sources: [`${srcNg}.opponent.leading_scorers[0]`] });
    }

    // goalies: official starters only when published; otherwise plain "not official" plus history
    const os = ng.official_starters ?? {};
    const official = ['home', 'away'].map(side => os[side] ? { side, ...os[side] } : null).filter(Boolean);
    if (official.length) {
      for (const o of official) {
        add({ id: `starter-official-${o.side}`, kind: 'goalie', priority: 1, valid_until: gameEndIso, team_id: null,
          headline: `Official starter (${o.side}): ${o.name}`.slice(0, 60),
          detail: `Confirmed in the published lineup. Source: ${o.source}.`, stat: null, player_id: o.id ?? null,
          sources: [`${srcNg}.official_starters.${o.side}`] });
      }
    } else {
      add({ id: 'starters-tba', kind: 'goalie', priority: 5, valid_until: gameEndIso,
        headline: 'Starting goalies: not official yet',
        detail: 'Lineups are posted close to puck drop. Names go up here once they are official.',
        sources: [`${srcNg}.official_starters`] });
    }
    const st5 = ng.opponent.goalie_starts_last5 ?? [];
    if (st5.length >= 5) {
      const cnt = new Map(); for (const s of st5) cnt.set(s.name, (cnt.get(s.name) ?? 0) + 1);
      if (cnt.size >= 2) {
        const order = [...cnt.entries()].sort((a, b) => b[1] - a[1]);
        const seasonLine = order.map(([n]) => { const g = ng.opponent.goalies.find(x => x.name === n); return g ? `${surname(n)} ${g.sv_pct.toFixed(3).slice(1)}` : null; }).filter(Boolean);
        add({ id: 'opp-goalie-history', kind: 'goalie', priority: 5, team_id: opp.id, valid_until: gameEndIso,
          headline: `${oTown} has split its goaltending this season`,
          detail: `History only. Last 5 starts: ${order.map(([n, c]) => `${surname(n)} ${c}`).join(', ')}.${seasonLine.length ? ` Season SV%: ${seasonLine.join(', ')}.` : ''}`.slice(0, 140),
          sources: [`${srcNg}.opponent.goalie_starts_last5`, `${srcNg}.opponent.goalies`] });
      }
    }

    // crowd watch
    const homeGames = recentOld.filter(g => g.home && g.attendance);
    if (ng.home && homeGames.length >= 3) {
      const best = homeGames.reduce((a, g) => (g.attendance > a.attendance ? g : a));
      const prev = raw.schedule_prev ?? [];
      const prevHome = prev.filter(g => g.final === '1' && g.home_team === String(me.id) && +g.attendance > 0);
      const prevMax = prevHome.reduce((a, g) => (+g.attendance > (a ? +a.attendance : 0) ? g : a), null);
      const prevOppIsMax = prevMax && (prevMax.visiting_team === String(opp.id));
      add({ id: 'crowd-watch', kind: 'gameday', priority: 4, team_id: me.id, valid_until: gameEndIso, confidence: 'derived',
        headline: `Season-high home crowd: ${best.attendance.toLocaleString('en-CA')}. Can ${gameDay} top it?`.slice(0, 60),
        detail: `Home average is ${ts.attendance.avg_home} over ${ts.attendance.home_games} games.${prevOppIsMax ? ` Last year's biggest home crowd, ${(+prevMax.attendance).toLocaleString('en-CA')}, came against ${oTown}.` : ''}`,
        stat: { value: best.attendance.toLocaleString('en-CA'), label: 'season-high home crowd' }, sources: ['board.json:recent[].attendance', 'board.json:team_stats.attendance'] });
    }
  }

  // ================================================================== MATCHUP: schedule run + other upcoming opponents
  const up = board.upcoming ?? [];
  if (up.length >= 3) {
    const d0 = up[0].start_iso.slice(0, 10);
    const inWin = up.filter(u => (new Date(u.start_iso.slice(0, 10)) - new Date(d0)) / 864e5 <= 3);
    if (inWin.length >= 3) {
      const parts = inWin.map(u => `${dayOf(u.start_iso.slice(0, 10)).slice(0, 3)} ${u.home ? 'vs' : 'at'} ${u.opp.abbr} ${clock12(u.start_iso).replace(':00', '')}`);
      const last = inWin[inWin.length - 1], lastDate = last.start_iso.slice(0, 10);
      const thanks = new Date(`${lastDate}T12:00:00Z`).getUTCDay() === 1 && +lastDate.slice(8, 10) >= 8 && +lastDate.slice(8, 10) <= 14 && lastDate.slice(5, 7) === '10';
      add({ id: 'run-of-games', kind: 'gameday', priority: 2, team_id: me.id, valid_until: endOfDay(lastDate), confidence: 'fact',
        headline: `${cap(word(inWin.length))} games in four days: ${parts.map(p => p.split(' ')[0]).join(', ')}`.slice(0, 60),
        detail: `${parts.join(', ')}${thanks ? '. Monday is the Thanksgiving matinee' : ''}.`,
        stat: { value: String(inWin.length), label: 'games in 4 days' }, sources: ['board.json:upcoming'] });
    }
    // Summerside doubleheader angle
    const swc = rows.find(r => r.abbr === 'SWC');
    const swcGames = up.filter(u => u.opp.abbr === 'SWC');
    if (swc && swcGames.length >= 2 && ams) {
      const ppRank = rank1(rows, 'pp_pct', swc), pkRank = rank1(rows, 'pk_pct', ams);
      if (ppRank <= 2) {
        add({ id: 'swc-pp', kind: 'matchup', priority: 3, team_id: swc.team_id, valid_until: endOfDay(swcGames[swcGames.length - 1].start_iso.slice(0, 10)),
          headline: `Summerside's PP is No. ${ppRank} in the MHL; we see them twice`.slice(0, 60),
          detail: `${swc.pp_pct}% (${swc.pp.replace('/', ' for ')}) against a Ramblers kill ${notable('team', pkRank) ? `that ranks ${ord(pkRank)} ` : ''}at ${ams.pk_pct}%. ${swcGames.map(u => `${dayOf(u.start_iso.slice(0, 10)).slice(0, 3)} ${u.home ? 'home' : 'away'}`).join(', ')}.`,
          stat: { value: `${swc.pp_pct}%`, label: 'Summerside PP' }, sources: ['board.json:standings', 'board.json:upcoming'] });
      }
    }
    // last season's series with the weekend opponent (cached 2025-26 schedule)
    if (swcGames.length >= 2 && raw.schedule_prev) {
      const swcId = String(swcGames[0].opp.id);
      const gs = raw.schedule_prev.filter(g => g.final === '1' && (g.home_team === swcId || g.visiting_team === swcId));
      const wins = gs.filter(g => { const home = g.home_team === String(me.id); return +(home ? g.home_goal_count : g.visiting_goal_count) > +(home ? g.visiting_goal_count : g.home_goal_count); }).length;
      if (gs.length >= 6 && wins / gs.length >= 0.5) {
        add({ id: 'swc-series-last-year', kind: 'history', priority: 3, team_id: swcGames[0].opp.id, valid_until: endOfDay(swcGames[swcGames.length - 1].start_iso.slice(0, 10)), confidence: 'derived',
          headline: `Ramblers went ${wins}-${gs.length - wins} against Summerside last season`,
          detail: `${cap(word(gs.length))} meetings in 2025-26. Round one is ${dayOf(swcGames[0].start_iso.slice(0, 10))} in Summerside, then ${dayOf(swcGames[1].start_iso.slice(0, 10))} in Amherst.`,
          stat: { value: `${wins}-${gs.length - wins}`, label: 'vs SWC in 2025-26' }, sources: ['raw/schedule_prev.json', 'board.json:upcoming'] });
      }
    }
    // rematch with a team already met this season (not the very next game)
    const seen = new Set();
    for (const u of up.slice(1)) {
      if (seen.has(u.opp.abbr)) continue; seen.add(u.opp.abbr);
      const met = recentOld.filter(g => g.opp.abbr === u.opp.abbr);
      if (met.length >= 2 && ng && u.opp.id !== ng.opponent.id) {
        const w = met.filter(g => g.result === 'W').length, l = met.length - w;
        add({ id: `rematch-${u.opp.abbr.toLowerCase()}`, kind: 'matchup', priority: 4, team_id: u.opp.id, valid_until: endOfDay(u.start_iso.slice(0, 10)),
          headline: `${town(u.opp.abbr)} rematch is ${dayOf(u.start_iso.slice(0, 10))}, ${mdOf(u.start_iso.slice(0, 10))}`,
          detail: `Ramblers are ${w}-${l} against ${town(u.opp.abbr)} this season (${met.map(g => `${g.score.for}-${g.score.against}`).join(', ')}). The next one is ${u.home ? 'at Amherst Stadium' : 'on the road'}.`,
          stat: { value: `${w}-${l}`, label: `vs ${u.opp.abbr} this year` }, sources: ['board.json:recent', 'board.json:upcoming'] });
        break;
      }
    }
  }

  // ================================================================== TEAM / GAMEDAY
  if (gp >= 6) {
    // points from extra time
    const xt = recentOld.filter(g => A.get(g.id).decided);
    const xtPts = xt.reduce((n, g) => n + ptsOf(g), 0);
    const oneGoal = recentOld.filter(g => Math.abs(g.score.for - g.score.against) === 1).length;
    if (xt.length >= 2 && xtPts >= 3) {
      const what = xt.map(g => `${g.result === 'W' ? 'won' : 'lost'} ${A.get(g.id).decided === 'SO' ? 'a shootout' : 'in OT'} ${g.home ? 'vs' : 'at'} ${town(g.opp.abbr)}`);
      void oneGoal;
      add({ id: 'extra-time-points', kind: 'team', priority: 2, team_id: me.id, confidence: 'derived',
        headline: `${xtPts} of the Ramblers' ${ams.pts} points came in extra time`,
        detail: `${cap(word(xt.length))} of ${word(gp)} games went past regulation. Ramblers ${list(what)}.`.slice(0, 140),
        stat: { value: `${xtPts} of ${ams.pts}`, label: 'points in OT/SO games' }, sources: ['board.json:recent', 'board.json:standings'] });
    }

    // third periods
    const reg = regGoals.f;
    if (reg >= 12 && periodGoals.f[2] > periodGoals.f[0] && periodGoals.f[2] > periodGoals.f[1]) {
      add({ id: 'third-period-goals', kind: 'gameday', priority: 2, team_id: me.id, confidence: 'derived',
        headline: `Third periods are the Ramblers' best: ${periodGoals.f[2]} of ${reg} goals`,
        detail: `By period: ${periodGoals.f[0]} in the first, ${periodGoals.f[1]} in the second, ${periodGoals.f[2]} in the third (regulation goals only).`,
        stat: { value: String(periodGoals.f[2]), label: 'third-period goals' }, sources: ['board.json:recent[].goals'] });
    }

    // trailing after two
    const trailing = recentOld.filter(g => A.get(g.id).after2 < 0);
    const pointsFromTrailing = trailing.filter(g => g.result !== 'L');
    if (trailing.length >= 4 && pointsFromTrailing.length >= 2) {
      const wins = trailing.filter(g => g.result === 'W').length;
      add({ id: 'comebacks-after-two', kind: 'gameday', priority: 3, team_id: me.id, confidence: 'derived',
        headline: `Down after two periods? Ramblers still took points in ${pointsFromTrailing.length} of ${trailing.length}`,
        detail: `Ramblers trailed after two periods ${word(trailing.length)} times and took ${plural(pointsFromTrailing.length, 'a point', 'points')} in ${word(pointsFromTrailing.length)} (${wins} ${plural(wins, 'win')}).`.slice(0, 140),
        stat: { value: `${pointsFromTrailing.length} of ${trailing.length}`, label: 'points when trailing after 2' }, sources: ['board.json:recent[].goals'] });
    }

    // late goals
    let late = 0, lateOpp = 0, latest = null;
    for (const g of recentOld) for (const x of A.get(g.id).goals) {
      if (PERIOD[x.period] === undefined || secs(x.time) < 18 * 60) continue;
      if (x.mine) { late++; latest = { g, x }; } else lateOpp++;
    }
    if (late >= 4) {
      add({ id: 'late-period-goals', kind: 'gameday', priority: 3, team_id: me.id, confidence: 'derived',
        headline: `Watch the clock: ${late} Ramblers goals in a period's last 2:00`,
        detail: `${late} of ${reg} regulation goals. Most recent: ${surname(latest.x.scorer)} at ${latest.x.time} of the ${latest.x.period}, ${mdOf(latest.g.date)}. Opponents have ${lateOpp}.`.slice(0, 140),
        stat: { value: String(late), label: 'goals in final 2 min of a period' }, sources: ['board.json:recent[].goals'] });
    }

    // the Grand Falls rally
    for (const g of recentOld) {
      const a = A.get(g.id);
      if (g.result !== 'W' || a.decided !== 'OT' || a.episodes2 < 1) continue;
      const otGoal = a.goals.find(x => /OT/.test(x.period) && x.mine);
      const idx = a.goals.indexOf(otGoal), prev = a.goals[idx - 1];
      const tiedByHim = prev && prev.mine && prev.scorer_id === otGoal.scorer_id && prev.period === '3rd';
      const s = byId.get(otGoal.scorer_id);
      const left = tiedByHim ? 1200 - secs(prev.time) : null;
      const leftTxt = left !== null ? `${Math.floor(left / 60)}:${String(left % 60).padStart(2, '0')}` : null;
      const otSecs = secs(otGoal.time);
      add({ id: `rally-${g.date}`, kind: 'history', priority: 2, team_id: me.id, player_id: s?.id ?? null, headshot_url: s?.headshot_url ?? null,
        headline: `${cap(word(a.episodes2) === 'two' ? 'twice' : a.episodes2 + ' times')} down two goals, then OT win at ${town(g.opp.abbr)}`.slice(0, 60),
        detail: `${mdOf(g.date)}, ${g.score.for}-${g.score.against}. ${tiedByHim ? `${surname(otGoal.scorer)} tied it with ${leftTxt} left, then scored ${otSecs} seconds into overtime.` : `${surname(otGoal.scorer)} won it in overtime.`}`,
        confidence: 'fact', sources: [`board.json:recent[id=${g.id}].goals`] });
    }

    // home opener penalty minutes
    const opener = recentOld.find(g => g.home);
    if (opener && opener.pim && opener.pim.for + opener.pim.against >= 80) {
      add({ id: 'home-opener-pim', kind: 'fun', priority: 4, team_id: me.id,
        headline: `Home opener vs ${town(opener.opp.abbr)}: ${opener.pim.for + opener.pim.against} penalty minutes`,
        detail: `${mdOf(opener.date)}: Ramblers won ${opener.score.for}-${opener.score.against}. ${opener.pim.for} PIM for Amherst, ${opener.pim.against} for ${town(opener.opp.abbr)}.`,
        stat: { value: String(opener.pim.for + opener.pim.against), label: 'combined PIM' }, sources: [`board.json:recent[id=${opener.id}].pim`] });
    }

    // shot-count story
    const maxFor = recentOld.reduce((a, g) => (g.shots.for > a.shots.for ? g : a));
    const lastGame = board.recent[0];
    if (lastGame.shots.for - lastGame.shots.against >= 10 && lastGame.starting_goalie?.against?.sv >= 30 && lastGame.result === 'L' && lastGame.three_stars?.[0]?.id === lastGame.starting_goalie.against.id) {
      const og = lastGame.starting_goalie.against;
      add({ id: 'shots-last-game', kind: 'team', priority: 4, team_id: me.id,
        headline: `${lastGame.shots.for} shots ${mdOf(lastGame.date)}: ${town(lastGame.opp.abbr)}'s goalie stopped ${og.sv}`.slice(0, 60),
        detail: `Outshot ${town(lastGame.opp.abbr)} ${lastGame.shots.for}-${lastGame.shots.against}, and the goalie was named first star. Season high is ${maxFor.shots.for} at ${town(maxFor.opp.abbr)} (${mdOf(maxFor.date)}).`,
        stat: { value: String(lastGame.shots.for), label: `shots vs ${lastGame.opp.abbr}` }, sources: [`board.json:recent[id=${lastGame.id}].shots`] });
    }

    // last year's start
    const prev = raw.schedule_prev;
    if (prev) {
      const fin = prev.filter(g => g.final === '1').sort((a, b) => a.GameDateISO8601.localeCompare(b.GameDateISO8601)).slice(0, gp);
      if (fin.length === gp) {
        let w = 0, l = 0, otl = 0;
        for (const g of fin) {
          const home = g.home_team === String(me.id), f = +(home ? g.home_goal_count : g.visiting_goal_count), a = +(home ? g.visiting_goal_count : g.home_goal_count);
          if (f > a) w++; else if (g.overtime === '1' || g.shootout !== '0' || /OT|SO/.test(g.game_status)) otl++; else l++;
        }
        const lastPts = w * 2 + otl, thisPts = ams.pts;
        if (thisPts > lastPts) {
          add({ id: 'vs-last-year', kind: 'history', priority: 4, team_id: me.id, confidence: 'derived',
            headline: `${thisPts - lastPts} ${plural(thisPts - lastPts, 'point')} better than last year's start`,
            detail: `After ${gp} games: ${ams.w}-${ams.l}-${ams.otl}-${ams.sol} (${thisPts} pts) now, ${w}-${l}-${otl} (${lastPts} pts) in 2025-26.`,
            stat: { value: String(thisPts), label: `pts after ${gp} (was ${lastPts})` }, sources: ['board.json:standings', 'raw/schedule_prev.json'] });
        }
      }
    }
  }

  // ================================================================== LEAGUE
  {
    const south = mySouth.rows;
    const top = south.slice(0, 4);
    const hl = `${mySouth.name.replace('Eastlink ', '')} race: ${top.map(r => `${town(r.abbr)} ${r.pts}`).join(', ')}`;
    const unbeaten = rows.filter(r => r.l === 0 && r.otl === 0 && r.sol === 0 && r.gp >= 4);
    add({ id: 'division-race', kind: 'league', priority: 3, team_id: me.id, confidence: 'fact',
      headline: hl.length <= 60 ? hl : hl.slice(0, 57) + '...',
      detail: `Ramblers are ${ord(ams.rank)} with ${ams.pts} (${ams.gp} GP). ${south.filter(r => r.rank > ams.rank).map(r => `${town(r.abbr)} ${r.pts}`).join(', ')} follow. Top 4 qualify.`.slice(0, 140),
      stat: notable('division', ams.rank) ? { value: ord(ams.rank), label: 'in Eastlink South' } : { value: String(ams.pts), label: 'Ramblers points' }, sources: ['board.json:standings'] });
    for (const u of unbeaten) {
      add({ id: `unbeaten-${u.abbr.toLowerCase()}`, kind: 'league', priority: 4, team_id: u.team_id,
        headline: `${town(u.abbr)} is ${u.w}-0, the MHL's only unbeaten team`,
        detail: `${u.gf}-${u.ga} in goals over ${u.gp} games, ${u.pp_pct}% on the power play. ${u.division === ams.division ? 'They share our division.' : ''}`.trim(),
        stat: { value: `${u.w}-0`, label: `${u.abbr} record` }, sources: ['board.json:standings'] });
    }
    const lead = board.league_leaders.points.slice(0, 5);
    if (lead.length >= 5) {
      const l0 = lead[0];
      const best = lead.reduce((a, p) => (p.ppg > a.ppg ? p : a));
      add({ id: 'league-scoring-race', kind: 'league', priority: 5, headshot_url: l0.headshot_url, team_id: null,
        headline: `MHL scoring race: ${surname(l0.name)} (${town(l0.team)}) leads with ${l0.pts}`.slice(0, 60),
        detail: `${lead.slice(1).map(p => `${surname(p.name)} ${p.pts}`).join(', ')}. ${surname(best.name)} (${town(best.team)}) is scoring ${best.ppg} points a game.`,
        stat: { value: String(l0.pts), label: `${surname(l0.name)} points` }, sources: ['board.json:league_leaders.points'] });
    }
    const hot = (board.streaks.point_streaks ?? []).filter(s => s.active).sort((a, b) => b.games - a.games)[0];
    if (hot && hot.games >= 5) {
      add({ id: 'league-hot-streak', kind: 'streak', priority: 5, team_id: null, player_id: null,
        headline: `${hot.name} has a point in ${hot.games} straight`,
        detail: `${town(hot.team)}: ${hot.g} goals and ${hot.a} assists since ${mdOfFeed(hot.from)}. Longest active run on the MHL streaks list.`.slice(0, 140),
        stat: { value: String(hot.games), label: 'game point streak' }, sources: ['board.json:streaks.point_streaks'] });
    }
  }

  // ================================================================== PLAYERS
  const skaterOnly = skaters.filter(s => s.pos !== 'G');
  // scoring leader
  {
    const bypts = [...skaterOnly].sort((a, b) => b.pts - a.pts || b.g - a.g);
    const w = bypts[0];
    const rk = board.league_leaders.ramblers_ranks;
    const mhlRank = rk?.points?.find(x => x.id === w.id)?.rank;
    const rest = bypts.slice(1, 5);
    const grouped = [];
    for (const s of rest) { const last = grouped[grouped.length - 1]; if (last && last.pts === s.pts) last.names.push(surname(s.name)); else grouped.push({ pts: s.pts, names: [surname(s.name)] }); }
    const goalLead = [...skaterOnly].sort((a, b) => b.g - a.g)[0];
    playerItem(w, { id: 'team-points-leader', kind: 'player', priority: 3,
      headline: `${surname(w.name)} leads the Ramblers with ${w.pts} points`,
      detail: `${w.g} goals, ${w.a} assists${notable('player', mhlRank) ? `; ${ord(mhlRank)} in MHL scoring` : ''}. Next: ${grouped.map(g => `${list(g.names)} ${g.pts}`).join(', ')}.`.slice(0, 140),
      stat: { value: String(w.pts), label: `${surname(w.name)} points` }, sources: [`board.json:skaters[id=${w.id}]`, 'board.json:league_leaders.ramblers_ranks'] });
    void goalLead;
  }
  // opening point streak (season-best list) and active streaks from last5
  for (const st of (board.streaks.ramblers ?? []).filter(x => x.kind === 'points' && x.games >= 4)) {
    const s = byId.get(st.id); const firstGame = recentOld[0]?.date;
    const opening = firstGame && mdOfFeed(st.from) === mdOf(firstGame) && recentOld.length >= st.games && recentOld[st.games - 1].date >= firstGame;
    if (!s) continue;
    playerItem(s, { id: `streak-points-${s.id}`, kind: 'streak', priority: 3,
      headline: opening ? `${surname(s.name)} opened the year with a point in ${st.games} straight`.slice(0, 60) : `${surname(s.name)} has had a point in ${st.games} straight`,
      detail: `${mdOfFeed(st.from)} to ${st.active ? 'now' : mdOfFeed(st.to)}: ${st.g} goals and ${st.a} assists. ${st.active ? 'Still going.' : 'His longest run this season.'}`,
      stat: { value: String(st.games), label: 'game point streak' }, sources: [`board.json:streaks.ramblers`] });
  }
  for (const s of skaterOnly) { // active streaks from the last five games (min 3 points / 2 goals)
    let p = 0, gStreak = 0;
    for (const x of s.last5) { if (x.pts > 0) p++; else break; }
    for (const x of s.last5) { if (x.g > 0) gStreak++; else break; }
    if (p >= 3) playerItem(s, { id: `active-points-${s.id}`, kind: 'streak', priority: 2, headline: `${surname(s.name)} has a point in ${p} straight`, detail: `Active run through ${mdOf(s.last5[0].date)}.`, stat: { value: String(p), label: 'game point streak' }, sources: [`board.json:skaters[id=${s.id}].last5`] });
    if (gStreak >= 2) playerItem(s, { id: `active-goals-${s.id}`, kind: 'streak', priority: 2, headline: `${surname(s.name)} has scored in ${gStreak} straight`, detail: `Active run through ${mdOf(s.last5[0].date)}.`, stat: { value: String(gStreak), label: 'game goal streak' }, sources: [`board.json:skaters[id=${s.id}].last5`] });
  }
  // first MHL goals
  const goalDate = id => { for (const g of recentOld) { const x = g.scorers.find(q => q.scorer_id === id); if (x) return { g, x }; } return null; };
  const firsts = skaterOnly.filter(s => s.g > 0 && s.career && s.career.g === s.g).map(s => ({ s, hit: goalDate(s.id) })).filter(f => f.hit);
  if (firsts.length >= 3) {
    firsts.sort((a, b) => b.s.g - a.s.g || a.s.name.localeCompare(b.s.name));
    add({ id: 'first-goal-class', kind: 'player', priority: 2, team_id: me.id, confidence: 'derived',
      headline: `${cap(word(firsts.length))} Ramblers have scored their first MHL goals this season`.slice(0, 60),
      detail: `${list(firsts.map(f => surname(f.s.name)))}. Ages ${Math.min(...firsts.map(f => f.s.age))} to ${Math.max(...firsts.map(f => f.s.age))}.`,
      stat: { value: String(firsts.length), label: 'first career MHL goals' }, sources: firsts.map(f => `board.json:skaters[id=${f.s.id}].career`) });
  }
  // hometown heroes (Amherst)
  for (const s of skaterOnly.filter(x => /^Amherst,/.test(x.hometown ?? ''))) {
    const first = firsts.find(f => f.s.id === s.id);
    const gamesBefore = s.career.gp - s.gp;
    if (first) {
      const a = first.hit.x.assists.length ? ` Assist: ${first.hit.x.assists.map(surname).join(', ')}.` : '';
      const left = 1200 - secs(first.hit.x.time);
      playerItem(s, { id: `local-first-goal-${s.id}`, kind: 'player', priority: 1, confidence: 'derived',
        headline: `Amherst's own ${s.name} has his first MHL goal`.slice(0, 60),
        detail: `#${s.number} scored ${left < 120 ? `with ${left} seconds left ` : ''}against ${town(first.hit.g.opp.abbr)} on ${mdOf(first.hit.g.date)}.${a}`.replace('  ', ' '),
        stat: { value: '1', label: 'career MHL goal' }, sources: [`board.json:skaters[id=${s.id}]`, `board.json:recent[id=${first.hit.g.id}].scorers`] });
    } else if (s.career.gp >= 100 && gamesBefore < 100) {
      playerItem(s, { id: `local-century-${s.id}`, kind: 'milestone', priority: 3, confidence: 'derived',
        headline: `Amherst's own ${s.name} is past 100 MHL games`.slice(0, 60),
        detail: `#${s.number}, a ${s.pos === 'D' ? 'defenceman' : 'forward'} from ${s.hometown.replace(', NS', ', N.S.')}, is at ${s.career.gp} career regular-season MHL games.`,
        stat: { value: String(s.career.gp), label: 'career MHL games' }, sources: [`board.json:skaters[id=${s.id}].career`] });
    }
  }
  // birthdays
  {
    const [ty, tm, td] = today.split('-').map(Number);
    const bd = [];
    for (const s of [...skaters, ...board.goalies.filter(g => !byId.has(g.id))]) {
      const b = parseBirth(s.birthdate); if (!b) continue;
      const when = `${ty}-${String(b.mon).padStart(2, '0')}-${String(b.day).padStart(2, '0')}`;
      const delta = Math.round((new Date(`${when}T12:00:00Z`) - new Date(`${today}T12:00:00Z`)) / 864e5);
      if (delta >= 0 && delta <= 6) bd.push({ s, when, delta, turning: ty - b.year });
    }
    void tm; void td;
    if (bd.length) {
      bd.sort((a, b) => a.delta - b.delta || a.s.name.localeCompare(b.s.name));
      const first = bd[0];
      const sameDay = bd.filter(x => x.when === first.when);
      const dayWord = first.delta === 0 ? 'today' : `on ${dayOf(first.when)}`;
      const names = list(sameDay.map(x => surname(x.s.name)));
      add({ id: 'birthdays', kind: 'fun', priority: 2, team_id: me.id, valid_until: endOfDay(first.when), confidence: 'fact',
        player_id: sameDay.length === 1 ? sameDay[0].s.id : null, headshot_url: sameDay.length === 1 ? sameDay[0].s.headshot_url : null,
        headline: sameDay.length > 1 && sameDay.every(x => x.turning === sameDay[0].turning)
          ? `Birthday boys: ${names} both turn ${sameDay[0].turning} ${dayWord}`.slice(0, 60)
          : `Birthday ${dayWord}: ${names}`.slice(0, 60),
        detail: `${sameDay.map(x => `${x.s.name} (#${x.s.number}, ${x.s.hometown.replace(/,\s*$/, '')}, turning ${x.turning})`).join('; ')}.${bd.length > sameDay.length ? ` More birthdays later this week.` : ''}`.slice(0, 140),
        stat: { value: String(sameDay[0].turning), label: sameDay.length > 1 ? 'both turning' : 'turning' }, sources: sameDay.map(x => `board.json:skaters[id=${x.s.id}].birthdate`) });
    }
  }
  // rookie spotlight
  {
    const rookies = skaterOnly.filter(s => s.rookie);
    const topR = [...rookies].sort((a, b) => b.pts - a.pts || b.g - a.g)[0];
    if (topR && topR.pts >= 5) {
      const ranksTeam = 1 + skaterOnly.filter(s => s.pts > topR.pts).length;
      const youngest = [...skaterOnly].sort((a, b) => (parseBirth(b.birthdate).year * 400 + parseBirth(b.birthdate).mon * 32 + parseBirth(b.birthdate).day) - (parseBirth(a.birthdate).year * 400 + parseBirth(a.birthdate).mon * 32 + parseBirth(a.birthdate).day))[0];
      let rankTxt = '';
      const lg = raw.league_skaters;
      if (lg) {
        const rk = lg.filter(r => r.rookie === '1');
        const ahead = rk.filter(r => +r.points > topR.pts).length;
        const tiedN = rk.filter(r => +r.points === topR.pts).length;
        if (notable('rookie', ahead + 1) && rk.some(r => +r.player_id === topR.id) && lg.length >= 100 && Math.min(...lg.map(r => +r.points)) <= topR.pts)
          rankTxt = ` ${tiedN > 1 ? 'Tied for ' : ''}${ord(ahead + 1)} among MHL rookies.`;
      }
      playerItem(topR, { id: `rookie-${topR.id}`, kind: 'player', priority: 2,
        headline: `${topR.age}-year-old rookie ${surname(topR.name)} has ${topR.pts} points`.slice(0, 60),
        detail: `${topR.g} goals, ${topR.a} assists. From ${topR.hometown}${youngest.id === topR.id ? ', youngest skater on the roster' : ''}. ${ranksTeam === 1 ? 'Tops' : ord(ranksTeam)} on the team.${rankTxt}`.slice(0, 140),
        stat: { value: String(topR.pts), label: 'rookie points' }, sources: [`board.json:skaters[id=${topR.id}]`, ...(rankTxt ? ['raw/league_skaters.json'] : [])] });
    }
    const rookGoals = rookies.reduce((n, s) => n + s.g, 0), rookPts = rookies.reduce((n, s) => n + s.pts, 0);
    const allGoals = skaterOnly.reduce((n, s) => n + s.g, 0);
    if (rookies.length >= 4) {
      add({ id: 'rookie-class', kind: 'team', priority: 4, team_id: me.id, confidence: 'derived',
        headline: `The rookie class has ${rookPts} points and ${rookGoals} goals`,
        detail: `${cap(word(rookies.length))} rookie skaters: ${rookies.sort((a, b) => b.pts - a.pts).filter(s => s.pts > 0).map(s => `${surname(s.name)} ${s.pts}`).join(', ')}. That is ${rookGoals} of the ${allGoals} skater goals.`.slice(0, 140),
        stat: { value: String(rookPts), label: 'rookie points' }, sources: ['board.json:skaters[rookie=true]'] });
    }
  }
  // milestones
  {
    const near = (board.milestones_near ?? []).filter(m => byId.has(m.id) && byId.get(m.id).pos !== 'G');
    const byPlayer = new Map();
    for (const m of near) { (byPlayer.get(m.id) ?? byPlayer.set(m.id, []).get(m.id)).push(m); }
    const used = new Set();
    for (const [id, ms] of [...byPlayer.entries()].sort((a, b) => Math.min(...a[1].map(m => m.needs)) - Math.min(...b[1].map(m => m.needs)) || b[1][0].current - a[1][0].current)) {
      const s = byId.get(id);
      const biggest = [...ms].sort((a, b) => b.milestone - a.milestone)[0];
      if (!(biggest.milestone >= 50 || ms.length >= 2)) continue;
      const lines = ms.sort((a, b) => a.needs - b.needs).map(m => `${m.needs} ${m.stat.includes('goals') ? plural(m.needs, 'goal') : plural(m.needs, 'point')} from ${m.milestone}`);
      used.add(id);
      playerItem(s, { id: `milestone-${s.id}`, kind: 'milestone', priority: 2,
        headline: ms.length === 1 ? `${surname(s.name)} is ${lines[0].replace(' from ', ' shy of ')} career MHL ${biggest.stat.includes('goals') ? 'goals' : 'points'}`.slice(0, 60) : `${surname(s.name)}: ${lines.map(l => l.replace(' from ', ' shy of ')).join(', ')} (career)`.slice(0, 60),
        detail: `${s.name} (#${s.number}, ${s.hometown}) has ${ms.map(m => `${m.current} career MHL ${m.stat.replace('career MHL ', '')}`).join(' and ')}${ms.length === 1 && s.career.pts ? `, ${s.career.pts} points in ${s.career.gp} games` : ''}.`.slice(0, 140),
        stat: { value: String(Math.min(...ms.map(m => m.needs))), label: ms.length === 1 ? `${biggest.stat.includes('goals') ? 'goals' : 'points'} to ${biggest.milestone}` : 'to the next mark' },
        confidence: 'derived', sources: ['board.json:milestones_near', `board.json:skaters[id=${s.id}].career`] });
    }
    const others = near.filter(m => !used.has(m.id)).sort((a, b) => a.needs - b.needs);
    if (others.length >= 2) {
      add({ id: 'milestones-more', kind: 'milestone', priority: 4, team_id: me.id, confidence: 'derived',
        headline: 'More career milestones within reach',
        detail: others.map(m => `${byId.get(m.id).name}: ${m.needs} ${m.stat.includes('goals') ? plural(m.needs, 'goal') : plural(m.needs, 'point')} from ${m.milestone}`).join('. ').slice(0, 139) + '.',
        stat: { value: String(others.length), label: 'players near a milestone' }, sources: ['board.json:milestones_near'] });
    }
    // a milestone passed this season
    for (const s of skaterOnly) {
      if (s.career.pts >= 100 && s.career.pts - s.pts < 100) {
        playerItem(s, { id: `passed-100-${s.id}`, kind: 'milestone', priority: 3, confidence: 'derived',
          headline: `${surname(s.name)} has gone past 100 career MHL points`,
          detail: `${s.name} (#${s.number}, ${s.hometown}): ${s.career.g} goals and ${s.career.a} assists in ${s.career.gp} career MHL games.`,
          stat: { value: String(s.career.pts), label: 'career MHL points' }, sources: [`board.json:skaters[id=${s.id}].career`] });
      }
    }
  }
  // balanced scoring
  {
    const scorers = skaterOnly.filter(s => s.g > 0).sort((a, b) => b.g - a.g);
    if (scorers.length >= 8) {
      const ones = scorers.filter(s => s.g === 1).length;
      const top = scorers.filter(s => s.g > 1);
      add({ id: 'ten-scorers', kind: 'team', priority: 3, team_id: me.id,
        headline: `${cap(word(scorers.length))} different Ramblers have scored this season`,
        detail: `${top.map(s => `${surname(s.name)} ${s.g}`).join(', ')}${ones ? `, and ${word(ones)} with one each` : ''}.`.slice(0, 140),
        stat: { value: String(scorers.length), label: 'different goal scorers' }, sources: ['board.json:skaters'] });
    }
  }
  // roster geography
  {
    const counts = new Map(); let known = 0;
    for (const s of [...skaters]) { const p = provOf(s.hometown); if (p) { counts.set(p, (counts.get(p) ?? 0) + 1); known++; } }
    const order = [...counts.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
    if (known >= 20 && order.length >= 3) {
      add({ id: 'roster-map', kind: 'fun', priority: 4, team_id: me.id,
        headline: `Home towns: ${order[0][0] === 'NS' ? 'Nova Scotia' : order[0][0]} leads the roster with ${order[0][1]}`.slice(0, 60),
        detail: `${order.map(([p, n]) => `${p === 'PE' ? 'PEI' : p} ${n}`).join(', ')}. ${known} players listed.`,
        stat: { value: String(order.length), label: 'provinces on the roster' }, sources: ['board.json:skaters[].hometown'] });
    }
  }

  // ================================================================== GOALIES (plain season facts)
  {
    const g = board.goalies.slice().sort((a, b) => b.gp - a.gp)[0];
    const svBoard = board.league_leaders.goalies?.sv_pct ?? [];
    const idx = svBoard.findIndex(x => x.id === g.id);
    if (g && idx >= 0 && notable('player', idx + 1)) {
      add({ id: 'goalie-svpct-rank', kind: 'goalie', priority: 3, team_id: me.id, player_id: g.id, headshot_url: g.headshot_url,
        headline: `${surname(g.name)} ranks ${ord(idx + 1)} in the MHL in save percentage`.slice(0, 60),
        detail: `${String(g.sv_pct.toFixed(3)).slice(1)} over ${g.gp} games (${g.saves} saves on ${g.shots_against} shots), among goalies with ${board.league_leaders.goalies.min_gp}+ GP.`.slice(0, 140),
        stat: { value: g.sv_pct.toFixed(3).slice(1), label: 'save percentage' }, sources: [`board.json:goalies[id=${g.id}]`, 'board.json:league_leaders.goalies.sv_pct'] });
    }
    const lg = raw.league_goalies;
    const gp0 = recentOld.map(r => ({ r, sg: r.starting_goalie?.for })).filter(x => x.sg && x.sg.id === g.id);
    const heavy = gp0.reduce((a, x) => (!a || x.sg.sa > a.sg.sa ? x : a), null);
    if (lg && heavy && heavy.sg.sa >= 55) {
      const mostSaves = Math.max(...lg.map(r => +r.saves));
      const leads = g.saves === mostSaves;
      add({ id: 'goalie-workload', kind: 'goalie', priority: 4, team_id: me.id, player_id: g.id, headshot_url: g.headshot_url,
        headline: `${surname(g.name)} faced ${heavy.sg.sa} shots at ${town(heavy.r.opp.abbr)}`.slice(0, 60),
        detail: `${mdOf(heavy.r.date)}: ${heavy.sg.sv} saves.${leads ? ` His ${g.saves} saves this season lead the MHL.` : ''}`.slice(0, 140),
        stat: { value: String(heavy.sg.sv), label: 'saves in one game' }, sources: [`board.json:recent[id=${heavy.r.id}].starting_goalie`, ...(leads ? ['raw/league_goalies.json'] : [])] });
    }
  }

  return { items, notes };
}

// ------------------------------------------------------------------ validation
const KINDS = new Set(['streak', 'milestone', 'matchup', 'team', 'player', 'goalie', 'league', 'history', 'gameday', 'fun']);
const NEGATIVE = /\b(slump|drought|struggl\w*|cold|pointless|scoreless|without a (point|goal)|hasn't|haven't|winless|worst|poor|bad|bottom)\b/i;
export function validate(items, board) {
  const errs = [];
  const ids = new Set();
  const goalieNames = [...board.goalies.map(g => g.name), ...(board.next_game?.opponent?.goalies ?? []).map(g => g.name),
    ...board.recent.flatMap(r => [r.starting_goalie?.for?.name, r.starting_goalie?.against?.name].filter(Boolean))].flatMap(n => [n, surname(n)]);
  const ramblers = [...board.skaters, ...board.goalies].map(p => surname(p.name));
  for (const it of items) {
    if (ids.has(it.id)) errs.push(`duplicate id ${it.id}`); ids.add(it.id);
    if (!KINDS.has(it.kind)) errs.push(`${it.id}: bad kind ${it.kind}`);
    if (!Number.isInteger(it.priority) || it.priority < 1 || it.priority > 5) errs.push(`${it.id}: bad priority`);
    if (!it.headline || it.headline.length > 60) errs.push(`${it.id}: headline ${it.headline?.length} chars: ${it.headline}`);
    if (!it.detail || it.detail.length > 140) errs.push(`${it.id}: detail ${it.detail?.length} chars: ${it.detail}`);
    if (!['fact', 'derived'].includes(it.confidence)) errs.push(`${it.id}: bad confidence`);
    if (!it.sources?.length) errs.push(`${it.id}: no sources`);
    const text = `${it.headline} ${it.detail} ${it.stat?.value ?? ''} ${it.stat?.label ?? ''}`;
    if (/undefined|\bnull\b|NaN|\[object/.test(text)) errs.push(`${it.id}: junk in text: ${text}`);
    if (/\b1-game\b|\b1 game streak\b/i.test(text)) errs.push(`${it.id}: trivial streak`);
    // negativity filter for anything that names a Ramblers player
    if (ramblers.some(n => n.length > 3 && text.includes(n)) && NEGATIVE.test(text)) errs.push(`${it.id}: negative wording about a Ramblers player: ${text}`);
  }
  errs.push(...goalieViolations(items, goalieNames));
  // any item naming a goalie of an upcoming-game opponent must be the plain history item or an official-starter item
  const oppGoalies = (board.next_game?.opponent?.goalies ?? []).flatMap(g => [g.name, surname(g.name)]);
  for (const it of items) {
    if (it.valid_until && it.kind !== 'goalie' && oppGoalies.some(n => `${it.headline} ${it.detail}`.includes(n))) errs.push(`${it.id}: names an upcoming-game opposing goalie outside a goalie item`);
  }
  const starters = board.next_game?.official_starters;
  const officialIds = new Set([starters?.home?.id, starters?.away?.id].filter(Boolean));
  for (const it of items) if (it.id.startsWith('starter-official') && (!starters || !officialIds.size)) errs.push(`${it.id}: official starter item without official_starters`);
  if (items.length < 15 || items.length > 40) errs.push(`item count ${items.length} outside 15-40`);
  const heads = new Set(); for (const it of items) { const h = it.headline.toLowerCase(); if (heads.has(h)) errs.push(`duplicate headline ${h}`); heads.add(h); }
  return errs;
}

// ------------------------------------------------------------------ CLI
function loadRaw() {
  const raw = {};
  const sp = readRaw('schedule_prev.json'); if (sp?.SiteKit?.Schedule) raw.schedule_prev = sp.SiteKit.Schedule;
  const sc = readRaw('schedule.json'); if (sc?.SiteKit?.Schedule) raw.schedule_total = sc.SiteKit.Schedule.length;
  const rows = j => j?.[0]?.sections?.[0]?.data?.map(x => x.row);
  const ls = rows(readRaw('league_skaters.json')); if (ls) raw.league_skaters = ls;
  const lg = rows(readRaw('league_goalies.json')); if (lg) raw.league_goalies = lg;
  return raw;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  selfTest();
  const board = readJson(path.join(DATA, 'board.json'));
  const { items, notes } = buildItems(board, { raw: loadRaw() });
  // Official-starter path: prove it works and stays cited, using a copy of the data (never written).
  {
    const fake = JSON.parse(JSON.stringify(board));
    fake.next_game.official_starters = { home: { id: 3766, name: 'Brandon Lavoie', number: 29, source: 'statviewfeed gameSummary' }, away: null };
    const t = buildItems(fake, { raw: loadRaw() }).items.find(i => i.id === 'starter-official-home');
    if (!t || !/Source:/.test(t.detail)) throw new Error('official-starter path did not cite its source');
    if (items.some(i => i.id.startsWith('starter-official'))) throw new Error('starter item produced without official_starters');
  }
  items.sort((a, b) => a.priority - b.priority || a.id.localeCompare(b.id));
  const errs = validate(items, board);
  for (const n of notes) console.warn(`data note: ${n}`);
  if (process.argv.includes('--print')) for (const i of items) console.log(`[${i.priority}] ${i.kind.padEnd(9)} ${i.id}\n    ${i.headline}\n    ${i.detail}\n    stat=${i.stat ? i.stat.value + ' | ' + i.stat.label : '-'} h${i.headline.length} d${i.detail.length}`);
  if (errs.length) { console.error(`insights validation FAILED:\n- ${errs.join('\n- ')}`); process.exit(1); }
  const out = { generated_at: new Date().toISOString(), source_generated_at: board.generated_at, items };
  const counts = {}; for (const i of items) counts[i.kind] = (counts[i.kind] ?? 0) + 1;
  const pr = {}; for (const i of items) pr[i.priority] = (pr[i.priority] ?? 0) + 1;
  console.log(`${items.length} items`, JSON.stringify(counts), 'priority', JSON.stringify(pr));
  if (!process.argv.includes('--check')) {
    fs.writeFileSync(path.join(DATA, 'insights.json'), JSON.stringify(out, null, 2) + '\n');
    console.log('wrote bakeoff/data/insights.json');
  }
}
