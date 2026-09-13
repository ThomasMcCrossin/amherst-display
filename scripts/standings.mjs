import { config, request, moduleRows, statView, statSections } from './hockeytech.mjs';

// API team membership and division labels, not page-default season or fixed divisions.
export async function buildMHLStandings({ nameToSlug }) {
  const teams = moduleRows(await request({ feed: 'modulekit', view: 'teamsbyseason', season_id: config.season_ids[0] }), 'Teamsbyseason');
  if (!teams.length) throw new Error('MHL standings: empty team directory');
  const teamByName = new Map(teams.map(team => [team.name, team]));
  const sections = statSections(await statView('teams', { groupTeamsBy: 'division', context: 'overall', division: -1, special: 'false' }));
  const fields = { gp: 'games_played', w: 'wins', l: 'losses', otl: 'ot_losses', sol: 'shootout_losses', pts: 'points',
    rw: 'regulation_wins', otw: 'ot_wins', sow: 'shootout_wins', gf: 'goals_for', ga: 'goals_against', diff: 'goals_diff', pim: 'penalty_minutes' };
  const seen = new Set(), rows = [];
  for (const section of sections) {
    for (const item of section.data) {
      const data = item.row;
      const team = teamByName.get(data?.name);
      const slug = nameToSlug.get(data?.name?.toLowerCase().trim());
      if (!team || !slug || seen.has(team.id)) throw new Error('MHL standings: unknown or duplicate team');
      seen.add(team.id);
      const row = {};
      for (const [key, field] of Object.entries(fields)) {
        const value = Number(data[field]);
        if (data[field] == null || !Number.isFinite(value)) throw new Error(`MHL standings: invalid ${field}`);
        row[key] = value;
      }
      row.pct = data.percentage;
      row.streak = data.streak;
      row.p10 = data.past_10;
      row.division_rank = Number(data.rank);
      row.team = data.name;
      row.division = team.division_long_name.replace(/^EastLink/, 'Eastlink');
      row.slug = slug;
      rows.push(row);
    }
  }
  if (seen.size !== teams.length) throw new Error('MHL standings: incomplete team coverage');
  return { generated_at: new Date().toISOString(), season: config.season_label, league: 'MHL',
    divisions: [...new Set(rows.map(row => row.division))], playoff_format: 'Top 4 per division qualify', rows };
}
