// Ramblers season calendar (ramblers.ics) built from the nightly HockeyTech schedule.
// Replaces the hand-uploaded 2025-26 HockeyTech export that nothing regenerated. Written to
// both published paths (ramblers.ics, data/ramblers.ics) so existing subscriptions roll over.
// Remove: delete this file, its call in build_all.mjs and the two paths in build-jsons.yml.

const TEAM_SLUG = 'amherst-ramblers';
const GAME_HOURS = 3;

const pad = n => String(n).padStart(2, '0');
const utcStamp = d =>
  `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}T${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}Z`;
const escapeText = s => String(s ?? '').replace(/\\/g, '\\\\').replace(/;/g, '\\;').replace(/,/g, '\\,').replace(/\r?\n/g, '\\n');

// RFC 5545 3.1: lines longer than 75 octets are folded with CRLF + space.
function fold(line) {
  const bytes = Buffer.from(line, 'utf8');
  if (bytes.length <= 75) return line;
  const parts = [];
  let current = '';
  for (const ch of line) {
    const limit = parts.length ? 74 : 75;
    if (Buffer.byteLength(current + ch, 'utf8') > limit) { parts.push(current); current = ''; }
    current += ch;
  }
  parts.push(current);
  return parts.join('\r\n ');
}

function resultText(game) {
  const r = game?.result;
  if (!r || r.ramblers_score == null || r.opponent_score == null) return '';
  const outcome = r.won ? 'W' : (r.overtime || r.shootout ? 'OTL' : 'L');
  const suffix = r.shootout ? ' SO' : (r.overtime ? ' OT' : '');
  return `${outcome} ${r.ramblers_score}-${r.opponent_score}${suffix}`;
}

export function buildRamblersICS(events, { seasonLabel = '', results = new Map() } = {}) {
  const lines = [
    'BEGIN:VCALENDAR', 'VERSION:2.0', 'PRODID:-//amherst-display//Ramblers schedule//EN', 'CALSCALE:GREGORIAN',
    'METHOD:PUBLISH', `X-WR-CALNAME:${escapeText(`Amherst Ramblers ${seasonLabel}`.trim())}`,
    'X-WR-TIMEZONE:America/Halifax', 'REFRESH-INTERVAL;VALUE=DURATION:PT12H', 'X-PUBLISHED-TTL:PT12H',
  ];
  const games = events
    .filter(e => e.home_slug === TEAM_SLUG || e.away_slug === TEAM_SLUG)
    .filter(e => e.start && !Number.isNaN(Date.parse(e.start)))
    .sort((a, b) => Date.parse(a.start) - Date.parse(b.start));
  for (const e of games) {
    const start = new Date(e.start);
    const end = new Date(start.getTime() + GAME_HOURS * 3600 * 1000);
    const result = resultText(results.get(String(e.game_id)));
    const summary = `${e.away_team} @ ${e.home_team}${result ? ` (${result})` : ''}`;
    const description = [e.league, e.game_id ? `HockeyTech game ${e.game_id}` : '', result ? `Final: ${result}` : '']
      .filter(Boolean).join('\n');
    lines.push(
      'BEGIN:VEVENT',
      `UID:mhl-${e.game_id || utcStamp(start)}@amherst-display`,
      // Stable stamp (the start) so a nightly rebuild with no schedule change is not a diff.
      `DTSTAMP:${utcStamp(start)}`,
      `DTSTART:${utcStamp(start)}`,
      `DTEND:${utcStamp(end)}`,
      `SUMMARY:${escapeText(summary)}`,
      `LOCATION:${escapeText(e.location || '')}`,
      `DESCRIPTION:${escapeText(description)}`,
      'STATUS:CONFIRMED',
      'END:VEVENT',
    );
  }
  lines.push('END:VCALENDAR');
  return lines.map(fold).join('\r\n') + '\r\n';
}
