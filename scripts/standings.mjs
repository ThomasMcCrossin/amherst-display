import fetch from 'node-fetch';
import cheerio from 'cheerio';
import { formatInTimeZone } from 'date-fns-tz';

const TZ = 'America/Halifax';
const nowISO = () => formatInTimeZone(new Date(), TZ, "yyyy-MM-dd'T'HH:mm:ssXXX");

/**
 * Normalize for matching aliases.
 */
function normalize(name){ return name?.replace(/\s+/g,' ').trim().toLowerCase() || ''; }

export async function buildMHLStandings({ nameToSlug, standingsUrl }){
  const html = await (await fetch(standingsUrl)).text();
  const $ = cheerio.load(html);
  const rows = [];
  // TODO: confirm the correct table selector on themhla.ca
  $('table tbody tr').each((_, tr)=>{
    const tds = $(tr).find('td');
    if(tds.length < 6) return;
    const teamName = $(tds[0]).text().trim();
    const gp = parseInt($(tds[1]).text(),10)||0;
    const w  = parseInt($(tds[2]).text(),10)||0;
    const l  = parseInt($(tds[3]).text(),10)||0;
    const ot = parseInt($(tds[4]).text(),10)||0;  // OTL/SOL depending on site
    const pts= parseInt($(tds[5]).text(),10)||0;
    const slug = nameToSlug.get(normalize(teamName));
    if(!slug){ console.warn('[MHL] Unmapped team:', teamName); return; }
    rows.push({ team_slug: slug, gp, w, l, otl: ot, pts });
  });
  return { generated_at: nowISO(), season: '', league: 'MHL', rows };
}

export async function buildBSHLStandings({ nameToSlug, standingsUrl }){
  const html = await (await fetch(standingsUrl)).text();
  const $ = cheerio.load(html);
  const rows = [];
  // TODO: confirm the correct table selector on bshlhockey.com
  $('table tbody tr').each((_, tr)=>{
    const tds = $(tr).find('td');
    if(tds.length < 6) return;
    const teamName = $(tds[0]).text().trim();
    const gp = parseInt($(tds[1]).text(),10)||0;
    const w  = parseInt($(tds[2]).text(),10)||0;
    const l  = parseInt($(tds[3]).text(),10)||0;
    const ot = parseInt($(tds[4]).text(),10)||0;
    const pts= parseInt($(tds[5]).text(),10)||0;
    const slug = nameToSlug.get(normalize(teamName));
    if(!slug){ console.warn('[BSHL] Unmapped team:', teamName); return; }
    rows.push({ team_slug: slug, gp, w, l, otl: ot, pts });
  });
  return { generated_at: nowISO(), season: '', league: 'BSHL', rows };
}
