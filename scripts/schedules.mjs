import fetch from 'node-fetch';
import cheerio from 'cheerio';
import { formatInTimeZone } from 'date-fns-tz';

const TZ = 'America/Halifax';
const toISO = (d) => formatInTimeZone(d, TZ, "yyyy-MM-dd'T'HH:mm:ssXXX");
const normalize = (s) => s?.replace(/\s+/g,' ').trim() || '';

export async function fetchRamblersSchedule({ scheduleUrl, nameToSlug }){
  const html = await (await fetch(scheduleUrl)).text();
  const $ = cheerio.load(html);
  const events = [];
  // TODO: adjust selector for Ramblers schedule rows on themhla.ca
  $('table tbody tr').each((_, tr)=>{
    const tds = $(tr).find('td');
    if(tds.length < 5) return;
    const dateStr = normalize($(tds[0]).text());
    const timeStr = normalize($(tds[1]).text());
    const homeN   = normalize($(tds[2]).text());
    const awayN   = normalize($(tds[3]).text());
    const venue   = normalize($(tds[4]).text());
    const start   = toISO(new Date(`${dateStr} ${timeStr}`));
    const home    = nameToSlug.get(homeN.toLowerCase());
    const away    = nameToSlug.get(awayN.toLowerCase());
    if(!home||!away) return;
    events.push({
      league: 'MHL',
      home_team: homeN, away_team: awayN,
      home_slug: home,  away_slug: away,
      start, location: venue
    });
  });
  return events;
}

export async function fetchDucksSchedule({ scheduleUrl, nameToSlug }){
  const html = await (await fetch(scheduleUrl)).text();
  const $ = cheerio.load(html);
  const events = [];
  // TODO: adjust selector for Ducks schedule rows on bshlhockey.com
  $('table tbody tr').each((_, tr)=>{
    const tds = $(tr).find('td');
    if(tds.length < 5) return;
    const dateStr = normalize($(tds[0]).text());
    const timeStr = normalize($(tds[1]).text());
    const homeN   = normalize($(tds[2]).text());
    const awayN   = normalize($(tds[3]).text());
    const venue   = normalize($(tds[4]).text());
    const start   = toISO(new Date(`${dateStr} ${timeStr}`));
    const home    = nameToSlug.get(homeN.toLowerCase());
    const away    = nameToSlug.get(awayN.toLowerCase());
    if(!home||!away) return;
    events.push({
      league: 'BSHL',
      home_team: homeN, away_team: awayN,
      home_slug: home,  away_slug: away,
      start, location: venue
    });
  });
  return events;
}
