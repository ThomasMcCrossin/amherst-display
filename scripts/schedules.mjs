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

export async function fetchDucksSchedule({ scheduleUrl, nameToSlug }) {
  const html = await (await fetch(scheduleUrl)).text();
  const $ = cheerio.load(html);

  // Grab the main content text; the schedule appears as lines with “ -- ” separators.
  const text = $('body').text(); // you can narrow this if you find a specific container
  const lines = text.split('\n').map(s => s.trim()).filter(Boolean);

  const events = [];
  const dateRe = /(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),\s+([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,\s+(\d{4})/i;
  for (const line of lines) {
    // Example: Friday, October 10th, 2025 Bouctouche  -- Miramichi  --  8:15 PM Civic
    if (!dateRe.test(line) || !line.includes('--')) continue;
    try {
      const [datePart, rest] = line.split(/(?<=\d{4})\s+/); // split after the year
      const m = datePart.match(dateRe);
      if (!m) continue;
      const [_, dow, mon, day, year] = m;
      const dateISO = `${mon} ${day}, ${year}`;

      const parts = rest.split('--').map(s => s.trim());
      if (parts.length < 3) continue;
      const awayN = parts[0].replace(/-\s*$/, '').trim();
      const homeN = parts[1].replace(/^\s*-/, '').trim();
      const timeArena = parts[2];
      const timeMatch = timeArena.match(/(\d{1,2}:\d{2}\s*[AP]M)/i);
      const timeStr = timeMatch ? timeMatch[1] : '7:00 PM';
      const venue = timeMatch ? timeArena.replace(timeMatch[1], '').trim() : timeArena.trim();

      const start = new Date(`${dateISO} ${timeStr}`);
      const homeSlug = nameToSlug.get(homeN.toLowerCase());
      const awaySlug = nameToSlug.get(awayN.toLowerCase());
      if (!homeSlug || !awaySlug) continue;

      events.push({
        league: 'BSHL',
        home_team: homeN, away_team: awayN,
        home_slug: homeSlug, away_slug: awaySlug,
        start: formatInTimeZone(start, 'America/Halifax', "yyyy-MM-dd'T'HH:mm:ssXXX"),
        location: venue
      });
    } catch {}
  }

  return events.sort((a,b)=> new Date(a.start) - new Date(b.start));
}
