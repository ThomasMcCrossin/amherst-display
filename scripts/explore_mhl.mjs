/**
 * Explore themhl.ca to discover available data sources
 * Uses Playwright to render JS and capture what's available
 */

import { chromium } from 'playwright';

const PAGES_TO_EXPLORE = [
  { url: 'https://www.themhl.ca/stats/player-stats', name: 'Player Stats' },
  { url: 'https://www.themhl.ca/stats/goalie-stats', name: 'Goalie Stats' },
  { url: 'https://www.themhl.ca/stats/league-leaders', name: 'League Leaders' },
  { url: 'https://www.themhl.ca/stats/streaks', name: 'Streaks' },
  { url: 'https://www.themhl.ca/stats/standings', name: 'Standings' },
  { url: 'https://www.themhl.ca/schedule', name: 'Schedule' },
  { url: 'https://www.themhl.ca/team/1/amherst-ramblers', name: 'Team Page (Ramblers)' },
];

async function explorePage(page, url, name) {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`EXPLORING: ${name}`);
  console.log(`URL: ${url}`);
  console.log('='.repeat(60));

  try {
    await page.goto(url, { waitUntil: 'networkidle', timeout: 30000 });
    await page.waitForTimeout(2000); // Wait for any dynamic content

    // Get page title
    const title = await page.title();
    console.log(`\nPage Title: ${title}`);

    // Find all tables and their headers
    const tables = await page.evaluate(() => {
      const results = [];
      const tables = document.querySelectorAll('table');

      tables.forEach((table, idx) => {
        const headers = [];
        const headerCells = table.querySelectorAll('thead th, thead td, tr:first-child th, tr:first-child td');
        headerCells.forEach(cell => {
          const text = cell.textContent.trim();
          if (text) headers.push(text);
        });

        // Get first few rows as sample data
        const rows = [];
        const bodyRows = table.querySelectorAll('tbody tr');
        for (let i = 0; i < Math.min(3, bodyRows.length); i++) {
          const cells = [];
          bodyRows[i].querySelectorAll('td').forEach(cell => {
            cells.push(cell.textContent.trim().substring(0, 30));
          });
          if (cells.length > 0) rows.push(cells);
        }

        if (headers.length > 0 || rows.length > 0) {
          results.push({ tableIndex: idx, headers, sampleRows: rows, rowCount: bodyRows.length });
        }
      });

      return results;
    });

    if (tables.length > 0) {
      console.log(`\nFound ${tables.length} table(s):`);
      tables.forEach(t => {
        console.log(`\n  Table #${t.tableIndex + 1} (${t.rowCount} rows):`);
        console.log(`    Headers: ${t.headers.join(' | ')}`);
        if (t.sampleRows.length > 0) {
          console.log(`    Sample row: ${t.sampleRows[0].join(' | ')}`);
        }
      });
    }

    // Find select/dropdown filters
    const filters = await page.evaluate(() => {
      const results = [];
      const selects = document.querySelectorAll('select');
      selects.forEach(select => {
        const options = [];
        select.querySelectorAll('option').forEach(opt => {
          options.push(opt.textContent.trim());
        });
        const label = select.getAttribute('aria-label') ||
                     select.getAttribute('name') ||
                     select.previousElementSibling?.textContent?.trim() ||
                     'Unknown';
        if (options.length > 0) {
          results.push({ label, options: options.slice(0, 10), total: options.length });
        }
      });
      return results;
    });

    if (filters.length > 0) {
      console.log(`\nFilters/Dropdowns:`);
      filters.forEach(f => {
        console.log(`  - ${f.label}: ${f.options.join(', ')}${f.total > 10 ? ` ... (${f.total} total)` : ''}`);
      });
    }

    // Look for any stats categories or tabs
    const tabs = await page.evaluate(() => {
      const results = [];
      // Look for tab-like elements
      const tabElements = document.querySelectorAll('[role="tab"], .tab, .nav-tab, .nav-link, .stat-tab');
      tabElements.forEach(tab => {
        const text = tab.textContent.trim();
        if (text && text.length < 50) results.push(text);
      });
      return [...new Set(results)];
    });

    if (tabs.length > 0) {
      console.log(`\nTabs/Categories: ${tabs.join(', ')}`);
    }

    // Look for specific stat sections
    const sections = await page.evaluate(() => {
      const results = [];
      const headings = document.querySelectorAll('h1, h2, h3, h4, .section-title, .stat-title');
      headings.forEach(h => {
        const text = h.textContent.trim();
        if (text && text.length < 100) results.push(text);
      });
      return results.slice(0, 15);
    });

    if (sections.length > 0) {
      console.log(`\nSection Headings: ${sections.join(' | ')}`);
    }

  } catch (e) {
    console.log(`ERROR: ${e.message}`);
  }
}

async function captureNetworkRequests(page) {
  const apiCalls = [];

  page.on('request', request => {
    const url = request.url();
    if (url.includes('hockeytech') || url.includes('api') || url.includes('feed')) {
      apiCalls.push({
        method: request.method(),
        url: url.substring(0, 200)
      });
    }
  });

  return apiCalls;
}

async function main() {
  console.log('Starting MHL Website Exploration with Playwright...\n');

  const browser = await chromium.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  const context = await browser.newContext({
    viewport: { width: 1920, height: 1080 }
  });

  const page = await context.newPage();

  // Capture network requests
  const apiCalls = [];
  page.on('request', request => {
    const url = request.url();
    if (url.includes('hockeytech') || url.includes('lscluster') ||
        (url.includes('api') && !url.includes('google'))) {
      apiCalls.push(url);
    }
  });

  // Explore each page
  for (const pageInfo of PAGES_TO_EXPLORE) {
    await explorePage(page, pageInfo.url, pageInfo.name);
  }

  // Print discovered API endpoints
  if (apiCalls.length > 0) {
    console.log(`\n${'='.repeat(60)}`);
    console.log('DISCOVERED API ENDPOINTS:');
    console.log('='.repeat(60));
    const uniqueUrls = [...new Set(apiCalls)];
    uniqueUrls.forEach(url => {
      // Parse and show key parameters
      try {
        const parsed = new URL(url);
        const view = parsed.searchParams.get('view');
        const feed = parsed.searchParams.get('feed');
        const type = parsed.searchParams.get('type');
        console.log(`\n  ${feed || 'unknown'}/${view || 'unknown'}${type ? '/' + type : ''}`);
        console.log(`    ${url.substring(0, 150)}...`);
      } catch {
        console.log(`  ${url.substring(0, 150)}`);
      }
    });
  }

  await browser.close();
  console.log('\n\nExploration complete!');
}

main().catch(console.error);
