/**
 * Test script to fetch game summary page and find API endpoints
 */

import { chromium } from 'playwright';

const GAME_URL = 'https://www.themhl.ca/stats/game-summary/4718';

async function inspectGameSummary() {
  console.log('Launching browser...');
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  // Listen for API calls
  const apiCalls = [];
  page.on('request', request => {
    const url = request.url();
    if (url.includes('hockeytech.com') || url.includes('feed') || url.includes('api')) {
      apiCalls.push({
        url,
        method: request.method()
      });
    }
  });

  try {
    console.log(`Navigating to ${GAME_URL}...`);
    await page.goto(GAME_URL, { waitUntil: 'networkidle', timeout: 60000 });

    await page.waitForTimeout(3000);

    console.log('\n=== API Calls Detected ===');
    apiCalls.forEach((call, i) => {
      console.log(`${i + 1}. ${call.method} ${call.url}`);
    });

    console.log('\n=== Looking for game data in page ===');
    const gameData = await page.evaluate(() => {
      // Try to find any JSON data embedded in the page
      const scripts = Array.from(document.querySelectorAll('script'));
      for (const script of scripts) {
        const text = script.textContent;
        if (text && (text.includes('game_id') || text.includes('player_id') || text.includes('SiteKit'))) {
          return text.substring(0, 2000); // First 2000 chars
        }
      }
      return 'No game data found';
    });

    console.log(gameData);

  } catch (e) {
    console.error('Error:', e.message);
  } finally {
    await browser.close();
  }
}

inspectGameSummary().catch(console.error);
