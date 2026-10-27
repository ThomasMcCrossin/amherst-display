// Screenshot every design at 1920x1080 and write designs/manifest.json for the gallery.
// Usage: node bakeoff/shoot.mjs [slug...]   (expects serve.py on 127.0.0.1:8794)
import { chromium } from 'playwright';
import fs from 'node:fs';
import path from 'node:path';

const root = path.dirname(new URL(import.meta.url).pathname);
const base = 'http://127.0.0.1:8794';
const want = process.argv.slice(2);
const slugs = fs.readdirSync(path.join(root, 'designs'), { withFileTypes: true })
  .filter((d) => d.isDirectory() && fs.existsSync(path.join(root, 'designs', d.name, 'index.html')))
  .map((d) => d.name).sort();

const browser = await chromium.launch();
const manifest = [];
for (const slug of slugs) {
  const dir = path.join(root, 'designs', slug);
  const meta = fs.existsSync(path.join(dir, 'meta.json')) ? JSON.parse(fs.readFileSync(path.join(dir, 'meta.json'), 'utf8')) : {};
  const entry = { slug, name: meta.name || slug, concept: meta.concept || '', panels: meta.panels || [], frames: [] };
  if (want.length && !want.includes(slug)) {
    entry.frames = fs.readdirSync(path.join(root, 'shots')).filter((f) => f.startsWith(`${slug}-`)).sort();
    manifest.push(entry); continue;
  }
  const page = await browser.newPage({ viewport: { width: 1920, height: 1080 } });
  const errors = [];
  page.on('pageerror', (e) => errors.push(String(e)));
  page.on('console', (m) => { if (m.type() === 'error') errors.push(m.text()); });
  await page.goto(`${base}/designs/${slug}/index.html`, { waitUntil: 'networkidle' });
  const frames = Math.max(1, Math.min(6, meta.frames || 4));
  const gap = meta.frame_ms || 12000;
  for (let i = 0; i < frames; i++) {
    await page.waitForTimeout(i === 0 ? 2500 : gap);
    const f = `${slug}-${i + 1}.png`;
    await page.screenshot({ path: path.join(root, 'shots', f) });
    entry.frames.push(f);
  }
  entry.errors = errors.slice(0, 10);
  entry.heap_mb = await page.evaluate(() => (performance.memory ? Math.round(performance.memory.usedJSHeapSize / 1048576) : null));
  await page.close();
  manifest.push(entry);
  console.log(slug, entry.frames.length, 'frames', errors.length, 'errors');
}
await browser.close();
fs.writeFileSync(path.join(root, 'designs', 'manifest.json'), JSON.stringify({ generated_at: new Date().toISOString(), designs: manifest }, null, 2));
