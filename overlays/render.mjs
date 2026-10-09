#!/usr/bin/env node
// Render overlay specs through a theme into transparent 1920x1080 PNGs, plus layout checks.
//
//   node overlays/render.mjs --theme baseline [--samples overlays/samples] [--out overlays/.bakeoff/out]
//   node overlays/render.mjs --theme baseline --spec one.json --output one.png   (production: one spec)
//
// A theme is overlays/themes/<name>/theme.mjs exporting `meta` and `render(spec, ctx)`
// that returns { css, html } (see overlays/README.md). Writes <out>/<theme>/<sample>.png
// and <out>/<theme>/checks.json (text clipped, off-frame, or over the broadcast scorebug).

import fs from "fs/promises";
import path from "path";
import { pathToFileURL } from "url";
import { chromium } from "playwright";

const HERE = path.dirname(new URL(import.meta.url).pathname);
const ROOT = path.resolve(HERE, "..");
const MIME = { ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp",
  ".svg": "image/svg+xml", ".ttf": "font/ttf", ".otf": "font/otf", ".woff": "font/woff", ".woff2": "font/woff2" };
// Lower-third kinds must keep clear of the broadcast's own scorebug (Flo: top-left).
const LOWER_KINDS = new Set(["score", "penalty", "fight", "save", "moment"]);
const SCOREBUG = { x: 0, y: 0, w: 720, h: 190 };

function args(argv) {
  const a = {};
  for (let i = 2; i < argv.length; i += 1) {
    if (argv[i].startsWith("--")) a[argv[i].slice(2)] = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
  }
  return a;
}

async function dataUri(file) {
  try {
    const raw = await fs.readFile(file);
    return `data:${MIME[path.extname(file).toLowerCase()] || "application/octet-stream"};base64,${raw.toString("base64")}`;
  } catch {
    return "";
  }
}

const esc = (v) => String(v ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;").replaceAll("'", "&#39;");

async function hydrate(spec) {
  const s = structuredClone(spec);
  const fallback = await dataUri(path.join(ROOT, "assets/logos/fallback.png"));
  for (const key of ["league", "home", "away"]) {
    if (s[key]) s[key].logo = (s[key].logo && (await dataUri(path.join(ROOT, s[key].logo)))) || fallback;
  }
  return s;
}

const BASE_FONTS = [
  ["Bebas Neue", "assets/fonts/BebasNeue-Regular.ttf", 400],
  ["Barlow Semi Condensed", "assets/fonts/BarlowSemiCondensed-Regular.ttf", 400],
  ["Barlow Semi Condensed", "assets/fonts/BarlowSemiCondensed-SemiBold.ttf", 600],
];

async function main() {
  const a = args(process.argv);
  if (!a.theme) {
    console.error("usage: node overlays/render.mjs --theme <name> [--samples dir] [--out dir] [--only sample]");
    process.exit(2);
  }
  const themeDir = path.join(HERE, "themes", a.theme);
  const theme = await import(pathToFileURL(path.join(themeDir, "theme.mjs")).href + `?t=${Date.now()}`);
  const single = Boolean(a.spec);
  const samplesDir = single ? path.dirname(path.resolve(a.spec)) : path.resolve(a.samples || path.join(HERE, "samples"));
  const outDir = single ? path.dirname(path.resolve(a.output)) : path.resolve(a.out || path.join(HERE, ".bakeoff/out"), a.theme);
  await fs.mkdir(outDir, { recursive: true });

  let fontCss = "";
  for (const [family, file, weight] of BASE_FONTS) {
    fontCss += `@font-face{font-family:"${family}";src:url("${await dataUri(path.join(ROOT, file))}");font-weight:${weight};}\n`;
  }
  const ctx = {
    esc,
    // data: URI for a file inside the theme directory (fonts, textures, icons)
    asset: (rel) => dataUri(path.join(themeDir, rel)),
    fontFace: async (family, rel, weight = 400, style = "normal") =>
      `@font-face{font-family:"${family}";src:url("${await dataUri(path.join(themeDir, rel))}");font-weight:${weight};font-style:${style};}`,
  };

  const names = single ? [path.basename(a.spec)] : (await fs.readdir(samplesDir)).filter((f) => f.endsWith(".json")).sort()
    .filter((f) => !a.only || f === `${a.only}.json`);
  const browser = await chromium.launch({ headless: true });
  const checks = {};
  try {
    for (const file of names) {
      const name = file.replace(/\.json$/, "");
      const spec = await hydrate(JSON.parse(await fs.readFile(path.join(samplesDir, file), "utf-8")));
      const problems = [];
      let out;
      try {
        out = await theme.render(spec, ctx);
      } catch (err) {
        checks[name] = [`render() threw: ${err.message}`];
        continue;
      }
      const html = `<!doctype html><html><head><meta charset="utf-8"><style>${fontCss}
        *{box-sizing:border-box}html,body{margin:0;width:${spec.width}px;height:${spec.height}px;overflow:hidden;background:transparent}
        ${out.css || ""}</style></head><body>${out.html || ""}</body></html>`;
      const page = await browser.newPage({ viewport: { width: spec.width, height: spec.height }, deviceScaleFactor: 1 });
      page.on("pageerror", (e) => problems.push(`page error: ${e.message}`));
      await page.setContent(html, { waitUntil: "load" });
      await page.evaluate(async () => { await document.fonts.ready; if (window.__overlayReady) await window.__overlayReady; });
      const found = await page.evaluate(({ lower, bug }) => {
        const issues = [];
        const W = window.innerWidth, H = window.innerHeight;
        const visible = (el) => {
          const cs = getComputedStyle(el);
          if (cs.visibility === "hidden" || cs.display === "none" || Number(cs.opacity) === 0) return false;
          const bg = cs.backgroundColor !== "rgba(0, 0, 0, 0)" || cs.backgroundImage !== "none";
          const text = [...el.childNodes].some((n) => n.nodeType === 3 && n.textContent.trim());
          return bg || text || el.tagName === "IMG" || el.tagName === "svg";
        };
        for (const el of document.body.querySelectorAll("*")) {
          if (!visible(el)) continue;
          const r = el.getBoundingClientRect();
          if (r.width === 0 || r.height === 0) continue;
          const label = `${el.tagName.toLowerCase()}${el.className && typeof el.className === "string" ? "." + el.className.trim().split(/\s+/).join(".") : ""}`;
          const text = (el.textContent || "").trim().slice(0, 40);
          if (r.left < -1 || r.top < -1 || r.right > W + 1 || r.bottom > H + 1) issues.push(`off-frame: ${label} "${text}"`);
          const ownText = [...el.childNodes].some((n) => n.nodeType === 3 && n.textContent.trim());
          if (ownText && (el.scrollWidth > el.clientWidth + 2 || el.scrollHeight > el.clientHeight * 1.5)) {
            const cs = getComputedStyle(el);
            if (cs.overflow !== "visible" || cs.textOverflow === "ellipsis") issues.push(`text clipped: ${label} "${text}"`);
          }
          if (lower && r.left < bug.x + bug.w && r.top < bug.y + bug.h && r.right > bug.x && r.bottom > bug.y)
            issues.push(`covers broadcast scorebug zone (0,0)-(720,190): ${label} "${text}"`);
        }
        return [...new Set(issues)];
      }, { lower: LOWER_KINDS.has(spec.kind), bug: SCOREBUG });
      problems.push(...found);
      await page.screenshot({ path: single ? path.resolve(a.output) : path.join(outDir, `${name}.png`), omitBackground: true });
      await page.close();
      checks[name] = problems;
    }
  } finally {
    await browser.close();
  }
  if (single) {
    const problems = Object.values(checks)[0] || [];
    for (const p of problems) console.error(`overlay check: ${p}`);
    process.exit(problems.some((p) => p.startsWith("render() threw")) ? 1 : 0);
  }
  await fs.writeFile(path.join(outDir, "checks.json"), JSON.stringify({ theme: a.theme, meta: theme.meta || {}, checks }, null, 2));
  const bad = Object.entries(checks).filter(([, v]) => v.length);
  console.log(`${a.theme}: ${names.length} rendered -> ${outDir}; ${bad.length} with problems`);
  for (const [k, v] of bad) console.log(`  ${k}: ${v.slice(0, 4).join(" | ")}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
