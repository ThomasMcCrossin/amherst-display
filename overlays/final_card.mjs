#!/usr/bin/env node
// Full-screen final-score card for one MHL game: both logos, FINAL (OT/SO), the score and
// each team's goal scorers, no goal times (Tom, 2026-10-26: "like they used to do on TSN or
// Sportsnet"). The closing frame of a Ramblers post-game highlight reel (watch-rams#10, #31).
//
//   node overlays/final_card.mjs --game 5072 --out final.png [--assists] [--fixture f.json] [--html f.html]
//
// Writes an opaque 1920x1080 PNG. Exit 0: written. Exit 2: the game is not final yet.
// Any other exit: no card (the reel ships without it).
//
// Data: HockeyTech statviewfeed gameSummary for the game id, cached once final under
// $FINAL_CARD_CACHE (default ~/.local/state/amherst-display/final-card). The key comes from
// HOCKEYTECH_API_KEY, else the highlights env file the board refresh also reads.
// --fixture renders a saved gameSummary JSON instead (review edge cases); nothing is fetched.
// Logos: the MHL league pack's local files by HockeyTech team id, else the summary's logo
// URL, else a disc with the team code.

import fs from "fs";
import os from "os";
import path from "path";
import { chromium } from "playwright";

const HERE = path.dirname(new URL(import.meta.url).pathname);
const ROOT = path.resolve(HERE, "..");
const CACHE = process.env.FINAL_CARD_CACHE || path.join(os.homedir(), ".local/state/amherst-display/final-card");
const ENV_FILE = path.join(os.homedir(), ".local/state/watch-rams/amherst-highlights.env");
const PACK = JSON.parse(fs.readFileSync(path.join(HERE, "leagues/mhl/league.json"), "utf8"));
const TZ = "America/Halifax";

function args(argv) {
  const a = {};
  for (let i = 2; i < argv.length; i += 1) {
    if (argv[i].startsWith("--")) a[argv[i].slice(2)] = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
  }
  return a;
}

function apiKey() {
  if (process.env.HOCKEYTECH_API_KEY) return;
  try {
    const line = fs.readFileSync(ENV_FILE, "utf8").split("\n").find((l) => l.startsWith("HOCKEYTECH_API_KEY="));
    if (line) process.env.HOCKEYTECH_API_KEY = line.split("=", 2)[1].trim().replace(/^["']|["']$/g, "");
  } catch { /* no env file: request() reports the missing key */ }
}

async function summary(gameId) {
  const file = path.join(CACHE, `gameSummary_${gameId}.json`);
  if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file, "utf8"));
  apiKey();
  const { config, request } = await import("../scripts/hockeytech.mjs");
  const data = await request({ feed: "statviewfeed", view: "gameSummary", game_id: gameId, site_id: config.site_id });
  if (String(data?.details?.final) === "1") {  // only a final summary is immutable
    fs.mkdirSync(CACHE, { recursive: true });
    fs.writeFileSync(file, JSON.stringify(data));
  }
  return data;
}

const dataUri = (file) => {
  try { return `data:image/png;base64,${fs.readFileSync(path.join(ROOT, file)).toString("base64")}`; } catch { return ""; }
};
const fontUri = (file) => `data:font/ttf;base64,${fs.readFileSync(path.join(ROOT, file)).toString("base64")}`;
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);

/** gameSummary -> what the card shows. Null when the game is not final. */
export function cardData(box, { assists = false } = {}) {
  const d = box?.details;
  if (!d || String(d.final) !== "1") return null;
  const status = String(d.status || "");
  const ot_so = box.hasShootout || /SO/i.test(status) ? "SO" : /OT/i.test(status) ? "OT" : null;
  const side = (t) => {
    const info = t.info || {};
    const pack = PACK.teams.find((p) => String(p.provider_id) === String(info.id));
    return {
      id: info.id, code: info.abbreviation || pack?.short || "", city: pack?.city || info.city || "",
      nick: pack?.nickname || info.nickname || info.name || "",
      logo: (pack && dataUri(pack.logo)) || (info.logo ? (/^https?:/.test(info.logo) ? info.logo : `https://${info.logo}`) : ""),
      primary: pack?.primary || "#3a2a5e", goals: Number(t.stats?.goals ?? 0), scorers: [], shutout: null,
    };
  };
  const away = side(box.visitingTeam), home = side(box.homeTeam);
  const goals = (box.periods || []).flatMap((p) => (p.goals || []).map((g) => ({ ...g, ot: Number(p.info?.id) >= 4 })));
  for (const g of goals) {
    const t = String(g.team?.id) === String(home.id) ? home : away;
    const name = `${g.scoredBy.firstName} ${g.scoredBy.lastName}`.trim();
    if (assists) {
      t.scorers.push({ name, n: 1, ot: g.ot, assists: (g.assists || []).map((a) => a.lastName).filter(Boolean) });
    } else {
      const row = t.scorers.find((s) => s.name === name);
      if (row) { row.n += 1; row.ot ||= g.ot; } else t.scorers.push({ name, n: 1, ot: g.ot });
    }
  }
  // Shootout: the goal list has no shootout goals (the team total counts the decider);
  // name the shooter whose attempt is flagged as the winner.
  if (ot_so === "SO") {
    const sd = box.shootoutDetails || {};
    const w = [...(sd.homeTeamShots || []), ...(sd.visitingTeamShots || [])].find((x) => x.isGoal && x.isGameWinningGoal);
    if (w?.shooter) (String(w.shooterTeam?.id) === String(home.id) ? home : away).so = `${w.shooter.firstName} ${w.shooter.lastName}`.trim();
  }
  // Shutout: credit the goalie who played the whole game for the team that allowed nothing.
  for (const [t, other, raw] of [[home, away, box.homeTeam], [away, home, box.visitingTeam]]) {
    if (other.goals !== 0 || t.goals === 0) continue;
    const played = (raw.goalies || []).filter((g) => Number(g.stats?.shotsAgainst) > 0 || g.stats?.timeOnIce && g.stats.timeOnIce !== "0:00");
    if (played.length === 1) {
      const g = played[0];
      t.shutout = { name: `${g.info.firstName} ${g.info.lastName}`.trim(), saves: g.stats?.saves ?? null };
    }
  }
  const when = Date.parse(d.GameDateISO8601);
  const date = isNaN(when) ? "" : new Intl.DateTimeFormat("en-US", { timeZone: TZ, weekday: "short", month: "short", day: "numeric" }).format(new Date(when));
  return { id: d.id, ot_so, away, home, date: date.replace(",", ""), venue: d.venue || "", assists };
}

// "Jean-Christophe Archambault-Thibodeau" -> "J. Archambault-Thibodeau", used only when the full name won't fit.
const short = (name) => { const p = name.split(" "); return p.length > 1 ? `${p[0][0]}. ${p.slice(1).join(" ")}` : name; };

// Name lines shrink in steps so the longest list still fits its column (checked after render).
function scorerHtml(t, assists, right) {
  if (!t.scorers.length) return `<div class="none">${t.shutout ? "" : "No goals"}</div>`;
  const as = (s) => (assists && s.assists ? `<span class="as">${s.assists.length ? esc(s.assists.join(", ")) : "unassisted"}</span>` : "");
  return t.scorers.map((s) => `<div class="sc">${right ? as(s) : ""}
      <span class="nm" data-short="${esc(short(s.name))}">${esc(s.name)}</span>${s.n > 1 ? `<span class="ct">${s.n}</span>` : ""}${s.ot ? `<span class="tag">OT</span>` : ""}${right ? "" : as(s)}
    </div>`).join("");
}

function teamHtml(t, won) {
  const disc = `<div class="disc" style="--ring:${t.primary}">${t.logo ? `<img src="${esc(t.logo)}" alt="" onerror="this.parentNode.classList.add('nologo');this.remove()">` : ""}<b>${esc(t.code)}</b></div>`;
  return `<div class="team ${won ? "won" : "lost"}${t.logo ? "" : " nologo-t"}">${disc}
    <div class="city">${esc(t.city.toUpperCase())}</div><div class="nick">${esc(t.nick.toUpperCase())}</div></div>`;
}

export function cardHtml(c) {
  const hw = c.home.goals > c.away.goals, aw = c.away.goals > c.home.goals;
  const shut = c.home.shutout || c.away.shutout;
  const so = c.home.so || c.away.so;
  const n = Math.max(...[c.home, c.away].map((t) => t.scorers.length + (t.shutout ? 1 : 0) + (t.so ? 1 : 0)));
  const size = n <= 4 ? "s4" : n <= 6 ? "s6" : n <= 8 ? "s8" : "s10";
  const fonts = `
    @font-face{font-family:"Bebas Neue";src:url("${fontUri("assets/fonts/BebasNeue-Regular.ttf")}")}
    @font-face{font-family:"Barlow SC";src:url("${fontUri("assets/fonts/BarlowSemiCondensed-SemiBold.ttf")}");font-weight:600}
    @font-face{font-family:"Barlow SC";src:url("${fontUri("assets/fonts/BarlowSemiCondensed-Regular.ttf")}");font-weight:400}`;
  return `<!doctype html><html><head><meta charset="utf-8"><style>${fonts}
:root{--bg:#0b0613;--gold:#f4b932;--ink:#fff;--dim:#c4b8dc;--pur2:#9a6fdc}
*{box-sizing:border-box;margin:0;padding:0}
html,body{width:1920px;height:1080px;overflow:hidden;background:var(--bg)}
#card{position:relative;width:1920px;height:1080px;overflow:hidden;color:var(--ink);font-family:"Barlow SC",sans-serif;
  background:radial-gradient(1500px 900px at 50% 30%,#2a1648 0,var(--bg) 72%)}
.wash{position:absolute;top:0;bottom:0;width:62%;opacity:.28}
.wash.l{left:0;background:linear-gradient(100deg,${c.away.primary} 0,transparent 78%);clip-path:polygon(0 0,100% 0,72% 100%,0 100%)}
.wash.r{right:0;background:linear-gradient(260deg,${c.home.primary} 0,transparent 78%);clip-path:polygon(28% 0,100% 0,100% 100%,0 100%)}
.top{position:absolute;top:0;left:0;right:0;height:14px;background:linear-gradient(90deg,${c.away.primary},var(--gold) 50%,${c.home.primary})}
.head{position:absolute;top:48px;left:0;right:0;text-align:center}
.fin{display:inline-block;font:400 118px/1 "Bebas Neue";letter-spacing:.14em;color:#1a0d2e;background:var(--gold);
  padding:14px 34px 4px 52px;transform:skewX(-8deg);box-shadow:0 10px 30px rgba(0,0,0,.45)}
.fin span{display:inline-block;transform:skewX(8deg)}
.fin i{font-style:normal;color:#5b2f9a;margin-left:.18em}
.meta{margin-top:20px;font:600 34px/1 "Barlow SC";letter-spacing:.16em;color:var(--dim)}
.row{position:absolute;top:262px;left:0;right:0;display:grid;grid-template-columns:1fr 250px 80px 250px 1fr;align-items:center}
.team{display:flex;flex-direction:column;align-items:center;text-align:center}
.disc{position:relative;width:224px;height:224px;border-radius:50%;background:#fff;display:flex;align-items:center;justify-content:center;
  box-shadow:0 0 0 8px var(--ring),0 16px 40px rgba(0,0,0,.55)}
.disc img{width:84%;height:84%;object-fit:contain}
.disc b{display:none;font:400 104px/1 "Bebas Neue";letter-spacing:.04em;color:var(--ring)}
.disc.nologo b,.nologo-t .disc b{display:block}
.city{margin-top:24px;font:600 32px/1 "Barlow SC";letter-spacing:.14em;color:var(--dim)}
.nick{margin-top:6px;font:400 72px/1 "Bebas Neue";letter-spacing:.05em}
.lost .disc{filter:saturate(.75) brightness(.92)}
.lost .nick{color:#e6def5}
.score{font:400 280px/1 "Bebas Neue";text-align:center;font-variant-numeric:tabular-nums;text-shadow:0 10px 0 rgba(0,0,0,.4)}
.score.l{color:#9f8fc0}.score.w{color:#fff}
.dash{font:400 150px/1 "Bebas Neue";color:var(--gold);text-align:center}
.cols{position:absolute;top:668px;bottom:56px;left:110px;right:110px;display:grid;grid-template-columns:1fr 1fr;gap:120px}
.col{border-top:4px solid var(--pur2);padding-top:20px;overflow:hidden}
.col.away{text-align:left}.col.home{text-align:right}
.ch{font:600 28px/1 "Barlow SC";letter-spacing:.2em;color:var(--gold);margin-bottom:14px}
.sc{display:block;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.nm{font-weight:600}
.ct{display:inline-block;margin-left:16px;min-width:1.4em;padding:0 .25em;border-radius:6px;background:var(--gold);color:#1a0d2e;
  font:400 1em/1.15 "Bebas Neue";text-align:center;vertical-align:.06em}
.tag{display:inline-block;margin-left:14px;padding:0 .3em;border:3px solid var(--gold);border-radius:6px;color:var(--gold);
  font:400 .85em/1.15 "Bebas Neue";letter-spacing:.06em;vertical-align:.1em}
.as{font-weight:400;color:var(--dim);font-size:.7em;margin-left:18px}
.home .as{margin:0 18px 0 0}
.s4 .sc{font-size:46px;line-height:1.36}
.s6 .sc{font-size:38px;line-height:1.26}
.s8 .sc{font-size:31px;line-height:1.2}
.s10 .sc{font-size:27px;line-height:1.16}
.none{font:600 36px/1.3 "Barlow SC";color:#8f82ad}
.so{margin-top:8px;font:600 34px/1.3 "Barlow SC";color:var(--dim)}
.so b{color:var(--gold);font-weight:600;letter-spacing:.1em;margin-right:12px}
.foot{position:absolute;left:0;right:0;bottom:0;height:14px;background:linear-gradient(90deg,${c.away.primary},var(--gold) 50%,${c.home.primary})}
</style></head><body><div id="card">
  <div class="wash l"></div><div class="wash r"></div><div class="top"></div>
  <div class="head"><div class="fin"><span>FINAL${c.ot_so ? `<i>/ ${c.ot_so}</i>` : ""}</span></div>
    <div class="meta">${esc([c.date.toUpperCase(), c.venue.toUpperCase()].filter(Boolean).join("  ·  "))}</div></div>
  <div class="row">
    ${teamHtml(c.away, aw)}
    <div class="score ${aw ? "w" : "l"}">${c.away.goals}</div><div class="dash">–</div><div class="score ${hw ? "w" : "l"}">${c.home.goals}</div>
    ${teamHtml(c.home, hw)}
  </div>
  <div class="cols ${size}${c.assists ? " assists" : ""}">
    ${[["away", c.away], ["home", c.home]].map(([k, t]) => `<div class="col ${k}"><div class="ch">${esc(t.code)} GOALS</div>${scorerHtml(t, c.assists, k === "home")}${
      t.shutout ? `<div class="so"><b>SHUTOUT</b>${esc(t.shutout.name)}${t.shutout.saves != null ? ` · ${esc(t.shutout.saves)} saves` : ""}</div>` : ""}${
      t.so ? `<div class="so"><b>SHOOTOUT WINNER</b>${esc(t.so)}</div>` : ""}</div>`).join("")}
  </div>
  <div class="foot"></div>
</div></body></html>`;
}

/** Render to PNG and return layout problems (text clipped or cut off). */
export async function renderPng(html, out, browser) {
  const page = await browser.newPage({ viewport: { width: 1920, height: 1080 } });
  try {
    await page.setContent(html, { waitUntil: "load", timeout: 20000 });
    await page.evaluate(() => document.fonts.ready);
    await page.evaluate(() => {  // a scorer line too wide for its column drops to initial + last name
      for (const el of document.querySelectorAll(".sc")) {
        const nm = el.querySelector(".nm");
        if (nm && el.scrollWidth > el.clientWidth + 1) nm.textContent = nm.dataset.short;
      }
    });
    const problems = await page.evaluate(() => {
      const bad = [];
      const W = document.body.clientWidth;
      for (const el of document.querySelectorAll(".sc,.nick,.city,.meta,.so")) {
        const r = el.getBoundingClientRect();
        if (el.scrollWidth > el.clientWidth + 1 || r.left < 0 || r.right > W) bad.push(`${el.className}: ${el.textContent.trim().slice(0, 60)}`);
      }
      for (const el of document.querySelectorAll(".col")) {  // every scorer line must sit inside its column
        if (el.scrollHeight > el.clientHeight + 1) bad.push(`${el.className}: too many lines`);
      }
      return bad;
    });
    await page.screenshot({ path: out, type: "png", omitBackground: false });
    return problems;
  } finally {
    await page.close();
  }
}

export async function launch() {
  // Keep headless Chromium off any desktop session (the staff display, 2026-10-15).
  const env = { ...process.env };
  for (const key of ["WAYLAND_DISPLAY", "WAYLAND_SOCKET", "XAUTHORITY", "DISPLAY"]) delete env[key];
  return chromium.launch({ headless: true, env, args: ["--ozone-platform=headless"] });
}

async function main() {
  const a = args(process.argv);
  if ((!a.game && !a.fixture) || !a.out) {
    console.error("usage: node overlays/final_card.mjs --game <id> --out card.png [--assists] [--fixture summary.json] [--html card.html]");
    process.exit(1);
  }
  const box = a.fixture ? JSON.parse(fs.readFileSync(a.fixture, "utf8")) : await summary(a.game);
  const c = cardData(box, { assists: Boolean(a.assists) });
  if (!c) {
    console.error(`game ${a.game || a.fixture}: not final`);
    process.exit(2);
  }
  const html = cardHtml(c);
  if (a.html) fs.writeFileSync(a.html, html);
  const browser = await launch();
  try {
    const problems = await renderPng(html, a.out, browser);
    if (problems.length) {
      console.error(`layout: ${problems.join(" | ")}`);
      process.exit(3);
    }
  } finally {
    await browser.close();
  }
  console.log(a.out);
}

if (import.meta.url === `file://${process.argv[1]}`) main().catch((e) => { console.error(e.message || e); process.exit(1); });
