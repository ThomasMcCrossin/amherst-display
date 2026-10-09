// community-rink: small-town junior hockey pride. Warm, friendly, big and readable
// from across the room, like a well-made community rink board. League-neutral:
// it only ever draws what the spec gives it.

export const meta = {
  name: "community-rink",
  author: "overlay designer",
  description: "Warm community-rink board: cream on charcoal, team-colour boards, big condensed type.",
};

const LOWER_KINDS = new Set(["score", "penalty", "fight", "save", "moment"]);
const CARD_KINDS = new Set(["break", "final", "intro"]);

const INK = "#f7f1e6";       // warm cream
const MUTED = "#bdb3a2";     // worn paint
const BRASS = "#e8b25e";     // warm board accent
const DARK = "#14161c";

// ---------- colour helpers ----------
function rgb(hex) {
  const h = String(hex || "").replace("#", "");
  const s = h.length === 3 ? h.split("").map((c) => c + c).join("") : h;
  const n = parseInt(s, 16);
  return Number.isFinite(n) ? { r: (n >> 16) & 255, g: (n >> 8) & 255, b: n & 255 } : { r: 20, g: 22, b: 28 };
}
function lum(hex) {
  const { r, g, b } = rgb(hex);
  return (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;
}
function mix(a, b, t) {
  const x = rgb(a), y = rgb(b);
  const c = (i) => Math.round(x[i] + (y[i] - x[i]) * t).toString(16).padStart(2, "0");
  return `#${c("r")}${c("g")}${c("b")}`;
}
// A team colour that still reads as text on the dark board.
function accent(hex) {
  return lum(hex) < 0.3 ? mix(hex, INK, 0.55) : hex;
}
function textOn(hex) {
  return lum(hex) > 0.55 ? DARK : INK;
}
// Shrink condensed type to fit an assumed box. Conservative factor keeps margins.
function fit(text, avail, base, min, factor = 0.48) {
  const len = Math.max(1, String(text).length);
  return Math.max(min, Math.min(base, Math.floor(avail / (len * factor))));
}

function teamOf(spec, side) {
  return side === "home" ? spec.home : side === "away" ? spec.away : null;
}
function neutral(spec) {
  return {
    primary: spec.league.primary,
    secondary: spec.league.secondary,
    text: textOn(spec.league.primary),
    logo: spec.league.logo,
    short: spec.league.short,
    name: spec.league.name,
  };
}

function plate(logo, cls = "") {
  return `<span class="plate ${cls}"><img src="${logo}" alt=""></span>`;
}

function tags(spec, e, team) {
  const fill = team.primary, ink = team.text;
  const list = (spec.tags || []).map((x) => `<i style="background:${fill};color:${ink}">${e(x)}</i>`);
  if (spec.badge) list.push(`<i class="badge">${e(spec.badge)}</i>`);
  return list.join("");
}

// ---------- lower third ----------
function lower(spec, e) {
  const kind = LOWER_KINDS.has(spec.kind) ? spec.kind : "moment";
  const primarySide = spec.side || (spec.subject && spec.subject.side) || null;
  const t = teamOf(spec, primarySide) || neutral(spec);
  const act = t.primary;
  const isFight = kind === "fight" && spec.subject;

  const heroes = [];
  if (spec.subject) heroes.push(spec.subject);
  for (const o of spec.others || []) heroes.push(o);

  const crestTeam = isFight ? neutral(spec) : t;
  const headColor = isFight ? BRASS : accent(act);

  let hero;
  if (isFight) {
    const fighters = heroes.map((f) => {
      const ft = teamOf(spec, f.side) || neutral(spec);
      const nm = String(f.name || "");
      const size = fit(nm, 470, 62, 34);
      return `<div class="fighter">
        ${f.number !== undefined && f.number !== null && f.number !== ""
          ? `<span class="tok" style="background:${ft.primary};color:${ft.text}">${e(f.number)}</span>` : ""}
        <span class="fname" style="font-size:${size}px">${e(nm)}</span></div>`;
    });
    hero = `<div class="hero fight">${fighters.join('<span class="vs">VS</span>')}</div>`;
  } else if (heroes.length) {
    const s = heroes[0];
    const nm = String(s.name || "");
    const size = fit(nm, 880, 96, 44);
    const st = teamOf(spec, s.side) || t;
    hero = `<div class="hero">
      ${s.number !== undefined && s.number !== null && s.number !== ""
        ? `<span class="tok" style="background:${st.primary};color:${st.text}">${e(s.number)}</span>` : ""}
      <span class="nm" style="font-size:${size}px">${e(nm)}</span></div>`;
  } else {
    hero = `<div class="hero"><span class="nm" style="font-size:64px">${e(spec.headline)}</span></div>`;
  }

  const lines = (spec.lines || []).filter(Boolean).map(e).join("  ·  ");
  const lineSize = fit(lines, 1020, 30, 21);

  let scores = "";
  if (spec.score) {
    scores = ["away", "home"].map((side) => {
      const st = spec[side];
      const active = spec.side === side ? " act" : "";
      return `<div class="srow${active}" style="background:${st.primary};color:${st.text}">
        <span class="sdisc"><img src="${st.logo}" alt=""></span>
        <span class="scode">${e(st.short)}</span>
        <span class="snum">${e(spec.score[side])}</span></div>`;
    }).join("");
  }

  return { css: LOWER_CSS, html: `
  <div class="lt">
    <span class="rail" style="background:${act}"></span>
    <div class="crest">
      ${plate(crestTeam.logo, "crestplate")}
      <div class="crestcode">${e(crestTeam.short)}</div>
    </div>
    <div class="main">
      <div class="head">
        <span class="kicker" style="color:${headColor}">${e(spec.headline)}</span>
        ${tags(spec, e, t)}
        ${spec.context ? `<span class="ctx">${e(spec.context)}</span>` : ""}
      </div>
      ${hero}
      ${lines ? `<div class="sub" style="font-size:${lineSize}px">${lines}</div>` : ""}
    </div>
    ${spec.clock || scores ? `<div class="side">
      ${spec.clock ? `<div class="clock">${e(spec.clock.text)}</div>` : ""}
      ${scores ? `<div class="scores">${scores}</div>` : ""}
    </div>` : ""}
  </div>` };
}

const LOWER_CSS = `
.lt{position:absolute;left:96px;right:96px;bottom:54px;height:240px;display:flex;align-items:stretch;
  border-radius:16px;overflow:hidden;color:${INK};font-family:"Barlow Semi Condensed",sans-serif;
  background:linear-gradient(180deg,#252932 0%,#181b22 100%);
  border:1px solid rgba(255,255,255,.10);
  box-shadow:0 18px 44px rgba(0,0,0,.55), inset 0 1px 0 rgba(255,255,255,.10)}
.lt::after{content:"";position:absolute;inset:0;pointer-events:none;opacity:.5;
  background:repeating-linear-gradient(115deg,rgba(255,255,255,.035) 0 2px,transparent 2px 34px)}
.lt .rail{width:14px;background:${BRASS};position:relative;z-index:1}
.crest{width:190px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:10px;
  background:linear-gradient(180deg,rgba(0,0,0,.10),rgba(0,0,0,.36));position:relative;z-index:1}
.plate{display:flex;align-items:center;justify-content:center;background:${INK};border-radius:50%;
  box-shadow:0 3px 10px rgba(0,0,0,.5), inset 0 0 0 3px rgba(20,22,28,.16)}
.crestplate{width:124px;height:124px}
.crestplate img{width:96px;height:96px;object-fit:contain}
.crestcode{font-family:"Bebas Neue",sans-serif;font-size:26px;letter-spacing:4px;color:${MUTED}}
.main{flex:1;min-width:0;display:flex;flex-direction:column;justify-content:center;gap:8px;
  padding:14px 26px;position:relative;z-index:1}
.head{display:flex;align-items:center;gap:12px;min-height:36px}
.kicker{font-family:"Bebas Neue",sans-serif;font-size:36px;letter-spacing:4px;line-height:1}
.head i{font-style:normal;font-family:"Bebas Neue",sans-serif;font-size:26px;letter-spacing:1px;
  padding:3px 12px;border-radius:6px;line-height:1}
.head i.badge{background:#c8102e;color:#fff}
.head .ctx{margin-left:auto;font-size:22px;letter-spacing:.5px;color:${MUTED};white-space:nowrap;
  text-transform:uppercase;overflow:visible}
.hero{display:flex;align-items:center;gap:20px;min-width:0}
.hero.fight{gap:16px}
.tok{flex:none;font-family:"Bebas Neue",sans-serif;font-size:58px;line-height:1;min-width:88px;height:82px;
  display:flex;align-items:center;justify-content:center;border-radius:12px;padding:0 8px;
  box-shadow:inset 0 0 0 3px rgba(0,0,0,.18)}
.fighter{display:flex;align-items:center;gap:12px;flex:1;min-width:0}
.fighter .tok{font-size:46px;min-width:68px;height:66px;border-radius:10px}
.nm,.fname{font-family:"Bebas Neue",sans-serif;color:${INK};line-height:1;letter-spacing:1px;white-space:nowrap;
  text-shadow:0 2px 0 rgba(0,0,0,.35)}
.fname{flex:1;min-width:0}
.vs{font-family:"Bebas Neue",sans-serif;font-size:38px;color:${BRASS};flex:none;padding:0 4px}
.sub{font-size:30px;color:${MUTED};white-space:nowrap;overflow:visible}
.side{width:406px;flex:none;display:flex;flex-direction:column;justify-content:center;gap:12px;
  padding:16px 24px;background:rgba(0,0,0,.30);position:relative;z-index:1}
.clock{font-family:"Bebas Neue",sans-serif;font-size:30px;letter-spacing:3px;color:${BRASS};text-align:center}
.scores{display:flex;flex-direction:column;gap:9px}
.srow{display:flex;align-items:center;gap:12px;height:56px;padding:0 14px 0 8px;border-radius:10px;
  box-shadow:inset 0 0 0 2px rgba(0,0,0,.20)}
.srow.act{box-shadow:inset 0 0 0 2px rgba(255,255,255,.75)}
.sdisc{width:42px;height:42px;flex:none;display:flex;align-items:center;justify-content:center;background:${INK};
  border-radius:50%;box-shadow:inset 0 0 0 2px rgba(0,0,0,.22)}
.sdisc img{width:31px;height:31px;object-fit:contain}
.scode{font-family:"Bebas Neue",sans-serif;font-size:30px;letter-spacing:2px;flex:1;min-width:0;
  white-space:nowrap;overflow:visible}
.snum{font-family:"Bebas Neue",sans-serif;font-size:44px;line-height:1}
`;

// ---------- full-screen card ----------
function card(spec, e) {
  const kind = CARD_KINDS.has(spec.kind) ? spec.kind : "break";
  const home = spec.home, away = spec.away;
  const hasScore = !!spec.score;
  const winner = hasScore
    ? spec.score.home > spec.score.away ? "home" : spec.score.away > spec.score.home ? "away" : null
    : null;

  const teamCol = (tm, side) => `
    <div class="team ${winner === side ? "win" : winner ? "dim" : ""}">
      <div class="tplate" style="--ring:${tm.primary}">
        <img src="${tm.logo}" alt=""></div>
      <div class="tname">${e(tm.name)}</div>
    </div>`;

  const mid = hasScore
    ? `<div class="scorebig">${e(spec.score.away)}<span class="dash">–</span>${e(spec.score.home)}</div>`
    : `<div class="scorebig vsbig">VS</div>`;

  const stats = (spec.stats || []).length
    ? `<table class="stats">
        <tr class="sh"><td>${e(away.short)}</td><td></td><td>${e(home.short)}</td></tr>
        ${spec.stats.map((s) => `<tr><td>${e(s.away)}</td><td class="lbl">${e(s.label)}</td><td>${e(s.home)}</td></tr>`).join("")}
      </table>` : "";

  const sub = (spec.lines || []).filter(Boolean).map(e).join("  ·  ");

  return { css: CARD_CSS, html: `
  <div class="card">
    <div class="cardtop">
      <span class="plate lplate"><img src="${spec.league.logo}" alt=""></span>
      <div class="lgl"><b>${e(spec.league.short)}</b><span>${e(spec.league.name)}</span></div>
      ${spec.context ? `<div class="cctx">${e(spec.context)}</div>` : ""}
    </div>
    <div class="cardbody">
      <div class="cardhead">${e(spec.headline)}${spec.clock && kind !== "final" ? `<span class="cdot">·</span>${e(spec.clock.text)}` : ""}</div>
      <div class="matchup">
        ${teamCol(away, "away")}
        ${mid}
        ${teamCol(home, "home")}
      </div>
      ${stats}
      ${sub ? `<div class="cardsub">${sub}</div>` : ""}
    </div>
    <div class="stripe">
      <i style="background:${away.primary}"></i><i style="background:${BRASS}"></i><i style="background:${home.primary}"></i></div>
  </div>` };
}

const CARD_CSS = `
.card{position:absolute;inset:0;color:${INK};font-family:"Barlow Semi Condensed",sans-serif;display:flex;
  flex-direction:column;overflow:hidden;
  background:radial-gradient(120% 100% at 50% 18%,#2b303c 0%,#171a21 52%,#0d0f14 100%)}
.card::before{content:"";position:absolute;inset:0;pointer-events:none;opacity:.6;
  background:repeating-linear-gradient(115deg,rgba(255,255,255,.04) 0 2px,transparent 2px 46px)}
.cardtop{display:flex;align-items:center;gap:20px;padding:54px 96px 0;position:relative;z-index:1}
.plate{display:flex;align-items:center;justify-content:center;background:${INK};border-radius:50%;
  box-shadow:0 4px 12px rgba(0,0,0,.45), inset 0 0 0 3px rgba(20,22,28,.14)}
.lplate{width:88px;height:88px;flex:none}
.lplate img{width:64px;height:64px;object-fit:contain}
.lgl{display:flex;flex-direction:column;line-height:1;min-width:0}
.lgl b{font-family:"Bebas Neue",sans-serif;font-size:38px;letter-spacing:5px;color:${INK}}
.lgl span{font-size:22px;letter-spacing:1.5px;color:${MUTED};text-transform:uppercase;margin-top:5px}
.cctx{margin-left:auto;text-align:right;font-size:24px;letter-spacing:1.5px;color:${BRASS};
  text-transform:uppercase;max-width:640px}
.cardbody{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:26px;
  padding:10px 96px 44px;position:relative;z-index:1;min-height:0}
.cardhead{font-family:"Bebas Neue",sans-serif;font-size:82px;letter-spacing:12px;line-height:1;color:${BRASS};
  text-align:center;text-shadow:0 3px 0 rgba(0,0,0,.4)}
.cardhead .cdot{margin:0 18px;color:${MUTED}}
.matchup{display:flex;align-items:center;justify-content:center;gap:70px;width:100%}
.team{width:420px;display:flex;flex-direction:column;align-items:center;text-align:center;gap:18px}
.tplate{width:196px;height:196px;border-radius:26px;background:${INK};display:flex;align-items:center;justify-content:center;
  box-shadow:0 0 0 4px var(--ring),0 10px 26px rgba(0,0,0,.45)}
.tplate img{width:150px;height:150px;object-fit:contain}
.team.dim .tplate{opacity:.86}
.team.win .tplate{background:#fff;box-shadow:0 0 0 5px ${BRASS}, 0 0 42px rgba(232,178,94,.55), 0 10px 26px rgba(0,0,0,.45)}
.tname{font-family:"Bebas Neue",sans-serif;font-size:44px;line-height:1.05;letter-spacing:2px;color:${INK}}
.team.win .tname{color:${BRASS}}
.scorebig{font-family:"Bebas Neue",sans-serif;font-size:230px;line-height:.9;color:${INK};flex:none;
  text-shadow:0 6px 0 rgba(0,0,0,.45)}
.scorebig .dash{margin:0 20px;color:${BRASS}}
.scorebig.vsbig{font-size:150px;color:${BRASS}}
.stats{border-collapse:collapse;font-size:30px;background:rgba(0,0,0,.28);border-radius:12px;overflow:hidden}
.stats td{padding:7px 34px;text-align:center;color:${INK}}
.stats tr.sh td{font-family:"Bebas Neue",sans-serif;font-size:28px;letter-spacing:3px;color:${BRASS};
  border-bottom:1px solid rgba(255,255,255,.14)}
.stats td.lbl{color:${MUTED};font-size:24px;letter-spacing:1px;text-transform:uppercase}
.cardsub{font-size:32px;color:${MUTED};text-align:center;max-width:1560px;line-height:1.25}
.stripe{position:absolute;left:0;right:0;bottom:0;height:16px;display:flex}
.stripe i{flex:1}
`;

export function render(spec, ctx) {
  const e = ctx.esc;
  if (LOWER_KINDS.has(spec.kind) || !CARD_KINDS.has(spec.kind)) return lower(spec, e);
  return card(spec, e);
}
