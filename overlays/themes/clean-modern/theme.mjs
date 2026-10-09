// clean-modern: minimal broadcast overlays. Flat solid panels, generous spacing,
// refined type, team colour used as an accent, logos on a consistent slate plate.
export const meta = {
  name: "clean-modern",
  author: "overlay-bakeoff",
  description: "Minimal modern streaming-app feel: flat panels, refined type, slate logo plates.",
};

const LOWER = new Set(["score", "penalty", "fight", "save", "moment"]);
const KNOWN_CARD = new Set(["break", "final", "intro"]);

/* ---------- tiny colour helpers (all inputs are spec hex colours) ---------- */
function rgb(hex) {
  let h = String(hex || "#000").replace("#", "").trim();
  if (h.length === 3) h = h.split("").map((c) => c + c).join("");
  if (h.length !== 6 || /[^0-9a-f]/i.test(h)) h = "000000";
  return [parseInt(h.slice(0, 2), 16), parseInt(h.slice(2, 4), 16), parseInt(h.slice(4, 6), 16)];
}
function rgba(hex, a) { const [r, g, b] = rgb(hex); return `rgba(${r},${g},${b},${a})`; }
function lum(hex) {
  const [r, g, b] = rgb(hex).map((v) => { v /= 255; return v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4; });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}
function toHex(r, g, b) { return "#" + [r, g, b].map((v) => Math.max(0, Math.min(255, Math.round(v))).toString(16).padStart(2, "0")).join(""); }
function brighten(hex, amt) { const [r, g, b] = rgb(hex); const m = (v) => Math.round(v + (255 - v) * amt); return toHex(m(r), m(g), m(b)); }
const PANEL = "#0C1018";
function contrast(a, b) { const l1 = lum(a), l2 = lum(b); return (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05); }
// A team/league hue kept as readable text: brightened until it clears 4.5:1 on the panel,
// then a neutral slate as the last resort. The rail/chip keep the vivid colour regardless.
function accent(hex) {
  let c = lum(hex) < 0.45 ? brighten(hex, 0.55) : hex;
  for (let amt = 0.65; contrast(c, PANEL) < 4.5 && amt <= 1.01; amt += 0.1) c = brighten(hex, amt);
  return contrast(c, PANEL) >= 4.5 ? c : "#C3CBD8";
}

function teamOf(spec, side) { return side === "home" ? spec.home : side === "away" ? spec.away : null; }
function nameSize(name, base) {
  const n = String(name || "").length;
  if (n > 30) return Math.round(base * 0.66);
  if (n > 24) return Math.round(base * 0.78);
  if (n > 18) return Math.round(base * 0.9);
  return base;
}
function lineSize(lines) {
  const n = (lines || []).join(" ").length;
  if (n > 96) return 19;
  if (n > 72) return 22;
  if (n > 54) return 24;
  return 25;
}
function plateStyle() { return "background:linear-gradient(180deg,rgba(255,255,255,.07),rgba(0,0,0,.16)),#333B4A;border:1px solid rgba(255,255,255,.11);"; }

/* ------------------------------- markup ---------------------------------- */
function tagsHtml(spec, e) {
  const tags = (spec.tags || []).map((x) => `<i class="pill">${e(x)}</i>`).join("");
  const badge = spec.badge ? `<i class="pill warn">${e(spec.badge)}</i>` : "";
  return tags + badge;
}

function scoreColumn(spec, e, { clock: showClock = true } = {}) {
  if (!spec.score) return "";
  const rows = [["away", spec.away, spec.score.away], ["home", spec.home, spec.score.home]].map(([side, tm, val]) => {
    const lead = spec.side === side ? " lead" : "";
    return `<div class="srow${lead}"><span class="dot" style="background:${e(tm.primary)}"></span>` +
      `<span class="ts">${e(tm.short)}</span><b>${e(val)}</b></div>`;
  }).join("");
  const context = spec.context ? `<div class="scontext">${e(spec.context)}</div>` : "";
  const clock = showClock && spec.clock ? `<div class="clock">${e(spec.clock.text)}</div>` : "";
  return `<div class="scorecol">${context}${clock}${rows}</div>`;
}

function lowerThird(spec, e) {
  const t = teamOf(spec, spec.side) || { primary: spec.league.primary, text: "#fff", logo: spec.league.logo };
  const subj = spec.subject || {};
  const pri = t.primary && t.primary !== "transparent" ? t.primary : spec.league.primary;
  const txt = t.text || "#fff";
  const num = subj.number ? `<span class="jersey" style="background:${e(pri)};color:${e(txt)}">${e(subj.number)}</span>` : "";
  const name = subj.name ? `<span class="name" style="font-size:${nameSize(subj.name, 74)}px">${e(subj.name)}</span>` : "";
  const lines = (spec.lines || []).length
    ? `<div class="lines" style="font-size:${lineSize(spec.lines)}px">${spec.lines.map(e).join(" <span class='sep'>·</span> ")}</div>` : "";
  const tint = rgba(pri, 0.30);
  return `<div class="lt" style="--pri:${e(pri)};--acc:${accent(pri)};--tint:${tint}">
    <div class="rail"></div>
    <div class="plate lowerplate"><img src="${t.logo}"></div>
    <div class="body">
      <div class="kicker">${e(spec.headline)}${tagsHtml(spec, e)}</div>
      <div class="namerow">${num}${name}</div>
      ${lines}
    </div>
    ${scoreColumn(spec, e)}
  </div>`;
}

function fightPanel(spec, e) {
  const players = [];
  if (spec.subject) players.push(spec.subject);
  for (const o of spec.others || []) players.push(o);
  const cells = players.slice(0, 2).map((p) => {
    const tm = teamOf(spec, p.side) || spec.home;
    const pri = tm.primary || spec.league.primary;
    const num = p.number ? `<span class="jersey" style="background:${e(pri)};color:${e(tm.text || "#fff")}">${e(p.number)}</span>` : "";
    return `<div class="fp">
      <div class="plate fplate"><img src="${tm.logo}"></div>
      <div class="fp-text">
        <div class="fp-name">${num}<span style="font-size:${nameSize(p.name, 46)}px">${e(p.name)}</span></div>
        <div class="fp-team"><span class="dot" style="background:${e(pri)}"></span>${e(tm.short)}</div>
      </div>
    </div>`;
  });
  const vs = cells.length === 2 ? `<div class="fvs">VS</div>` : "";
  const clock = spec.clock ? ` <span class="fdim">· ${e(spec.clock.text)}</span>` : "";
  const line = (spec.lines || []).length ? `<div class="fight-line">${spec.lines.map(e).join(" · ")}</div>` : "";
  const ftint = rgba(spec.league.primary, 0.30);
  return `<div class="lt fight" style="--pri:${e(spec.league.primary)};--acc:${accent(spec.league.primary)};--tint:${ftint}">
    <div class="rail"></div>
    <div class="fight-main">
      <div class="fight-head">${e(spec.headline)}${tagsHtml(spec, e)}${clock}</div>
      <div class="fight-row">${cells[0] || ""}${vs}${cells[1] || ""}</div>
      ${line}
    </div>
    ${scoreColumn(spec, e, { clock: false })}
  </div>`;
}

function cardStats(spec, e) {
  const stats = spec.stats || [];
  if (!stats.length) return "";
  const items = stats.map((s) => `<div class="stat">
    <span class="sv">${e(s.away)}</span><span class="sl">${e(s.label)}</span><span class="sv">${e(s.home)}</span>
  </div>`).join("");
  return `<div class="statrow">${items}</div>`;
}

function card(spec, e) {
  const away = spec.away, home = spec.home;
  const score = spec.score;
  const headline = spec.headline || "";
  const winner = score && (Number(score.home) > Number(score.away)) ? "home" : score && (Number(score.away) > Number(score.home)) ? "away" : null;
  const teamBlock = (side) => {
    const tm = side === "home" ? home : away;
    const cls = spec.kind === "final" && winner && winner !== side ? " lose" : "";
    return `<div class="cteam${cls}">
      <div class="plate cplate"><img src="${tm.logo}"></div>
      <div class="cshort"><span class="dot" style="background:${e(tm.primary)}"></span>${e(tm.short)}</div>
      <div class="cname" style="font-size:${nameSize(tm.name, 46)}px">${e(tm.name)}</div>
      <div class="ccity">${e(tm.city)}</div>
    </div>`;
  };
  const center = score
    ? `<div class="cscores"><span>${e(score.away)}</span><span class="dash">–</span><span>${e(score.home)}</span></div>`
    : `<div class="cscores vsScore">VS</div>`;
  const norm = (s) => String(s || "").trim().toLowerCase();
  const sub = spec.clock && spec.kind !== "intro" && norm(spec.clock.text) !== norm(headline)
    ? `<div class="cclock">${e(spec.clock.text)}</div>` : "";
  const context = spec.context ? `<div class="ccontext">${e(spec.context)}</div>` : "";
  const foot = (spec.lines || []).length ? `<div class="cfoot">${spec.lines.map(e).join(" <span class='sep'>·</span> ")}</div>` : "";
  const bg = `background:` +
    `radial-gradient(1000px 680px at 10% 14%,${rgba(away.primary, 0.22)},rgba(0,0,0,0) 62%),` +
    `radial-gradient(1000px 680px at 90% 86%,${rgba(home.primary, 0.22)},rgba(0,0,0,0) 62%),#090C12`;
  return `<div class="card" style="${bg};--acc:${accent(spec.league.primary)}">
    <div class="split"><i style="background:${e(away.primary)}"></i><i style="background:${e(home.primary)}"></i></div>
    <div class="cwrap">
      <div class="ctop">
        <div class="cleague"><div class="plate lplate"><img src="${spec.league.logo}"></div>
          <div class="cltext"><b>${e(spec.league.short)}</b><span>${e(spec.league.name)}</span></div></div>
        ${context}
      </div>
      <div class="chead">${e(headline)}</div>
      <div class="cmatch">${teamBlock("away")}<div class="ccenter">${center}${sub}</div>${teamBlock("home")}</div>
      ${cardStats(spec, e)}
      ${foot}
    </div>
  </div>`;
}

/* ---------------------------------- css ---------------------------------- */
function css() {
  return `
  *{box-sizing:border-box}
  .light{font-weight:400}
  .lt{position:absolute;left:96px;right:96px;bottom:64px;display:flex;align-items:stretch;
      min-height:172px;border-radius:18px;overflow:hidden;font-family:"Lato",sans-serif;color:#fff;
      background:linear-gradient(100deg,var(--tint,rgba(0,0,0,0)) 0%,rgba(0,0,0,0) 46%),
                 linear-gradient(180deg,#151A26 0%,#0C1018 60%,#090C12 100%);
      box-shadow:0 18px 50px rgba(0,0,0,.45)}
  .rail{position:absolute;left:0;top:0;bottom:0;width:10px;background:var(--pri);z-index:2}
  .plate{border-radius:16px;display:flex;align-items:center;justify-content:center;overflow:hidden}
  .lowerplate{flex:0 0 158px;margin:14px 0 14px 14px;${plateStyle()}}
  .lowerplate img{width:112px;height:112px;object-fit:contain}
  .body{flex:1 1 auto;min-width:0;padding:18px 28px 16px 26px;display:flex;flex-direction:column;justify-content:center}
  .kicker{font-family:"Lato",sans-serif;font-weight:700;font-size:21px;letter-spacing:.22em;text-transform:uppercase;
          color:var(--acc);display:flex;align-items:center;gap:10px;line-height:1}
  .pill{font-style:normal;font-family:"Lato",sans-serif;font-weight:700;font-size:15px;letter-spacing:.14em;
        text-transform:uppercase;color:#E8EDF5;border:1px solid rgba(255,255,255,.28);border-radius:999px;padding:4px 12px;line-height:1}
  .pill.warn{color:#F3C24B;border-color:rgba(243,194,75,.62);background:rgba(243,194,75,.10)}
  .namerow{display:flex;align-items:center;gap:20px;margin:10px 0 8px}
  .jersey{flex:0 0 auto;min-width:58px;height:58px;border-radius:13px;display:inline-flex;align-items:center;justify-content:center;
          font-family:"Bebas Neue",sans-serif;font-size:40px;line-height:1;padding:0 8px}
  .name{font-family:"Bebas Neue",sans-serif;line-height:1;letter-spacing:.012em;white-space:nowrap;color:#fff}
  .lines{font-family:"Lato",sans-serif;font-size:25px;color:#B4BECD;line-height:1.25;white-space:nowrap;
         max-width:100%;overflow:hidden;text-overflow:ellipsis}
  .sep{color:#5C6779}
  .scorecol{flex:0 0 auto;display:flex;flex-direction:column;justify-content:center;align-items:flex-end;
            gap:8px;padding:0 30px;border-left:1px solid rgba(255,255,255,.09);min-width:250px}
  .scontext{font-family:"Lato",sans-serif;font-weight:700;font-size:13px;letter-spacing:.2em;text-transform:uppercase;color:#8A96A8;text-align:right;line-height:1}
  .clock{font-family:"Lato",sans-serif;font-weight:700;font-size:17px;letter-spacing:.2em;text-transform:uppercase;color:#8C99AC}
  .srow{display:flex;align-items:center;justify-content:flex-end;gap:14px;color:#79879B}
  .srow .dot{width:11px;height:11px;border-radius:3px;display:inline-block}
  .srow .ts{font-family:"Lato",sans-serif;font-weight:700;font-size:22px;letter-spacing:.12em}
  .srow b{font-family:"Bebas Neue",sans-serif;font-size:50px;line-height:1;color:#8C99AC;min-width:36px;text-align:right}
  .srow.lead{color:#fff}
  .srow.lead b{color:#fff}

  /* fight */
  .lt.fight{min-height:224px}
  .fight-main{flex:1 1 auto;min-width:0;display:flex;flex-direction:column;justify-content:center;padding:18px 30px 16px 34px}
  .fight-head{font-family:"Lato",sans-serif;font-weight:700;font-size:20px;letter-spacing:.22em;text-transform:uppercase;
              color:var(--acc);display:flex;align-items:center;gap:12px}
  .fdim{color:#9AA6B8;font-weight:700;letter-spacing:.16em}
  .fight-row{display:flex;align-items:center;justify-content:center;gap:34px;margin:12px 0 8px}
  .fp{display:flex;align-items:center;gap:18px;min-width:0}
  .fp-text{min-width:0}
  .fplate{flex:0 0 96px;width:96px;height:96px;${plateStyle()}}
  .fplate img{width:66px;height:66px;object-fit:contain}
  .fp-name{display:flex;align-items:center;gap:14px;font-family:"Bebas Neue",sans-serif;line-height:1;white-space:nowrap;
           max-width:100%;overflow:hidden;text-overflow:ellipsis}
  .fp-name .jersey{min-width:46px;height:46px;font-size:32px;border-radius:11px}
  .fp-team{font-family:"Lato",sans-serif;font-weight:700;font-size:18px;letter-spacing:.18em;color:#93A0B2;margin-top:8px;display:flex;align-items:center;gap:9px}
  .fp-team .dot{width:9px;height:9px;border-radius:2px}
  .fvs{font-family:"Bebas Neue",sans-serif;font-size:58px;color:#4E5A6B;padding:0 6px;line-height:1}
  .fight-line{font-family:"Lato",sans-serif;font-size:22px;color:#B4BECD;text-align:center;letter-spacing:.02em}

  /* full-screen cards */
  .card{position:absolute;inset:0;overflow:hidden;font-family:"Lato",sans-serif;color:#fff}
  .card .split{position:absolute;top:0;left:0;right:0;height:9px;display:flex}
  .card .split i{flex:1}
  .cwrap{position:absolute;inset:0;padding:62px 96px 58px;display:flex;flex-direction:column;align-items:center}
  .ctop{width:100%;display:flex;align-items:center;justify-content:space-between}
  .cleague{display:flex;align-items:center;gap:16px;min-width:0}
  .lplate{width:62px;height:62px;flex:0 0 62px;${plateStyle()}}
  .lplate img{width:42px;height:42px;object-fit:contain}
  .cltext{display:flex;flex-direction:column;line-height:1.15;min-width:0}
  .cltext b{font-family:"Bebas Neue",sans-serif;font-size:30px;letter-spacing:.08em;font-weight:400}
  .cltext span{font-family:"Lato",sans-serif;font-size:15px;letter-spacing:.16em;text-transform:uppercase;color:#8C99AC}
  .ccontext{font-family:"Lato",sans-serif;font-weight:700;font-size:17px;letter-spacing:.18em;text-transform:uppercase;color:#939FB1;text-align:right;max-width:640px}
  .chead{font-family:"Bebas Neue",sans-serif;font-size:56px;letter-spacing:.16em;margin-top:30px;line-height:1;text-align:center;color:#fff}
  .cmatch{flex:1;width:100%;display:flex;align-items:center;justify-content:center;gap:24px}
  .cteam{width:400px;display:flex;flex-direction:column;align-items:center;text-align:center}
  .cplate{width:224px;height:224px;border-radius:30px;${plateStyle()}}
  .cplate img{width:160px;height:160px;object-fit:contain}
  .cshort{display:flex;align-items:center;gap:10px;font-family:"Lato",sans-serif;font-weight:700;font-size:22px;
          letter-spacing:.2em;color:#D6DCE6;margin-top:22px}
  .cshort .dot{width:11px;height:11px;border-radius:3px}
  .cname{font-family:"Bebas Neue",sans-serif;line-height:1.02;margin-top:10px;letter-spacing:.015em;max-width:400px}
  .ccity{font-family:"Lato",sans-serif;font-size:18px;letter-spacing:.14em;text-transform:uppercase;color:#7E8A9C;margin-top:8px}
  .cteam.lose{opacity:.62}
  .ccenter{flex:1 1 auto;display:flex;flex-direction:column;align-items:center;justify-content:center;min-width:0}
  .cscores{font-family:"Bebas Neue",sans-serif;font-size:190px;line-height:.9;display:flex;align-items:center;gap:34px}
  .cscores .dash{color:#586374;font-size:120px}
  .cscores.vsScore{font-size:120px;color:#566173;letter-spacing:.1em}
  .cclock{font-family:"Lato",sans-serif;font-weight:700;font-size:22px;letter-spacing:.24em;text-transform:uppercase;color:#98A4B6;margin-top:20px}
  .statrow{display:flex;gap:22px;margin-top:6px}
  .stat{display:flex;align-items:center;gap:22px;background:rgba(255,255,255,.045);border:1px solid rgba(255,255,255,.08);
        border-radius:16px;padding:16px 30px}
  .stat .sv{font-family:"Bebas Neue",sans-serif;font-size:38px;line-height:1;min-width:44px;text-align:center}
  .stat .sl{font-family:"Lato",sans-serif;font-weight:700;font-size:15px;letter-spacing:.18em;text-transform:uppercase;color:#8C99AC}
  .cfoot{font-family:"Lato",sans-serif;font-size:20px;letter-spacing:.02em;color:#A9B4C4;text-align:center;margin-top:24px;max-width:1560px}
  .cfoot .sep{color:#566173}
  `;
}

/* --------------------------------- render -------------------------------- */
export async function render(spec, ctx) {
  const e = ctx.esc;
  const fonts = [
    await ctx.fontFace("Lato", "fonts/Lato-Regular.ttf", 400),
    await ctx.fontFace("Lato", "fonts/Lato-Bold.ttf", 700),
    await ctx.fontFace("Lato", "fonts/Lato-Black.ttf", 900),
  ].join("\n");

  const kind = KNOWN_CARD.has(spec.kind) ? spec.kind : "moment";
  let body;
  if (spec.kind === "fight") body = fightPanel(spec, e);
  else if (LOWER.has(kind)) body = lowerThird(spec, e);
  else body = card(spec, e);

  return { css: fonts + css(), html: body };
}
