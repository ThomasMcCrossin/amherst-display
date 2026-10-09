// Baseline: a plain reference implementation of every overlay kind. Not a contestant.
export const meta = { name: "baseline", author: "reference", description: "Minimal dark panel; shows the theme API." };

const LOWER = new Set(["score", "penalty", "fight", "save", "moment"]);

function team(spec, side) {
  return side === "home" ? spec.home : side === "away" ? spec.away : null;
}

function scoreline(spec, e) {
  if (!spec.score) return "";
  return `<div class="sc"><img src="${spec.away.logo}"><b>${e(spec.away.short)}</b><span>${e(spec.score.away)}</span>
    <span>${e(spec.score.home)}</span><b>${e(spec.home.short)}</b><img src="${spec.home.logo}"></div>`;
}

export function render(spec, { esc: e }) {
  const t = team(spec, spec.side) || { primary: spec.league.primary, text: "#fff", logo: spec.league.logo };
  const who = spec.subject ? `${spec.subject.number ? "#" + e(spec.subject.number) + " " : ""}${e(spec.subject.name)}` : "";
  const vs = spec.others.map((o) => `${o.number ? "#" + e(o.number) + " " : ""}${e(o.name)}`).join(", ");
  const tags = spec.tags.map((x) => `<i>${e(x)}</i>`).join("") + (spec.badge ? `<i class="warn">${e(spec.badge)}</i>` : "");
  const css = `
    .lt{position:absolute;left:96px;right:96px;bottom:64px;display:flex;align-items:stretch;background:rgba(8,12,18,.88);
        border-left:12px solid ${t.primary};font-family:"Barlow Semi Condensed";color:#fff;min-height:150px}
    .lt .logo{width:150px;display:flex;align-items:center;justify-content:center;background:${t.primary}}
    .lt .logo img{width:112px;height:112px;object-fit:contain}
    .lt .body{flex:1;min-width:0;padding:14px 28px}
    .k{font-family:"Bebas Neue";font-size:40px;letter-spacing:2px;color:#f5b82e}
    .h{font-family:"Bebas Neue";font-size:64px;line-height:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .l{font-size:28px;color:#cfd8e3;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    i{font-style:normal;font-family:"Bebas Neue";font-size:30px;background:#f5b82e;color:#111;padding:0 10px;margin-left:10px}
    i.warn{background:#d33;color:#fff}
    .sc{display:flex;align-items:center;gap:12px;padding:0 24px;font-family:"Bebas Neue";font-size:56px;background:rgba(0,0,0,.35)}
    .sc img{width:56px;height:56px;object-fit:contain}.sc b{font-size:36px;color:#cfd8e3}
    .clock{font-size:26px;color:#9fb0c3;text-align:right;padding:0 24px 0 0;align-self:center;white-space:nowrap}
    .card{position:absolute;inset:0;background:radial-gradient(circle at 50% 40%,rgba(20,30,48,.94),rgba(4,8,14,.97));
          color:#fff;font-family:"Barlow Semi Condensed";display:flex;flex-direction:column;align-items:center;justify-content:center;gap:24px}
    .card .row{display:flex;align-items:center;gap:80px}
    .card .tm{display:flex;flex-direction:column;align-items:center;width:420px;text-align:center}
    .card .tm img{width:220px;height:220px;object-fit:contain}
    .card .tm div{font-family:"Bebas Neue";font-size:44px;margin-top:12px}
    .card .big{font-family:"Bebas Neue";font-size:200px;line-height:1}
    .card .hd{font-family:"Bebas Neue";font-size:72px;letter-spacing:6px;color:#f5b82e}
    .card .sub{font-size:34px;color:#cfd8e3;max-width:1500px;text-align:center}
    .card table{font-size:32px;border-collapse:collapse}.card td{padding:6px 28px;text-align:center}
    .card .lg{position:absolute;top:64px;right:96px;width:120px;height:120px;object-fit:contain}`;
  if (LOWER.has(spec.kind)) {
    const head = spec.kind === "fight" ? `${who} <span style="color:#9fb0c3">vs</span> ${vs}` : who;
    return { css, html: `<div class="lt"><div class="logo"><img src="${t.logo}"></div>
      <div class="body"><div class="k">${e(spec.headline)}${tags}</div><div class="h">${head}</div>
      <div class="l">${spec.lines.map(e).join(" · ")}</div></div>
      ${scoreline(spec, e)}<div class="clock">${spec.clock ? e(spec.clock.text) : ""}</div></div>` };
  }
  const stats = spec.stats.length
    ? `<table>${spec.stats.map((s) => `<tr><td>${e(s.away)}</td><td style="color:#9fb0c3">${e(s.label)}</td><td>${e(s.home)}</td></tr>`).join("")}</table>`
    : "";
  const mid = spec.score ? `<div class="big">${e(spec.score.away)} – ${e(spec.score.home)}</div>` : `<div class="big" style="font-size:120px">VS</div>`;
  return { css, html: `<div class="card"><img class="lg" src="${spec.league.logo}">
    <div class="hd">${e(spec.headline)}${spec.clock && spec.kind !== "final" ? " · " + e(spec.clock.text) : ""}</div>
    <div class="row"><div class="tm"><img src="${spec.away.logo}"><div>${e(spec.away.name)}</div></div>${mid}
    <div class="tm"><img src="${spec.home.logo}"><div>${e(spec.home.name)}</div></div></div>
    ${stats}<div class="sub">${[spec.context, ...spec.lines].filter(Boolean).map(e).join(" · ")}</div></div>` };
}
