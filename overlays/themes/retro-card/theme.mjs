export const meta = { name: "retro-card", author: "Expert Assistant", description: "Vintage trading card and arena scoreboard aesthetic" };

const LOWER = new Set(["score", "penalty", "fight", "save", "moment"]);

function team(spec, side) {
  return side === "home" ? spec.home : side === "away" ? spec.away : null;
}

function scoreline(spec, e) {
  if (!spec.score) return "";
  return `<div class="sc">
    <div class="team-box away"><img src="${spec.away.logo}"><b>${e(spec.away.short)}</b><span>${e(spec.score.away)}</span></div>
    <div class="vs-divider">VS</div>
    <div class="team-box home"><img src="${spec.home.logo}"><b>${e(spec.home.short)}</b><span>${e(spec.score.home)}</span></div>
    ${spec.clock ? `<div class="clock">${e(spec.clock.text)}</div>` : ""}
  </div>`;
}

export function render(spec, { esc: e }) {
  const t = team(spec, spec.side) || { primary: spec.league.primary, text: "#fff", logo: spec.league.logo };
  
  const subjectName = spec.subject ? `${spec.subject.number ? "#" + e(spec.subject.number) + " " : ""}${e(spec.subject.name)}` : "";
  const othersNames = spec.others.map((o) => `${o.number ? "#" + e(o.number) + " " : ""}${e(o.name)}`).join(", ");
  const headText = spec.kind === "fight" ? `${subjectName} <span class="vs-small">vs</span> ${othersNames}` : subjectName;
  
  const tags = spec.tags.map((x) => `<span class="tag">${e(x)}</span>`).join("");
  const badge = spec.badge ? `<span class="badge">${e(spec.badge)}</span>` : "";

  const css = `
    :root {
      --border-width: 6px;
      --card-bg: #f4f1ea;
      --ink: #222;
      --accent: #c0392b;
    }
    .lt {
      position: absolute;
      left: 96px;
      right: 96px;
      bottom: 64px;
      display: flex;
      align-items: stretch;
      background: var(--card-bg);
      border: var(--border-width) solid var(--ink);
      box-shadow: 8px 8px 0px rgba(0,0,0,0.3);
      font-family: "Barlow Semi Condensed";
      color: var(--ink);
      min-height: 160px;
      overflow: hidden;
    }
    .lt .logo-area {
      width: 160px;
      display: flex;
      align-items: center;
      justify-content: center;
      background: ${t.primary};
      border-right: var(--border-width) solid var(--ink);
      position: relative;
      padding: 20px;
      box-sizing: border-box;
    }
    .lt .logo-area img {
      width: 100%;
      height: 100%;
      object-fit: contain;
      filter: drop-shadow(2px 2px 0px rgba(0,0,0,0.2));
      background: ${t.primary === '#f2a900' ? '#fff' : 'transparent'};
      padding: 8px;
      border: 2px solid var(--ink);
      box-sizing: border-box;
    }
    .lt .body {
      flex: 1;
      min-width: 0;
      padding: 20px 24px;
      display: flex;
      flex-direction: column;
      justify-content: center;
      position: relative;
    }
    .lt .context-text {
      position: absolute;
      top: 20px;
      right: 24px;
      font-size: 16px;
      color: #888;
      text-transform: uppercase;
    }
    .lt .headline-row {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 4px;
    }
    .k {
      font-family: "Bebas Neue";
      font-size: 36px;
      letter-spacing: 1px;
      color: var(--accent);
      text-transform: uppercase;
    }
    .h {
      font-family: "Bebas Neue", "Barlow Semi Condensed";
      font-size: 72px;
      line-height: 0.9;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      text-transform: uppercase;
    }
    .l {
      font-size: 24px;
      color: #555;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      margin-top: 8px;
    }
    .tag {
      font-family: "Bebas Neue";
      font-size: 24px;
      background: var(--ink);
      color: var(--card-bg);
      padding: 2px 8px;
      border-radius: 2px;
    }
    .badge {
      font-family: "Bebas Neue";
      font-size: 24px;
      background: var(--accent);
      color: #fff;
      padding: 2px 8px;
      border-radius: 2px;
    }
    .vs-small {
      font-family: "Bebas Neue";
      color: #888;
      margin: 0 10px;
    }
    .sc {
      display: flex;
      align-items: center;
      gap: 0;
      background: var(--ink);
      color: var(--card-bg);
      font-family: "Bebas Neue";
      padding: 0 20px;
    }
    .team-box {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 0 15px;
    }
    .team-box img {
      width: 40px;
      height: 40px;
      object-fit: contain;
      background: rgba(255,255,255,0.1);
      border-radius: 50%;
    }
    .team-box b {
      font-size: 32px;
      color: #ccc;
    }
    .team-box span {
      font-size: 48px;
      margin-left: 8px;
    }
    .vs-divider {
      font-size: 24px;
      color: #666;
      padding: 0 10px;
      border-left: 1px solid #444;
      border-right: 1px solid #444;
    }
    .clock {
      font-family: "Bebas Neue";
      font-size: 28px;
      color: var(--card-bg);
      text-align: right;
      padding: 0 20px;
      align-self: center;
    }
    .card {
      position: absolute;
      inset: 0;
      background: var(--card-bg);
      color: var(--ink);
      font-family: "Barlow Semi Condensed";
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 40px;
      border: 20px solid var(--ink);
      box-sizing: border-box;
    }
    .card .league-logo {
      position: absolute;
      top: 60px;
      right: 60px;
      width: 100px;
      height: 100px;
      object-fit: contain;
      opacity: 0.8;
    }
    .card .hd {
      font-family: "Bebas Neue";
      font-size: 80px;
      letter-spacing: 4px;
      color: var(--accent);
      text-transform: uppercase;
      border-bottom: 4px double var(--ink);
      padding-bottom: 10px;
    }
    .card .row {
      display: flex;
      align-items: center;
      gap: 60px;
    }
    .card .tm {
      display: flex;
      flex-direction: column;
      align-items: center;
      width: 400px;
      text-align: center;
    }
    .card .tm img {
      width: 240px;
      height: 240px;
      object-fit: contain;
      border: 10px solid var(--ink);
      background: #fff;
      box-shadow: 10px 10px 0px rgba(0,0,0,0.1);
    }
    .card .tm div {
      font-family: "Bebas Neue";
      font-size: 48px;
      margin-top: 20px;
      text-transform: uppercase;
    }
    .card .big {
      font-family: "Bebas Neue";
      font-size: 220px;
      line-height: 1;
      color: var(--ink);
      text-shadow: 4px 4px 0px #ddd;
    }
    .card .sub {
      font-size: 32px;
      color: #666;
      max-width: 1200px;
      text-align: center;
      font-style: italic;
    }
    .card table {
      font-size: 32px;
      border-collapse: collapse;
      margin: 20px 0;
    }
    .card td {
      padding: 10px 30px;
      text-align: center;
      border-bottom: 1px solid #ccc;
    }
    .card td:nth-child(2) {
      font-weight: bold;
      color: #888;
    }
  `;

  if (LOWER.has(spec.kind)) {
    return {
      css,
      html: `<div class="lt">
        <div class="logo-area"><img src="${t.logo}"></div>
        <div class="body">
          <div class="headline-row">
            <div class="k">${e(spec.headline)}</div>
            ${tags}${badge}
          </div>
          <div class="h">${headText}</div>
          <div class="l">${spec.lines.map(e).join(" · ")}</div>
        </div>
        ${scoreline(spec, e)}
        <div class="clock">${spec.clock ? e(spec.clock.text) : ""}</div>
      </div>`
    };
  }

  const stats = spec.stats.length
    ? `<table>${spec.stats.map((s) => `<tr><td>${e(s.away)}</td><td>${e(s.label)}</td><td>${e(s.home)}</td></tr>`).join("")}</table>`
    : "";

  const mid = spec.score 
    ? `<div class="big">${e(spec.score.away)} – ${e(spec.score.home)}</div>` 
    : `<div class="big" style="font-size:140px">VS</div>`;

  return {
    css,
    html: `<div class="card">
      <img class="league-logo" src="${spec.league.logo}">
      <div class="hd">${e(spec.headline)}${spec.clock && spec.kind !== "final" ? " · " + e(spec.clock.text) : ""}</div>
      <div class="row">
        <div class="tm"><img src="${spec.away.logo}"><div>${e(spec.away.name)}</div></div>
        ${mid}
        <div class="tm"><img src="${spec.home.logo}"><div>${e(spec.home.name)}</div></div>
      </div>
      ${stats}
      <div class="sub">${[spec.context, ...spec.lines].filter(Boolean).map(e).join(" · ")}</div>
    </div>`
  };
}
