// Data-Chip: a modern analytics-style broadcast theme.
// Compact info chips, grid-aligned numbers, high contrast.
export const meta = { 
  name: "data-chip", 
  author: "AI Assistant", 
  description: "Modern analytics broadcast theme with a data-forward grid aesthetic." 
};

const LOWER = new Set(["score", "penalty", "fight", "save", "moment"]);

function isLowerThird(kind) {
  return LOWER.has(kind) || !["intro", "break", "final"].includes(kind);
}

function team(spec, side) {
  return side === "home" ? spec.home : side === "away" ? spec.away : null;
}

function scoreline(spec, e) {
  if (!spec.score) return "";
  return `
    <div class="sc">
      <div class="sc-chip">
        <img src="${spec.away.logo}" class="sc-logo">
        <span class="sc-name">${e(spec.away.short)}</span>
        <span class="sc-val">${e(spec.score.away)}</span>
      </div>
      <div class="sc-sep"></div>
      <div class="sc-chip">
        <span class="sc-val">${e(spec.score.home)}</span>
        <span class="sc-name">${e(spec.home.short)}</span>
        <img src="${spec.home.logo}" class="sc-logo">
      </div>
    </div>`;
}

export function render(spec, { esc: e }) {
  const t = team(spec, spec.side) || { primary: spec.league.primary, text: "#fff", logo: spec.league.logo };
  
  const subject = spec.subject;
  const subjectText = subject ? `${subject.number ? `#${e(subject.number)} ` : ""}${e(subject.name)}` : "";
  
  const othersText = spec.others.map(o => `${o.number ? `#${e(o.number)} ` : ""}${e(o.name)}`).join(", ");
  
  const headlineText = e(spec.headline);
  const tags = spec.tags.map(x => `<span class="tag">${e(x)}</span>`).join("");
  const badge = spec.badge ? `<span class="badge">${e(spec.badge)}</span>` : "";
  
  const css = `
    :root {
      --bg-dark: #0a0c10;
      --bg-panel: rgba(15, 18, 23, 0.95);
      --accent: #f5b82e;
      --text-main: #ffffff;
      --text-dim: #94a3b8;
    }
    body { margin: 0; padding: 0; overflow: hidden; }

    /* Lower Thirds */
    .lt {
      position: absolute;
      left: 96px;
      right: 96px;
      bottom: 80px;
      display: flex;
      align-items: stretch;
      font-family: "Barlow Semi Condensed";
      color: var(--text-main);
      height: 140px;
    }
    .lt-main {
      background: var(--bg-panel);
      flex: 1;
      display: flex;
      flex-direction: column;
      justify-content: center;
      padding: 0 40px;
      border-left: 8px solid ${t.primary};
      box-shadow: 20px 0 40px rgba(0,0,0,0.5);
      position: relative;
      min-width: 0;
      z-index: 2;
    }
    .lt-header {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 4px;
    }
    .lt-kind {
      font-family: "Bebas Neue";
      font-size: 32px;
      color: var(--accent);
      letter-spacing: 1px;
      line-height: 1;
    }
    .tag {
      font-family: "Bebas Neue";
      font-size: 20px;
      background: var(--accent);
      color: #000;
      padding: 2px 8px;
      border-radius: 2px;
      line-height: 1;
    }
    .badge {
      font-family: "Bebas Neue";
      font-size: 20px;
      background: #ef4444;
      color: #fff;
      padding: 2px 8px;
      border-radius: 2px;
      line-height: 1;
    }
    .lt-hero {
      font-family: "Bebas Neue";
      font-size: 64px;
      line-height: 1.1;
      word-wrap: break-word;
      overflow-wrap: break-word;
      display: block;
      letter-spacing: 1px;
    }
    .lt-lines {
      font-size: 24px;
      color: var(--text-dim);
      word-wrap: break-word;
      overflow-wrap: break-word;
    }
    .lt-logo-box {
      width: 140px;
      background: ${t.primary};
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 0 20px rgba(0,0,0,0.3);
      z-index: 3;
    }
    .lt-logo-box img {
      width: 100px;
      height: 100px;
      object-fit: contain;
    }
    
    /* Scorebug component in LT */
    .sc {
      display: flex;
      align-items: center;
      background: rgba(0,0,0,0.85);
      padding: 0 20px;
      font-family: "Bebas Neue";
    }
    .sc-chip {
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 0 15px;
    }
    .sc-logo {
      width: 40px;
      height: 40px;
      object-fit: contain;
    }
    .sc-name {
      font-size: 32px;
      color: #fff;
    }
    .sc-val {
      font-size: 48px;
      color: #fff;
      min-width: 40px;
      text-align: center;
    }
    .sc-sep {
      width: 2px;
      height: 40px;
      background: rgba(255,255,255,0.2);
    }
    .lt-clock {
      padding: 0 20px;
      display: flex;
      align-items: center;
      font-family: "Bebas Neue";
      font-size: 28px;
      color: #fff;
      background: rgba(0,0,0,0.88);
    }

    /* Full Screen Cards */
    .card {
      position: absolute;
      inset: 0;
      background: var(--bg-dark);
      color: #fff;
      font-family: "Barlow Semi Condensed";
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      padding: 0 96px;
    }
    .card-header {
      position: absolute;
      top: 64px;
      text-align: center;
      width: 100%;
    }
    .card-league-logo {
      width: 100px;
      height: 100px;
      object-fit: contain;
      margin-bottom: 20px;
    }
    .card-title {
      font-family: "Bebas Neue";
      font-size: 64px;
      letter-spacing: 4px;
      color: var(--accent);
      text-transform: uppercase;
    }
    .card-clock {
      font-family: "Bebas Neue";
      font-size: 32px;
      color: var(--text-dim);
      margin-left: 20px;
    }
    .card-main {
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 60px;
      width: 100%;
    }
    .card-team {
      display: flex;
      flex-direction: column;
      align-items: center;
      width: 400px;
      text-align: center;
    }
    .card-team img {
      width: 280px;
      height: 280px;
      object-fit: contain;
      margin-bottom: 20px;
    }
    .card-team-name {
      font-family: "Bebas Neue";
      font-size: 56px;
      line-height: 1;
    }
    .card-score-big {
      font-family: "Bebas Neue";
      font-size: 240px;
      line-height: 0.8;
      display: flex;
      align-items: center;
      gap: 40px;
    }
    .card-score-val {
      background: var(--bg-panel);
      padding: 20px 40px;
      border-bottom: 8px solid var(--accent);
      min-width: 160px;
      text-align: center;
    }
    .card-stats {
      margin: 40px 0;
      border-collapse: collapse;
      font-size: 32px;
    }
    .card-stats td {
      padding: 10px 40px;
      text-align: center;
    }
    .card-stats .label {
      color: var(--text-dim);
      font-weight: 600;
      text-transform: uppercase;
      font-size: 24px;
    }
    .card-footer {
      margin-top: 40px;
      font-size: 32px;
      color: var(--text-dim);
      text-align: center;
      max-width: 1200px;
    }
  `;

  if (isLowerThird(spec.kind)) {
    const hero = spec.kind === "fight" 
      ? `${subjectText} <span style="color:var(--text-dim)">vs</span> ${othersText}` 
      : subjectText;

    const lines = [spec.context, ...spec.lines].filter(Boolean);

    return {
      css,
      html: `
        <div class="lt">
          <div class="lt-logo-box"><img src="${t.logo}"></div>
          <div class="lt-main">
            <div class="lt-header">
              <span class="lt-kind">${headlineText}</span>
              ${tags}${badge}
            </div>
            <div class="lt-hero">${hero}</div>
            <div class="lt-lines">${lines.join(" · ")}</div>
          </div>
          ${scoreline(spec, e)}
          <div class="lt-clock">${spec.clock ? e(spec.clock.text) : ""}</div>
        </div>`
    };
  }

  const statsTable = spec.stats.length 
    ? `<table class="card-stats">
         ${spec.stats.map(s => `
           <tr>
             <td>${e(s.away)}</td>
             <td class="label">${e(s.label)}</td>
             <td>${e(s.home)}</td>
           </tr>
         `).join("")}
       </table>` 
    : "";

  const scoreDisplay = spec.score 
    ? `<div class="card-score-big">
         <div class="card-score-val">${e(spec.score.away)}</div>
         <div style="font-size:80px; color:var(--text-dim)">-</div>
         <div class="card-score-val">${e(spec.score.home)}</div>
       </div>`
    : `<div class="card-score-big" style="font-size:180px">VS</div>`;

  return {
    css,
    html: `
      <div class="card">
        <div class="card-header">
          <img class="card-league-logo" src="${spec.league.logo}">
          <div class="card-title">
            ${headlineText}
            ${spec.clock && spec.kind !== "final" ? `<span class="card-clock">${e(spec.clock.text)}</span>` : ""}
          </div>
        </div>
        <div class="card-main">
          <div class="card-team">
            <img src="${spec.away.logo}">
            <div class="card-team-name">${e(spec.away.name)}</div>
          </div>
          ${scoreDisplay}
          <div class="card-team">
            <img src="${spec.home.logo}">
            <div class="card-team-name">${e(spec.home.name)}</div>
          </div>
        </div>
        ${statsTable}
        <div class="card-footer">
          ${[spec.context, ...spec.lines].filter(Boolean).join(" · ")}
        </div>
      </div>`
    };
}
