// arena-bold — jumbotron energy: big angled slabs, heavy condensed capitals,
// high contrast, team colour floods the panel; loud but clean.
export const meta = {
  name: "arena-bold",
  author: "arena-bold",
  description: "Angled jumbotron slabs, Bebas display type, white keyline plates, team-colour floods.",
};

const INK = "#0b0e14";

const parseHex = (h) => {
  h = String(h || "").replace("#", "").trim();
  if (h.length === 3) h = h.split("").map((c) => c + c).join("");
  const n = parseInt(h, 16) || 0;
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
};
const mix = (a, b, t) => {
  const A = parseHex(a), B = parseHex(b);
  return "#" + A.map((v, i) => Math.round(v + (B[i] - v) * t).toString(16).padStart(2, "0")).join("");
};
const shade = (c, t) => mix(c, "#05070b", t);
const lum = (c) => { const [r, g, b] = parseHex(c); return (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255; };

const CARDS = new Set(["break", "final", "intro"]);

// Text colour that is guaranteed readable on the panel colour: use team.text
// when the pack provides it, otherwise derive from luminance.
const textOn = (t) => t.text || (lum(t.primary) > 0.55 ? "#141a22" : "#ffffff");

const vars = (t) => {
  const on = textOn(t);
  const dark = lum(on) < 0.45; // dark type on a light flood
  return `--pb:${t.primary};--pd:${shade(t.primary, 0.24)};--pt:${on};` +
    `--pl:${dark ? "rgba(16,20,28,.76)" : "rgba(255,255,255,.84)"};`;
};

function teamFor(spec, side) {
  if (side === "home" && spec.home) return spec.home;
  if (side === "away" && spec.away) return spec.away;
  const L = spec.league || {};
  return { primary: L.primary || "#101318", short: L.short || "", name: L.name || "", logo: (L && L.logo) || "" };
}

export async function render(spec, ctx) {
  const e = ctx.esc;
  const tags = (spec.tags || []).filter(Boolean);
  const badge = spec.badge;
  const lines = (spec.lines || []).filter(Boolean);
  const chipTags = tags.map((x) => `<i>${e(x)}</i>`).join("") + (badge ? `<i class="warn">${e(badge)}</i>` : "");

  const plate = (src, cls = "") => `<div class="plate ${cls}"><img src="${src}" alt=""></div>`;

  const ribbon = (headline) =>
    `<div class="rbwrap"><div class="rk"><div class="rb"><span class="rh">${e(headline)}</span>${chipTags}</div></div>` +
    `<span class="sliver s1"></span><span class="sliver s2"></span></div>`;

  const meta = () => {
    const bits = [];
    if (spec.clock) bits.push(`<div class="clk">${e(spec.clock.text)}</div>`);
    if (spec.score) bits.push(
      `<div class="ms"><b class="t">${e((spec.away || {}).short)}</b><b class="n">${e(spec.score.away)}</b>` +
      `<b class="x">-</b><b class="n">${e(spec.score.home)}</b><b class="t">${e((spec.home || {}).short)}</b></div>`);
    if (!bits.length) return "";
    return `<div class="meta">${bits.join("")}</div>`;
  };

  const heroOf = (p) =>
    (p && p.number ? `<span class="no">#${e(p.number)}</span>` : "") +
    `<span class="nm fit">${e(p ? p.name : "")}</span>`;
  const lineRow = lines.length ? `<div class="ln fit">${lines.map(e).join(" · ")}</div>` : "";

  // ---- lower third: one hero --------------------------------------------
  const subjectTeam = () => teamFor(spec, (spec.subject && spec.subject.side) || spec.side);

  const lowerSingle = () => {
    const t = subjectTeam();
    const hero = spec.subject
      ? `<div class="name">${heroOf(spec.subject)}</div>`
      : `<div class="name"><span class="nm fit">${e((spec.side ? teamFor(spec, spec.side) : teamFor(spec, null)).name)}</span></div>`;
    return `<div class="lt">${ribbon(spec.headline)}
      <div class="deck" style="${vars(t)}">${plate(t.logo)}
        <div class="copy">${hero}${lineRow}</div>${meta()}${!spec.clock && !spec.score ? "<div class=\"tail\"><span></span><span></span></div>" : ""}
      </div></div>`;
  };

  // ---- lower third: fight, both fighters --------------------------------
  const lowerFight = () => {
    const a = teamFor(spec, (spec.subject && spec.subject.side) || spec.side);
    const others = (spec.others || []).filter(Boolean);
    const b = teamFor(spec, (others[0] && others[0].side) || null);
    const face = (t, p) =>
      `<div class="face" style="${vars(t)}">${plate(t.logo, "sm")}` +
      `<div class="name">${heroOf(p)}</div></div>`;
    return `<div class="lt">${ribbon(spec.headline)}
      <div class="deck fight"><div class="faces">
        <div class="frow">${face(a, spec.subject)}<div class="vs">VS</div>${face(b, others[0])}</div>
        ${lineRow ? `<div class="fstrip"><span class="fit">${lines.map(e).join(" · ")}</span></div>` : ""}
      </div>${meta()}</div></div>`;
  };

  const isFight = spec.kind === "fight" && spec.subject && (spec.others || []).length > 0;

  // ---- full-screen cards (opaque) ----------------------------------------
  const card = () => {
    const a = spec.away || teamFor(spec, null);
    const h = spec.home || teamFor(spec, null);
    const slab = (t) =>
      `<div class="slab" style="${vars(t)}">${plate(t.logo, "big")}<div class="sname">${e(t.name)}</div></div>`;
    const mid = spec.score
      ? `<div class="score"><span class="cell" style="--tc:${a.primary}"><b class="d">${e(spec.score.away)}</b><span class="bar"></span><span class="lbl">${e(a.short)}</span></span>` +
        `<span class="sep">-</span>` +
        `<span class="cell" style="--tc:${h.primary}"><b class="d">${e(spec.score.home)}</b><span class="bar"></span><span class="lbl">${e(h.short)}</span></span></div>`
      : `<div class="vsmid">VS</div>`;
    const clockChip = spec.clock && spec.kind !== "final" ? `<div class="chip">${e(spec.clock.text)}</div>` : "";
    const stats = (spec.stats || []).length
      ? `<div class="stats">${spec.stats.map((s) =>
          `<div class="row"><span class="v a">${e(s.away)}</span><span class="k">${e(s.label)}</span><span class="v h">${e(s.home)}</span></div>`).join("")}</div>`
      : "";
    const stripBits = [spec.context, ...lines].filter(Boolean);
    const strip = stripBits.length || chipTags
      ? `<div class="strip"><span class="tx fit">${stripBits.map(e).join(" · ")}</span>${chipTags}</div>` : "";
    return `<div class="card" style="--wa:${a.primary};--wb:${h.primary}"><div class="wash"></div>
      <div class="lg">${(spec.league || {}).logo ? `<img src="${spec.league.logo}" alt="">` : ""}</div>
      <div class="crbwrap"><div class="rk"><div class="crb"><span class="rh big">${e(spec.headline)}</span></div></div>
        <span class="sliver dark s1"></span><span class="sliver dark s2"></span></div>
      ${clockChip}
      <div class="stage">${slab(a)}<div class="mid">${mid}</div>${slab(h)}</div>
      ${stats}${strip}</div>`;
  };

  const html = CARDS.has(spec.kind) ? card() : (isFight ? lowerFight() : lowerSingle());

  const css = `
    *{box-sizing:border-box}
    body{margin:0;color:#fff;font-family:"Barlow Semi Condensed",sans-serif}
    .fit{min-width:0}
    /* ---- shared slabs & ribbon ---------------------------------------- */
    .rbwrap{position:relative;z-index:4;display:flex;align-items:stretch;filter:drop-shadow(0 10px 16px rgba(5,8,14,.4))}
    .rk{position:relative;display:inline-flex}
    .rk::before{content:"";position:absolute;inset:-4px;background:#15181d;
        clip-path:polygon(28px 0,100% 0,calc(100% - 28px) 100%,0 100%)}
    .rb{position:relative;display:inline-flex;align-items:center;gap:14px;background:#fff;color:${INK};
        clip-path:polygon(24px 0,100% 0,calc(100% - 24px) 100%,0 100%);padding:10px 36px 10px 42px}
    .rh{font-family:"Bebas Neue";font-size:46px;line-height:1;letter-spacing:.06em;white-space:nowrap}
    .rb i{display:inline-block;font-style:normal;font-family:"Bebas Neue";font-size:29px;line-height:1;letter-spacing:.06em;
        background:${INK};color:#fff;padding:7px 16px 5px;clip-path:polygon(10px 0,100% 0,calc(100% - 10px) 100%,0 100%)}
    .rb i.warn{background:#e11d33}
    .sliver{align-self:stretch;flex:none;background:rgba(255,255,255,.5);
        clip-path:polygon(10px 0,100% 0,calc(100% - 10px) 100%,0 100%)}
    .s1{width:26px;margin-left:10px}
    .s2{width:16px;background:rgba(255,255,255,.26);margin-left:6px}
    /* ---- lower-third deck --------------------------------------------- */
    .lt{position:absolute;left:96px;right:96px;bottom:54px;display:flex;flex-direction:column;align-items:flex-start}
    .deck{position:relative;z-index:3;display:flex;align-items:stretch;width:100%;min-height:180px;margin-top:-2px;
        filter:drop-shadow(0 18px 26px rgba(5,8,14,.5))}
    .deck::before{content:"";position:absolute;inset:0;
        clip-path:polygon(38px 0,100% 0,calc(100% - 38px) 100%,0 100%);
        background:repeating-linear-gradient(114deg,rgba(255,255,255,.055) 0 3px,rgba(255,255,255,0) 3px 16px),
            linear-gradient(112deg,var(--pb) 8%,var(--pd) 96%);
        box-shadow:inset 0 0 0 2px rgba(255,255,255,.13)}
    .plate{position:relative;z-index:2;flex:none;align-self:center;width:138px;height:138px;margin:0 4px 0 34px;background:#fff;
        display:flex;align-items:center;justify-content:center;
        box-shadow:0 0 0 3px rgba(10,13,19,.55),inset 0 0 0 2px rgba(10,13,19,.07)}
    .plate img{max-width:84%;max-height:84%;object-fit:contain;
        filter:drop-shadow(0 0 1.5px rgba(10,13,19,.8)) drop-shadow(0 0 1px rgba(10,13,19,.5))}
    .copy{position:relative;z-index:2;flex:1;min-width:0;display:flex;flex-direction:column;justify-content:center;gap:7px;padding:22px 30px}
    .name{display:flex;align-items:center;gap:18px;min-width:0}
    .no{flex:none;font-family:"Bebas Neue";font-size:40px;line-height:1;background:${INK};color:#fff;
        padding:9px 15px 6px;clip-path:polygon(12px 0,100% 0,calc(100% - 12px) 100%,0 100%)}
    .nm{font-family:"Bebas Neue";font-size:94px;line-height:.98;letter-spacing:.015em;color:var(--pt);white-space:nowrap}
    .ln{font-weight:600;font-size:30px;line-height:1.12;color:var(--pl);white-space:nowrap;letter-spacing:.01em}
    .tail{position:relative;z-index:2;flex:none;align-self:center;display:flex;align-items:center;gap:12px;margin-right:52px}
    .tail span{width:34px;height:104px;background:rgba(255,255,255,.16);clip-path:polygon(12px 0,100% 0,calc(100% - 12px) 100%,0 100%)}
    .tail span + span{width:20px;height:78px;background:rgba(255,255,255,.09)}
    .meta{position:relative;z-index:2;flex:none;min-width:312px;display:flex;flex-direction:column;justify-content:center;gap:9px;
        align-items:flex-end;padding:0 30px 0 62px}
    .meta::before{content:"";position:absolute;inset:0;z-index:-1;background:rgba(8,11,17,.94);
        clip-path:polygon(36px 0,100% 0,100% 100%,0 100%)}
    .clk{font-family:"Bebas Neue";font-size:47px;line-height:1;letter-spacing:.05em;white-space:nowrap}
    .ms{display:flex;align-items:baseline;gap:12px;font-family:"Bebas Neue";white-space:nowrap}
    .ms .t{font-size:27px;line-height:1;color:#9aa6b8;letter-spacing:.06em}
    .ms .n{font-size:48px;line-height:1;color:#fff}
    .ms .x{font-size:38px;line-height:1;color:#758194}
    /* ---- fight deck ---------------------------------------------------- */
    .faces{position:relative;z-index:2;flex:1;min-width:0;display:flex;flex-direction:column}
    .frow{display:flex;align-items:stretch}
    .face{position:relative;flex:1;min-width:0;display:flex;align-items:center;gap:20px;padding:22px 22px 22px 44px}
    .face::before{content:"";position:absolute;inset:0;
        clip-path:polygon(32px 0,100% 0,calc(100% - 32px) 100%,0 100%);
        background:repeating-linear-gradient(114deg,rgba(255,255,255,.055) 0 3px,rgba(255,255,255,0) 3px 16px),
            linear-gradient(112deg,var(--pb) 8%,var(--pd) 96%);
        box-shadow:inset 0 0 0 2px rgba(255,255,255,.13)}
    .face .plate{width:112px;height:112px;margin:0 0 0 8px}
    .faces .name{position:relative;z-index:2}
    .face .nm{font-size:74px}
    .face .no{font-size:36px}
    .vs{position:relative;z-index:3;align-self:center;flex:none;font-family:"Bebas Neue";font-size:42px;line-height:1;background:#fff;color:${INK};
        padding:15px 19px 11px;clip-path:polygon(16px 0,100% 0,calc(100% - 16px) 100%,0 100%);margin:0 12px;
        box-shadow:0 8px 16px rgba(5,8,14,.45)}
    .fstrip{position:relative;z-index:2;background:rgba(8,11,17,.9);color:#d6dde9;font-weight:600;font-size:29px;
        padding:9px 26px 9px 58px;clip-path:polygon(28px 0,100% 0,calc(100% - 28px) 100%,0 100%);
        display:flex;min-width:0;align-items:center}
    /* ---- full-screen card ---------------------------------------------- */
    .card{position:absolute;inset:0;overflow:hidden;background:#0b0f17;display:flex;flex-direction:column;align-items:center;
        gap:16px;padding:52px 112px 52px;font-weight:600}
    .card::after{content:"";position:absolute;inset:0;pointer-events:none;z-index:1;
        background:repeating-linear-gradient(122deg,rgba(255,255,255,.024) 0 2px,rgba(255,255,255,0) 2px 12px)}
    .wash{position:absolute;inset:0}
    .wash::before{content:"";position:absolute;top:-120px;bottom:-120px;left:-90px;width:520px;transform:skewX(-13deg);
        background:linear-gradient(200deg,var(--wa) 0,rgba(0,0,0,0) 78%);opacity:.4}
    .wash::after{content:"";position:absolute;top:-120px;bottom:-120px;right:-90px;width:520px;transform:skewX(-13deg);
        background:linear-gradient(160deg,var(--wb) 0,rgba(0,0,0,0) 78%);opacity:.4}
    .lg{position:absolute;z-index:5;top:52px;right:96px;width:98px;height:98px;background:#fff;display:flex;align-items:center;
        justify-content:center;box-shadow:0 0 0 3px rgba(12,16,24,.65),0 10px 18px rgba(0,0,0,.45)}
    .lg img{max-width:82%;max-height:82%;object-fit:contain;
        filter:drop-shadow(0 0 1.5px rgba(10,13,19,.8)) drop-shadow(0 0 1px rgba(10,13,19,.5))}
    .crbwrap{position:relative;z-index:2;display:flex;align-items:stretch;filter:drop-shadow(0 14px 22px rgba(4,7,12,.55))}
    /* crb sits inside .rk (defined above) so it shares the ink keyline backing */
    .crb{display:inline-block;background:#fff;color:${INK};padding:16px 62px 13px;
        clip-path:polygon(32px 0,100% 0,calc(100% - 32px) 100%,0 100%)}
    .rh.big{font-family:"Bebas Neue";font-size:84px;line-height:.95;letter-spacing:.1em;white-space:nowrap}
    .rh.big, .crb .rh{display:block}
    .sliver.dark{background:rgba(255,255,255,.42)}
    .chip{position:relative;z-index:2;font-family:"Bebas Neue";font-size:37px;letter-spacing:.09em;color:#eef2f8;
        background:rgba(12,16,24,.92);padding:9px 28px 6px;clip-path:polygon(16px 0,100% 0,calc(100% - 16px) 100%,0 100%)}
    .stage{position:relative;z-index:2;flex:1;width:100%;min-height:0;display:flex;align-items:stretch;justify-content:center;gap:30px}
    .slab{flex:1;min-width:0;max-width:610px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:20px;
        padding:36px 44px;clip-path:polygon(36px 0,100% 0,calc(100% - 36px) 100%,0 100%);
        background:repeating-linear-gradient(114deg,rgba(255,255,255,.05) 0 3px,rgba(255,255,255,0) 3px 16px),
            linear-gradient(155deg,var(--pb) 14%,var(--pd) 100%);
        box-shadow:inset 0 0 0 2px rgba(255,255,255,.13)}
    .plate.big{width:198px;height:198px;margin:0;align-self:center}
    .sname{font-family:"Bebas Neue";font-size:80px;line-height:.94;letter-spacing:.02em;text-align:center;color:var(--pt);max-width:100%}
    .mid{position:relative;z-index:2;flex:none;display:flex;align-items:center;justify-content:center;min-width:0}
    .score{display:flex;align-items:center;gap:24px}
    .cell{display:flex;flex-direction:column;align-items:center}
    .d{font-family:"Bebas Neue";font-size:290px;line-height:.82;color:#fff;text-shadow:0 14px 34px rgba(0,0,0,.55)}
    .bar{width:132px;height:14px;margin-top:14px;background:var(--tc);box-shadow:0 0 22px var(--tc)}
    .lbl{font-family:"Bebas Neue";font-size:41px;line-height:1;letter-spacing:.1em;color:#a9b4c6;margin-top:10px}
    .sep{font-family:"Bebas Neue";font-size:210px;line-height:.9;color:#3c475c;transform:translateY(-44px)}
    .vsmid{font-family:"Bebas Neue";font-size:220px;line-height:1;color:#fff;text-shadow:0 14px 34px rgba(0,0,0,.55)}
    .stats{position:relative;z-index:2;display:flex;flex-direction:column;gap:3px;background:rgba(12,16,24,.86);
        padding:13px 52px 11px;clip-path:polygon(24px 0,100% 0,calc(100% - 24px) 100%,0 100%)}
    .row{display:flex;align-items:baseline;justify-content:center;gap:34px}
    .v{font-family:"Bebas Neue";font-size:42px;line-height:1;color:#fff;min-width:74px}
    .v.a{text-align:right}
    .v.h{text-align:left}
    .k{font-size:26px;letter-spacing:.16em;text-transform:uppercase;color:#9aa7b9}
    .strip{position:relative;z-index:2;display:flex;align-items:center;gap:16px;max-width:100%;background:#fff;color:${INK};
        padding:11px 40px 11px 50px;clip-path:polygon(28px 0,100% 0,calc(100% - 28px) 100%,0 100%)}
    .tx{flex:0 1 auto;white-space:nowrap;font-size:29px;letter-spacing:.01em}
    .strip i{display:inline-block;flex:none;font-style:normal;font-family:"Bebas Neue";font-size:28px;line-height:1;
        padding:7px 15px 5px;background:${INK};color:#fff;clip-path:polygon(10px 0,100% 0,calc(100% - 10px) 100%,0 100%)}
    .strip i.warn{background:#e11d33}
  `;

  return {
    css,
    html: html + `<script>window.__overlayReady=(async()=>{await document.fonts.ready;` +
      `for(const el of document.querySelectorAll(".fit")){el.style.whiteSpace="nowrap";` +
      `let s=parseFloat(getComputedStyle(el).fontSize)||40,t=400;` +
      `while(el.scrollWidth>el.clientWidth+1&&s>22&&t-->0){s-=2;el.style.fontSize=s+"px"}` +
      `}})();</script>`,
  };
}