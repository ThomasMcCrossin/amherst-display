// broadcast-glass — network-sports style (TSN / Sportsnet flavour):
// glassy dark panels, a team-colour wedge + logo plate, crisp condensed type,
// compact score chip. League-neutral: every colour/word comes from the spec.
export const meta = {
  name: "broadcast-glass",
  author: "broadcast graphics",
  description: "Dark glass lower thirds with team wedges, skewed kicker chips, compact score chip; opaque full-screen cards with a giant score hero.",
};

const CARD = new Set(["break", "final", "intro"]);

// vars for a team: primary / secondary / on-primary text colour
function tvars(t) {
  return `--p:${t.primary};--s:${t.secondary || "#ffffff"};--tx:${t.text || "#ffffff"}`;
}
function teamOf(spec, side) {
  return side === "home" ? spec.home : side === "away" ? spec.away : null;
}
function neutral(spec) {
  return { logo: spec.league.logo, primary: spec.league.primary, secondary: spec.league.secondary, text: "#ffffff" };
}

// slanted chip (broadcast tab): outer skew, inner counter-skew keeps type crisp
const chip = (cls, inner) => `<b class="cSk ${cls}"><span>${inner}</span></b>`;

const dot = '<i class="dt"></i>';

function lower(spec, e) {
  const st = teamOf(spec, spec.side) || neutral(spec);
  const tagHtml = spec.tags.map((x) => chip("tag", e(x))).join("");
  const badge = spec.badge ? chip("tag badge", e(spec.badge)) : "";
  const kick = `<div class="kick">${chip("kH", e(spec.headline))}${tagHtml}${badge}</div>`;

  let hero = "";
  if (spec.kind === "fight") {
    const blocks = [spec.subject, ...spec.others].filter(Boolean).map((p) => {
      const pt = teamOf(spec, p.side);
      const style = pt ? tvars(pt) : tvars(neutral(spec));
      const num = p.number ? chip("pnum", `#${e(p.number)}`) : "";
      return `<div class="pl" style="${style}">${pt ? `<div class="plg"><img src="${pt.logo}" alt=""></div>` : ""}${num}<div class="pname">${e(p.name)}</div></div>`;
    });
    hero = `<div class="duel">${blocks.join('<div class="vx">VS</div>')}</div>`;
  } else if (spec.subject) {
    const num = spec.subject.number ? `<b class="cSk num"><span>#${e(spec.subject.number)}</span></b>` : "";
    hero = `<div class="who">${num}<div class="pname">${e(spec.subject.name)}</div></div>`;
  }
  const lines = spec.lines.length
    ? `<div class="lrow">${spec.lines.map((x) => `<span>${e(x)}</span>`).join(dot)}</div>` : "";

  // score chip: away ... home (bug order), clock over it; team colour hairline on top
  let chipHtml = "";
  if ((spec.score || spec.clock) && spec.home && spec.away) {
    const srow = spec.score
      ? `<div class="srow"><img src="${spec.away.logo}" alt=""><b>${e(spec.away.short)}</b><span class="n">${e(spec.score.away)}</span>
         <i class="dv"></i><span class="n">${e(spec.score.home)}</span><b>${e(spec.home.short)}</b><img src="${spec.home.logo}" alt=""></div>`
      : "";
    const ck = spec.clock && spec.clock.text ? `<div class="ck">${e(spec.clock.text)}</div>` : "";
    const bar = `background:linear-gradient(90deg,${e(spec.away.primary)} 0 50%,${e(spec.home.primary)} 50% 100%)`;
    chipHtml = `<div class="chip"><i class="cbar" style="${bar}"></i>${ck}${srow}</div>`;
  } else {
    chipHtml = `<div class="rlg"><img src="${spec.league.logo}" alt=""></div>`;
  }

  const css = `
    body{font-family:"Barlow Semi Condensed";color:#fff}
    .cSk{display:inline-flex;align-items:center;transform:skewX(-10deg);border-radius:4px;
         line-height:1;font-family:"Bebas Neue";margin:0;box-shadow:0 2px 10px rgba(0,0,0,.35)}
    .cSk>span{display:inline-block;transform:skewX(10deg);white-space:nowrap}
    .kH{background:var(--p);color:var(--tx);font-size:28px;letter-spacing:3px;padding:8px 16px}
    .tag{background:rgba(255,255,255,.17);color:#fff;font-size:21px;letter-spacing:2.5px;padding:9px 12px}
    .tag.badge{background:#e0392b;color:#fff}
    .num{background:var(--p);color:var(--tx);font-size:33px;letter-spacing:1px;padding:5px 0}
    .cSk.num>span{padding:0 15px}
    .dt{width:7px;height:7px;background:rgba(255,255,255,.28);transform:rotate(45deg);flex:0 0 7px}

    .lt{position:absolute;left:96px;bottom:58px;min-height:172px;width:fit-content;max-width:1728px;display:flex;align-items:stretch;
        background:linear-gradient(100deg,rgba(13,18,28,.98) 0%,rgba(20,26,39,.98) 60%,rgba(24,31,45,.98) 100%);
        border:1px solid rgba(255,255,255,.14);border-radius:12px;overflow:hidden;
        box-shadow:0 26px 60px rgba(0,0,0,.55)}
    .lt::before{content:"";position:absolute;left:0;right:0;top:0;height:46%;
                background:linear-gradient(180deg,rgba(255,255,255,.08),rgba(255,255,255,0));pointer-events:none}
    .lt::after{content:"";position:absolute;left:44%;top:-80px;width:300px;height:340px;transform:rotate(22deg);
               background:linear-gradient(180deg,rgba(255,255,255,.055),rgba(255,255,255,0));pointer-events:none}
    .bbar{position:absolute;left:0;right:0;bottom:0;height:6px;
          background:linear-gradient(90deg,var(--p) 0 168px,var(--s) 168px 182px)}
    .wedge{position:relative;flex:0 0 168px;width:168px;background:var(--p);
           clip-path:polygon(0 0,100% 0,calc(100% - 40px) 100%,0 100%)}
    .wedge::after{content:"";position:absolute;inset:0;
                  background:linear-gradient(165deg,rgba(255,255,255,.22),rgba(255,255,255,0) 42%,rgba(0,0,0,.22))}
    .wst{flex:0 0 14px;width:14px;background:var(--s);
         clip-path:polygon(0 0,100% 0,calc(100% - 40px) 100%,0 100%)}
    .lp{position:absolute;top:50%;left:24px;transform:translateY(-50%);width:112px;height:112px;
        background:linear-gradient(180deg,rgba(15,20,30,.94),rgba(8,11,17,.94));
        border:1px solid rgba(255,255,255,.14);border-radius:10px;display:flex;align-items:center;justify-content:center;
        box-shadow:0 8px 22px rgba(0,0,0,.45)}
    .lp img{width:86px;height:86px;object-fit:contain;filter:drop-shadow(0 2px 6px rgba(0,0,0,.5)) drop-shadow(0 0 3px rgba(255,255,255,.12))}

    .body{flex:1;min-width:0;display:flex;flex-direction:column;justify-content:center;gap:12px;padding:18px 28px 22px 34px}
    .kick{display:flex;align-items:center;gap:0;flex-wrap:wrap}
    .kick .tag{margin-left:10px}
    .who{display:flex;align-items:center;gap:16px}
    .pname{font-family:"Bebas Neue";font-size:68px;line-height:.96;letter-spacing:.5px;
           text-shadow:0 3px 14px rgba(0,0,0,.5);overflow-wrap:anywhere}
    .lrow{display:flex;align-items:center;gap:14px;color:#b8c3d6;font-size:27px;line-height:1.22;flex-wrap:wrap}
    .duel{display:flex;align-items:center;gap:24px}
    .duel .pl{flex:1;min-width:0;display:flex;align-items:center;gap:14px;
              border-bottom:4px solid var(--p);padding-bottom:6px;align-self:stretch;align-items:center}
    .duel .pname{font-size:47px;line-height:.95}
    .pnum{background:var(--p);color:var(--tx);font-size:28px;padding:6px 0}
    .plg{width:62px;height:62px;flex:0 0 62px;background:linear-gradient(180deg,rgba(15,20,30,.94),rgba(8,11,17,.94));
         border:1px solid rgba(255,255,255,.15);border-radius:8px;display:flex;align-items:center;justify-content:center;
         box-shadow:0 5px 16px rgba(0,0,0,.4)}
    .plg img{width:46px;height:46px;object-fit:contain;filter:drop-shadow(0 2px 5px rgba(0,0,0,.5))}
    .vx{flex:0 0 auto;font-family:"Bebas Neue";font-size:31px;color:#8ba1c2;letter-spacing:2px}

    .chip{position:relative;flex:0 0 auto;display:flex;flex-direction:column;align-items:center;justify-content:center;
          gap:9px;min-width:252px;padding:12px 26px;background:rgba(5,9,16,.62);
          border-left:1px solid rgba(255,255,255,.1)}
    .cbar{position:absolute;top:0;left:0;right:0;height:4px}
    .ck{font-family:"Bebas Neue";font-size:23px;letter-spacing:3px;color:#93a5c0;white-space:nowrap}
    .srow{display:flex;align-items:center;gap:11px;font-family:"Bebas Neue"}
    .srow img{width:56px;height:56px;object-fit:contain;padding:5px;background:rgba(255,255,255,.1);border-radius:7px;
              box-shadow:inset 0 0 0 1px rgba(255,255,255,.12);filter:drop-shadow(0 2px 5px rgba(0,0,0,.45))}
    .srow b{font-family:"Barlow Semi Condensed";font-weight:600;font-size:23px;letter-spacing:1px;color:#9fb0c9}
    .srow .n{font-size:58px;line-height:1;min-width:54px;text-align:center}
    .dv{width:3px;height:46px;background:rgba(255,255,255,.18);border-radius:2px;margin:0 3px}
    .rlg{flex:0 0 150px;display:flex;align-items:center;justify-content:center;
         background:rgba(5,9,16,.55);border-left:1px solid rgba(255,255,255,.1)}
    .rlg img{width:88px;height:88px;object-fit:contain;filter:drop-shadow(0 2px 8px rgba(0,0,0,.5))}`;
  const html = `<div class="lt" style="${tvars(st)}">
      <div class="wedge"><div class="lp"><img src="${st.logo}" alt=""></div></div>
      <div class="wst"></div>
      <div class="body">${kick}${hero}${lines}</div>
      ${chipHtml}
      <i class="bbar"></i>
    </div>`;
  return { css, html };
}

function card(spec, e) {
  const H = spec.home || neutral(spec);
  const A = spec.away || neutral(spec);
  const sub = spec.clock && spec.clock.text &&
    spec.clock.text.trim().toUpperCase() !== String(spec.headline).trim().toUpperCase()
      ? `<div class="csub">${e(spec.clock.text)}</div>` : "";
  const hero = spec.score
    ? `<div class="hero"><span>${e(spec.score.away)}</span><i class="hdash"></i><span>${e(spec.score.home)}</span></div>`
    : `<div class="hero"><i class="sl"></i><span class="vst">VS</span><i class="sl r"></i></div>`;
  const stats = spec.stats.length
    ? `<div class="stats">${spec.stats.map((s) =>
        `<div class="stat"><b>${e(s.away)}</b><span>${e(s.label)}</span><b>${e(s.home)}</b></div>`).join("")}</div>` : "";
  const foot = [spec.context, ...spec.lines].filter(Boolean);
  const footRow = foot.length
    ? `<div class="cfoot">${foot.map((x) => `<span>${e(x)}</span>`).join(dot)}</div>` : "";

  const css = `
    body{font-family:"Barlow Semi Condensed";color:#fff}
    .card{position:absolute;inset:0;overflow:hidden;
          background:radial-gradient(1150px 740px at 50% 34%,#1c2637 0%,#111826 52%,#0a0e16 100%)}
    .card::before{content:"";position:absolute;inset:0;
                  background:repeating-linear-gradient(115deg,rgba(255,255,255,.024) 0 2px,rgba(255,255,255,0) 2px 110px)}
    .gl{position:absolute;inset:0;pointer-events:none}
    .btop,.bbot{position:absolute;left:0;right:0;height:8px}
    .btop{top:0;background:linear-gradient(90deg,var(--ap) 0 42%,#c8d2e2 42% 46%,var(--hp) 46% 100%)}
    .bbot{bottom:0;background:linear-gradient(90deg,var(--ap) 0 54%,#c8d2e2 54% 58%,var(--hp) 58% 100%)}
    .clg{position:absolute;top:64px;left:50%;transform:translateX(-50%);width:96px;height:96px;object-fit:contain;
         filter:drop-shadow(0 6px 16px rgba(0,0,0,.55))}
    .cwrap{position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;
           gap:30px;padding:120px 96px 80px}
    .chead{display:flex;align-items:center;gap:26px;font-family:"Bebas Neue";font-size:86px;line-height:1;
           letter-spacing:11px;text-shadow:0 4px 24px rgba(0,0,0,.55);margin-top:-8px}
    .sl{width:18px;height:66px;background:var(--ap);transform:skewX(-16deg);flex:0 0 18px;
        box-shadow:0 4px 14px rgba(0,0,0,.4)}
    .sl.r{background:var(--hp)}
    .csub{font-family:"Bebas Neue";font-size:34px;letter-spacing:5px;color:#9db0d2;margin-top:-18px}
    .mid{display:flex;align-items:center;gap:74px}
    .tpl{width:380px;display:flex;flex-direction:column;align-items:center;gap:20px}
    .tlp{position:relative;width:238px;height:238px;background:var(--p);
         clip-path:polygon(0 0,100% 0,calc(100% - 46px) 100%,0 100%);
         display:flex;align-items:center;justify-content:center;padding-right:16px;
         box-shadow:0 18px 46px rgba(0,0,0,.45)}
    .tlp::after{content:"";position:absolute;inset:0;
                background:linear-gradient(165deg,rgba(255,255,255,.22),rgba(255,255,255,0) 42%,rgba(0,0,0,.24))}
    .tin{width:192px;height:192px;background:linear-gradient(180deg,rgba(16,21,32,.96),rgba(8,11,18,.96));
         border:1px solid rgba(255,255,255,.16);border-radius:16px;display:flex;align-items:center;justify-content:center;
         box-shadow:0 10px 26px rgba(0,0,0,.5)}
    .tin img{width:150px;height:150px;object-fit:contain;filter:drop-shadow(0 3px 8px rgba(0,0,0,.55)) drop-shadow(0 0 4px rgba(255,255,255,.12))}
    .tnm{display:flex;align-items:center;justify-content:center;min-height:100px;width:100%}
    .tns{display:block;text-align:center;font-family:"Bebas Neue";line-height:1.05;letter-spacing:1px;max-width:372px}
    .tk{white-space:nowrap;overflow-wrap:anywhere}
    .hero{display:flex;align-items:center;gap:44px;font-family:"Bebas Neue";font-size:252px;line-height:.88;color:#fff;
          text-shadow:0 10px 46px rgba(0,0,0,.6)}
    .vst{font-size:150px;letter-spacing:8px}
    .hdash{width:30px;height:158px;background:linear-gradient(180deg,var(--ap),var(--hp));
           transform:skewX(-14deg);border-radius:4px;box-shadow:0 8px 24px rgba(0,0,0,.4)}
    .stats{display:flex;gap:20px;flex-wrap:wrap;justify-content:center}
    .stat{display:flex;align-items:center;gap:20px;background:rgba(255,255,255,.055);
          border:1px solid rgba(255,255,255,.1);border-radius:8px;padding:10px 26px}
    .stat b{font-family:"Bebas Neue";font-weight:400;font-size:40px;line-height:1;min-width:64px;text-align:center}
    .stat span{font-weight:600;font-size:22px;letter-spacing:2.5px;color:#93a7c6;text-transform:uppercase}
    .cfoot{display:flex;align-items:center;gap:14px;justify-content:center;flex-wrap:wrap;
           font-size:28px;color:#aeb9cd;max-width:1520px;text-align:center}
    .dt{width:7px;height:7px;background:rgba(255,255,255,.25);transform:rotate(45deg);margin:0 4px}`;
  const html = `<div class="card" style="--ap:${e(A.primary)};--hp:${e(H.primary)}">
      <i class="gl" style="background:radial-gradient(600px 420px at 140px 120px,${e(A.primary)}42,rgba(0,0,0,0) 72%),radial-gradient(600px 460px at 1780px 960px,${e(H.primary)}3e,rgba(0,0,0,0) 72%)"></i>
      <i class="btop"></i><i class="bbot"></i>
      <img class="clg" src="${spec.league.logo}" alt="">
      <div class="cwrap">
        <div class="chead"><i class="sl"></i><span>${e(spec.headline)}</span><i class="sl r"></i></div>
        ${sub}
        <div class="mid">${plate(A, e)}${hero}${plate(H, e)}</div>
        ${stats}
        ${footRow}
      </div>
    </div>`;
  return { css, html };
}

function plate(t, e) {
  // Space-separated tokens never break internally (unless a single token alone
  // can't fit the box), so long city names wrap at word boundaries. The size
  // steps down just enough for the longest token to sit on one line.
  const tokenMax = Math.max(...String(t.name).split(/\s+/).map((w) => w.length), 1);
  const size = Math.min(46, Math.max(30, Math.round(360 / (0.43 * tokenMax))));
  const toks = String(t.name).split(/\s+/).map((w) => `<span class="tk">${e(w)}</span>`).join(" ");
  return `<div class="tpl" style="--p:${e(t.primary)}">
      <div class="tlp"><div class="tin"><img src="${t.logo}" alt=""></div></div>
      <div class="tnm"><span class="tns" style="font-size:${size}px">${toks}</span></div>
    </div>`;
}

export function render(spec, ctx) {
  const e = ctx.esc;
  return CARD.has(spec.kind) ? card(spec, e) : lower(spec, e); // unknown kinds fall back to the lower-third/moment layout
}