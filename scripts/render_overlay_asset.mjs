#!/usr/bin/env node

import fs from "fs/promises";
import path from "path";
import { pathToFileURL } from "url";
import { chromium } from "playwright";

const REPO_ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const part = argv[i];
    if (!part.startsWith("--")) {
      continue;
    }
    const key = part.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) {
      args[key] = true;
      continue;
    }
    args[key] = next;
    i += 1;
  }
  return args;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function safeText(value, fallback = "") {
  const text = String(value ?? "").trim();
  return escapeHtml(text || fallback);
}

async function fileToDataUri(filePath) {
  if (!filePath) {
    return "";
  }
  const absolute = path.resolve(String(filePath));
  const ext = path.extname(absolute).toLowerCase();
  const mimeByExt = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".svg": "image/svg+xml",
    ".ttf": "font/ttf",
    ".otf": "font/otf",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
  };
  const mime = mimeByExt[ext] || "application/octet-stream";
  const raw = await fs.readFile(absolute);
  return `data:${mime};base64,${raw.toString("base64")}`;
}

function cssColor(value, fallback) {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function teamShortLabel(value, fallback) {
  const text = String(value ?? "").trim();
  return escapeHtml(text || fallback);
}

function buildDocument({ spec, bebasUri, barlowRegularUri, barlowSemiUri }) {
  const kind = String(spec.type || "").trim();
  const transparent = Boolean(spec.transparent);
  const theme = {
    accent: cssColor(spec.accentPrimary, "#19c37d"),
    accentSoft: cssColor(spec.accentSecondary, "#114a39"),
    home: cssColor(spec.homePrimary, "#19c37d"),
    away: cssColor(spec.awayPrimary, "#c33e4e"),
  };

  const commonCss = `
    @font-face {
      font-family: "Overlay Bebas";
      src: url("${bebasUri}") format("truetype");
      font-display: swap;
    }
    @font-face {
      font-family: "Overlay Barlow";
      src: url("${barlowRegularUri}") format("truetype");
      font-display: swap;
      font-weight: 400;
    }
    @font-face {
      font-family: "Overlay Barlow";
      src: url("${barlowSemiUri}") format("truetype");
      font-display: swap;
      font-weight: 600;
    }
    :root {
      --accent: ${theme.accent};
      --accent-soft: ${theme.accentSoft};
      --home: ${theme.home};
      --away: ${theme.away};
      --ink: #eef3f7;
      --muted: rgba(226, 234, 242, 0.78);
      --panel: rgba(5, 13, 20, 0.82);
      --panel-strong: rgba(4, 10, 16, 0.94);
      --line: rgba(255, 255, 255, 0.12);
      --shadow: rgba(0, 0, 0, 0.46);
    }
    * {
      box-sizing: border-box;
    }
    html, body {
      margin: 0;
      width: 100%;
      height: 100%;
      overflow: hidden;
      background: ${transparent ? "transparent" : "#071018"};
      color: var(--ink);
      font-family: "Overlay Barlow", sans-serif;
    }
    body {
      position: relative;
    }
    .fit-single {
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .grain::after {
      content: "";
      position: absolute;
      inset: 0;
      pointer-events: none;
      opacity: 0.12;
      background-image:
        repeating-linear-gradient(
          0deg,
          rgba(255, 255, 255, 0.04) 0px,
          rgba(255, 255, 255, 0.04) 1px,
          transparent 1px,
          transparent 3px
        );
      mix-blend-mode: soft-light;
    }
    .logo {
      object-fit: contain;
      display: block;
    }
    .screen {
      position: relative;
      width: 100%;
      height: 100%;
      overflow: hidden;
    }
  `;

  if (kind === "goal_overlay" || kind === "penalty_overlay") {
    const logoData = spec.primaryLogo ? `<img class="logo primary-logo" src="${spec.primaryLogo}" alt="">` : "";
    const badge = spec.badgeText
      ? `<div class="badge-pill">${safeText(spec.badgeText)}</div>`
      : "";
    const leftMeta = [
      safeText(spec.secondaryText),
      safeText(spec.metaText),
    ]
      .filter(Boolean)
      .join('<span class="bullet">•</span>');
    const awayActive = spec.scoringSide === "away" ? " active" : "";
    const homeActive = spec.scoringSide === "home" ? " active" : "";
    return `
      <!doctype html>
      <html>
        <head>
          <meta charset="utf-8">
          <style>
            ${commonCss}
            body {
              padding: 0;
            }
            .overlay-shell {
              position: relative;
              width: 100%;
              height: 100%;
            }
            .overlay-card {
              position: relative;
              display: grid;
              grid-template-columns: minmax(0, 1fr) 250px;
              width: 100%;
              height: 100%;
              overflow: hidden;
              border-radius: 34px;
              border: 1px solid var(--line);
              background:
                radial-gradient(circle at 18% 22%, color-mix(in srgb, var(--accent) 28%, transparent) 0%, transparent 42%),
                linear-gradient(135deg, rgba(255, 255, 255, 0.07), rgba(255, 255, 255, 0.01)),
                var(--panel);
              box-shadow:
                0 30px 90px var(--shadow),
                inset 0 1px 0 rgba(255, 255, 255, 0.06);
            }
            .overlay-card::before {
              content: "";
              position: absolute;
              inset: 0;
              background:
                linear-gradient(90deg, color-mix(in srgb, var(--accent) 80%, transparent) 0px, color-mix(in srgb, var(--accent) 35%, transparent) 14px, transparent 14px),
                linear-gradient(180deg, rgba(255, 255, 255, 0.05), transparent 55%);
              pointer-events: none;
            }
            .content {
              position: relative;
              padding: 18px 26px 18px 34px;
              display: flex;
              flex-direction: column;
              justify-content: center;
              gap: 10px;
              min-width: 0;
            }
            .kicker-row {
              display: flex;
              align-items: center;
              gap: 12px;
              min-width: 0;
            }
            .kicker {
              font-size: 22px;
              font-weight: 600;
              letter-spacing: 0.24em;
              text-transform: uppercase;
              color: color-mix(in srgb, var(--accent) 78%, white);
            }
            .hero-row {
              display: flex;
              align-items: center;
              gap: 16px;
              min-width: 0;
            }
            .primary-logo {
              width: 78px;
              height: 78px;
              flex: 0 0 auto;
              filter: drop-shadow(0 8px 18px rgba(0, 0, 0, 0.35));
            }
            .hero-copy {
              flex: 1 1 auto;
              min-width: 0;
            }
            .hero {
              font-family: "Overlay Bebas", sans-serif;
              font-size: 86px;
              letter-spacing: 0.03em;
              line-height: 0.95;
              text-transform: uppercase;
              color: #fbfdff;
              text-shadow: 0 8px 18px rgba(0, 0, 0, 0.34);
            }
            .subline {
              display: flex;
              align-items: center;
              gap: 12px;
              min-width: 0;
              font-size: 26px;
              line-height: 1.1;
              color: var(--muted);
            }
            .subline .bullet {
              color: color-mix(in srgb, var(--accent) 78%, white);
              flex: 0 0 auto;
            }
            .score-panel {
              position: relative;
              padding: 18px 18px 18px 12px;
              border-left: 1px solid rgba(255, 255, 255, 0.08);
              background:
                linear-gradient(180deg, rgba(255, 255, 255, 0.08), rgba(255, 255, 255, 0.01)),
                rgba(4, 10, 16, 0.48);
            }
            .score-label {
              margin-bottom: 12px;
              font-size: 14px;
              font-weight: 600;
              letter-spacing: 0.24em;
              text-transform: uppercase;
              color: rgba(240, 245, 249, 0.62);
            }
            .score-rows {
              display: flex;
              flex-direction: column;
              gap: 10px;
            }
            .score-row {
              display: grid;
              grid-template-columns: 30px 1fr auto;
              align-items: center;
              gap: 10px;
              padding: 12px 14px;
              border-radius: 18px;
              border: 1px solid rgba(255, 255, 255, 0.06);
              background: rgba(255, 255, 255, 0.03);
            }
            .score-row.active {
              background: linear-gradient(90deg, color-mix(in srgb, var(--accent) 24%, rgba(255,255,255,0.05)), rgba(255,255,255,0.04));
              border-color: color-mix(in srgb, var(--accent) 45%, rgba(255,255,255,0.12));
              box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.04);
            }
            .score-logo {
              width: 28px;
              height: 28px;
            }
            .score-team {
              font-size: 18px;
              font-weight: 600;
              letter-spacing: 0.16em;
              text-transform: uppercase;
              color: rgba(244, 247, 250, 0.82);
            }
            .score-value {
              font-family: "Overlay Bebas", sans-serif;
              font-size: 58px;
              line-height: 0.88;
              color: #fbfdff;
            }
            .badge-pill {
              padding: 7px 12px 6px;
              border-radius: 999px;
              border: 1px solid rgba(255, 255, 255, 0.1);
              background: color-mix(in srgb, var(--accent) 28%, rgba(255,255,255,0.05));
              color: #f6fbff;
              font-size: 16px;
              font-weight: 600;
              letter-spacing: 0.18em;
              text-transform: uppercase;
              flex: 0 0 auto;
            }
          </style>
        </head>
        <body>
          <div class="screen overlay-shell">
            <section class="overlay-card grain">
              <div class="content">
                <div class="kicker-row">
                  <div class="kicker fit-single" data-fit data-min="12">${safeText(spec.kicker)}</div>
                  ${badge}
                </div>
                <div class="hero-row">
                  ${logoData}
                  <div class="hero-copy">
                    <div class="hero fit-single" data-fit data-min="28">${safeText(spec.hero)}</div>
                    <div class="subline fit-single" data-fit data-min="14">${leftMeta}</div>
                  </div>
                </div>
              </div>
              <aside class="score-panel">
                <div class="score-label">${safeText(spec.scoreLabel || "Series Score")}</div>
                <div class="score-rows">
                  <div class="score-row${awayActive}">
                    <img class="logo score-logo" src="${spec.awayLogo}" alt="">
                    <div class="score-team fit-single" data-fit data-min="10">${teamShortLabel(spec.awayShortLabel, "AWY")}</div>
                    <div class="score-value">${safeText(spec.awayScore, "0")}</div>
                  </div>
                  <div class="score-row${homeActive}">
                    <img class="logo score-logo" src="${spec.homeLogo}" alt="">
                    <div class="score-team fit-single" data-fit data-min="10">${teamShortLabel(spec.homeShortLabel, "HOM")}</div>
                    <div class="score-value">${safeText(spec.homeScore, "0")}</div>
                  </div>
                </div>
              </aside>
            </section>
          </div>
          <script>
            ${fitScript()}
          </script>
        </body>
      </html>
    `;
  }

  if (kind === "series_open") {
    return fullCardDocument({
      commonCss,
      kicker: spec.kicker,
      headline: spec.headline,
      subheadline: spec.subheadline,
      eyebrow: spec.eyebrow,
      footer: spec.footer,
      badge: spec.badgeText,
      homeLogo: spec.homeLogo,
      awayLogo: spec.awayLogo,
      homeName: spec.homeName,
      awayName: spec.awayName,
      centerBadge: spec.centerBadge,
      theme,
      mode: "open",
    });
  }

  if (kind === "game_break") {
    return gameBreakDocument({ commonCss, spec, theme });
  }

  if (kind === "series_outro") {
    return fullCardDocument({
      commonCss,
      kicker: spec.kicker,
      headline: spec.headline,
      subheadline: spec.subheadline,
      eyebrow: spec.eyebrow,
      footer: [safeText(spec.venue), safeText(spec.location)].filter(Boolean).join(" • "),
      badge: spec.badgeText,
      homeLogo: spec.homeLogo,
      awayLogo: spec.awayLogo,
      homeName: spec.homeName,
      awayName: spec.awayName,
      centerBadge: spec.centerBadge,
      theme,
      mode: "outro",
      detailLine: safeText(spec.datetimeLabel),
    });
  }

  throw new Error(`Unsupported overlay asset type: ${kind}`);
}

function fitScript() {
  return `
    const fitElements = () => {
      for (const el of document.querySelectorAll('[data-fit]')) {
        const min = Number(el.dataset.min || 12);
        let size = parseFloat(getComputedStyle(el).fontSize);
        while ((el.scrollWidth > el.clientWidth || el.scrollHeight > el.clientHeight) && size > min) {
          size -= 1;
          el.style.fontSize = size + 'px';
        }
      }
    };
    const waitForImages = async () => {
      await Promise.all(Array.from(document.images).map((img) => {
        if (img.complete) {
          return Promise.resolve();
        }
        return new Promise((resolve) => {
          img.addEventListener('load', resolve, { once: true });
          img.addEventListener('error', resolve, { once: true });
        });
      }));
    };
    window.__overlayReady = (async () => {
      if (document.fonts?.ready) {
        await document.fonts.ready;
      }
      await waitForImages();
      fitElements();
      await new Promise((resolve) => requestAnimationFrame(() => resolve()));
    })();
  `;
}

function fullCardDocument({ commonCss, kicker, headline, subheadline, eyebrow, footer, badge, homeLogo, awayLogo, homeName, awayName, centerBadge, theme, mode, detailLine = "" }) {
  const ring = mode === "outro" ? "rgba(255,255,255,0.16)" : "rgba(255,255,255,0.12)";
  const badgeMarkup = badge ? `<div class="status-pill">${safeText(badge)}</div>` : "";
  const detailMarkup = detailLine ? `<div class="detail-line fit-single" data-fit data-min="12">${safeText(detailLine)}</div>` : "";
  return `
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8">
        <style>
          ${commonCss}
          body {
            background:
              radial-gradient(circle at 14% 18%, color-mix(in srgb, var(--home) 42%, transparent) 0%, transparent 42%),
              radial-gradient(circle at 86% 20%, color-mix(in srgb, var(--away) 40%, transparent) 0%, transparent 38%),
              radial-gradient(circle at 72% 84%, color-mix(in srgb, var(--accent) 20%, transparent) 0%, transparent 32%),
              linear-gradient(135deg, #061018 0%, #0a1520 42%, #050d16 100%);
          }
          .frame {
            position: absolute;
            inset: 48px;
            border-radius: 42px;
            border: 1px solid ${ring};
            background:
              linear-gradient(180deg, rgba(255, 255, 255, 0.05), rgba(255, 255, 255, 0.02)),
              rgba(4, 10, 16, 0.38);
            box-shadow:
              0 30px 90px rgba(0, 0, 0, 0.36),
              inset 0 1px 0 rgba(255, 255, 255, 0.05);
            overflow: hidden;
          }
          .frame::before {
            content: "";
            position: absolute;
            inset: 0;
            background:
              linear-gradient(120deg, rgba(255,255,255,0.06) 0%, transparent 22%, transparent 72%, rgba(255,255,255,0.04) 100%),
              linear-gradient(90deg, color-mix(in srgb, var(--home) 75%, transparent) 0px, transparent 14px),
              linear-gradient(270deg, color-mix(in srgb, var(--away) 72%, transparent) 0px, transparent 14px);
            pointer-events: none;
          }
          .content {
            position: absolute;
            inset: 88px 92px 96px;
            display: grid;
            grid-template-columns: minmax(0, 1.1fr) 520px;
            gap: 48px;
          }
          .copy {
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            min-width: 0;
          }
          .top-copy {
            display: flex;
            flex-direction: column;
            gap: 18px;
            min-width: 0;
          }
          .eyebrow {
            font-size: 23px;
            font-weight: 600;
            letter-spacing: 0.28em;
            text-transform: uppercase;
            color: rgba(231, 238, 244, 0.74);
          }
          .headline {
            font-family: "Overlay Bebas", sans-serif;
            font-size: 180px;
            line-height: 0.88;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            color: #fbfdff;
            text-shadow: 0 14px 32px rgba(0, 0, 0, 0.35);
          }
          .subheadline {
            font-size: 40px;
            font-weight: 600;
            line-height: 1.05;
            color: rgba(239, 244, 248, 0.88);
          }
          .detail-line {
            font-size: 24px;
            font-weight: 400;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: rgba(228, 234, 240, 0.68);
          }
          .footer {
            font-size: 22px;
            line-height: 1.3;
            color: rgba(231, 238, 244, 0.74);
          }
          .status-pill {
            display: inline-flex;
            align-self: flex-start;
            padding: 10px 16px 9px;
            border-radius: 999px;
            background: color-mix(in srgb, var(--accent) 28%, rgba(255,255,255,0.05));
            border: 1px solid rgba(255,255,255,0.1);
            font-size: 17px;
            font-weight: 600;
            letter-spacing: 0.18em;
            text-transform: uppercase;
            color: #fbfdff;
          }
          .matchup-panel {
            position: relative;
            display: grid;
            grid-template-rows: 1fr auto 1fr;
            align-items: center;
            justify-items: center;
            padding: 18px 24px;
            border-radius: 34px;
            border: 1px solid rgba(255,255,255,0.1);
            background:
              linear-gradient(180deg, rgba(255, 255, 255, 0.06), rgba(255, 255, 255, 0.02)),
              rgba(6, 12, 18, 0.52);
          }
          .team-lockup {
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 18px;
            min-width: 0;
          }
          .team-logo {
            width: 188px;
            height: 188px;
            filter: drop-shadow(0 12px 28px rgba(0, 0, 0, 0.32));
          }
          .team-name {
            max-width: 100%;
            text-align: center;
            font-size: 30px;
            font-weight: 600;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: rgba(248, 251, 253, 0.9);
          }
          .center-badge {
            padding: 18px 24px 14px;
            border-radius: 999px;
            border: 1px solid rgba(255,255,255,0.12);
            background:
              linear-gradient(90deg, color-mix(in srgb, var(--home) 18%, rgba(255,255,255,0.06)), color-mix(in srgb, var(--away) 18%, rgba(255,255,255,0.04)));
            font-family: "Overlay Bebas", sans-serif;
            font-size: 44px;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            color: #fbfdff;
          }
        </style>
      </head>
      <body>
        <div class="screen grain">
          <section class="frame">
            <div class="content">
              <div class="copy">
                <div class="top-copy">
                  <div class="eyebrow fit-single" data-fit data-min="12">${safeText(eyebrow)}</div>
                  ${badgeMarkup}
                  <div class="headline fit-single" data-fit data-min="46">${safeText(headline)}</div>
                  <div class="subheadline" data-fit data-min="18">${safeText(subheadline)}</div>
                  ${detailMarkup}
                </div>
                <div class="footer" data-fit data-min="14">${safeText(footer)}</div>
              </div>
              <div class="matchup-panel">
                <div class="team-lockup">
                  <img class="logo team-logo" src="${awayLogo}" alt="">
                  <div class="team-name" data-fit data-min="12">${safeText(awayName)}</div>
                </div>
                <div class="center-badge">${safeText(centerBadge)}</div>
                <div class="team-lockup">
                  <img class="logo team-logo" src="${homeLogo}" alt="">
                  <div class="team-name" data-fit data-min="12">${safeText(homeName)}</div>
                </div>
              </div>
            </div>
          </section>
          <script>
            ${fitScript()}
          </script>
        </div>
      </body>
    </html>
  `;
}

function gameBreakDocument({ commonCss, spec }) {
  const venueLine = [safeText(spec.venue), safeText(spec.attendanceText)].filter(Boolean).join(" • ");
  const nextLabel = safeText(spec.nextGameLabel);
  return `
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8">
        <style>
          ${commonCss}
          body {
            background:
              radial-gradient(circle at 18% 16%, color-mix(in srgb, var(--home) 34%, transparent) 0%, transparent 42%),
              radial-gradient(circle at 84% 18%, color-mix(in srgb, var(--away) 34%, transparent) 0%, transparent 38%),
              linear-gradient(135deg, #07101a 0%, #0d1823 44%, #07111b 100%);
          }
          .frame {
            position: absolute;
            inset: 54px;
            border-radius: 40px;
            border: 1px solid rgba(255,255,255,0.12);
            background:
              linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.02)),
              rgba(4, 10, 16, 0.42);
            overflow: hidden;
          }
          .frame::before {
            content: "";
            position: absolute;
            inset: 0;
            background:
              linear-gradient(90deg, color-mix(in srgb, var(--accent) 76%, transparent) 0px, transparent 14px),
              linear-gradient(180deg, rgba(255,255,255,0.06), transparent 26%);
          }
          .content {
            position: absolute;
            inset: 86px 92px 88px;
            display: grid;
            grid-template-columns: minmax(0, 1fr) 500px;
            gap: 50px;
          }
          .meta {
            display: flex;
            flex-direction: column;
            min-width: 0;
          }
          .kicker {
            font-size: 22px;
            font-weight: 600;
            letter-spacing: 0.28em;
            text-transform: uppercase;
            color: rgba(230, 237, 243, 0.72);
          }
          .headline {
            margin-top: 18px;
            font-family: "Overlay Bebas", sans-serif;
            font-size: 116px;
            line-height: 0.88;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            color: #fbfdff;
          }
          .series-status {
            margin-top: 18px;
            font-size: 36px;
            font-weight: 600;
            line-height: 1.05;
            color: rgba(239, 244, 248, 0.88);
          }
          .venue {
            margin-top: 22px;
            font-size: 22px;
            color: rgba(226, 234, 242, 0.72);
          }
          .up-next {
            margin-top: auto;
            display: inline-flex;
            align-self: flex-start;
            padding: 12px 18px 10px;
            border-radius: 999px;
            border: 1px solid rgba(255,255,255,0.1);
            background: rgba(255,255,255,0.04);
            font-size: 18px;
            font-weight: 600;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            color: #fbfdff;
          }
          .score-panel {
            display: grid;
            align-content: center;
            gap: 18px;
            padding: 26px;
            border-radius: 34px;
            border: 1px solid rgba(255,255,255,0.1);
            background:
              linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02)),
              rgba(6, 12, 18, 0.5);
          }
          .score-row {
            display: grid;
            grid-template-columns: 72px 1fr auto;
            gap: 18px;
            align-items: center;
            padding: 16px 18px;
            border-radius: 24px;
            background: rgba(255,255,255,0.035);
          }
          .score-row.winner {
            background: linear-gradient(90deg, color-mix(in srgb, var(--accent) 28%, rgba(255,255,255,0.04)), rgba(255,255,255,0.04));
            border: 1px solid color-mix(in srgb, var(--accent) 38%, rgba(255,255,255,0.1));
          }
          .team-logo {
            width: 64px;
            height: 64px;
          }
          .team-name {
            font-size: 28px;
            font-weight: 600;
            line-height: 1.05;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            color: rgba(248, 251, 253, 0.92);
          }
          .score {
            font-family: "Overlay Bebas", sans-serif;
            font-size: 90px;
            line-height: 0.86;
            letter-spacing: 0.04em;
            color: #fbfdff;
          }
          .result-pill {
            justify-self: center;
            padding: 16px 22px 12px;
            border-radius: 999px;
            border: 1px solid rgba(255,255,255,0.12);
            background:
              linear-gradient(90deg, color-mix(in srgb, var(--home) 16%, rgba(255,255,255,0.06)), color-mix(in srgb, var(--away) 16%, rgba(255,255,255,0.04)));
            font-family: "Overlay Bebas", sans-serif;
            font-size: 42px;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            color: #fbfdff;
          }
        </style>
      </head>
      <body>
        <div class="screen grain">
          <section class="frame">
            <div class="content">
              <div class="meta">
                <div class="kicker fit-single" data-fit data-min="12">${safeText(spec.kicker)}</div>
                <div class="headline fit-single" data-fit data-min="44">${safeText(spec.headline)}</div>
                <div class="series-status" data-fit data-min="18">${safeText(spec.seriesStatus)}</div>
                <div class="venue" data-fit data-min="14">${venueLine}</div>
                ${nextLabel ? `<div class="up-next">${nextLabel}</div>` : ""}
              </div>
              <div class="score-panel">
                <div class="score-row ${safeText(spec.awayResultClass)}">
                  <img class="logo team-logo" src="${spec.awayLogo}" alt="">
                  <div class="team-name" data-fit data-min="12">${safeText(spec.awayName)}</div>
                  <div class="score">${safeText(spec.awayScore, "0")}</div>
                </div>
                <div class="result-pill">${safeText(spec.centerBadge || "Final")}</div>
                <div class="score-row ${safeText(spec.homeResultClass)}">
                  <img class="logo team-logo" src="${spec.homeLogo}" alt="">
                  <div class="team-name" data-fit data-min="12">${safeText(spec.homeName)}</div>
                  <div class="score">${safeText(spec.homeScore, "0")}</div>
                </div>
              </div>
            </div>
          </section>
          <script>
            ${fitScript()}
          </script>
        </div>
      </body>
    </html>
  `;
}

async function hydrateJob(job, fontUris) {
  const spec = { ...(job.spec || {}) };
  const logoKeys = [
    "primaryLogoPath",
    "homeLogoPath",
    "awayLogoPath",
  ];
  for (const key of logoKeys) {
    if (spec[key]) {
      const dataUri = await fileToDataUri(spec[key]);
      if (key === "primaryLogoPath") {
        spec.primaryLogo = dataUri;
      } else if (key === "homeLogoPath") {
        spec.homeLogo = dataUri;
      } else if (key === "awayLogoPath") {
        spec.awayLogo = dataUri;
      }
    }
  }
  return buildDocument({ spec, ...fontUris });
}

async function renderJob(browser, job, fontUris) {
  const spec = job.spec || {};
  const outputPath = path.resolve(String(job.output_path));
  await fs.mkdir(path.dirname(outputPath), { recursive: true });

  const page = await browser.newPage({
    viewport: {
      width: Number(spec.width || 1920),
      height: Number(spec.height || 1080),
    },
    deviceScaleFactor: 1,
  });
  const html = await hydrateJob(job, fontUris);
  await page.setContent(html, { waitUntil: "load" });
  await page.evaluate(async () => {
    if (window.__overlayReady) {
      await window.__overlayReady;
    }
  });
  await page.screenshot({
    path: outputPath,
    omitBackground: Boolean(spec.transparent),
  });
  await page.close();
}

async function main() {
  const args = parseArgs(process.argv);
  if (!args.spec || !args.output) {
    console.error("Usage: node scripts/render_overlay_asset.mjs --spec spec.json --output out.png");
    process.exit(2);
  }

  const raw = JSON.parse(await fs.readFile(path.resolve(String(args.spec)), "utf-8"));
  const jobs = Array.isArray(raw.jobs)
    ? raw.jobs
    : [{ output_path: String(args.output), spec: raw }];
  if (!Array.isArray(raw.jobs)) {
    jobs[0].output_path = String(args.output);
  }

  const fontUris = {
    bebasUri: await fileToDataUri(path.join(REPO_ROOT, "assets", "fonts", "BebasNeue-Regular.ttf")),
    barlowRegularUri: await fileToDataUri(path.join(REPO_ROOT, "assets", "fonts", "BarlowSemiCondensed-Regular.ttf")),
    barlowSemiUri: await fileToDataUri(path.join(REPO_ROOT, "assets", "fonts", "BarlowSemiCondensed-SemiBold.ttf")),
  };

  const browser = await chromium.launch({ headless: true });
  try {
    for (const job of jobs) {
      await renderJob(browser, job, fontUris);
    }
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
