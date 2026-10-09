#!/usr/bin/env bash
# Game TV board: rebuild board.json/insights.json and publish them, plus the hybrid page, into the deploy dir.
# Runs every minute (game-board-refresh.timer). The builder's 60 s cache means one scorebar request per run;
# everything else refetches every 20 min. A failed build keeps the last good files on screen.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
DEPLOY="${GAME_BOARD_DIR:-$HOME/.local/share/watch-rams/game-board}"
set -a; . "$HOME/.local/state/watch-rams/amherst-highlights.env"; set +a   # HOCKEYTECH_API_KEY
cd "$HERE/.."
node bakeoff/build_board_data.mjs >/dev/null
node bakeoff/build_insights.mjs >/dev/null
mkdir -p "$DEPLOY/data" "$DEPLOY/designs/hybrid"
rsync -a --delete "$HERE/designs/hybrid/" "$DEPLOY/designs/hybrid/"
for f in board.json insights.json; do cp "$HERE/data/$f" "$DEPLOY/data/.$f.tmp" && mv "$DEPLOY/data/.$f.tmp" "$DEPLOY/data/$f"; done
