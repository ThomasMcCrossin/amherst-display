#!/usr/bin/env bash
# Port engine changes from this repo into a HockeyHighlightExtractor checkout (the public,
# league-neutral repo). The public repo has its own edits (league packs, providers, a neutral
# config), so this never copies files wholesale. It takes the diff since the last ported
# commit (recorded in the public repo's .amherst-display-port), renames paths
# (highlight_extractor -> hockey_extractor), and applies it as a 3-way merge. Conflicts are
# left in the index for a human to resolve. Nothing here touches this repo.
#
#   scripts/port_to_public.sh ../HockeyHighlightExtractor [<to-commit>]
#
# Then: resolve conflicts, port anything listed under "review by hand", run the public tests
# and scripts/leak_scan.sh there, and commit (the script updates .amherst-display-port).
set -euo pipefail

SRC="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$(cd "${1:?usage: $0 <HockeyHighlightExtractor checkout> [to-commit]}" && pwd)"
TO="$(git -C "$SRC" rev-parse "${2:-HEAD}")"
MARK="$DEST/.amherst-display-port"
[ -d "$DEST/hockey_extractor" ] || { echo "not a HockeyHighlightExtractor checkout: $DEST" >&2; exit 1; }
[ -f "$MARK" ] || { echo "missing $MARK (the amherst-display commit last ported)" >&2; exit 1; }
FROM="$(tr -d '[:space:]' < "$MARK")"
[ "$FROM" != "$TO" ] || { echo "nothing to port: already at $TO"; exit 0; }

# Generic code that exists in both repos. Team data, display, Drive, rosters and config stay here.
PORTED=(highlight_extractor scorebug_profiles.py scorebug_detect.py goal_locator.py penalty_incidents.py
        clip_review skills/hockey-clip-review scripts/review_game.py
        overlays/README.md overlays/render.mjs overlays/spec.py overlays/samples.py overlays/themes
        assets/scorebugs tests/fixtures/scorebugs tests)
# Generic-looking but built on this repo's private data or setup: never ported.
NEVER=(tests/test_eval_clip_windows.py overlays/.bakeoff)
# Diverged on purpose in the public repo: show the change, port it by hand.
BY_HAND=(config.py drive_config.py README.md)

patch="$(mktemp)"; trap 'rm -f "$patch"' EXIT
git -C "$SRC" diff --binary "$FROM" "$TO" -- "${PORTED[@]}" "${NEVER[@]/#/:(exclude)}" \
  | sed -E -e 's#(^(diff --git|---|\+\+\+|rename from|rename to) .*)highlight_extractor/#\1hockey_extractor/#g' \
           -e 's#( [ab]/)highlight_extractor/#\1hockey_extractor/#g' > "$patch"

# Tests: only those the public repo already carries, or new ones (reported for a look).
echo "== porting $FROM..$TO"
applied=0
if [ -s "$patch" ]; then
  # 3-way merge needs this repo's blobs; borrow them read-only instead of fetching.
  if GIT_ALTERNATE_OBJECT_DIRECTORIES="$(git -C "$SRC" rev-parse --absolute-git-dir)/objects" \
       git -C "$DEST" apply -3 --index "$patch"; then
    echo "applied cleanly"; applied=1
  elif git -C "$DEST" diff --name-only --diff-filter=U | grep -q .; then
    echo "CONFLICTS: resolve them in $DEST (git diff --name-only --diff-filter=U)"; applied=1
  else
    echo "patch did not apply; nothing changed, marker not moved" >&2; exit 1
  fi
  # module rename inside the ported files
  git -C "$DEST" diff --cached --name-only | grep -E '\.(py|md|sh)$' \
    | (cd "$DEST" && xargs -r sed -i 's/highlight_extractor/hockey_extractor/g')
else
  echo "no generic code changed"; applied=1
fi
echo "== review by hand (diverged in the public repo):"
git -C "$SRC" diff --stat "$FROM" "$TO" -- "${BY_HAND[@]}" || true
echo "== new tests ported (check they need nothing team-specific):"
git -C "$SRC" diff --name-only --diff-filter=A "$FROM" "$TO" -- tests || true

echo "$TO" > "$MARK"
git -C "$DEST" add "$MARK"
if [ -x "$DEST/scripts/leak_scan.sh" ]; then (cd "$DEST" && scripts/leak_scan.sh) || echo "LEAK SCAN FAILED: fix before committing"; fi
git -C "$DEST" status --short
