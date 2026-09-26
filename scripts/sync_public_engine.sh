#!/usr/bin/env bash
# Copy the generic highlight engine into a HockeyHighlightExtractor checkout (the public,
# team-agnostic repo): highlight_extractor/ -> hockey_extractor/, the scorebug catalog and
# detector, reference crops, and the engine tests that don't need this repo's config.
# Team data, Drive program layouts, config.py and scripts stay here. Review, run the
# public tests, then commit and open a PR there.
#
#   scripts/sync_public_engine.sh ../HockeyHighlightExtractor
#
# Remove: delete this file; nothing references it.
set -euo pipefail

SRC="$(cd "$(dirname "$0")/.." && pwd)"
DEST="${1:?usage: $0 <HockeyHighlightExtractor checkout>}"
DEST="$(cd "$DEST" && pwd)"
[ -d "$DEST/hockey_extractor" ] || { echo "not a HockeyHighlightExtractor checkout: $DEST" >&2; exit 1; }

rsync -a --delete --exclude __pycache__ "$SRC/highlight_extractor/" "$DEST/hockey_extractor/"
cp "$SRC/scorebug_profiles.py" "$SRC/scorebug_detect.py" "$SRC/goal_locator.py" "$SRC/drive_config.py" "$DEST/"
mkdir -p "$DEST/assets/scorebugs" "$DEST/tests/fixtures/scorebugs"
rsync -a --delete "$SRC/assets/scorebugs/" "$DEST/assets/scorebugs/"
rsync -a --delete "$SRC/tests/fixtures/scorebugs/" "$DEST/tests/fixtures/scorebugs/"
for t in test_ocr_parser_variants.py test_ocr_parse_time_text.py test_ocr_candidate_scoring.py \
         test_scorebug_layouts.py test_scorebug_detect.py test_shootout_clip.py \
         test_goal_locator.py test_partial_recording.py test_normalization_prefers_high_confidence.py \
         test_pipeline_pp_penalty_insertion.py; do
    [ -f "$SRC/tests/$t" ] && cp "$SRC/tests/$t" "$DEST/tests/$t"
done

grep -rl "highlight_extractor" "$DEST/hockey_extractor" "$DEST/scorebug_detect.py" "$DEST/goal_locator.py" "$DEST/tests" \
    | xargs -r sed -i 's/highlight_extractor/hockey_extractor/g'

# Nothing private may cross: fail loudly on personal paths, emails or key-shaped strings.
if grep -rn -I -E "/home/[a-z]+/|@curlys\.ca|sk-[a-f0-9]{32}|BEGIN (RSA )?PRIVATE KEY" \
        "$DEST/hockey_extractor" "$DEST/scorebug_profiles.py" "$DEST/scorebug_detect.py" "$DEST/goal_locator.py" "$DEST/drive_config.py"; then
    echo "private-looking content found above; fix before committing" >&2
    exit 1
fi
git -C "$DEST" status --short
