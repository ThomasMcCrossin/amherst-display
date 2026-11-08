# Reel build performance — issue #48, 2026-11-08

The production and reviewed builds use the same renderer. Lower-third jobs share one Chromium session. The final encoder now defaults to CRF 18 / medium; `--encoder-preset slow` restores the former preset. Overlay specs, filter graph, audio and crossfades are unchanged. The optional series cards still render immediately. The basic reel uses a different concat path and was not changed or benchmarked.

## Real-game measurements

Canteenhub, November 7 Amherst–Truro game, original input clips and reviewed manifest, isolated outputs under `~/.local/state/watch-rams/issue-48/reel`. Baseline script was `fb37ec04e2b123c613cb5c6272a0b47e46f58a4d`; optimized script is the commit containing this report. No production outputs were written. Wall time includes overlays, probing and encode; no services or notifications were involved.

| Build | Baseline | Optimized | Qualification |
|---|---:|---:|---|
| Reviewed, 8 clips | 394.00 s | 365.10 s | Sequential; shared host, load falling from around 27 to 20. Difference is not a controlled speedup estimate. |
| Production, 8 clips | 556.81 s | 753.64 s | Host load rose substantially between runs; do not interpret as a regression or speedup. |
| Production `--dry-run` | 17.12 s | 8.02 s | Sequential under busy shared load; includes overlay rendering and probes, not encoding. |

A separate production trial limiting both filter-complex and encoder threads to 4 took 1473.83 s; no thread limit was adopted. These original busy-host runs do not establish a reliable end-to-end speedup. The measurements below evaluate the later preset change.

Output checks: baseline and optimized MP4 SHA-256 were identical for reviewed (`6f1fa825b690f40d71caeba8da1b2aad7f8310b36774fa4b051938c55aa3f30a`) and production (`702e3641024f460902064e20cc9639e4ba92bb050fd948409b0eca0f0acc2769`). The eight production overlay PNG hashes also matched pairwise. ffprobe outputs matched: reviewed 258.491567 s, 1280x720 H.264, 60000/1001 fps, stereo 48 kHz AAC; production 334.250000 s, 1280x720 H.264, 2997/50 fps, stereo 48 kHz AAC. Byte equality covers all encoded frames, transitions and audio for these inputs, not every possible game.

The optimized reviewed status history recorded `reel_overlays` 9/9 and `reel_encode` 258/258. Encoding progress uses ffmpeg's output timestamp in video seconds, with a duration-based ETA; it is an estimate, not a count of clips. Encoding failure is recorded as failed. Progress is a no-op without `PIPELINE_PROGRESS_JSON`.

## Preset comparison — 2026-11-08

The same reviewed game and manifest were used in isolated `reel/medium` and `reel/preset-pair` directories. The full medium build took 403.85 s, versus the previously recorded slow optimized build's 365.10 s; these are not a controlled pair and host load differed. The complete medium reel was 258.491567 s, 1280×720, 60000/1001 fps, stereo 48 kHz AAC, matching the slow baseline. AAC packet SHA-256 matched (`8446ec87cd37ec41827fc2778060ac8c506af18df7ed7ada140719cbc443de2d`). Full-reel SSIM versus slow was 0.993287 (15494 frames); sizes were 158 MiB medium versus 154 MiB slow. This measures agreement with the prior lossy encode, not fidelity to the original camera feed.

A fresh sequential, end-to-end 60-second comparison used the same real inputs, overlays, complete filter graph including the first crossfade, audio, and CRF 18. Slow took 112.36 s; medium took 91.10 s (19% less wall time). One-minute load averages were 15.45 at slow start, 25.97 at medium start and 23.59 at finish. The host is shared, this is one pair, and neither this result nor the full-run times guarantees a full-reel speedup. Paired 60-second SSIM was 0.993158. Medium is the default based on this material representative gain and the quality checks; slow remains available for comparison or compression preference.

Actual full-reel frames at 2 s (lower third), 35 s (game action), and 49.9 s (first crossfade) were inspected against slow: content, overlay and transition matched visually at those points. The lower-third bottom is clipped in both versions; this is pre-existing and not changed here. This spot check and matching audio packets are not a complete editorial or listening review. The renderer also reports low timing confidence for three clips, so review remains necessary before publication.

Reproduce the paired run from the setup below, after making `reel/medium` with the same links as `optimized` and copying the current script and `pipeline_progress.py` there. The 60-second limit is inserted only in an isolated script copy, immediately before the output filename; it retains the full filter graph rather than re-encoding an already encoded sample:

```bash
s="$HOME/.local/state/watch-rams/issue-48/reel"
x="$s/medium"
g='/home/canteenhub/amherst-display/Games/2026-11-07_Amherst Ramblers_vs_Truro Bearcats'
py=/home/canteenhub/amherst-display/.venv/bin/python
mkdir -p "$s/preset-pair"
python3 - "$x/scripts/build_production_highlight_reel.py" "$x/scripts/bench_60.py" <<'PY'
from pathlib import Path
import sys
text = Path(sys.argv[1]).read_text()
needle = '    if args.dry_run:\n        print(" ".join(cmd))'
assert text.count(needle) == 1
Path(sys.argv[2]).write_text(text.replace(needle, '    cmd[-1:-1] = ["-t", "60"]\n\n' + needle))
PY
for p in slow medium; do
  uptime
  /usr/bin/time -f 'elapsed=%e' -o "$s/preset-pair/graph-$p.time" "$py" "$x/scripts/bench_60.py" --game-dir "$x/game" --clips-manifest "$g/data/review/reel_main.json" --skip-major-approved --encoder-preset "$p" --output "$s/preset-pair/graph-$p.mp4" > "$s/preset-pair/graph-$p.log" 2>&1
done
ffmpeg -i "$s/preset-pair/graph-slow.mp4" -i "$s/preset-pair/graph-medium.mp4" -lavfi ssim -f null -
ffmpeg -i "$s/baseline/game/output/reviewed.mp4" -i "$x/game/output/reviewed.mp4" -lavfi ssim -f null -
for v in "$s/baseline/game/output/reviewed.mp4" "$x/game/output/reviewed.mp4"; do
  ffprobe -v error -show_entries format=duration:stream=codec_name,width,height,avg_frame_rate,sample_rate,channels,duration -of json "$v"
  ffmpeg -v error -i "$v" -map 0:a:0 -c copy -f hash -hash sha256 -
done
```

The complete medium reel was built using the reviewed command in the next section with `--encoder-preset medium` and the `medium` paths. The 60-second limit is output-side, so startup/overlay work and full graph initialization are included; it is not a full-length timing.

## Reproduce original slow measurements

Run from this worktree; use a fresh state directory or retain the original results before replacing files. The main checkout supplies browser dependencies/assets and the actual game supplies read-only inputs. `render_overlay_asset.mjs` is symlinked to the main checkout so Node resolves its dependencies there. The copied game data and clips are symlinks; only `game/output` is written.

```bash
set -eu
s="$HOME/.local/state/watch-rams/issue-48/reel"
g='/home/canteenhub/amherst-display/Games/2026-11-07_Amherst Ramblers_vs_Truro Bearcats'
py=/home/canteenhub/amherst-display/.venv/bin/python
for x in baseline optimized; do
  mkdir -p "$s/$x/scripts" "$s/$x/game/output"
  ln -sfn /home/canteenhub/amherst-display/scripts/render_overlay_asset.mjs "$s/$x/scripts/render_overlay_asset.mjs"
  for p in assets rosters teams_highlights.json; do ln -sfn "/home/canteenhub/amherst-display/$p" "$s/$x/$p"; done
  ln -sfn "$g/clips" "$s/$x/game/clips"
  ln -sfn "$g/data" "$s/$x/game/data"
done
git show fb37ec04e2b123c613cb5c6272a0b47e46f58a4d:scripts/build_production_highlight_reel.py > "$s/baseline/scripts/build_production_highlight_reel.py"
cp scripts/build_production_highlight_reel.py "$s/optimized/scripts/"
cp pipeline_progress.py "$s/optimized/"
for x in baseline optimized; do
  /usr/bin/time -f 'elapsed=%e' -o "$s/$x/review-time.txt" env PIPELINE_PROGRESS_JSON="$s/$x/review-status.json" "$py" "$s/$x/scripts/build_production_highlight_reel.py" --game-dir "$s/$x/game" --clips-manifest "$g/data/review/reel_main.json" --skip-major-approved --encoder-preset slow --output "$s/$x/game/output/reviewed.mp4" > "$s/$x/review.log" 2>&1
  /usr/bin/time -f 'elapsed=%e' -o "$s/$x/time.txt" env PIPELINE_PROGRESS_JSON="$s/$x/status.json" "$py" "$s/$x/scripts/build_production_highlight_reel.py" --game-dir "$s/$x/game" --encoder-preset slow > "$s/$x/run.log" 2>&1
  /usr/bin/time -f 'elapsed=%e' -o "$s/$x/dry-time.txt" "$py" "$s/$x/scripts/build_production_highlight_reel.py" --game-dir "$s/$x/game" --dry-run > "$s/$x/dry.log" 2>&1
  ffprobe -v error -show_entries format=duration:stream=codec_name,width,height,avg_frame_rate,sample_rate,channels -of json "$s/$x/game/output/reviewed.mp4" > "$s/$x/review-probe.json"
done
sha256sum "$s"/{baseline,optimized}/game/output/{reviewed,highlights_production}.mp4
diff -u "$s/baseline/review-probe.json" "$s/optimized/review-probe.json"
for x in baseline optimized; do sha256sum "$s/$x/game/output/overlays/"*.png | awk '{print $1}'; done
```

`--dry-run` still renders overlays and writes the warning report, so use the isolated game directory. The real runs above were performed in baseline/optimized pairs, not in the per-version order of this convenience loop. Targeted tests: `.venv/bin/python -m pytest -q tests/test_filtered_reel_production_support.py tests/test_production_major_ordering.py` (6 passed, using the main checkout venv). Full gates belong to the captain.
