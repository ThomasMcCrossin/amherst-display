# Drive ingest (Google Drive API / service account)

This repo can process new game videos automatically from Google Drive using the
service account configured via `GOOGLE_APPLICATION_CREDENTIALS`.

## Bootstrap the shared drive

For a new empty Shared Drive, bootstrap the canonical tree first:

```bash
./venv/bin/python scripts/setup_highlight_drive.py \
  --drive-id <shared_drive_id> \
  --creds-path /path/to/service-account.json \
  --program-manifest programs/mhl-amherst-ramblers-2025-26.json \
  --write-env ~/.local/state/amherst-display/highlight-drive.env \
  --write-manifest ~/.local/state/amherst-display/highlight-drive.manifest.json
```

The generated env file contains both the new generic variables and the legacy
aliases still used by older scripts.

## Drive folder layout

Canonical tree for the seeded Amherst program:

- `Programs/MHL/Amherst Ramblers/2025-26/01_Ingest/Inbox`
- `Programs/MHL/Amherst Ramblers/2025-26/02_Games`
- `Programs/MHL/Amherst Ramblers/2025-26/03_Reels/Games`
- `Programs/MHL/Amherst Ramblers/2025-26/04_Review/Major Penalties/Incoming`
- `Programs/MHL/Amherst Ramblers/2025-26/05_Reference`

The ingest script expects a single **ingest folder** and will create/manage
these subfolders inside `01_Ingest/Inbox`:

- `processing/` — file is moved here while being processed (acts like a lock)
- `failed/` — failures are moved here
- `processed/` — successes are moved here if you are not uploading into a games folder

If you configure a **games folder** destination, outputs are uploaded into:

- `<games>/<game_folder_name>/{data,clips,output,logs}/`
- and the source video is moved into `<games>/<game_folder_name>/source/`
- if the source required TS/PTS repair and/or A/V sync correction, a stable working MP4 is uploaded as `<games>/<game_folder_name>/source/working.mp4`
- a compact debug bundle is generated as `output/debug_bundle.zip` and uploaded into `<games>/<game_folder_name>/output/`

## Run

One pass:

```bash
./venv/bin/python scripts/drive_ingest.py --once
```

Continuous loop (poll every 60s):

```bash
./venv/bin/python scripts/drive_ingest.py
```

## Notes

- The ingest folder can be provided as an ID (`--ingest-folder-id`) or as a path under the Drive root (`--ingest-folder-path`).
- For Shared Drives, pass `--drive-id` or set `HIGHLIGHTS_DRIVE_ID`. Older `RAMBLERS_DRIVE_ID` env still works as an alias.
- Canonical env names are `HIGHLIGHTS_*`; legacy `DRIVE_*` and `RAMBLERS_DRIVE_ID` aliases are still accepted.
- Ingest only picks up files older than `--min-age` (default `2m`) to avoid partial uploads.
- The highlight extraction pipeline always writes locally to `./Games/…`.
- The production reel is generated as `output/highlights_production.mp4` unless the pipeline pauses for major review.
- Source videos are downloaded to `temp/drive_ingest/incoming/` and deleted after each attempt by default (set `--keep-local-videos` to retain them).
- When ffprobe indicates a huge A/V start offset (default threshold: 60s), ingest will auto-delay audio during repair/remux; you can also force a specific value with `--audio-delay-seconds` (positive delays audio, negative advances it).
- If a capture has multiple audio/video streams and the default stream isn't correct, you can force a specific stream by index via `--audio-stream-index` / `--video-stream-index`.
- The script skips locking/processing when disk is low (tune with `--min-free-gb` and `--disk-headroom-gb`).
- `--profile auto` uses the scorebug catalog to choose seeded layouts such as the Yarmouth home variant before falling back to generic OCR behavior.
