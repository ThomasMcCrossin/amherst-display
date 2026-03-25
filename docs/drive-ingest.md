# Drive ingest (Google Drive API / service account)

This repo can process new game videos automatically from Google Drive using the
service account configured via `GOOGLE_APPLICATION_CREDENTIALS`.

## Drive folder layout

The ingest script expects a single **ingest folder** and will create/manage
these subfolders inside it:

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

- The ingest folder can be provided as an ID (`--ingest-folder-id`) or as a path under the Drive root (`--ingest-folder-path`, default: `Inbox`).
- For Shared Drives, pass `--drive-id` (or set `RAMBLERS_DRIVE_ID` in `.env`).
- Ingest only picks up files older than `--min-age` (default `2m`) to avoid partial uploads.
- The highlight extraction pipeline always writes locally to `./Games/…`.
- The production reel is generated as `output/highlights_production.mp4` unless the pipeline pauses for major review.
- Source videos are downloaded to `temp/drive_ingest/incoming/` and deleted after each attempt by default (set `--keep-local-videos` to retain them).
- When ffprobe indicates a huge A/V start offset (default threshold: 60s), ingest will auto-delay audio during repair/remux; you can also force a specific value with `--audio-delay-seconds` (positive delays audio, negative advances it).
- If a capture has multiple audio/video streams and the default stream isn't correct, you can force a specific stream by index via `--audio-stream-index` / `--video-stream-index`.
- The script skips locking/processing when disk is low (tune with `--min-free-gb` and `--disk-headroom-gb`).
