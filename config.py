"""
🏒 Local-First Config — fast, reliable writes; optional Google Drive integration via service account API.

This keeps all working files LOCAL (Games, logs, temp).
Drive operations (ingest, major review upload, etc.) use the Google Drive API with the
service account configured in `GOOGLE_APPLICATION_CREDENTIALS`.
"""

import os
import shutil
from pathlib import Path

# Load repo-local .env if present (keeps cron/service runs simple).
try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).parent / ".env", override=False)
except Exception:
    pass

# ---------- Local repo (script lives here) ----------
LOCAL_REPO_DIR = Path(__file__).parent
GOOGLE_HOCKEY_DIR = None
GOOGLE_GAMES_DIR = None
GOOGLE_INPUT_DIR = None

# ---------- Working directories (ALWAYS LOCAL) ----------
# Your script reads/prints these; keeping the same names avoids breakage.
GAMES_DIR = LOCAL_REPO_DIR / "Games"   # script writes here

TEAMS_FILE = LOCAL_REPO_DIR / "teams.json"

# Logs and temp: always local
LOGS_DIR = LOCAL_REPO_DIR / "logs"
TEMP_DIR = LOCAL_REPO_DIR / "temp"

def ensure_logs_directory():
    try:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        print(f"⚠️ Could not create logs directory {LOGS_DIR}: {e}")
        return False

def ensure_temp_directory():
    try:
        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        print(f"⚠️ Could not create temp directory {TEMP_DIR}: {e}")
        return False

# ---------- Video / analysis settings (unchanged) ----------
SUPPORTED_FORMATS = ['.ts', '.mp4', '.avi', '.mov', '.mkv', '.flv', '.wmv', '.webm', '.m4v']

# ffmpeg / encoding defaults
# MoviePy uses ffmpeg under the hood; prefer modern H.264 with CRF (quality-based) encoding.
OUTPUT_CODEC = 'libx264'
OUTPUT_PRESET = 'slow'          # slower = better compression / quality per bitrate
OUTPUT_CRF = 18                 # 18-20 is typical "visually lossless-ish" for 720p/1080p
OUTPUT_AUDIO_CODEC = 'aac'
OUTPUT_AUDIO_BITRATE = '192k'
OUTPUT_AUDIO_SAMPLE_RATE = 48000
OUTPUT_PIXEL_FORMAT = 'yuv420p'

AUDIO_SAMPLE_RATE = 22050
GOAL_ENERGY_THRESHOLD = 0.75
SAVE_ENERGY_THRESHOLD = 0.65
ANNOUNCER_EXCITEMENT_THRESHOLD = 0.7

MAX_HIGHLIGHT_CLIPS = 12
DEFAULT_CLIP_BEFORE_TIME = 15
DEFAULT_CLIP_AFTER_TIME = 4
BOX_SCORE_TIME_IS_ELAPSED = True  # Box scores list time elapsed in period

# ---------- OCR backend + health settings ----------
# Backends are tried in order for probing and fallback. "easyocr" is optional and
# only used when installed; otherwise it is skipped.
OCR_BACKENDS = ["tesseract", "easyocr"]
OCR_ENABLE_EASYOCR_FALLBACK = True
OCR_EASYOCR_LANGS = ["en"]
OCR_EASYOCR_GPU = False

# Health thresholds for hybrid behavior (probe + rerun sampling before failing).
OCR_MIN_SUCCESS_RATE = 0.05
OCR_MIN_PERIOD_RATE = 0.20
OCR_MIN_AVG_CONFIDENCE = 55.0
OCR_HEALTH_BAD_CONSECUTIVE_SAMPLES_RESET = 10

# Local OCR refinement for low-confidence event matches.
EVENT_LOCAL_OCR_WINDOW_SECONDS = 60.0
EVENT_LOCAL_OCR_STEP_SECONDS = 0.5
EVENT_LOCAL_OCR_PERSISTENCE_WINDOW_SECONDS = 6.0
EVENT_LOCAL_OCR_MIN_HITS = 3
EVENT_LOCAL_OCR_MAX_DIFF_SECONDS = 6.0

# ---------- Penalty clip settings ----------
# PP contributing penalties (shown before powerplay goals)
PENALTY_PP_BEFORE_SECONDS = 2.0  # 2 seconds before the penalty call
PENALTY_PP_AFTER_SECONDS = 3.0   # 3 seconds after (5s total clip)

# 5-minute major settings (require manual review)
# Note: Clock freezes at penalty time for 10-30s while refs sort things out,
# so we need to go back further than the OCR timestamp to capture the incident
MAJOR_PENALTY_BEFORE_SECONDS = 30.0
MAJOR_PENALTY_AFTER_SECONDS = 90.0  # 1:30 after = ~2 min total clip
MAJOR_REVIEW_TIMEOUT_DAYS = 7

# ---------- Text overlay settings ----------
OVERLAY_ENABLED = True
OVERLAY_FONT_SIZE = 42
OVERLAY_FONT = 'Arial-Bold'
OVERLAY_DURATION_SECONDS = 5.0

# ---------- Major penalty review workflow ----------
# Google Drive folder for major penalty review clips
MAJOR_REVIEW_DRIVE_FOLDER_ID = os.environ.get('MAJOR_REVIEW_DRIVE_FOLDER_ID', '')

# Email notification (via Resend)
RESEND_API_KEY = os.environ.get('RESEND_API_KEY', '')
NOTIFICATION_EMAIL_TO = os.environ.get('NOTIFICATION_EMAIL', '')
# Use a Resend-verified default sender unless the user provides their own verified domain.
NOTIFICATION_EMAIL_FROM = os.environ.get('NOTIFICATION_EMAIL_FROM', 'onboarding@resend.dev')

# Review monitor settings
MAJOR_REVIEW_FLAG_FILE = Path('/tmp/major_review_active')
MAJOR_REVIEW_CHECK_INTERVAL_MINUTES = 5

# ---------- Helpers used by your script (names preserved) ----------
def find_video_locations():
    """Return list of directories to search for videos (local-first, optionally Google Drive)."""
    video_locations = [
        LOCAL_REPO_DIR,                 # repo folder
        Path.home() / "Downloads",      # downloads
        Path.home() / "Desktop",        # desktop
    ]
    # Add Google Drive input location if available (read-only is fine)
    if GOOGLE_INPUT_DIR and GOOGLE_INPUT_DIR.exists():
        video_locations.insert(1, GOOGLE_INPUT_DIR)     # check GDrive early
        print(f"🔍 Will check Google Drive videos: {GOOGLE_INPUT_DIR}")
    return video_locations

def ensure_output_directory():
    """Ensure the local games output directory exists."""
    try:
        GAMES_DIR.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        print(f"⚠️ Could not create output directory {GAMES_DIR}: {e}")
        return False

# ---------- Optional post-run mirror (call this AFTER rendering finishes) ----------
def mirror_game_to_gdrive(local_game_dir: Path) -> Path | None:
    """
    Mirror a finished local game folder to Google Drive using the service account API.

    Set `DRIVE_GAMES_FOLDER_ID` (or pass `--games-folder-id` to `scripts/drive_ingest.py`)
    to enable uploads.

    Returns the local path on success, or None if Drive is unavailable/misconfigured.
    """
    folder_id = os.environ.get("DRIVE_GAMES_FOLDER_ID", "").strip()
    if not folder_id:
        print("ℹ️ Skipping mirror: DRIVE_GAMES_FOLDER_ID not set.")
        return None

    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not creds_path or not Path(creds_path).exists():
        print("ℹ️ Skipping mirror: GOOGLE_APPLICATION_CREDENTIALS not configured.")
        return None

    try:
        from googleapiclient.discovery import build
        from google.oauth2 import service_account
        from googleapiclient.http import MediaFileUpload
    except Exception as e:
        print(f"⚠️ Mirror skipped: Google API deps missing ({e})")
        return None

    def normalize_folder_id(value: str) -> str:
        import re

        v = str(value or "").strip()
        m = re.search(r"/folders/([a-zA-Z0-9_-]+)", v)
        return m.group(1) if m else v

    folder_id = normalize_folder_id(folder_id)

    try:
        credentials = service_account.Credentials.from_service_account_file(
            creds_path,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        service = build("drive", "v3", credentials=credentials)
    except Exception as e:
        print(f"⚠️ Mirror skipped: failed to create Drive client ({e})")
        return None

    def _escape_q(value: str) -> str:
        return str(value or "").replace("'", "\\'")

    def ensure_folder(parent: str, name: str) -> str:
        q = (
            f"'{parent}' in parents and trashed=false and "
            f"mimeType='application/vnd.google-apps.folder' and name='{_escape_q(name)}'"
        )
        res = service.files().list(
            q=q,
            fields="files(id,name)",
            pageSize=1,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        files = res.get("files", []) or []
        if files:
            return str(files[0]["id"])
        created = service.files().create(
            body={"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent]},
            fields="id",
            supportsAllDrives=True,
        ).execute()
        return str(created["id"])

    def upsert_file(parent: str, src: Path) -> None:
        q = f"'{parent}' in parents and trashed=false and name='{_escape_q(src.name)}'"
        res = service.files().list(
            q=q,
            fields="files(id,name,mimeType)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        existing = next((f for f in (res.get("files", []) or []) if f.get("mimeType") != "application/vnd.google-apps.folder"), None)
        media = MediaFileUpload(str(src), resumable=True)
        if existing:
            service.files().update(fileId=existing["id"], media_body=media, supportsAllDrives=True).execute()
        else:
            service.files().create(
                body={"name": src.name, "parents": [parent]},
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute()

    def upload_tree(src_dir: Path, parent: str) -> None:
        for child in src_dir.iterdir():
            if child.is_dir():
                sub = ensure_folder(parent, child.name)
                upload_tree(child, sub)
            elif child.is_file():
                upsert_file(parent, child)

    try:
        remote_game_folder = ensure_folder(folder_id, local_game_dir.name)
        upload_tree(local_game_dir, remote_game_folder)
        print(f"☁️ Mirrored to Google Drive folder: {local_game_dir.name}")
        return local_game_dir
    except Exception as e:
        print(f"⚠️ Mirror failed ({e}). Local output remains at: {local_game_dir}")
        return None

# ---------- Summary ----------
# Avoid noisy import-time output in cron/test contexts.
