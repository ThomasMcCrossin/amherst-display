"""
🏒 Local-First Config — fast, reliable writes; optional Google Drive integration via service account API.

This keeps all working files LOCAL (Games, logs, temp).
Drive operations (ingest, major review upload, etc.) use the Google Drive API with the
service account configured in `GOOGLE_APPLICATION_CREDENTIALS`.
"""

import os
import shutil
from pathlib import Path

from drive_config import default_state_env_path, resolve_drive_config
from scorebug_profiles import resolve_scorebug_profile

# Load repo-local/state env if present (keeps cron/service runs simple without
# committing Drive config into the repo).
try:
    from dotenv import load_dotenv

    def _load_env_candidates() -> None:
        candidates = [
            Path(__file__).parent / ".env",
            default_state_env_path(),
        ]
        explicit = str(os.environ.get("HIGHLIGHTS_ENV_FILE", "") or "").strip()
        if explicit:
            candidates.insert(1, Path(explicit).expanduser())
        seen = set()
        for candidate in candidates:
            resolved = str(candidate.expanduser())
            if resolved in seen:
                continue
            seen.add(resolved)
            load_dotenv(candidate, override=False)

    _load_env_candidates()
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
OUTPUT_PRESET = 'veryfast'      # clip extraction needs practical encode speed
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
AUTO_UPLOAD_GAME_ARCHIVES_TO_DRIVE = str(
    os.environ.get("AUTO_UPLOAD_GAME_ARCHIVES_TO_DRIVE", "1")
).strip().lower() not in {"0", "false", "no", "off"}

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

# Save scorebug-only crops for failed / low-confidence samples so FloHockey OCR
# issues can be diagnosed without storing full-frame images for every attempt.
OCR_DEBUG_SAVE_SCOREBUG_CROPS = True
OCR_DEBUG_SCOREBUG_CROP_DIRNAME = "ocr_scorebug_crops"
OCR_DEBUG_FAILURE_CROP_LIMIT = 40
OCR_DEBUG_LOW_CONFIDENCE_THRESHOLD = 65.0
OCR_DEBUG_LOW_CONFIDENCE_CROP_LIMIT = 25

# Local OCR refinement for low-confidence event matches.
EVENT_LOCAL_OCR_WINDOW_SECONDS = 60.0
EVENT_LOCAL_OCR_STEP_SECONDS = 0.5
EVENT_LOCAL_OCR_PERSISTENCE_WINDOW_SECONDS = 6.0
EVENT_LOCAL_OCR_MIN_HITS = 3
EVENT_LOCAL_OCR_MAX_DIFF_SECONDS = 6.0

# For recorded full-game workflows, an event cannot occur before its own elapsed
# game time relative to the detected puck-drop. This guard prevents P1 warmup
# clocks from matching real goals later in the game if OCR samples are taken too early.
EVENT_ENFORCE_MIN_VIDEO_TIME_FROM_GAME_START = True
EVENT_MIN_VIDEO_TIME_BUFFER_SECONDS = 240.0

# ---------- Penalty clip settings ----------
# PP contributing penalties (shown before powerplay goals)
PENALTY_PP_BEFORE_SECONDS = 2.0  # 2 seconds before the penalty call
PENALTY_PP_AFTER_SECONDS = 3.0   # 3 seconds after (5s total clip)

# Goal clips refined from the scoreboard clock-stop are typically anchored at the
# whistle/stoppage, not the puck crossing the line. Give them more lead-in so the
# scoring play is actually visible.
GOAL_CLOCK_STOP_BEFORE_SECONDS = 32.0
GOAL_CLOCK_STOP_AFTER_SECONDS = 3.0
GOAL_FALLBACK_BEFORE_SECONDS = 20.0
GOAL_FALLBACK_AFTER_SECONDS = 4.0
GOAL_OT_BEFORE_SECONDS = 60.0
GOAL_OT_POWER_PLAY_BEFORE_SECONDS = 120.0
GOAL_OT_AFTER_SECONDS = 4.0
# Default goal timing rule: the goal moment is the first stable scoreboard clock
# freeze at the official goal time. Keep legacy near-match / projected fallbacks
# disabled unless a specific broken-scorebug run needs them.
GOAL_ENABLE_LEGACY_TIMING_FALLBACK = False
GOAL_CLOCK_STOP_ALLOW_CLOSE_SECONDS = 0
GOAL_ENABLE_PROJECTED_CLOCK_FALLBACK = False
GOAL_PROJECTED_CLOCK_FALLBACK_REQUIRES_UNRELIABLE = True
GOAL_LOCAL_OCR_ALLOW_CLOSE_SECONDS = 0
GOAL_ENABLE_LOCAL_OCR_CLOSEST_FALLBACK = False
GOAL_LOCAL_OCR_CLOSEST_FALLBACK_REQUIRES_UNRELIABLE = True

# 5-minute major settings (require manual review)
# Note: Clock freezes at penalty time for 10-30s while refs sort things out,
# so we need to go back further than the OCR timestamp to capture the incident
MAJOR_PENALTY_BEFORE_SECONDS = 30.0
MAJOR_PENALTY_AFTER_SECONDS = 90.0  # 1:30 after = ~2 min total clip
MAJOR_REVIEW_TIMEOUT_DAYS = 7

# ---------- Text overlay settings ----------
OVERLAY_ENABLED = True
OVERLAY_FONT_SIZE = 42
OVERLAY_FONT = '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf'
OVERLAY_DURATION_SECONDS = 5.0

# ---------- Highlight execution profiles / reel modes ----------
# HockeyTech / MHL box score times are ELAPSED in period.
# Broadcast OCR clocks are REMAINING in period.
DEFAULT_REEL_MODE = "goals_only"
SUPPORTED_REEL_MODES = (
    "goals_only",
    "goals_with_pp_penalties",
    "goals_with_approved_majors",
    "full_production",
)

DEFAULT_HIGHLIGHT_EXECUTION_PROFILE = "flohockey_recording"
HIGHLIGHT_EXECUTION_PROFILES = {
    # Dense, scorebug-first OCR profile for local Flo recordings.
    "flohockey_recording": {
        "sample_interval": 5,
        "tolerance_seconds": 30,
        "before_seconds": 8.0,
        "after_seconds": 6.0,
        "parallel_ocr": True,
        "ocr_workers": 4,
        "broadcast_type": "flohockey",
        "auto_detect_start": True,
        "reel_mode": DEFAULT_REEL_MODE,
    },
    # Faster FloHockey profile for multi-game backfills where 5-second OCR
    # sampling is too expensive but we still want the Flo-specific scorebug path.
    "flohockey_fast_recording": {
        "sample_interval": 15,
        "tolerance_seconds": 30,
        "before_seconds": 8.0,
        "after_seconds": 6.0,
        "parallel_ocr": True,
        "ocr_workers": 4,
        "broadcast_type": "flohockey",
        "auto_detect_start": True,
        "reel_mode": DEFAULT_REEL_MODE,
    },
    # Amherst home MHL layout: wide white Flo strip with right-side period/clock.
    "mhl_amherst_recording": {
        "sample_interval": 15,
        "tolerance_seconds": 30,
        "before_seconds": 8.0,
        "after_seconds": 6.0,
        "parallel_ocr": True,
        "ocr_workers": 4,
        "broadcast_type": "mhl_amherst",
        "auto_detect_start": True,
        "reel_mode": DEFAULT_REEL_MODE,
    },
    # Summerside home MHL layout: centered black banner with a tighter clock block.
    "mhl_summerside_recording": {
        "sample_interval": 15,
        "tolerance_seconds": 30,
        "before_seconds": 8.0,
        "after_seconds": 6.0,
        "parallel_ocr": True,
        "ocr_workers": 4,
        "broadcast_type": "mhl_summerside",
        "auto_detect_start": True,
        "reel_mode": DEFAULT_REEL_MODE,
    },
    # Backwards-compatible generic profile for non-Flo or manually tuned runs.
    "generic_recording": {
        "sample_interval": 30,
        "tolerance_seconds": 30,
        "before_seconds": 8.0,
        "after_seconds": 6.0,
        "parallel_ocr": True,
        "ocr_workers": 4,
        "broadcast_type": "auto",
        "auto_detect_start": True,
        "reel_mode": DEFAULT_REEL_MODE,
    },
    # Seeded non-standard MHL scorebug profile for Yarmouth home broadcasts.
    "yarmouth_recording": {
        "sample_interval": 5,
        "tolerance_seconds": 30,
        "before_seconds": 8.0,
        "after_seconds": 6.0,
        "parallel_ocr": True,
        "ocr_workers": 4,
        "broadcast_type": "yarmouth",
        "auto_detect_start": True,
        "reel_mode": DEFAULT_REEL_MODE,
    },
}


def resolve_highlight_execution_selection(
    name: str | None = None,
    *,
    game_info: dict | None = None,
    source_game_info: dict | None = None,
    **overrides,
):
    """
    Resolve both the execution-profile settings and the matched scorebug profile.
    """
    selected = str(name or "").strip()
    scorebug_profile, scorebug_context = resolve_scorebug_profile(
        game_info=game_info,
        source_game_info=source_game_info,
    )
    if not selected or selected.lower() == "auto":
        selected = str(scorebug_profile.execution_profile_name or DEFAULT_HIGHLIGHT_EXECUTION_PROFILE).strip()
    if selected not in HIGHLIGHT_EXECUTION_PROFILES:
        raise ValueError(
            f"Unknown highlight execution profile '{selected}'. "
            f"Expected one of: {', '.join(sorted(HIGHLIGHT_EXECUTION_PROFILES))}"
        )

    profile = dict(HIGHLIGHT_EXECUTION_PROFILES[selected])
    for key, value in overrides.items():
        if value is not None:
            profile[key] = value

    return {
        "execution_profile_name": selected,
        "execution_profile": profile,
        "scorebug_profile": scorebug_profile.to_dict(),
        "scorebug_context": scorebug_context,
    }


def get_highlight_execution_profile(name: str | None = None, **overrides):
    """
    Return a copy of a named highlight execution profile.

    Callers can pass explicit overrides for one-off tuning without mutating the
    shared config dictionary.
    """
    selection = resolve_highlight_execution_selection(name, **overrides)
    return dict(selection["execution_profile"])

_DRIVE_RUNTIME = resolve_drive_config()
HIGHLIGHTS_DRIVE_ID = _DRIVE_RUNTIME.drive_id
HIGHLIGHTS_INGEST_FOLDER_ID = _DRIVE_RUNTIME.ingest_folder_id
HIGHLIGHTS_INGEST_FOLDER_PATH = _DRIVE_RUNTIME.ingest_folder_path
HIGHLIGHTS_GAMES_FOLDER_ID = _DRIVE_RUNTIME.games_folder_id
HIGHLIGHTS_GAMES_FOLDER_PATH = _DRIVE_RUNTIME.games_folder_path
HIGHLIGHTS_REELS_FOLDER_ID = _DRIVE_RUNTIME.reels_folder_id
HIGHLIGHTS_REELS_FOLDER_PATH = _DRIVE_RUNTIME.reels_folder_path
HIGHLIGHTS_MAJOR_REVIEW_FOLDER_ID = _DRIVE_RUNTIME.major_review_folder_id
HIGHLIGHTS_MAJOR_REVIEW_FOLDER_PATH = _DRIVE_RUNTIME.major_review_folder_path
HIGHLIGHTS_REFERENCE_FOLDER_ID = _DRIVE_RUNTIME.reference_folder_id
HIGHLIGHTS_REFERENCE_FOLDER_PATH = _DRIVE_RUNTIME.reference_folder_path


def get_drive_runtime_config():
    """Resolve Drive config from current process env, including alias fallbacks."""
    return resolve_drive_config()


# ---------- Major penalty review workflow ----------
# Backward-compatible config names for older scripts.
MAJOR_REVIEW_DRIVE_FOLDER_ID = HIGHLIGHTS_MAJOR_REVIEW_FOLDER_ID
MAJOR_REVIEW_DRIVE_FOLDER_PATH = HIGHLIGHTS_MAJOR_REVIEW_FOLDER_PATH

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
    drive_cfg = resolve_drive_config()
    folder_id = str(drive_cfg.games_folder_id or "").strip()
    if not folder_id:
        print("ℹ️ Skipping mirror: HIGHLIGHTS_GAMES_FOLDER_ID not set.")
        return None

    creds_path = drive_cfg.credentials_path
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
