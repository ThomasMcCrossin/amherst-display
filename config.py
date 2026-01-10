"""
🏒 Local-First Config — fast, reliable writes; optional post-run mirror to Google Drive.

This keeps all working files LOCAL (Games, logs, temp).
If Google Drive is detected, you can call mirror_game_to_gdrive(...) AFTER everything is done.
"""

import os
import shutil
from pathlib import Path

# ---------- Google Drive detection (no write test, just presence) ----------
def find_toms_google_drive():
    """Find tom@curlys.ca Google Drive root for optional mirroring (read-only is fine)."""
    possible_drives = ['G:', 'J:', 'C:', 'D:', 'E:', 'F:', 'H:', 'I:', 'K:']
    print("🔍 Looking for tom@curlys.ca Google Drive (for optional mirroring).")
    for drive in possible_drives:
        for path in [
            Path(f"{drive}/My Drive"),
            Path(f"{drive}/Google Drive"),
            Path(f"{drive}/GoogleDrive"),
            Path(f"{drive}/Drive"),
            Path(f"{drive}/tom@curlys.ca/My Drive"),
            Path(f"{drive}/GoogleDrive - tom@curlys.ca"),
            Path(f"{drive}/My Drive - tom@curlys.ca"),
        ]:
            projects_folder = path / "Projects"
            if projects_folder.exists():
                print(f"✅ Found Google Drive: {path}")
                return path
    print("⚠️ Google Drive not found — running local-only (that’s fine).")
    return None

# ---------- Local repo (script lives here) ----------
LOCAL_REPO_DIR = Path(__file__).parent
print(f"📁 Local Repository: {LOCAL_REPO_DIR}")

# ---------- Optional Google Drive locations (for MIRRORING only) ----------
GOOGLE_DRIVE = find_toms_google_drive()
if GOOGLE_DRIVE:
    GOOGLE_HOCKEY_DIR = GOOGLE_DRIVE / "Projects" / "HockeyHighlights"
    GOOGLE_GAMES_DIR = GOOGLE_HOCKEY_DIR / "Games"     # mirror target
    GOOGLE_INPUT_DIR = GOOGLE_HOCKEY_DIR / "Videos"    # optional input source
else:
    GOOGLE_HOCKEY_DIR = None
    GOOGLE_GAMES_DIR = None
    GOOGLE_INPUT_DIR = None

# ---------- Working directories (ALWAYS LOCAL) ----------
# Your script reads/prints these; keeping the same names avoids breakage.
GAMES_DIR = LOCAL_REPO_DIR / "Games"   # script writes here
print(f"🎮 Games Output: Local ({GAMES_DIR})")

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
OUTPUT_CODEC = 'mpeg4'

AUDIO_SAMPLE_RATE = 22050
GOAL_ENERGY_THRESHOLD = 0.75
SAVE_ENERGY_THRESHOLD = 0.65
ANNOUNCER_EXCITEMENT_THRESHOLD = 0.7

MAX_HIGHLIGHT_CLIPS = 12
DEFAULT_CLIP_BEFORE_TIME = 8
DEFAULT_CLIP_AFTER_TIME = 6

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
    Mirror a finished local game folder to Google Drive 'Games'.
    Returns the destination path (or None if GDrive is unavailable).
    Safe on read-only / sync-delayed setups: copies only; does not write during processing.
    """
    if not GOOGLE_GAMES_DIR:
        print("ℹ️ Skipping mirror: Google Drive not available.")
        return None

    dest = GOOGLE_GAMES_DIR / local_game_dir.name
    try:
        # Make sure parent exists
        dest.parent.mkdir(parents=True, exist_ok=True)
        # Copy tree (overwrite newer files only)
        if dest.exists():
            # Incremental copy: mirror files
            for root, dirs, files in os.walk(local_game_dir):
                rel = Path(root).relative_to(local_game_dir)
                (dest / rel).mkdir(parents=True, exist_ok=True)
                for f in files:
                    src_f = Path(root) / f
                    dst_f = dest / rel / f
                    if not dst_f.exists() or src_f.stat().st_mtime > dst_f.stat().st_mtime:
                        shutil.copy2(src_f, dst_f)
        else:
            shutil.copytree(local_game_dir, dest)
        print(f"☁️ Mirrored to Google Drive: {dest}")
        return dest
    except Exception as e:
        print(f"⚠️ Mirror failed ({e}). Local output remains at: {local_game_dir}")
        return None

# ---------- Summary ----------
print("=" * 60)
print("🏒 LOCAL-FIRST SETUP SUMMARY")
print("=" * 60)
print(f"💻 Development: Local (Visual Studio)")
print(f"   Code: {LOCAL_REPO_DIR}")
print(f"   Teams: {TEAMS_FILE}")
if GOOGLE_DRIVE:
    print(f"☁️ Optional Mirror Target: {GOOGLE_GAMES_DIR}")
    if GOOGLE_INPUT_DIR:
        print(f"   Videos (read): {GOOGLE_INPUT_DIR}")
else:
    print("☁️ Optional Mirror Target: (none)")
print(f"📁 Output (write): {GAMES_DIR}")
print("=" * 60)
