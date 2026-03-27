"""
Major Penalty Handler - Manages 5-minute major penalty review workflow

This module handles the async review workflow for 5-minute major penalties:
1. Detect 5-minute majors in box score data
2. Create 2-minute clips (10s before, 1:50 after)
3. Upload clips to Google Drive with JSON review files
4. Send email notification via Resend
5. Track review status for cronjob monitoring
"""

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict

from drive_config import resolve_drive_config

from .penalty_analyzer import PenaltyInfo, find_major_penalties, parse_penalties, group_coincidental_penalties
from .time_utils import time_string_to_seconds, PERIOD_LENGTH_SECONDS, OT_LENGTH_SECONDS

logger = logging.getLogger(__name__)


@dataclass
class MajorReviewClip:
    """Represents a major penalty clip awaiting review"""
    game_id: str
    game_date: str
    period: int
    time: str
    players: List[Dict]  # List of {name, team, infraction, minutes}
    clip_video_start: float
    clip_duration_seconds: float
    clip_filename: str
    json_filename: str
    reviewed: bool = False
    include: Optional[bool] = None
    trim_start: Optional[float] = None
    trim_end: Optional[float] = None


def detect_major_penalties(
    penalties_data: List[Dict],
    *,
    time_is_elapsed: bool = True,
) -> List[List[PenaltyInfo]]:
    """
    Detect all 5-minute major penalties and group coincidental ones.

    Returns groups of majors that occurred at the same time (e.g., fighting majors).
    """
    penalties = parse_penalties(penalties_data, time_is_elapsed=time_is_elapsed)
    majors = find_major_penalties(penalties)

    if not majors:
        return []

    # Group majors by period and time (coincidental majors)
    groups = group_coincidental_penalties(majors)

    # Filter to only groups containing at least one 5-minute major
    major_groups = []
    for group in groups:
        if any(p.is_major for p in group):
            major_groups.append(group)

    return major_groups


def create_major_review_clip(
    video_processor,
    penalty_group: List[PenaltyInfo],
    output_dir: Path,
    config
) -> Optional[Tuple[Path, Path]]:
    """
    Create a 2-minute clip for major penalty review.

    Args:
        video_processor: VideoProcessor instance
        penalty_group: List of penalties at same time (could be 1 for single major, 2 for fight)
        output_dir: Directory to save clip and JSON
        config: Configuration module

    Returns:
        Tuple of (clip_path, json_path) or None if failed
    """
    if not penalty_group:
        return None

    # Use first penalty for timing (all in group are at same time)
    primary = penalty_group[0]

    before_seconds = float(getattr(config, "MAJOR_PENALTY_BEFORE_SECONDS", 10.0))
    after_seconds = float(getattr(config, "MAJOR_PENALTY_AFTER_SECONDS", 110.0))
    video_time = primary.video_time
    if video_time is None:
        logger.warning(f"No video time for major penalty at P{primary.period} {primary.time}")
        return None

    start_time = max(0.0, float(video_time) - before_seconds)
    end_time = min(video_processor.duration, float(video_time) + after_seconds)
    clip_duration = end_time - start_time

    # Build filename
    player_names = '_'.join([p.player_name.replace(' ', '') for p in penalty_group[:2]])
    infraction = penalty_group[0].infraction.lower().replace(' ', '_')[:15]
    clip_filename = f"P{primary.period}_{primary.time.replace(':', '-')}_{infraction}_{player_names}.mp4"
    json_filename = clip_filename.replace('.mp4', '.json')

    clip_path = output_dir / clip_filename
    json_path = output_dir / json_filename

    # Create clip without overlay (user will review and decide)
    try:
        clip = video_processor.create_clip(start_time, end_time, clip_path)
        if clip:
            clip.close()
        else:
            logger.error(f"Failed to create major penalty clip: {clip_filename}")
            return None
    except Exception as e:
        logger.error(f"Error creating major penalty clip: {e}")
        return None

    # Create review JSON
    review_data = MajorReviewClip(
        game_id='',  # Will be filled by caller
        game_date='',  # Will be filled by caller
        period=primary.period,
        time=primary.time,
        players=[
            {
                'name': p.player_name,
                'team': p.team,
                'infraction': p.infraction,
                'minutes': p.minutes
            }
            for p in penalty_group
        ],
        clip_video_start=start_time,
        clip_duration_seconds=clip_duration,
        clip_filename=clip_filename,
        json_filename=json_filename,
        reviewed=False,
        include=None,
        trim_start=None,
        trim_end=None
    )

    try:
        with open(json_path, 'w') as f:
            json.dump(asdict(review_data), f, indent=2)
    except Exception as e:
        logger.error(f"Error creating review JSON: {e}")
        # Clean up clip if JSON failed
        clip_path.unlink(missing_ok=True)
        return None

    logger.info(f"Created major review clip: {clip_filename}")
    return (clip_path, json_path)

def _period_length_seconds(period: int) -> int:
    try:
        p = int(period or 0)
    except Exception:
        p = 0
    return OT_LENGTH_SECONDS if p >= 4 else PERIOD_LENGTH_SECONDS


def _find_penalty_video_time_from_timestamps(
    penalty: PenaltyInfo,
    video_timestamps: List[Dict],
    config,
) -> Optional[Tuple[float, Dict]]:
    """
    Find a penalty's video timestamp using OCR samples.

    Unlike the legacy "pre-freeze extrapolation" approach, this matches to the
    closest observed clock reading (including frozen clocks) and only applies a
    small adjustment for the remaining-seconds delta.
    """
    if not video_timestamps:
        return None

    try:
        penalty_period = int(penalty.period or 1)
    except Exception:
        penalty_period = 1

    try:
        penalty_remaining = int(getattr(penalty, "time_seconds"))
    except Exception:
        # Fall back to parsing the raw time string as remaining.
        penalty_remaining = time_string_to_seconds(str(getattr(penalty, "time", "0:00")))

    period_length = _period_length_seconds(penalty_period)
    if not (0 <= penalty_remaining <= period_length):
        return None

    period_timestamps = [
        ts for ts in video_timestamps
        if ts.get("period") == penalty_period and ts.get("video_time") is not None
    ]
    if not period_timestamps:
        return None

    # Consider both real and interpolated samples, but prefer real ones when equally close.
    # Interpolated samples can be much closer than sparse real OCR hits, especially when OCR
    # misses the period token and we rely on clock monotonicity + interpolation to densify.
    best_match = None
    best_key: Optional[Tuple[int, int]] = None  # (diff_seconds, interpolated_flag)
    for ts in period_timestamps:
        ts_remaining = ts.get("game_time_seconds")
        if ts_remaining is None:
            ts_remaining = time_string_to_seconds(ts.get("game_time", "0:00"))
        diff = abs(int(ts_remaining) - int(penalty_remaining))
        interpolated_flag = 1 if ts.get("interpolated") else 0
        key = (int(diff), int(interpolated_flag))
        if best_key is None or key < best_key:
            best_key = key
            best_match = ts

    max_diff = float(getattr(config, "PENALTY_VIDEO_TIME_MAX_DIFF_SECONDS", 600))
    if best_match is None:
        return None
    if best_key is None or best_key[0] > max_diff:
        return None

    base_video_time = float(best_match.get("video_time") or 0.0)
    best_remaining = best_match.get("game_time_seconds")
    if best_remaining is None:
        best_remaining = time_string_to_seconds(best_match.get("game_time", "0:00"))

    # Clock counts down: later video_time == smaller remaining time.
    adjusted_time = base_video_time + float(best_remaining - penalty_remaining)

    real_samples = sum(1 for ts in period_timestamps if not ts.get("interpolated"))
    meta = {
        "period_samples": len(period_timestamps),
        "period_real_samples": real_samples,
        "interpolated": bool(best_match.get("interpolated")),
        "match_diff_seconds": int(best_key[0]) if best_key else None,
    }

    return max(0.0, adjusted_time), meta


def _scan_video_for_penalty_time(
    video_processor,
    ocr_engine,
    penalty: PenaltyInfo,
    *,
    approx_video_time: Optional[float],
    config,
) -> Optional[float]:
    """
    Full-video OCR scan for the penalty clock time.

    This is a slower fallback when OCR timestamps are too sparse or unreliable.
    """
    get_frame = getattr(video_processor, "get_frame_at_time", None)
    if not callable(get_frame):
        return None
    if ocr_engine is None:
        return None

    duration = float(getattr(video_processor, "duration", 0.0) or 0.0)
    if duration <= 0:
        return None

    try:
        penalty_period = int(penalty.period or 1)
    except Exception:
        penalty_period = 1

    try:
        target_remaining = int(getattr(penalty, "time_seconds"))
    except Exception:
        target_remaining = time_string_to_seconds(str(getattr(penalty, "time", "0:00")))

    period_length = _period_length_seconds(penalty_period)
    if not (0 <= target_remaining <= period_length):
        return None

    coarse_step_seconds = float(getattr(config, "MAJOR_REVIEW_FULL_SCAN_STEP_SECONDS", 10.0))
    max_diff_seconds = float(getattr(config, "MAJOR_REVIEW_FULL_SCAN_MAX_DIFF_SECONDS", 5.0))
    if coarse_step_seconds <= 0:
        coarse_step_seconds = 10.0
    if max_diff_seconds < 0:
        max_diff_seconds = 0.0

    best = None  # (period_match, diff, tie, t)
    t = 0.0
    while t <= duration:
        frame = get_frame(t)
        if frame is not None:
            result = ocr_engine.extract_time_from_frame(frame, broadcast_type="auto")
            if result:
                p_raw, time_str = result
                p = None
                try:
                    p_int = int(p_raw)
                    if 1 <= p_int <= 5:
                        p = p_int
                except Exception:
                    p = None
                sec = time_string_to_seconds(str(time_str))
                if 0 <= sec <= period_length:
                    diff = abs(int(sec) - int(target_remaining))
                    if diff <= max_diff_seconds:
                        period_match = 1 if (p == penalty_period) else 0
                        if p is None:
                            period_match = 0
                        if approx_video_time is not None:
                            tie = abs(float(t) - float(approx_video_time))
                        else:
                            tie = float(t)
                        key = (period_match, -diff, -1 if approx_video_time is None else -tie, -t)
                        if best is None or key > best[0]:
                            best = (key, float(t), int(sec), p)
        t += coarse_step_seconds

    if best is None:
        return None

    # best[0] is the key tuple: (period_match, -diff, ...)
    # Only proceed if we found an explicit period match
    period_match_found = best[0][0] == 1
    if not period_match_found:
        return None  # Don't use timestamps from wrong/unknown period

    candidate_t = best[1]
    refined = _refine_penalty_video_time_by_local_ocr(
        video_processor,
        ocr_engine,
        penalty,
        approx_video_time=candidate_t,
        config=config,
    )
    return refined if refined is not None else candidate_t

def _refine_penalty_video_time_by_local_ocr(
    video_processor,
    ocr_engine,
    penalty: PenaltyInfo,
    *,
    approx_video_time: float,
    config,
) -> Optional[float]:
    """
    Refine a penalty timestamp by scanning nearby frames for the exact clock reading.

    For majors (especially fights), the clock usually freezes at the penalty time for a
    long stoppage. This method tries to find the first stable hit of the exact time on
    the scoreboard near the approximate position.
    """
    get_frame = getattr(video_processor, "get_frame_at_time", None)
    if not callable(get_frame):
        return None
    if ocr_engine is None:
        return None

    duration = float(getattr(video_processor, "duration", 0.0) or 0.0)
    if duration <= 0:
        return None

    try:
        penalty_period = int(penalty.period or 1)
    except Exception:
        penalty_period = 1

    try:
        target_remaining = int(getattr(penalty, "time_seconds"))
    except Exception:
        target_remaining = time_string_to_seconds(str(getattr(penalty, "time", "0:00")))

    period_length = _period_length_seconds(penalty_period)
    if not (0 <= target_remaining <= period_length):
        return None

    window_seconds = float(getattr(config, "MAJOR_REVIEW_LOCAL_OCR_WINDOW_SECONDS", 600.0))
    coarse_step_seconds = float(getattr(config, "MAJOR_REVIEW_LOCAL_OCR_COARSE_STEP_SECONDS", 5.0))
    fine_step_seconds = float(getattr(config, "MAJOR_REVIEW_LOCAL_OCR_FINE_STEP_SECONDS", 1.0))
    persistence_window_seconds = float(getattr(config, "MAJOR_REVIEW_LOCAL_OCR_PERSISTENCE_WINDOW_SECONDS", 8.0))
    min_target_hits = int(getattr(config, "MAJOR_REVIEW_LOCAL_OCR_MIN_HITS", 2))
    max_diff_seconds = float(getattr(config, "MAJOR_REVIEW_LOCAL_OCR_MAX_DIFF_SECONDS", 8.0))

    if window_seconds <= 0:
        return None
    if coarse_step_seconds <= 0:
        coarse_step_seconds = 5.0
    if fine_step_seconds <= 0:
        fine_step_seconds = 1.0
    if persistence_window_seconds <= 0:
        persistence_window_seconds = 0.0
    if min_target_hits <= 0:
        min_target_hits = 1
    if max_diff_seconds < 0:
        max_diff_seconds = 0.0

    center = max(0.0, min(duration, float(approx_video_time)))
    start = max(0.0, center - window_seconds)
    end = min(duration, center + window_seconds)
    if end <= start:
        return None

    best = None  # (diff, t, sec, period_match)
    t = start
    while t <= end:
        frame = get_frame(t)
        if frame is not None:
            result = ocr_engine.extract_time_from_frame(frame, broadcast_type="auto")
            if result:
                p_raw, time_str = result
                p = None
                try:
                    p_int = int(p_raw)
                    if 1 <= p_int <= 5:
                        p = p_int
                except Exception:
                    p = None
                sec = time_string_to_seconds(str(time_str))
                if 0 <= sec <= period_length:
                    diff = abs(int(sec) - int(target_remaining))
                    if diff <= max_diff_seconds:
                        # Only count as period match if OCR explicitly detected the correct period
                        # (period=None means OCR couldn't detect period - don't trust it)
                        period_match = 1 if (p == penalty_period) else 0
                        if best is None or diff < best[0] or (diff == best[0] and period_match > best[3]) or (
                            diff == best[0] and period_match == best[3] and t < best[1]
                        ):
                            best = (diff, float(t), int(sec), period_match)
        t += coarse_step_seconds

    if best is None:
        return None

    candidate_t = best[1]
    fine_start = max(0.0, candidate_t - 90.0)
    fine_end = min(duration, candidate_t + 90.0)

    samples = []
    t = fine_start
    while t <= fine_end:
        frame = get_frame(t)
        sec = None
        p = None
        if frame is not None:
            result = ocr_engine.extract_time_from_frame(frame, broadcast_type="auto")
            if result:
                p_raw, time_str = result
                try:
                    p_int = int(p_raw)
                    if 1 <= p_int <= 5:
                        p = p_int
                except Exception:
                    p = None
                parsed = time_string_to_seconds(str(time_str))
                if 0 <= parsed <= period_length:
                    sec = int(parsed)
        samples.append({"t": float(t), "period": p, "sec": sec})
        t += fine_step_seconds

    if not samples:
        return None

    # Require that the clock has been seen running (> target) before we accept a freeze hit.
    running_prefix = []
    seen_running = False
    for s in samples:
        sec = s["sec"]
        if sec is not None and sec > target_remaining:
            seen_running = True
        running_prefix.append(seen_running)

    for i, s in enumerate(samples):
        if s["sec"] != target_remaining:
            continue
        if not running_prefix[i]:
            continue
        # Require explicit period match - don't accept period=None (OCR couldn't detect period)
        # This prevents matching timestamps from the wrong period when clock readings are similar
        if s["period"] != penalty_period:
            continue
        window_end = s["t"] + persistence_window_seconds
        hits = 0
        for j in range(i, len(samples)):
            if samples[j]["t"] > window_end:
                break
            if samples[j]["sec"] == target_remaining:
                # Only count as a hit if period explicitly matches
                if samples[j]["period"] == penalty_period:
                    hits += 1
        if hits >= min_target_hits:
            return float(s["t"])

    # Fallback: shift from the best nearby reading by the delta.
    # Only use fallback if we found an explicit period match in the coarse scan
    if best is None or best[3] != 1:
        return None  # No confirmed period match - don't guess from wrong period
    adjusted = float(best[1]) + float(best[2] - target_remaining)
    return max(0.0, min(duration, adjusted))


def get_drive_service():
    """Get authenticated Google Drive service using service account credentials."""
    try:
        from googleapiclient.discovery import build
        from google.oauth2 import service_account
    except ImportError:
        logger.error("Google API client not installed. Run: pip install google-api-python-client google-auth")
        return None

    creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if not creds_path or not Path(creds_path).exists():
        logger.error(f"Service account credentials not found: {creds_path}")
        return None

    try:
        credentials = service_account.Credentials.from_service_account_file(
            creds_path,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logger.error(f"Failed to create Drive service: {e}")
        return None


def upload_to_drive(
    clips_and_jsons: List[Tuple[Path, Path]],
    game_id: str,
    game_date: str,
    config,
    *,
    game_info: Optional[Dict] = None,
) -> Optional[str]:
    """
    Upload major penalty clips and JSONs to Google Drive using service account API.

    Args:
        clips_and_jsons: List of (clip_path, json_path) tuples
        game_id: Game identifier
        game_date: Game date string
        config: Configuration module

    Returns:
        Drive folder URL or None if failed
    """
    from googleapiclient.http import MediaFileUpload

    service = get_drive_service()
    if not service:
        return None

    folder_id = resolve_drive_config().major_review_folder_id
    if not folder_id:
        folder_id = getattr(config, 'MAJOR_REVIEW_DRIVE_FOLDER_ID', '')
    if isinstance(folder_id, str):
        m = re.search(r"/folders/([a-zA-Z0-9_-]+)", folder_id)
        if m:
            folder_id = m.group(1)
    if not folder_id:
        logger.error("MAJOR_REVIEW_DRIVE_FOLDER_ID not configured")
        return None

    def _escape_q(value: str) -> str:
        return str(value or "").replace("'", "\\'")

    def _find_child_file_id(parent_id: str, name: str) -> Optional[str]:
        name_esc = _escape_q(name)
        results = service.files().list(
            q=f"'{parent_id}' in parents and trashed=false and name='{name_esc}'",
            fields="files(id,name,mimeType)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        for f in results.get("files", []) or []:
            if f.get("mimeType") != "application/vnd.google-apps.folder":
                return str(f.get("id") or "")
        return None

    def _upsert_file(parent_id: str, local_path: Path, *, mimetype: Optional[str] = None) -> None:
        existing_id = _find_child_file_id(parent_id, local_path.name)
        media = MediaFileUpload(str(local_path), mimetype=mimetype, resumable=True)
        if existing_id:
            service.files().update(
                fileId=existing_id,
                media_body=media,
                supportsAllDrives=True,
            ).execute()
            return
        service.files().create(
            body={"name": local_path.name, "parents": [parent_id]},
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()

    def _build_review_folder_name(game_id_value: str, game_date_value: str, info: Optional[Dict]) -> str:
        date_str = str(game_date_value or "").strip()
        home_team = ""
        away_team = ""
        opponent_name = ""

        if isinstance(info, dict):
            home_team = str(info.get("home_team") or "").strip()
            away_team = str(info.get("away_team") or "").strip()
            opponent = info.get("opponent")
            if isinstance(opponent, dict):
                opponent_name = str(opponent.get("team_name") or "").strip()
            elif opponent:
                opponent_name = str(opponent).strip()

        matchup = ""
        if home_team and away_team:
            matchup = f"{home_team} vs {away_team}"
        elif opponent_name:
            matchup = f"Amherst vs {opponent_name}"

        parts = []
        if date_str:
            parts.append(date_str)
        if matchup:
            parts.append(matchup)
        parts.append("Major Review")

        name = " - ".join([p for p in parts if p]).strip()
        if not name:
            name = "Major Review"

        if game_id_value:
            name = f"{name} ({game_id_value})"
        return name

    try:
        # Create subfolder for this game
        subfolder_name = _build_review_folder_name(str(game_id or ""), str(game_date or ""), game_info)
        # Reuse existing folder if present to avoid duplicates on retries.
        escaped_name = _escape_q(str(subfolder_name))
        existing = service.files().list(
            q=(
                f"'{folder_id}' in parents and trashed=false and "
                "mimeType='application/vnd.google-apps.folder' and "
                f"name='{escaped_name}'"
            ),
            fields='files(id,name)',
            pageSize=1,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        existing_files = existing.get('files', []) or []
        if existing_files:
            subfolder_id = existing_files[0]['id']
            logger.info(f"Using existing Drive folder: {subfolder_name} (id: {subfolder_id})")
        else:
            folder_metadata = {
                'name': subfolder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [folder_id]
            }
            folder = service.files().create(
                body=folder_metadata,
                fields='id',
                supportsAllDrives=True
            ).execute()
            subfolder_id = folder['id']
            logger.info(f"Created Drive folder: {subfolder_name} (id: {subfolder_id})")

        # Upload each clip and JSON
        for clip_path, json_path in clips_and_jsons:
            # Update JSON with game info
            with open(json_path, 'r') as f:
                review_data = json.load(f)
            review_data['game_id'] = game_id
            review_data['game_date'] = game_date
            with open(json_path, 'w') as f:
                json.dump(review_data, f, indent=2)

            logger.info(f"Uploading clip: {clip_path.name}")
            _upsert_file(subfolder_id, clip_path)
            logger.info(f"Uploading JSON: {json_path.name}")
            _upsert_file(subfolder_id, json_path, mimetype="application/json")

            logger.info(f"Uploaded to Drive: {clip_path.name} and {json_path.name}")

        folder_url = f"https://drive.google.com/drive/folders/{subfolder_id}"
        logger.info(f"Major penalty clips uploaded to: {folder_url}")
        return folder_url

    except Exception as e:
        logger.error(f"Drive upload failed: {e}")
        return None


def send_review_notification(
    folder_url: str,
    game_info: Dict,
    major_clips: List[Tuple[Path, Path]],
    config
) -> bool:
    """
    Send email notification about major penalties requiring review.

    Args:
        folder_url: Google Drive folder URL
        game_info: Game information dict
        major_clips: List of (clip_path, json_path) tuples
        config: Configuration module

    Returns:
        True if email sent successfully
    """
    api_key = getattr(config, 'RESEND_API_KEY', '') or os.environ.get('RESEND_API_KEY', '')
    to_email = getattr(config, 'NOTIFICATION_EMAIL_TO', '') or os.environ.get('NOTIFICATION_EMAIL', '')
    preferred_from = getattr(config, 'NOTIFICATION_EMAIL_FROM', '') or os.environ.get('NOTIFICATION_EMAIL_FROM', '')
    fallback_from = 'onboarding@resend.dev'
    from_email = preferred_from or fallback_from

    if not api_key:
        logger.warning("RESEND_API_KEY not configured, skipping email notification")
        return False

    if not to_email:
        logger.warning("NOTIFICATION_EMAIL_TO not configured, skipping email notification")
        return False

    try:
        import resend
        resend.api_key = api_key
    except ImportError:
        logger.error("Resend package not installed. Run: pip install resend")
        return False

    # Build email content
    opponent = game_info.get('opponent', {})
    opponent_name = opponent.get('team_name', 'Unknown') if isinstance(opponent, dict) else str(opponent)
    game_date = game_info.get('date', 'Unknown')

    subject = f"Major Penalty Review Required - Ramblers vs {opponent_name} ({game_date})"

    # Build list of penalties
    penalty_list = []
    for clip_path, json_path in major_clips:
        try:
            with open(json_path, 'r') as f:
                review_data = json.load(f)
            players = review_data.get('players', [])
            period = review_data.get('period', '?')
            time = review_data.get('time', '?')

            for player in players:
                penalty_list.append(
                    f"- P{period} {time}: {player.get('name')} ({player.get('team')}) - "
                    f"{player.get('infraction')} ({player.get('minutes')} min)"
                )
        except Exception:
            penalty_list.append(f"- {clip_path.name}")

    html_content = f"""
    <h2>Major Penalty Clips Require Review</h2>
    <p><strong>Game:</strong> Amherst Ramblers vs {opponent_name}</p>
    <p><strong>Date:</strong> {game_date}</p>

    <h3>Penalties:</h3>
    <ul>
    {''.join(f'<li>{p[2:]}</li>' for p in penalty_list)}
    </ul>

    <h3>Instructions:</h3>
    <ol>
        <li>Open the <a href="{folder_url}">Google Drive folder</a></li>
        <li>Watch each clip</li>
        <li>Edit the corresponding .json file:
            <ul>
                <li>Set <code>"reviewed": true</code></li>
                <li>Set <code>"include": true</code> or <code>false</code></li>
                <li>Optionally set <code>"trim_start"</code> and <code>"trim_end"</code> (seconds from clip start)</li>
            </ul>
        </li>
        <li>Once all JSONs are marked reviewed, clips will be processed automatically</li>
    </ol>

    <p><strong>Review deadline:</strong> 7 days</p>
    <p><a href="{folder_url}">Open Review Folder</a></p>
    """

    text_content = f"""
Major Penalty Clips Require Review

Game: Amherst Ramblers vs {opponent_name}
Date: {game_date}

Penalties:
{chr(10).join(penalty_list)}

Instructions:
1. Open the Google Drive folder: {folder_url}
2. Watch each clip
3. Edit the corresponding .json file:
   - Set "reviewed": true
   - Set "include": true or false
   - Optionally set "trim_start" and "trim_end" (seconds from clip start)
4. Once all JSONs are marked reviewed, clips will be processed automatically

Review deadline: 7 days
"""

    try:
        params = {
            "from": from_email,
            "to": [to_email],
            "subject": subject,
            "html": html_content,
            "text": text_content
        }
        try:
            resend.Emails.send(params)
            logger.info(f"Review notification sent to {to_email}")
            return True
        except Exception as e:
            # Retry with a known Resend-verified sender if the configured sender isn't verified.
            if from_email != fallback_from:
                logger.warning(f"Email send failed from '{from_email}', retrying with '{fallback_from}': {e}")
                params["from"] = fallback_from
                resend.Emails.send(params)
                logger.info(f"Review notification sent to {to_email}")
                return True
            raise
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


def enable_review_monitor(
    game_id: str,
    config,
    *,
    game_date: Optional[str] = None,
    game_dir: Optional[Path] = None,
    major_review_dir: Optional[Path] = None,
    drive_folder_url: Optional[str] = None,
    resume_state_path: Optional[Path] = None,
) -> bool:
    """
    Enable the major review monitor cronjob by creating flag file.

    Args:
        game_id: Game identifier for tracking
        config: Configuration module

    Returns:
        True if flag file created successfully
    """
    flag_file = getattr(config, 'MAJOR_REVIEW_FLAG_FILE', Path('/tmp/major_review_active'))
    flag_file = Path(flag_file)

    try:
        drive_folder_id = None
        if drive_folder_url:
            m = re.search(r"/folders/([a-zA-Z0-9_-]+)", str(drive_folder_url))
            if m:
                drive_folder_id = m.group(1)

        flag_data = {
            'enabled': True,
            'game_id': game_id,
            'game_date': game_date or '',
            'game_dir': str(game_dir) if game_dir else '',
            'major_review_dir': str(major_review_dir) if major_review_dir else '',
            'drive_folder_url': drive_folder_url or '',
            'drive_folder_id': drive_folder_id or '',
            'resume_state_path': str(resume_state_path) if resume_state_path else '',
            'enabled_at': datetime.now().isoformat(),
            'check_count': 0
        }
        flag_file.write_text(json.dumps(flag_data, indent=2))
        logger.info(f"Major review monitor enabled: {flag_file}")
        return True
    except Exception as e:
        logger.error(f"Failed to enable review monitor: {e}")
        return False


def process_major_penalties(
    video_processor,
    penalties_data: List[Dict],
    game_id: str,
    game_date: str,
    game_info: Dict,
    output_dir: Path,
    config,
    video_timestamps: List[Dict] = None,
    resume_state_path: Optional[Path] = None,
    ocr_engine=None,
) -> Dict:
    """
    Main entry point: Process all major penalties for a game.

    This function:
    1. Detects major penalties
    2. Creates 2-minute clips
    3. Uploads to Google Drive
    4. Sends email notification
    5. Enables review monitor

    Args:
        video_processor: VideoProcessor instance with video loaded
        penalties_data: Raw penalty data from box score
        game_id: Game identifier
        game_date: Game date string
        game_info: Full game info dict
        output_dir: Local directory for temporary files
        config: Configuration module

    Returns:
        Dict with:
        - major_count: Number of major penalties found
        - clips_created: Number of clips created
        - drive_folder: Drive folder URL (or None)
        - email_sent: Whether notification was sent
    """
    result = {
        'major_count': 0,
        'clips_created': 0,
        'drive_folder': None,
        'email_sent': False
    }

    # Detect major penalties
    time_is_elapsed = bool(getattr(config, "BOX_SCORE_TIME_IS_ELAPSED", True))
    major_groups = detect_major_penalties(penalties_data, time_is_elapsed=time_is_elapsed)

    if not major_groups:
        logger.info("No major penalties detected in this game")
        return result

    result['major_count'] = sum(len(group) for group in major_groups)
    logger.info(f"Detected {result['major_count']} major penalties in {len(major_groups)} groups")

    # Assign video times to penalties using timestamps.
    # For majors, the clock typically *freezes* at the penalty time for an extended stoppage;
    # matching to the closest observed clock reading (including the freeze) is usually far
    # more accurate than extrapolating forward from the last running-clock sample.
    if video_timestamps:
        for group in major_groups:
            for penalty in group:
                if penalty.video_time is not None:
                    continue
                match = _find_penalty_video_time_from_timestamps(
                    penalty,
                    video_timestamps,
                    config,
                )
                match_meta = None
                if match is not None:
                    penalty.video_time, match_meta = match

                if penalty.video_time is not None and ocr_engine is not None:
                    refined = _refine_penalty_video_time_by_local_ocr(
                        video_processor,
                        ocr_engine,
                        penalty,
                        approx_video_time=float(penalty.video_time),
                        config=config,
                    )
                    if refined is not None:
                        penalty.video_time = refined
                    else:
                        # If our timestamp series was sparse/interpolated, run a broader OCR scan.
                        low_confidence = False
                        if match_meta:
                            if match_meta.get("interpolated"):
                                low_confidence = True
                            if int(match_meta.get("period_real_samples") or 0) < 3:
                                low_confidence = True
                        else:
                            low_confidence = True

                        if low_confidence:
                            scanned = _scan_video_for_penalty_time(
                                video_processor,
                                ocr_engine,
                                penalty,
                                approx_video_time=float(penalty.video_time),
                                config=config,
                            )
                            if scanned is not None:
                                penalty.video_time = scanned
                if penalty.video_time is None:
                    logger.warning(f"Could not assign video time for major P{penalty.period} {penalty.time} ({penalty.player_name})")

    # Create clips for each group
    clips_and_jsons = []
    for group in major_groups:
        # Check video times for penalties
        for penalty in group:
            if penalty.video_time is None:
                logger.warning(f"Major penalty P{penalty.period} {penalty.time} has no video time")

        clip_result = create_major_review_clip(
            video_processor,
            group,
            output_dir,
            config
        )
        if clip_result:
            clips_and_jsons.append(clip_result)

    result['clips_created'] = len(clips_and_jsons)

    if not clips_and_jsons:
        logger.warning("No major penalty clips could be created")
        return result

    # Upload to Drive
    folder_url = upload_to_drive(
        clips_and_jsons,
        game_id,
        game_date,
        config,
        game_info=game_info,
    )
    result['drive_folder'] = folder_url

    if folder_url:
        # Send email notification
        result['email_sent'] = send_review_notification(
            folder_url,
            game_info,
            clips_and_jsons,
            config
        )

        # Enable review monitor with enough state to resume later.
        inferred_game_dir = None
        try:
            inferred_game_dir = output_dir.parent.parent
        except Exception:
            inferred_game_dir = None
        enable_review_monitor(
            game_id,
            config,
            game_date=game_date,
            game_dir=inferred_game_dir,
            major_review_dir=output_dir,
            drive_folder_url=folder_url,
            resume_state_path=resume_state_path,
        )

    return result
