"""
Event Matcher - Syncs box score events with video timestamps using OCR data

This module matches events from box scores (goals, penalties) to their
corresponding timestamps in the video using OCR-extracted time data.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Union
import numpy as np

from .goal import Goal
from .time_utils import (
    time_string_to_seconds,
    period_time_to_absolute_seconds,
    seconds_to_time_string,
    PERIOD_LENGTH_MINUTES,
    PERIOD_LENGTH_SECONDS,
    OT_LENGTH_MINUTES,
    OT_LENGTH_SECONDS,
)

logger = logging.getLogger(__name__)


@dataclass
class EventMatchLog:
    """
    Detailed log entry for a single event match attempt.

    This captures all the information needed to debug why a particular
    event was matched to a particular video timestamp.
    """
    # Event info (from box score)
    event_type: str
    event_period: int
    event_time_boxscore: str  # Original time string from box score
    event_time_elapsed_seconds: int  # Interpreted elapsed seconds
    event_time_remaining_seconds: int  # Converted to remaining (for OCR match)

    # Match result
    matched_video_time: Optional[float] = None
    match_confidence: float = 0.0
    match_time_diff_seconds: float = 0.0
    match_method: str = "none"  # "exact_period", "interpolation", "fallback"
    match_unreliable: bool = False
    match_unreliable_reason: Optional[str] = None

    # OCR candidate info (what timestamps were considered)
    candidates_in_period: int = 0
    best_candidate_ocr_time: Optional[str] = None
    best_candidate_ocr_seconds: Optional[int] = None
    best_candidate_video_time: Optional[float] = None

    # All candidates (for detailed debugging)
    all_candidates: List[Dict] = field(default_factory=list)

    # Additional context
    player: Optional[str] = None
    team: Optional[str] = None
    extra_info: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "event": {
                "type": self.event_type,
                "period": self.event_period,
                "time_boxscore": self.event_time_boxscore,
                "time_elapsed_seconds": self.event_time_elapsed_seconds,
                "time_remaining_seconds": self.event_time_remaining_seconds,
                "player": self.player,
                "team": self.team,
            },
            "match_result": {
                "video_time": self.matched_video_time,
                "confidence": self.match_confidence,
                "time_diff_seconds": self.match_time_diff_seconds,
                "method": self.match_method,
                "unreliable": self.match_unreliable,
                "unreliable_reason": self.match_unreliable_reason,
            },
            "ocr_candidates": {
                "count_in_period": self.candidates_in_period,
                "best_ocr_time": self.best_candidate_ocr_time,
                "best_ocr_seconds": self.best_candidate_ocr_seconds,
                "best_video_time": self.best_candidate_video_time,
            },
            "extra": self.extra_info,
        }

    def to_human_readable(self) -> str:
        """Format as human-readable log entry."""
        lines = [
            f"{'='*60}",
            f"EVENT: {self.event_type.upper()}",
            f"  Box Score: P{self.event_period} {self.event_time_boxscore} "
            f"(elapsed: {self.event_time_elapsed_seconds}s, remaining: {self.event_time_remaining_seconds}s)",
        ]

        if self.player:
            lines.append(f"  Player: {self.player} ({self.team})")

        if self.matched_video_time is not None:
            lines.extend([
                f"",
                f"MATCH RESULT:",
                f"  Video Time: {self.matched_video_time:.1f}s "
                f"({self._format_video_time(self.matched_video_time)})",
                f"  Confidence: {self.match_confidence:.0%}",
                f"  Time Diff: {self.match_time_diff_seconds:.1f}s",
                f"  Method: {self.match_method}",
            ])

            if self.match_unreliable:
                lines.append(f"  ⚠️ UNRELIABLE: {self.match_unreliable_reason}")
        else:
            lines.extend([
                f"",
                f"MATCH RESULT: NO MATCH FOUND",
            ])

        lines.extend([
            f"",
            f"OCR CANDIDATES (P{self.event_period}):",
            f"  Total in period: {self.candidates_in_period}",
        ])

        if self.best_candidate_ocr_time:
            lines.append(
                f"  Best candidate: {self.best_candidate_ocr_time} "
                f"({self.best_candidate_ocr_seconds}s remaining) "
                f"at video {self.best_candidate_video_time:.1f}s"
            )

        # Show top 5 candidates if available
        if self.all_candidates:
            lines.append(f"  Top candidates by time diff:")
            for i, cand in enumerate(self.all_candidates[:5]):
                lines.append(
                    f"    {i+1}. OCR: {cand.get('ocr_time', 'N/A')} "
                    f"(remaining: {cand.get('ocr_seconds', 'N/A')}s) "
                    f"at video {cand.get('video_time', 0):.1f}s "
                    f"| diff: {cand.get('time_diff', 0):.1f}s"
                )

        return "\n".join(lines)

    @staticmethod
    def _format_video_time(seconds: float) -> str:
        """Format video time as HH:MM:SS."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        if hours > 0:
            return f"{hours}:{minutes:02d}:{secs:02d}"
        return f"{minutes}:{secs:02d}"


class EventMatchLogger:
    """
    Manages detailed logging for event matching.

    Creates a log file for each game with detailed information about
    every match attempt, useful for debugging timing issues.
    """

    def __init__(self, output_dir: Optional[Path] = None, game_id: str = "unknown"):
        self.output_dir = output_dir
        self.game_id = game_id
        self.entries: List[EventMatchLog] = []
        self.scoreboard_health: Optional[Dict] = None
        self.start_time = datetime.now()

    def add_entry(self, entry: EventMatchLog):
        """Add a match log entry."""
        self.entries.append(entry)

    def set_scoreboard_health(self, health: "ScoreboardHealth"):
        """Store scoreboard health for the log."""
        self.scoreboard_health = health.to_dict()

    def write_logs(self):
        """Write logs to files (both JSON and human-readable)."""
        if not self.output_dir:
            logger.debug("No output_dir specified - skipping log file write")
            return

        output_path = Path(self.output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Write JSON log (for programmatic analysis)
        json_path = output_path / "event_matching_log.json"
        try:
            json_data = {
                "game_id": self.game_id,
                "timestamp": self.start_time.isoformat(),
                "scoreboard_health": self.scoreboard_health,
                "total_events": len(self.entries),
                "matched_events": sum(1 for e in self.entries if e.matched_video_time is not None),
                "unreliable_events": sum(1 for e in self.entries if e.match_unreliable),
                "entries": [e.to_dict() for e in self.entries],
            }
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=2)
            logger.info(f"Wrote event matching JSON log to: {json_path}")
        except Exception as e:
            logger.error(f"Failed to write JSON log: {e}")

        # Write human-readable log (for manual review)
        txt_path = output_path / "event_matching_log.txt"
        try:
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(f"EVENT MATCHING LOG - {self.game_id}\n")
                f.write(f"Generated: {self.start_time.isoformat()}\n")
                f.write(f"{'='*60}\n\n")

                # Summary
                matched = sum(1 for e in self.entries if e.matched_video_time is not None)
                unreliable = sum(1 for e in self.entries if e.match_unreliable)
                f.write(f"SUMMARY:\n")
                f.write(f"  Total events: {len(self.entries)}\n")
                f.write(f"  Matched: {matched}\n")
                f.write(f"  Unreliable: {unreliable}\n")
                f.write(f"\n")

                # Scoreboard health
                if self.scoreboard_health:
                    f.write(f"SCOREBOARD HEALTH:\n")
                    f.write(f"  Score: {self.scoreboard_health.get('health_score', 0):.0%}\n")
                    f.write(f"  Healthy: {self.scoreboard_health.get('is_healthy', False)}\n")
                    issues = self.scoreboard_health.get('issues', [])
                    if issues:
                        f.write(f"  Issues:\n")
                        for issue in issues:
                            f.write(f"    - {issue}\n")
                    f.write(f"\n")

                # Detailed entries
                f.write(f"DETAILED MATCH LOG:\n")
                f.write(f"\n")
                for entry in self.entries:
                    f.write(entry.to_human_readable())
                    f.write("\n\n")

            logger.info(f"Wrote event matching text log to: {txt_path}")
        except Exception as e:
            logger.error(f"Failed to write text log: {e}")


@dataclass
class NormalizationLogEntry:
    """Log entry for a sample during normalization."""
    video_time: float
    original_period: Optional[int]
    original_time: str
    original_seconds: int
    action: str  # "kept", "discarded"
    reason: Optional[str] = None
    assigned_period: Optional[int] = None
    notes: Optional[str] = None

    def to_dict(self) -> Dict:
        return {
            "video_time": self.video_time,
            "original": {
                "period": self.original_period,
                "time": self.original_time,
                "seconds": self.original_seconds,
            },
            "action": self.action,
            "reason": self.reason,
            "assigned_period": self.assigned_period,
            "notes": self.notes,
        }


class NormalizationLogger:
    """
    Logs normalization decisions for debugging OCR timestamp cleanup.

    Tracks which samples are kept vs discarded and why, which is critical
    for understanding why event matching may have too few candidates.
    """

    def __init__(self, output_dir: Optional[Path] = None, game_id: str = "unknown"):
        self.output_dir = output_dir
        self.game_id = game_id
        self.entries: List[NormalizationLogEntry] = []
        self.period_transitions: List[Dict] = []
        self.start_time = datetime.now()

    def add_entry(self, entry: NormalizationLogEntry):
        """Add a normalization log entry."""
        self.entries.append(entry)

    def add_period_transition(self, video_time: float, from_period: int, to_period: int, reason: str):
        """Log a detected period transition."""
        self.period_transitions.append({
            "video_time": video_time,
            "from_period": from_period,
            "to_period": to_period,
            "reason": reason,
        })

    def write_logs(self):
        """Write normalization logs to files."""
        if not self.output_dir:
            return

        output_path = Path(self.output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Calculate stats
        total = len(self.entries)
        kept = sum(1 for e in self.entries if e.action == "kept")
        discarded = total - kept

        # Group discards by reason
        discard_reasons: Dict[str, int] = {}
        for e in self.entries:
            if e.action == "discarded" and e.reason:
                discard_reasons[e.reason] = discard_reasons.get(e.reason, 0) + 1

        # Write JSON log
        json_path = output_path / "normalization_log.json"
        try:
            json_data = {
                "game_id": self.game_id,
                "timestamp": self.start_time.isoformat(),
                "summary": {
                    "total_samples": total,
                    "kept": kept,
                    "discarded": discarded,
                    "keep_rate": f"{100*kept/total:.1f}%" if total > 0 else "N/A",
                },
                "discard_reasons": discard_reasons,
                "period_transitions": self.period_transitions,
                "entries": [e.to_dict() for e in self.entries],
            }
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=2)
            logger.info(f"Wrote normalization log to: {json_path}")
        except Exception as e:
            logger.error(f"Failed to write normalization JSON log: {e}")

        # Write human-readable log
        txt_path = output_path / "normalization_log.txt"
        try:
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(f"NORMALIZATION LOG - {self.game_id}\n")
                f.write(f"Generated: {self.start_time.isoformat()}\n")
                f.write("=" * 70 + "\n\n")

                # Summary
                f.write("SUMMARY:\n")
                f.write(f"  Total samples: {total}\n")
                f.write(f"  Kept: {kept} ({100*kept/total:.1f}%)\n" if total > 0 else "  Kept: N/A\n")
                f.write(f"  Discarded: {discarded}\n")
                f.write("\n")

                # Discard reasons
                if discard_reasons:
                    f.write("DISCARD REASONS:\n")
                    for reason, count in sorted(discard_reasons.items(), key=lambda x: -x[1]):
                        f.write(f"  {reason}: {count}\n")
                    f.write("\n")

                # Period transitions
                if self.period_transitions:
                    f.write("PERIOD TRANSITIONS DETECTED:\n")
                    for pt in self.period_transitions:
                        vt = pt['video_time']
                        vt_fmt = f"{int(vt//60)}:{int(vt%60):02d}"
                        f.write(f"  [{vt_fmt}] P{pt['from_period']} -> P{pt['to_period']}: {pt['reason']}\n")
                    f.write("\n")

                # Per-period stats
                period_kept = {}
                period_discarded = {}
                for e in self.entries:
                    p = e.assigned_period or e.original_period or 0
                    if e.action == "kept":
                        period_kept[p] = period_kept.get(p, 0) + 1
                    else:
                        period_discarded[p] = period_discarded.get(p, 0) + 1

                f.write("PER-PERIOD STATS:\n")
                all_periods = set(period_kept.keys()) | set(period_discarded.keys())
                for p in sorted(all_periods):
                    k = period_kept.get(p, 0)
                    d = period_discarded.get(p, 0)
                    label = f"P{p}" if p > 0 else "Unknown"
                    f.write(f"  {label}: {k} kept, {d} discarded\n")
                f.write("\n")

                # Detailed log
                f.write("DETAILED SAMPLE LOG:\n")
                f.write("-" * 70 + "\n")
                for e in self.entries:
                    vt = e.video_time
                    vt_fmt = f"{int(vt//60)}:{int(vt%60):02d}"
                    orig_p = f"P{e.original_period}" if e.original_period else "P?"
                    if e.action == "kept":
                        assigned = f"->P{e.assigned_period}" if e.assigned_period != e.original_period else ""
                        f.write(f"[{vt_fmt}] KEPT   {orig_p} {e.original_time}{assigned}\n")
                    else:
                        f.write(f"[{vt_fmt}] DISCARD {orig_p} {e.original_time} | {e.reason}\n")

            logger.info(f"Wrote normalization text log to: {txt_path}")
        except Exception as e:
            logger.error(f"Failed to write normalization text log: {e}")


@dataclass
class ScoreboardHealth:
    """
    Tracks scoreboard OCR health metrics to detect broken/unreliable scoreboards.

    A scoreboard is considered unhealthy when:
    - Most period tokens are undetected (period=0 or None)
    - Clock readings are wildly inconsistent (high variance)
    - Clock doesn't start near 20:00 at game start
    - Too few usable samples
    """
    total_samples: int = 0
    samples_with_period: int = 0
    samples_period_unknown: int = 0
    samples_near_20_00_at_start: int = 0
    clock_variance: float = 0.0
    usable_samples_after_normalize: int = 0

    # Computed health indicators
    is_healthy: bool = True
    health_score: float = 1.0
    issues: List[str] = field(default_factory=list)

    # Thresholds
    MIN_PERIOD_DETECTION_RATIO: float = 0.3  # At least 30% must have period detected
    MIN_USABLE_SAMPLES: int = 10
    MIN_SAMPLES_NEAR_START: int = 1  # At least 1 sample near 20:00 at game start
    MAX_CLOCK_VARIANCE: float = 300.0  # Seconds - high variance indicates broken clock

    def assess(self) -> "ScoreboardHealth":
        """Assess overall scoreboard health based on collected metrics."""
        self.issues = []
        self.health_score = 1.0

        if self.total_samples == 0:
            self.issues.append("No OCR samples available")
            self.health_score = 0.0
            self.is_healthy = False
            return self

        # Check period detection ratio
        period_ratio = self.samples_with_period / self.total_samples if self.total_samples > 0 else 0
        if period_ratio < self.MIN_PERIOD_DETECTION_RATIO:
            self.issues.append(
                f"Low period detection: {period_ratio:.1%} "
                f"({self.samples_with_period}/{self.total_samples} samples have period)"
            )
            self.health_score -= 0.4

        # Check usable samples after normalization
        if self.usable_samples_after_normalize < self.MIN_USABLE_SAMPLES:
            self.issues.append(
                f"Too few usable samples: {self.usable_samples_after_normalize} "
                f"(need at least {self.MIN_USABLE_SAMPLES})"
            )
            self.health_score -= 0.3

        # Check for valid start-of-game clock reading
        if self.samples_near_20_00_at_start < self.MIN_SAMPLES_NEAR_START:
            self.issues.append(
                f"Missing game start: no samples near 20:00 in early P1 "
                f"(scoreboard may have been stuck at 0:00)"
            )
            self.health_score -= 0.2

        # Check clock consistency
        if self.clock_variance > self.MAX_CLOCK_VARIANCE:
            self.issues.append(
                f"High clock variance: {self.clock_variance:.1f}s "
                f"(clock readings inconsistent, possible scoreboard glitch)"
            )
            self.health_score -= 0.3

        self.health_score = max(0.0, min(1.0, self.health_score))
        self.is_healthy = self.health_score >= 0.5 and len(self.issues) <= 1

        return self

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "total_samples": self.total_samples,
            "samples_with_period": self.samples_with_period,
            "samples_period_unknown": self.samples_period_unknown,
            "samples_near_20_00_at_start": self.samples_near_20_00_at_start,
            "clock_variance": self.clock_variance,
            "usable_samples_after_normalize": self.usable_samples_after_normalize,
            "is_healthy": self.is_healthy,
            "health_score": self.health_score,
            "issues": self.issues,
        }

    def log_status(self, game_id: str = "unknown"):
        """Log scoreboard health status with appropriate severity."""
        if self.is_healthy:
            logger.info(f"[{game_id}] Scoreboard health: OK (score={self.health_score:.2f})")
        else:
            logger.warning(
                f"⚠️ [{game_id}] SCOREBOARD UNHEALTHY (score={self.health_score:.2f})"
            )
            for issue in self.issues:
                logger.warning(f"  - {issue}")
            logger.warning(
                "  → Event matching may be unreliable. "
                "Manual review recommended for this game."
            )

    def write_alert_file(self, output_dir: Path, game_id: str = "unknown"):
        """Write an alert file when scoreboard is unhealthy for manual review."""
        if self.is_healthy:
            return

        alert_path = output_dir / "SCOREBOARD_ALERT.txt"
        try:
            with open(alert_path, "w", encoding="utf-8") as f:
                f.write(f"SCOREBOARD HEALTH ALERT - {game_id}\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Health Score: {self.health_score:.2f}\n")
                f.write(f"Status: {'HEALTHY' if self.is_healthy else 'UNHEALTHY'}\n\n")
                f.write("Issues Detected:\n")
                for issue in self.issues:
                    f.write(f"  - {issue}\n")
                f.write("\n")
                f.write("Metrics:\n")
                f.write(f"  Total OCR samples: {self.total_samples}\n")
                f.write(f"  Samples with period detected: {self.samples_with_period}\n")
                f.write(f"  Samples with unknown period: {self.samples_period_unknown}\n")
                f.write(f"  Samples near 20:00 at start: {self.samples_near_20_00_at_start}\n")
                f.write(f"  Clock variance: {self.clock_variance:.1f}s\n")
                f.write(f"  Usable samples after normalization: {self.usable_samples_after_normalize}\n")
                f.write("\n")
                f.write("RECOMMENDATION:\n")
                f.write("  Manual review required. Event-to-video matching may be incorrect.\n")
                f.write("  Check clip timing before publishing highlights.\n")
            logger.info(f"Wrote scoreboard alert to: {alert_path}")
        except Exception as e:
            logger.error(f"Failed to write scoreboard alert file: {e}")

    def send_email_alert(self, game_id: str = "unknown", config=None):
        """Send email alert via Resend when scoreboard is unhealthy."""
        if self.is_healthy:
            return

        # Get Resend config
        api_key = None
        email_to = None
        email_from = None

        if config:
            api_key = getattr(config, 'RESEND_API_KEY', None)
            email_to = getattr(config, 'NOTIFICATION_EMAIL_TO', None)
            email_from = getattr(config, 'NOTIFICATION_EMAIL_FROM', 'onboarding@resend.dev')

        if not api_key or not email_to:
            logger.debug("Resend not configured - skipping email alert")
            return

        try:
            import resend
            resend.api_key = api_key

            subject = f"⚠️ Scoreboard Alert: {game_id}"

            issues_html = "".join(f"<li>{issue}</li>" for issue in self.issues)

            html_body = f"""
            <h2>🏒 Scoreboard Health Alert</h2>
            <p><strong>Game:</strong> {game_id}</p>
            <p><strong>Health Score:</strong> {self.health_score:.0%}</p>
            <p><strong>Status:</strong> <span style="color: red; font-weight: bold;">UNHEALTHY</span></p>

            <h3>Issues Detected:</h3>
            <ul style="color: #c00;">
                {issues_html}
            </ul>

            <h3>Metrics:</h3>
            <table style="border-collapse: collapse;">
                <tr><td style="padding: 4px 12px;">Total OCR samples:</td><td>{self.total_samples}</td></tr>
                <tr><td style="padding: 4px 12px;">Samples with period:</td><td>{self.samples_with_period}</td></tr>
                <tr><td style="padding: 4px 12px;">Unknown period:</td><td>{self.samples_period_unknown}</td></tr>
                <tr><td style="padding: 4px 12px;">Near 20:00 at start:</td><td>{self.samples_near_20_00_at_start}</td></tr>
                <tr><td style="padding: 4px 12px;">Usable after normalize:</td><td>{self.usable_samples_after_normalize}</td></tr>
            </table>

            <h3>What This Means:</h3>
            <p>The broadcast scoreboard was broken, stuck, or unreadable during parts of this game.
            Event-to-video matching may be incorrect. <strong>Manual review required</strong> before publishing highlights.</p>

            <h3>Next Steps:</h3>
            <ol>
                <li>Check the <code>SCOREBOARD_ALERT.txt</code> file in the game's data folder</li>
                <li>Review clips manually to verify timing</li>
                <li>Consider adding manual time overrides to a JSON file for problematic periods</li>
            </ol>

            <hr>
            <p style="color: #666; font-size: 12px;">
                This alert was generated automatically by the highlight extraction pipeline.
            </p>
            """

            text_body = f"""
SCOREBOARD HEALTH ALERT
=======================

Game: {game_id}
Health Score: {self.health_score:.0%}
Status: UNHEALTHY

Issues Detected:
{chr(10).join('  - ' + issue for issue in self.issues)}

Metrics:
  Total OCR samples: {self.total_samples}
  Samples with period: {self.samples_with_period}
  Unknown period: {self.samples_period_unknown}
  Near 20:00 at start: {self.samples_near_20_00_at_start}
  Usable after normalize: {self.usable_samples_after_normalize}

The broadcast scoreboard was broken or unreadable. Manual review required.
            """

            resend.Emails.send({
                "from": email_from,
                "to": [email_to],
                "subject": subject,
                "html": html_body,
                "text": text_body,
            })

            logger.info(f"📧 Sent scoreboard alert email to {email_to}")

        except ImportError:
            logger.warning("Resend package not installed - cannot send email alert")
        except Exception as e:
            logger.error(f"Failed to send scoreboard alert email: {e}")


class EventMatcher:
    """Matches box score events to video timestamps"""

    # Hockey period lengths (using centralized constants)
    PERIOD_LENGTH = PERIOD_LENGTH_MINUTES
    OT_LENGTH = OT_LENGTH_MINUTES

    # Minimum confidence threshold - matches below this are flagged as unreliable
    MIN_CONFIDENCE_THRESHOLD = 0.5

    def __init__(self, config=None):
        """
        Initialize EventMatcher

        Args:
            config: Optional configuration object
        """
        self.config = config
        self.scoreboard_health: Optional[ScoreboardHealth] = None

    def assess_scoreboard_health(
        self,
        video_timestamps: List[Dict],
        normalized_timestamps: List[Dict],
    ) -> ScoreboardHealth:
        """
        Assess the health of the scoreboard based on OCR samples.

        Args:
            video_timestamps: Raw OCR samples
            normalized_timestamps: Samples after normalization

        Returns:
            ScoreboardHealth object with assessment results
        """
        health = ScoreboardHealth()
        health.total_samples = len(video_timestamps)
        health.usable_samples_after_normalize = len(normalized_timestamps)

        if not video_timestamps:
            return health.assess()

        # Count period detection
        for ts in video_timestamps:
            period = ts.get("period")
            if period is not None and period != 0 and 1 <= period <= 5:
                health.samples_with_period += 1
            else:
                health.samples_period_unknown += 1

        # Check for valid start-of-game readings (near 20:00 in early video)
        sorted_ts = sorted(video_timestamps, key=lambda t: t.get("video_time", float("inf")))
        # Check first 20% of samples or first 5 minutes of video
        early_cutoff_video_time = min(
            sorted_ts[len(sorted_ts) // 5]["video_time"] if len(sorted_ts) >= 5 else float("inf"),
            sorted_ts[0].get("video_time", 0) + 300 if sorted_ts else 300
        )

        for ts in sorted_ts:
            if ts.get("video_time", float("inf")) > early_cutoff_video_time:
                break
            game_seconds = ts.get("game_time_seconds", 0)
            # Near 20:00 means 19:00+ (1140+ seconds remaining)
            if game_seconds >= 1140:
                health.samples_near_20_00_at_start += 1

        # Calculate clock variance within consecutive samples
        # High variance indicates erratic/broken clock
        if len(normalized_timestamps) >= 2:
            sorted_norm = sorted(normalized_timestamps, key=lambda t: t.get("video_time", 0))
            deltas = []
            for i in range(1, len(sorted_norm)):
                prev_clock = sorted_norm[i - 1].get("game_time_seconds", 0)
                curr_clock = sorted_norm[i].get("game_time_seconds", 0)
                prev_video = sorted_norm[i - 1].get("video_time", 0)
                curr_video = sorted_norm[i].get("video_time", 0)

                video_delta = curr_video - prev_video
                clock_delta = prev_clock - curr_clock  # Clock counts down

                if video_delta > 0:
                    # Expected: clock_delta ≈ video_delta (within some tolerance)
                    # Deviation from expected indicates clock issues
                    expected_clock_delta = video_delta
                    deviation = abs(clock_delta - expected_clock_delta)
                    deltas.append(deviation)

            if deltas:
                health.clock_variance = float(np.std(deltas)) if len(deltas) > 1 else deltas[0]

        self.scoreboard_health = health.assess()
        return self.scoreboard_health

    def match_events_to_video(
        self,
        events: List[Dict],
        video_timestamps: List[Dict],
        tolerance_seconds: int = 30,
        game_id: str = "unknown",
        output_dir: Optional[Path] = None,
        recording_game_start_time: Optional[float] = None,
    ) -> List[Dict]:
        """
        Match box score events to video timestamps

        Args:
            events: List of event dicts from box score (with period, time)
            video_timestamps: List of sampled video timestamps (with video_time, period, game_time)
            tolerance_seconds: Maximum time difference for matching (seconds)
            game_id: Game identifier for logging
            output_dir: Directory to write alert files (optional)

        Returns:
            List of events with video_time added
        """
        matched_events = []

        # Initialize detailed match logger
        match_logger = EventMatchLogger(
            output_dir=Path(output_dir) if output_dir else None,
            game_id=game_id
        )

        if not video_timestamps:
            logger.warning("No video timestamps available for matching")
            return events

        normalized_timestamps = self._normalize_video_timestamps(
            video_timestamps,
            output_dir=Path(output_dir) if output_dir else None,
            game_id=game_id,
        )
        if not normalized_timestamps:
            logger.warning("No usable video timestamps after normalization")
            return events

        # Assess scoreboard health before matching
        health = self.assess_scoreboard_health(video_timestamps, normalized_timestamps)
        health.log_status(game_id)
        match_logger.set_scoreboard_health(health)

        if output_dir:
            health.write_alert_file(output_dir, game_id)

        # Send email alert if scoreboard is unhealthy
        if not health.is_healthy:
            health.send_email_alert(game_id, config=self.config)

        # Determine confidence threshold based on scoreboard health
        # If scoreboard is unhealthy, require higher confidence for matches
        min_confidence = self.MIN_CONFIDENCE_THRESHOLD
        if not health.is_healthy:
            min_confidence = max(0.7, min_confidence)  # Require 70%+ confidence
            logger.warning(
                f"[{game_id}] Scoreboard unhealthy - raising minimum confidence "
                f"threshold to {min_confidence:.0%}"
            )

        logger.info(
            f"Matching {len(events)} events to {len(normalized_timestamps)} video timestamps"
        )

        low_confidence_count = 0
        for event in events:
            try:
                event_period = event.get('period', 1)
                event_time = event.get('time', '00:00')
                event_seconds_elapsed = self._time_to_seconds(event_time)
                event_seconds_remaining = self._event_time_to_remaining_seconds(event_period, event_time)

                # Create log entry for this event
                log_entry = EventMatchLog(
                    event_type=event.get('type', 'unknown'),
                    event_period=event_period,
                    event_time_boxscore=event_time,
                    event_time_elapsed_seconds=event_seconds_elapsed,
                    event_time_remaining_seconds=event_seconds_remaining,
                    player=event.get('player') or event.get('scorer'),
                    team=event.get('team'),
                )

                # Get OCR candidates in this period for logging
                period_candidates = [
                    ts for ts in normalized_timestamps
                    if ts.get('period') == event_period
                ]
                log_entry.candidates_in_period = len(period_candidates)

                # Calculate time diff for all candidates and sort by closest
                candidates_with_diff = []
                for ts in period_candidates:
                    ts_seconds = ts.get('game_time_seconds', 0)
                    time_diff = abs(event_seconds_remaining - ts_seconds)
                    candidates_with_diff.append({
                        'video_time': ts.get('video_time', 0),
                        'ocr_time': ts.get('game_time', '0:00'),
                        'ocr_seconds': ts_seconds,
                        'time_diff': time_diff,
                        'period': ts.get('period'),
                    })
                candidates_with_diff.sort(key=lambda c: c['time_diff'])
                log_entry.all_candidates = candidates_with_diff[:10]  # Keep top 10

                if candidates_with_diff:
                    best = candidates_with_diff[0]
                    log_entry.best_candidate_ocr_time = best['ocr_time']
                    log_entry.best_candidate_ocr_seconds = best['ocr_seconds']
                    log_entry.best_candidate_video_time = best['video_time']

                # Find closest video timestamp for this event with confidence
                match_result = self._find_closest_timestamp_with_confidence(
                    event,
                    normalized_timestamps,
                    tolerance_seconds,
                    recording_game_start_time=recording_game_start_time,
                )

                if match_result is not None:
                    video_time, confidence, time_diff, match_method = match_result

                    # Update log entry with match result
                    log_entry.matched_video_time = video_time
                    log_entry.match_confidence = confidence
                    log_entry.match_time_diff_seconds = time_diff
                    log_entry.match_method = match_method

                    # Create new event dict with video_time and confidence
                    matched_event = event.copy()
                    matched_event['match_confidence'] = confidence
                    matched_event['match_time_diff_seconds'] = time_diff
                    matched_event['scoreboard_health_score'] = health.health_score

                    # FAIL-SAFE: Skip low-confidence matches when scoreboard is broken
                    if confidence < min_confidence:
                        low_confidence_count += 1
                        matched_event['match_unreliable'] = True
                        unreliable_reason = (
                            f"Low confidence ({confidence:.0%}) with unhealthy scoreboard"
                            if not health.is_healthy
                            else f"Low confidence ({confidence:.0%})"
                        )
                        matched_event['match_unreliable_reason'] = unreliable_reason
                        log_entry.match_unreliable = True
                        log_entry.match_unreliable_reason = unreliable_reason

                        if not health.is_healthy:
                            # Don't assign video_time for unreliable matches on broken scoreboards
                            logger.warning(
                                f"⚠️ SKIPPED unreliable match: {event['type']} at P{event['period']} {event['time']} "
                                f"(confidence: {confidence:.0%}, diff: {time_diff:.0f}s) - scoreboard unhealthy"
                            )
                        else:
                            # Assign but flag as unreliable
                            matched_event['video_time'] = video_time
                            logger.warning(
                                f"⚠️ LOW CONFIDENCE match: {event['type']} at P{event['period']} {event['time']} "
                                f"to video time {video_time:.1f}s (confidence: {confidence:.0%}, diff: {time_diff:.0f}s)"
                            )
                    else:
                        matched_event['video_time'] = video_time
                        matched_event['match_unreliable'] = False
                        log_entry.match_unreliable = False
                        logger.debug(
                            f"Matched {event['type']} at P{event['period']} {event['time']} "
                            f"to video time {video_time:.1f}s (confidence: {confidence:.2f}, diff: {time_diff:.1f}s)"
                        )

                    matched_events.append(matched_event)
                else:
                    logger.warning(
                        f"Could not match {event['type']} at P{event['period']} {event['time']}"
                    )
                    # Still include event but without video_time
                    event_copy = event.copy()
                    event_copy['match_unreliable'] = True
                    event_copy['match_unreliable_reason'] = "No matching timestamp found"
                    log_entry.match_unreliable = True
                    log_entry.match_unreliable_reason = "No matching timestamp found"
                    matched_events.append(event_copy)

                # Add log entry
                match_logger.add_entry(log_entry)

            except Exception as e:
                logger.error(f"Error matching event: {e}")
                event_copy = event.copy()
                event_copy['match_unreliable'] = True
                event_copy['match_unreliable_reason'] = f"Matching error: {e}"
                matched_events.append(event_copy)

        # Write detailed logs
        match_logger.write_logs()

        # Count successful matches
        successful = sum(1 for e in matched_events if e.get('video_time') is not None)
        reliable = sum(1 for e in matched_events if e.get('video_time') is not None and not e.get('match_unreliable'))

        logger.info(f"Matched {successful}/{len(events)} events ({reliable} reliable, {low_confidence_count} low-confidence)")

        if not health.is_healthy:
            logger.warning(
                f"⚠️ [{game_id}] SCOREBOARD ISSUES DETECTED - {successful - reliable} events may have incorrect timing. "
                "Manual review recommended."
            )

        return matched_events

    def _find_closest_timestamp(
        self,
        event: Dict,
        video_timestamps: List[Dict],
        tolerance_seconds: int,
        *,
        recording_game_start_time: Optional[float] = None,
    ) -> Optional[float]:
        """
        Find the closest video timestamp for a box score event

        Args:
            event: Event dictionary with period and time
            video_timestamps: List of video timestamp dictionaries
            tolerance_seconds: Maximum allowed time difference

        Returns:
            Video time in seconds or None if no match found
        """
        event_period = event.get('period')
        event_time = event.get('time', '00:00')

        # Convert event time to seconds remaining (OCR clock is countdown)
        event_seconds = self._event_time_to_remaining_seconds(event_period, event_time)

        # Filter timestamps for matching period
        period_timestamps = [
            ts for ts in video_timestamps
            if ts.get('period') == event_period
        ]

        minimum_video_time = self.minimum_video_time_for_event(
            event,
            recording_game_start_time=recording_game_start_time,
        )
        if minimum_video_time is not None:
            period_timestamps = [
                ts for ts in period_timestamps
                if float(ts.get("video_time", -1.0) or -1.0) >= minimum_video_time
            ]

        if not period_timestamps:
            # Try interpolation if we have timestamps before and after this period
            return self._interpolate_timestamp(
                event,
                video_timestamps,
                recording_game_start_time=recording_game_start_time,
            )

        # Find timestamp with closest game time
        best_match = None
        best_diff = float('inf')

        for ts in period_timestamps:
            ts_seconds = ts.get('game_time_seconds', 0)

            # Calculate time difference
            # Note: Hockey clocks count DOWN, so we need to handle this
            time_diff = abs(event_seconds - ts_seconds)

            if time_diff < best_diff:
                best_diff = time_diff
                best_match = ts

        # Check if match is within tolerance
        if best_match and best_diff <= tolerance_seconds:
            return best_match['video_time']

        # If exact period match failed, try interpolation
        return self._interpolate_timestamp(
            event,
            video_timestamps,
            recording_game_start_time=recording_game_start_time,
        )

    def _find_closest_timestamp_with_confidence(
        self,
        event: Dict,
        video_timestamps: List[Dict],
        tolerance_seconds: int,
        *,
        recording_game_start_time: Optional[float] = None,
    ) -> Optional[Tuple[float, float, float, str]]:
        """
        Find the closest video timestamp for a box score event with confidence score

        Args:
            event: Event dictionary with period and time
            video_timestamps: List of video timestamp dictionaries
            tolerance_seconds: Maximum allowed time difference

        Returns:
            Tuple of (video_time, confidence, time_diff, match_method) or None if no match found
            confidence: 1.0 for exact match, decreases linearly to 0.0 at tolerance limit
            match_method: "exact_period", "interpolation_in_tolerance", "interpolation_fallback"
        """
        event_period = event.get('period')
        event_time = event.get('time', '00:00')

        # Convert event time to seconds remaining (OCR clock is countdown)
        event_seconds = self._event_time_to_remaining_seconds(event_period, event_time)

        # Filter timestamps for matching period
        period_timestamps = [
            ts for ts in video_timestamps
            if ts.get('period') == event_period
        ]

        minimum_video_time = self.minimum_video_time_for_event(
            event,
            recording_game_start_time=recording_game_start_time,
        )
        if minimum_video_time is not None:
            period_timestamps = [
                ts for ts in period_timestamps
                if float(ts.get("video_time", -1.0) or -1.0) >= minimum_video_time
            ]

        if not period_timestamps:
            # Try interpolation if we have timestamps before and after this period
            video_time = self._interpolate_timestamp(
                event,
                video_timestamps,
                recording_game_start_time=recording_game_start_time,
            )
            if video_time is not None:
                # Lower confidence for interpolated matches
                return (video_time, 0.5, tolerance_seconds / 2, "interpolation_no_period_match")
            return None

        # Find timestamp with closest game time
        best_match = None
        best_diff = float('inf')

        for ts in period_timestamps:
            ts_seconds = ts.get('game_time_seconds', 0)

            # Calculate time difference
            # Note: Hockey clocks count DOWN, so we need to handle this
            time_diff = abs(event_seconds - ts_seconds)

            if time_diff < best_diff:
                best_diff = time_diff
                best_match = ts

        # Check if match is within tolerance
        if best_match and best_diff <= tolerance_seconds:
            video_time = best_match['video_time']

            # Calculate confidence: 1.0 for exact match, 0.0 at tolerance limit
            # Using linear decay for simplicity
            if best_diff == 0:
                confidence = 1.0
            else:
                confidence = max(0.0, 1.0 - (best_diff / tolerance_seconds))

            return (video_time, confidence, best_diff, "exact_period")

        # If exact period match failed, try interpolation
        video_time = self._interpolate_timestamp(
            event,
            video_timestamps,
            recording_game_start_time=recording_game_start_time,
        )
        if video_time is not None:
            # Very low confidence for interpolated matches outside tolerance
            return (video_time, 0.3, tolerance_seconds, "interpolation_fallback")

        return None

    def _interpolate_timestamp(
        self,
        event: Dict,
        video_timestamps: List[Dict],
        *,
        recording_game_start_time: Optional[float] = None,
    ) -> Optional[float]:
        """
        Interpolate video timestamp when exact period match not found

        Args:
            event: Event dictionary
            video_timestamps: List of video timestamps

        Returns:
            Interpolated video time or None
        """
        try:
            event_period = event.get('period')
            event_time = event.get('time', '00:00')
            event_seconds = self._event_time_to_remaining_seconds(event_period, event_time)
            minimum_video_time = self.minimum_video_time_for_event(
                event,
                recording_game_start_time=recording_game_start_time,
            )

            # Convert event to absolute game time (seconds from game start)
            event_game_seconds = self._event_to_absolute_time(event_period, event_seconds)

            # Find timestamps before and after the event
            before = None
            after = None

            for ts in video_timestamps:
                if minimum_video_time is not None and float(ts.get("video_time", -1.0) or -1.0) < minimum_video_time:
                    continue
                ts_game_seconds = self._event_to_absolute_time(
                    ts['period'],
                    ts['game_time_seconds']
                )

                if ts_game_seconds <= event_game_seconds:
                    if before is None or ts_game_seconds > before['abs_time']:
                        before = {
                            'video_time': ts['video_time'],
                            'abs_time': ts_game_seconds
                        }

                if ts_game_seconds >= event_game_seconds:
                    if after is None or ts_game_seconds < after['abs_time']:
                        after = {
                            'video_time': ts['video_time'],
                            'abs_time': ts_game_seconds
                        }

            # Interpolate between before and after
            if before and after:
                # Linear interpolation
                total_time_diff = after['abs_time'] - before['abs_time']
                event_offset = event_game_seconds - before['abs_time']

                if total_time_diff > 0:
                    ratio = event_offset / total_time_diff
                    video_time_diff = after['video_time'] - before['video_time']
                    interpolated_time = before['video_time'] + (ratio * video_time_diff)

                    logger.debug(
                        f"Interpolated P{event_period} {event_time} to {interpolated_time:.1f}s"
                    )
                    return interpolated_time

            # If only before or after exists, use that
            if before:
                logger.debug(f"Using nearest timestamp before event: {before['video_time']:.1f}s")
                return before['video_time']

            if after:
                logger.debug(f"Using nearest timestamp after event: {after['video_time']:.1f}s")
                return after['video_time']

            return None

        except Exception as e:
            logger.error(f"Interpolation failed: {e}")
            return None

    def _event_to_absolute_time(self, period: int, time_seconds: int) -> int:
        """
        Convert period + time to absolute game time (seconds from start)

        Args:
            period: Period number (1, 2, 3, 4=OT)
            time_seconds: Time remaining in period (seconds)

        Returns:
            Absolute game time in seconds
        """
        return period_time_to_absolute_seconds(period, time_seconds)

    def _event_time_to_remaining_seconds(self, period: Optional[int], time_str: str) -> int:
        """
        Convert event time to seconds remaining in period.

        If box score times are elapsed, convert to remaining for OCR matching.
        """
        period_num = period or 1
        event_seconds = self._time_to_seconds(time_str)

        # Default to True to match config.py default (box scores use elapsed time)
        if getattr(self.config, 'BOX_SCORE_TIME_IS_ELAPSED', True):
            period_length = OT_LENGTH_SECONDS if period_num >= 4 else PERIOD_LENGTH_SECONDS
            event_seconds = max(0, min(event_seconds, period_length))
            return max(0, period_length - event_seconds)

        return event_seconds

    def event_time_to_remaining_seconds(self, period: Optional[int], time_str: str) -> int:
        """Public wrapper for event time → remaining seconds conversion."""
        return self._event_time_to_remaining_seconds(period, time_str)

    def minimum_video_time_for_event(
        self,
        event: Dict,
        *,
        recording_game_start_time: Optional[float] = None,
    ) -> Optional[float]:
        """
        Return the earliest plausible video timestamp for an event.

        For recorded full-game inputs, a goal at P1 15:21 cannot happen before
        15:21 of *game elapsed* time after puck drop. This guard rejects warmup
        samples that happen to show the same period/clock later used in-game.
        """
        if recording_game_start_time is None:
            return None
        if not bool(getattr(self.config, "EVENT_ENFORCE_MIN_VIDEO_TIME_FROM_GAME_START", True)):
            return None

        try:
            start_time = float(recording_game_start_time)
        except Exception:
            return None

        try:
            period = int(event.get("period") or 1)
        except Exception:
            period = 1
        time_str = str(event.get("time") or "0:00")

        try:
            remaining_seconds = self._event_time_to_remaining_seconds(period, time_str)
            absolute_game_seconds = float(self._event_to_absolute_time(period, remaining_seconds))
        except Exception:
            return None

        try:
            buffer_seconds = float(getattr(self.config, "EVENT_MIN_VIDEO_TIME_BUFFER_SECONDS", 240.0) or 240.0)
        except Exception:
            buffer_seconds = 240.0
        buffer_seconds = max(0.0, buffer_seconds)

        return max(0.0, start_time + absolute_game_seconds - buffer_seconds)

    def _time_to_seconds(self, time_str: str) -> int:
        """
        Convert MM:SS time string to seconds

        Args:
            time_str: Time in MM:SS format

        Returns:
            Time in seconds
        """
        return time_string_to_seconds(time_str)

    def match_goals_to_video(
        self,
        goals: List[Goal],
        video_timestamps: List[Dict],
        tolerance_seconds: int = 30,
        *,
        recording_game_start_time: Optional[float] = None,
    ) -> List[Goal]:
        """
        Match Goal objects to video timestamps.

        This is the preferred method for type-safe goal matching.

        Args:
            goals: List of Goal objects
            video_timestamps: List of video timestamp dictionaries
            tolerance_seconds: Maximum time difference for matching

        Returns:
            List of Goal objects with video_time and match_confidence set
        """
        matched_goals = []

        if not video_timestamps:
            logger.warning("No video timestamps available for matching")
            return goals

        normalized_timestamps = self._normalize_video_timestamps(video_timestamps)
        if not normalized_timestamps:
            logger.warning("No usable video timestamps after normalization")
            return goals

        logger.info(
            f"Matching {len(goals)} goals to {len(normalized_timestamps)} video timestamps"
        )

        for goal in goals:
            try:
                # Create event dict for matching using existing logic
                event_dict = {
                    'type': 'goal',
                    'period': goal.period,
                    'time': goal.time,
                    'team': goal.team,
                }

                # Find closest video timestamp
                match_result = self._find_closest_timestamp_with_confidence(
                    event_dict,
                    normalized_timestamps,
                    tolerance_seconds,
                    recording_game_start_time=recording_game_start_time,
                )

                if match_result is not None:
                    video_time, confidence, time_diff, match_method = match_result
                    matched_goal = goal.with_video_time(video_time, confidence)
                    matched_goals.append(matched_goal)

                    logger.debug(
                        f"Matched {goal} to video time {video_time:.1f}s "
                        f"(confidence: {confidence:.2f}, diff: {time_diff:.1f}s, method: {match_method})"
                    )
                else:
                    logger.warning(f"Could not match goal: {goal}")
                    matched_goals.append(goal)

            except Exception as e:
                logger.error(f"Error matching goal: {e}")
                matched_goals.append(goal)

        successful = sum(1 for g in matched_goals if g.is_matched)
        logger.info(f"Successfully matched {successful}/{len(goals)} goals")

        return matched_goals

    def _normalize_video_timestamps(
        self,
        video_timestamps: List[Dict],
        output_dir: Optional[Path] = None,
        game_id: str = "unknown"
    ) -> List[Dict]:
        """
        Normalize OCR timestamps to handle period misreads and time glitches.

        - Enforces mostly decreasing clock within a period
        - Infers period transitions on large time resets
        - Drops outliers that would break matching

        Args:
            video_timestamps: Raw OCR samples
            output_dir: Optional directory to write normalization logs
            game_id: Game identifier for logging
        """
        if not video_timestamps:
            return []

        # Initialize normalization logger
        norm_logger = NormalizationLogger(
            output_dir=Path(output_dir) if output_dir else None,
            game_id=game_id
        )

        increase_tolerance = 15
        max_rate_slack_seconds = 8

        sorted_ts = sorted(video_timestamps, key=lambda t: t.get('video_time', 0))
        normalized = []

        current_period = 1
        last_time_remaining = None
        last_video_time = None
        last_confidence = None
        # Track whether we've seen the start of the current period (≈20:00 / 5:00 OT).
        # This prevents a common failure mode at game start where OCR briefly reads "0:00"
        # before stabilizing at "20:00", which would otherwise be misinterpreted as a
        # period transition (0:00 → 20:00) and shift all periods by +1.
        seen_near_start_in_period = False

        for ts in sorted_ts:
            video_time = ts.get('video_time')
            if video_time is None:
                continue

            try:
                ts_period_raw = ts.get('period')
                ts_period = int(ts_period_raw) if ts_period_raw is not None else None
            except (TypeError, ValueError):
                ts_period = None
            if ts_period is not None and not (1 <= ts_period <= 5):
                ts_period = None

            time_remaining = ts.get('game_time_seconds')
            time_str = ts.get('game_time', '0:00')
            if time_remaining is None:
                time_remaining = self._time_to_seconds(time_str)

            try:
                ts_confidence = float(ts.get("ocr_confidence") or 0.0)
            except Exception:
                ts_confidence = 0.0

            # Validate against expected period length (OT is 5:00).
            expected_period = ts_period or current_period
            period_length = OT_LENGTH_SECONDS if expected_period >= 4 else PERIOD_LENGTH_SECONDS
            if time_remaining < 0 or time_remaining > period_length:
                norm_logger.add_entry(NormalizationLogEntry(
                    video_time=video_time,
                    original_period=ts_period,
                    original_time=time_str,
                    original_seconds=time_remaining,
                    action="discarded",
                    reason=f"Time out of range (0-{period_length}s)",
                ))
                continue

            if last_time_remaining is not None:
                # Guard against impossible "fast-forward" OCR errors.
                # The game clock can't run faster than real time, so if the
                # clock appears to drop more than dt (+slack), it's likely a bad read.
                if last_video_time is not None and video_time > last_video_time:
                    dt = video_time - last_video_time
                    clock_drop = last_time_remaining - time_remaining
                    if clock_drop > (dt + max_rate_slack_seconds):
                        norm_logger.add_entry(NormalizationLogEntry(
                            video_time=video_time,
                            original_period=ts_period,
                            original_time=time_str,
                            original_seconds=time_remaining,
                            action="discarded",
                            reason=f"Fast-forward error: clock dropped {clock_drop:.0f}s in {dt:.0f}s video",
                        ))
                        continue

                dt = (video_time - last_video_time) if (last_video_time is not None and video_time > last_video_time) else None
                long_gap = dt is not None and dt >= 300

                # Prefer explicit OCR period jumps when they look plausible.
                if ts_period is not None and ts_period > current_period:
                    ts_period_length = OT_LENGTH_SECONDS if ts_period >= 4 else PERIOD_LENGTH_SECONDS
                    near_start = time_remaining >= (ts_period_length - 60)
                    near_end_prev = last_time_remaining <= 120
                    if near_start and (near_end_prev or long_gap):
                        old_period = current_period
                        current_period = ts_period
                        norm_logger.add_period_transition(
                            video_time, old_period, current_period,
                            f"OCR period jump (near start={near_start}, near end prev={near_end_prev})"
                        )
                        last_time_remaining = None
                        last_video_time = None
                        last_confidence = None
                        seen_near_start_in_period = True

                if last_time_remaining is not None:
                    # Period reset: big jump back up to ~20:00 (or to ~5:00 for OT if inferred).
                    inferred_period_length = OT_LENGTH_SECONDS if current_period >= 4 else PERIOD_LENGTH_SECONDS
                    period_reset_threshold = int(inferred_period_length * 0.75)

                    if time_remaining > last_time_remaining + period_reset_threshold:
                        near_start = time_remaining >= (inferred_period_length - 60)
                        near_end = last_time_remaining <= 120
                        if near_start and (near_end or long_gap):
                            if seen_near_start_in_period or long_gap:
                                # Legitimate intermission reset.
                                old_period = current_period
                                current_period = min(5, current_period + 1)
                                norm_logger.add_period_transition(
                                    video_time, old_period, current_period,
                                    f"Clock reset to ~{time_remaining}s (intermission)"
                                )
                                last_time_remaining = None
                                last_video_time = None
                                last_confidence = None
                                seen_near_start_in_period = False
                            else:
                                # Likely a pre-game OCR glitch (e.g., "0:00" misread)
                                # followed by the true start-of-period clock ("20:00").
                                # Reset our baseline without incrementing the period.
                                last_time_remaining = None
                                last_video_time = None
                                last_confidence = None
                                seen_near_start_in_period = False
                        else:
                            # Likely an OCR glitch (e.g., misread minute digit)
                            norm_logger.add_entry(NormalizationLogEntry(
                                video_time=video_time,
                                original_period=ts_period,
                                original_time=time_str,
                                original_seconds=time_remaining,
                                action="discarded",
                                reason=f"Clock jumped up {time_remaining - last_time_remaining}s (OCR glitch)",
                            ))
                            continue
                    elif time_remaining > last_time_remaining + increase_tolerance:
                        # Allow the 3rd→OT transition even if OCR period isn't trusted:
                        # 0:xx → 4:xx (OT clock) is a legitimate reset but much smaller than a 20:00 reset.
                        if current_period == 3:
                            near_ot_start = time_remaining >= (OT_LENGTH_SECONDS - 60)
                            near_end_prev = last_time_remaining <= 30
                            if near_ot_start and (near_end_prev or long_gap):
                                old_period = current_period
                                current_period = 4
                                norm_logger.add_period_transition(
                                    video_time, old_period, current_period,
                                    "P3->OT transition (small clock jump)"
                                )
                                last_time_remaining = None
                                last_video_time = None
                                seen_near_start_in_period = False
                            else:
                                norm_logger.add_entry(NormalizationLogEntry(
                                    video_time=video_time,
                                    original_period=ts_period,
                                    original_time=time_str,
                                    original_seconds=time_remaining,
                                    action="discarded",
                                    reason=f"Clock increased {time_remaining - last_time_remaining}s (not OT transition)",
                                ))
                                continue
                        else:
                            # If the previous baseline was low-confidence and the current read is
                            # high-confidence, prefer resetting our baseline to the current sample
                            # rather than discarding it (common when a single OCR read misreads a digit).
                            prev_conf = float(last_confidence or 0.0)
                            if prev_conf < 40.0 and ts_confidence > 75.0:
                                # Drop the previous low-confidence sample so we don't keep a non-monotonic clock.
                                if normalized:
                                    try:
                                        normalized.pop()
                                    except Exception:
                                        pass
                                last_time_remaining = None
                                last_video_time = None
                                last_confidence = None
                                # fall through and keep this sample
                            else:
                                norm_logger.add_entry(NormalizationLogEntry(
                                    video_time=video_time,
                                    original_period=ts_period,
                                    original_time=time_str,
                                    original_seconds=time_remaining,
                                    action="discarded",
                                    reason=f"Clock increased {time_remaining - last_time_remaining}s (unexpected)",
                                ))
                                continue

            # Sample passed all checks - keep it
            norm_logger.add_entry(NormalizationLogEntry(
                video_time=video_time,
                original_period=ts_period,
                original_time=time_str,
                original_seconds=time_remaining,
                action="kept",
                assigned_period=current_period,
                notes=f"ocr_conf={ts_confidence:.0f}" if ts_confidence else None,
            ))

            normalized.append({
                **ts,
                'period': current_period,
                'game_time_seconds': time_remaining,
                'game_time': seconds_to_time_string(time_remaining),
            })
            last_time_remaining = time_remaining
            last_video_time = video_time
            last_confidence = ts_confidence
            # Update "seen near start" after any period transitions for this sample.
            period_len_for_current = OT_LENGTH_SECONDS if current_period >= 4 else PERIOD_LENGTH_SECONDS
            if time_remaining >= (period_len_for_current - 60):
                seen_near_start_in_period = True

        # Write normalization logs
        norm_logger.write_logs()

        return normalized

    def filter_events_by_type(
        self,
        events: List[Dict],
        event_types: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Filter events by type

        Args:
            events: List of events
            event_types: Types to include (None for all). E.g., ['goal']

        Returns:
            Filtered event list
        """
        if event_types is None:
            return events

        return [e for e in events if e.get('type') in event_types]

    def sort_events_by_video_time(self, events: List[Dict]) -> List[Dict]:
        """
        Sort events by video timestamp

        Args:
            events: List of events

        Returns:
            Sorted event list
        """
        # Only sort events that have video_time
        with_time = [e for e in events if e.get('video_time') is not None]
        without_time = [e for e in events if e.get('video_time') is None]

        # Sort those with time
        with_time.sort(key=lambda e: e['video_time'])

        # Return sorted + unsorted
        return with_time + without_time

    def estimate_missing_timestamps(
        self,
        video_timestamps: List[Dict],
        video_duration: float
    ) -> List[Dict]:
        """
        Fill in missing timestamps using linear interpolation

        This can help when OCR misses some frames

        Args:
            video_timestamps: Existing timestamps
            video_duration: Total video duration in seconds

        Returns:
            Enhanced timestamp list with interpolated values
        """
        if len(video_timestamps) < 2:
            return video_timestamps

        enhanced = video_timestamps.copy()

        # Sort by video time
        enhanced.sort(key=lambda t: t['video_time'])

        # Estimate a typical sampling interval from the non-interpolated samples.
        dts = []
        for a, b in zip(enhanced, enhanced[1:]):
            try:
                if a.get("interpolated") or b.get("interpolated"):
                    continue
                dt = float(b["video_time"]) - float(a["video_time"])
            except Exception:
                continue
            if dt > 0:
                dts.append(dt)
        dts.sort()
        typical_dt = dts[len(dts) // 2] if dts else 5.0
        fill_every = max(10.0, min(30.0, float(typical_dt) * 3.0))
        gap_threshold = max(60.0, fill_every * 4.0)

        # Find gaps and interpolate
        i = 0
        while i < len(enhanced) - 1:
            current = enhanced[i]
            next_ts = enhanced[i + 1]

            video_gap = next_ts['video_time'] - current['video_time']

            # If gap is large, interpolate
            if video_gap > gap_threshold:
                # Check if this might be a period break
                if current['period'] != next_ts['period']:
                    logger.debug(
                        f"Detected period break: P{current['period']} -> P{next_ts['period']}"
                    )
                    # Don't interpolate across period breaks
                    i += 1
                    continue

                # Avoid interpolating if endpoints are low-confidence or imply an unrealistic clock rate.
                try:
                    c_conf = float(current.get("ocr_confidence") or 0.0)
                    n_conf = float(next_ts.get("ocr_confidence") or 0.0)
                except Exception:
                    c_conf, n_conf = 0.0, 0.0
                if (c_conf and c_conf < 60.0) or (n_conf and n_conf < 60.0):
                    i += 1
                    continue

                # Interpolate timestamps in the gap
                num_interpolated = max(0, int(video_gap / fill_every) - 1)

                # Clock should count down roughly in real-time.
                try:
                    game_time_diff = float(current['game_time_seconds']) - float(next_ts['game_time_seconds'])
                except Exception:
                    game_time_diff = 0.0
                if game_time_diff <= 0:
                    i += 1
                    continue
                if abs(game_time_diff - float(video_gap)) > max(12.0, 0.25 * float(video_gap)):
                    i += 1
                    continue

                for j in range(1, num_interpolated + 1):
                    ratio = j / (num_interpolated + 1)

                    interp_video_time = current['video_time'] + (ratio * video_gap)

                    # Estimate game time (counting down)
                    interp_game_time_sec = current['game_time_seconds'] - int(ratio * game_time_diff)

                    enhanced.append({
                        'video_time': interp_video_time,
                        'period': current['period'],
                        'game_time': f"{interp_game_time_sec//60}:{interp_game_time_sec%60:02d}",
                        'game_time_seconds': interp_game_time_sec,
                        'interpolated': True
                    })

            i += 1

        # Re-sort after adding interpolated timestamps
        enhanced.sort(key=lambda t: t['video_time'])

        logger.info(f"Enhanced timestamps: {len(video_timestamps)} -> {len(enhanced)}")

        return enhanced
