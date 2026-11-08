"""Live progress for the postgame pipeline (amherst-display#48).

Every long step reports where it is: stage, item n/total, last activity time and an ETA once the
rate is known. The record lives under the "progress" key of one JSON file, named by the
PIPELINE_PROGRESS_JSON environment variable (watch-rams points it at the game's
highlights-status.json); nothing else in that file is touched. Without the variable every call
is a no-op, so scripts run by hand behave as before.

    from pipeline_progress import progress
    progress.stage("extract_timestamps", total=len(samples), detail="OCR capture")
    progress.tick()                       # one item done
    progress.note("retrying OCR")         # activity without a count
    progress.finish("review done")        # closes the stage; the parent may start the next one

A finished stage is appended to progress.history with its seconds and a one-line summary, which
is also logged. Writes are atomic and throttled to one every PROGRESS_MIN_INTERVAL seconds,
except stage changes, which are written at once. Several processes may share the file in turn
(the postgame runner, then each step it starts); each read-modify-writes only "progress".
"""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)
PROGRESS_MIN_INTERVAL = float(os.environ.get("PIPELINE_PROGRESS_MIN_INTERVAL", "2"))
HISTORY_MAX = 40


def _iso(t: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(t))


def _fmt_s(s: Optional[float]) -> str:
    if s is None:
        return "?"
    s = int(round(s))
    return f"{s // 60}m{s % 60:02d}s" if s >= 60 else f"{s}s"


class Progress:
    def __init__(self, path: Optional[Path]):
        self.path = path
        self._lock = threading.Lock()
        self._last_write = 0.0
        self.name: Optional[str] = None
        self.total: Optional[int] = None
        self.done = 0
        self.detail = ""
        self.started = 0.0
        self.last_activity = 0.0
        self._pending_history: Optional[Dict[str, Any]] = None

    @classmethod
    def from_env(cls) -> "Progress":
        p = os.environ.get("PIPELINE_PROGRESS_JSON")
        return cls(Path(p) if p else None)

    @property
    def enabled(self) -> bool:
        return self.path is not None

    # -- public --------------------------------------------------------------------------
    def stage(self, name: str, total: Optional[int] = None, detail: str = "") -> None:
        with self._lock:
            if self.name and self.name != name:
                self._close_locked("")
            if self.name != name:
                self.started = time.time()
                self.done = 0
            self.name, self.total, self.detail = name, total, detail
            self.last_activity = time.time()
            self._write_locked(force=True)

    def set_total(self, total: Optional[int]) -> None:
        with self._lock:
            self.total = total
            self._write_locked(force=True)

    def tick(self, n: int = 1, detail: Optional[str] = None) -> None:
        with self._lock:
            self.done += n
            if detail is not None:
                self.detail = detail
            self.last_activity = time.time()
            self._write_locked(force=self.total is not None and self.done >= self.total)

    def note(self, detail: str) -> None:
        with self._lock:
            self.detail = detail
            self.last_activity = time.time()
            self._write_locked()

    def finish(self, summary: str = "", ok: bool = True) -> None:
        with self._lock:
            if self.name:
                self._close_locked(summary, ok)
                self.name = None
                self._write_locked(force=True)

    # -- internals -----------------------------------------------------------------------
    def _eta(self, now: float) -> Optional[float]:
        if not self.total or self.done <= 0 or self.done >= self.total:
            return 0.0 if self.total and self.done >= self.total else None
        rate = self.done / max(1e-6, now - self.started)
        return (self.total - self.done) / rate

    def _line(self, now: float) -> str:
        if not self.name:
            return "idle"
        count = f" {self.done}/{self.total}" if self.total else (f" {self.done}" if self.done else "")
        pct = f" ({100 * self.done / self.total:.0f}%)" if self.total else ""
        eta = self._eta(now)
        tail = f", ETA {_fmt_s(eta)}" if eta else ""
        det = f" - {self.detail}" if self.detail else ""
        return f"{self.name}{count}{pct}, {_fmt_s(now - self.started)} in{tail}{det}"

    def _close_locked(self, summary: str, ok: bool = True) -> None:
        now = time.time()
        secs = round(now - self.started, 1)
        count = f" {self.done}/{self.total}" if self.total else (f" {self.done} items" if self.done else "")
        line = f"{self.name} {'done' if ok else 'FAILED'} in {_fmt_s(secs)}{count}" + (f": {summary}" if summary else "")
        logger.info("progress: %s", line)
        self._pending_history = {"stage": self.name, "ok": ok, "seconds": secs, "done": self.done, "total": self.total,
                                 "ended_at": _iso(now), "summary": line}
        self._write_locked(force=True)

    def _write_locked(self, force: bool = False) -> None:
        if self.path is None:
            return
        now = time.time()
        if not force and now - self._last_write < PROGRESS_MIN_INTERVAL:
            return
        self._last_write = now
        try:
            try:
                doc: Dict[str, Any] = json.loads(self.path.read_text())
                if not isinstance(doc, dict):
                    doc = {}
            except (OSError, ValueError):
                doc = {}
            prev = doc.get("progress") if isinstance(doc.get("progress"), dict) else {}
            history = list(prev.get("history") or [])
            pending = self._pending_history
            if pending:
                history.append(pending)
                self._pending_history = None
            eta = self._eta(now) if self.name else None
            doc["progress"] = {
                "stage": self.name, "detail": self.detail or None, "done": self.done if self.name else None,
                "total": self.total if self.name else None,
                "pct": round(100 * self.done / self.total, 1) if self.name and self.total else None,
                "stage_started_at": _iso(self.started) if self.name else None,
                "last_activity_at": _iso(self.last_activity) if self.last_activity else None,
                "last_activity_epoch": round(self.last_activity, 1) if self.last_activity else None,
                "eta_s": round(eta) if eta is not None else None,
                "eta_at": _iso(now + eta) if eta else None,
                "line": self._line(now), "updated_at": _iso(now), "pid": os.getpid(), "host": socket.gethostname(),
                "history": history[-HISTORY_MAX:],
            }
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
            tmp.write_text(json.dumps(doc, indent=2, default=str) + "\n")
            os.replace(tmp, self.path)
        except OSError as exc:  # progress must never break the pipeline
            logger.debug("progress write failed: %s", exc)


progress = Progress.from_env()
