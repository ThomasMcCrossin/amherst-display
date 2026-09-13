"""Single-writer, append-only public HockeyTech scorebar observations (no phase inference)."""
from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
from http.client import HTTPException
import json
import os
import re
from pathlib import Path
import signal
import sqlite3
import sys
import threading
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

CONFIG = json.loads((Path(__file__).resolve().parents[1] / "config/hockeytech.json").read_text())
CLIENT_CODE, LEAGUE_ID, TEAM_ID = (CONFIG[key] for key in ("client_code", "league_id", "team_id"))
BASE_FEED_URL = CONFIG["base_feed_url"]
USER_AGENT = "Mozilla/5.0 (compatible; AmherstRamblersGames/1.0)"
PLAN_URL = "https://raw.githubusercontent.com/ThomasMcCrossin/amherst-display/main/monitor_plan.json"
REQUEST = {"feed": "modulekit", "view": "scorebar", "client_code": CLIENT_CODE,
           "league_id": str(LEAGUE_ID), "team_id": str(TEAM_ID)}


def strip_jsonp(payload):
    match = re.match(r"^[^(]+\((.*)\)\s*;?\s*$", payload, re.S)
    if not match:
        raise ValueError("Response was not valid JSONP")
    return match.group(1)


ERRORS = {
    "missing_key": "HOCKEYTECH_API_KEY is not configured",
    "interrupted": "Collector stopped before the attempt completed",
    "timeout": "Public feed request timed out",
    "network": "Public feed request failed",
    "http": "Public feed returned an unsuccessful HTTP status",
    "response_too_large": "Public feed exceeded the response byte limit; body not retained",
    "invalid_payload": "Public feed did not contain a valid SiteKit.Scorebar array",
}


def now():
    return datetime.now(timezone.utc).isoformat()


def decode_rows(body):
    payload = json.loads(strip_jsonp(body.decode("utf-8-sig")))
    rows = payload["SiteKit"]["Scorebar"]
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("invalid scorebar")
    return rows


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_json(path, value, immutable=False):
    data = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    temporary = path.with_name("." + path.name + ".tmp")
    with temporary.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    if immutable:
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != data:
                raise RuntimeError("Immutable poll archive conflict") from None
        finally:
            temporary.unlink()
    else:
        os.replace(temporary, path)
    fsync_dir(path.parent)


class Journal:
    def __init__(self, root, instance_key=None):
        self.root = Path(root).expanduser()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.lease = (self.root / "writer.lock").open("a+")
        try:
            fcntl.flock(self.lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lease.close()
            raise RuntimeError("Another collector owns this storage root") from None
        try:
            (self.root / "polls").mkdir(exist_ok=True)
            self.db = sqlite3.connect(self.root / "journal.sqlite3")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS instance (key TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS polls (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    envelope TEXT NOT NULL, complete INTEGER NOT NULL DEFAULT 0,
                    published INTEGER NOT NULL DEFAULT 0);
                CREATE INDEX IF NOT EXISTS polls_outcome ON polls
                    (json_extract(envelope,'$.outcome'),sequence) WHERE complete=1;
                CREATE INDEX IF NOT EXISTS polls_pending ON polls (sequence) WHERE complete=0;
                CREATE INDEX IF NOT EXISTS polls_unpublished ON polls (sequence)
                    WHERE complete=1 AND published=0;
            """)
            row = self.db.execute("SELECT key FROM instance").fetchone()
            self.instance_key = row[0] if row else instance_key or str(uuid4())
            if instance_key and instance_key != self.instance_key:
                raise RuntimeError("Storage root belongs to another source instance")
            if not row:
                with self.db:
                    self.db.execute("INSERT INTO instance VALUES (?)", (self.instance_key,))
            fsync_dir(self.root)
            pending = self.db.execute("SELECT sequence, envelope FROM polls WHERE complete=0").fetchall()
            for sequence, text in pending:
                envelope = json.loads(text)
                envelope.update(sequence=sequence, received_at=now(), error_code="interrupted",
                                error_message=ERRORS["interrupted"])
                self.finish(envelope)
            self.publish()
        except BaseException:
            self.close()
            raise

    def close(self):
        if hasattr(self, "db"):
            self.db.close()
        self.lease.close()

    def begin(self, interval):
        envelope = dict(schema_version="hockeytech.scorebar.poll.v1",
                        source_instance_key=self.instance_key, sequence=0, poll_id=str(uuid4()),
                        request_started_at=now(), received_at=None, poll_interval_seconds=interval,
                        request=REQUEST, outcome="error", http_status=None, error_code=None,
                        error_message=None, response_body_base64=None, response_sha256=None)
        with self.db:
            cursor = self.db.execute("INSERT INTO polls(envelope) VALUES (?)", (json.dumps(envelope),))
        envelope["sequence"] = cursor.lastrowid
        return envelope

    def finish(self, envelope):
        with self.db:
            changed = self.db.execute("UPDATE polls SET envelope=?,complete=1 WHERE sequence=? AND complete=0",
                                      (json.dumps(envelope), envelope["sequence"])).rowcount
            if changed != 1:
                raise RuntimeError("Attempt was already completed or is unknown")
        self.publish()

    def publish(self):
        for sequence, text in self.db.execute(
                "SELECT sequence,envelope FROM polls WHERE complete=1 AND published=0 ORDER BY sequence"):
            atomic_json(self.root / "polls" / f"{sequence:020d}.json", json.loads(text), immutable=True)
            with self.db:
                self.db.execute("UPDATE polls SET published=1 WHERE sequence=?", (sequence,))

    def latest(self, outcome=None):
        sql = "SELECT envelope FROM polls WHERE complete=1"
        params = ()
        if outcome:
            sql += " AND json_extract(envelope,'$.outcome')=?"
            params = (outcome,)
        row = self.db.execute(sql + " ORDER BY sequence DESC LIMIT 1", params).fetchone()
        return json.loads(row[0]) if row else None

    def status(self, state, args, interval, **planning):
        latest, success, error = self.latest(), self.latest("ok"), self.latest("error")
        atomic_json(self.root / "status.json", dict(
            source_instance_key=self.instance_key, state=state, updated_at=now(),
            poll_interval_seconds=interval, last_attempt_at=latest["received_at"] if latest else None,
            last_success_at=success["received_at"] if success else None,
            last_error_at=error["received_at"] if error else None,
            last_error_code=error["error_code"] if error else None,
            latest_poll_path=f"polls/{latest['sequence']:020d}.json" if latest else None,
            latest_success_path=f"polls/{success['sequence']:020d}.json" if success else None,
            configuration=dict(game_id=args.game_id, **CONFIG["monitoring"],
                               timeout_seconds=args.timeout_seconds,
                               max_response_bytes=args.max_response_bytes), **planning))


def fetch(timeout, max_bytes):
    """One bounded request. Never expose exception text, URLs, headers, or keys."""
    key = os.environ.get("HOCKEYTECH_API_KEY")
    if not key:
        return None, None, "missing_key"
    params = dict(REQUEST, key=key, site_id=str(CONFIG["site_id"]), lang="en", callback="cb",
                  numberofdaysahead=2, numberofdaysback=1, limit=100)
    request = Request(BASE_FEED_URL + "?" + urlencode(params),
                      headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"})
    status, body, code = None, None, None

    def deadline(signum, frame):
        raise TimeoutError()

    previous = signal.signal(signal.SIGALRM, deadline)
    signal.setitimer(signal.ITIMER_REAL, timeout)
    try:
        try:
            response = urlopen(request, timeout=timeout)
        except HTTPError as exc:
            response = exc
        with response:
            status = response.status
            body = response.read(max_bytes + 1)
        if len(body) > max_bytes:
            body, code = None, "response_too_large"
        elif not 200 <= status < 300:
            code = "http"
        else:
            try:
                decode_rows(body)
            except (ValueError, KeyError, TypeError, RecursionError):
                code = "invalid_payload"
    except TimeoutError:
        code = "timeout"
    except (URLError, OSError, HTTPException):
        code = "network"
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
    return status, body, code


def poll(journal, interval, args, fetcher=fetch):
    envelope = journal.begin(interval)
    status, body, code = fetcher(args.timeout_seconds, args.max_response_bytes)
    envelope.update(received_at=now(), outcome="error" if code else "ok", http_status=status,
                    error_code=code, error_message=ERRORS.get(code),
                    response_body_base64=base64.b64encode(body).decode("ascii") if body is not None else None,
                    response_sha256=hashlib.sha256(body).hexdigest() if body is not None else None)
    journal.finish(envelope)
    return envelope


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def instant(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("Timezone required")
    return result


def validate_plan(plan, timestamp):
    if plan["schema_version"] != "amherst.monitor-plan.v1" or plan["complete"] is not True:
        raise ValueError("Incomplete plan")
    for key in ("client_code", "league_id", "team_id", "season_ids", "monitoring"):
        if plan[key] != CONFIG[key]:
            raise ValueError("Configuration mismatch")
    generated = instant(plan["generated_at"])
    expires = generated + timedelta(seconds=CONFIG["monitoring"]["plan_max_age_seconds"])
    if not generated <= timestamp < expires:
        raise ValueError("Expired or future plan")
    if not isinstance(plan["games"], list):
        raise ValueError("Invalid games")
    seen = set()
    for game in plan["games"]:
        identity = (game["season_id"], game["game_id"])
        if (not all(isinstance(v, str) and v for v in identity) or identity in seen
                or game["season_id"] not in CONFIG["season_ids"]
                or CONFIG["team_id"] not in (game["home_team_id"], game["away_team_id"])
                or type(game["final"]) is not bool or type(game["monitorable"]) is not bool):
            raise ValueError("Invalid game")
        seen.add(identity)
        if game["monitorable"] or game["starts_at"] is not None:
            instant(game["starts_at"])
    return expires


class Plan:
    """Bounded conditional data reads; never an upstream discovery fallback."""
    def __init__(self, args, root, timestamp):
        self.args = args
        self.path = root / "monitor-plan-cache.json"
        self.source = str(args.plan_file.resolve()) if args.plan_file else args.plan_url
        self.plan, self.etag, self.modified = None, None, None
        self.error = None
        self.refresh_at = timestamp
        try:
            cache = json.loads(self.path.read_text())
            if cache["source"] == self.source:
                self.refresh_at = min(instant(cache["refresh_at"]), timestamp + timedelta(
                    seconds=CONFIG["monitoring"]["plan_refresh_seconds"]))
                self.error = cache["error"]
                if cache["plan"] is not None:
                    validate_plan(cache["plan"], timestamp)
                    self.plan = cache["plan"]
                    self.etag, self.modified = cache["etag"], cache["modified"]
        except (OSError, ValueError, KeyError, TypeError, OverflowError):
            self.plan = None
            self.refresh_at = timestamp

    def refresh(self, timestamp):
        if timestamp < self.refresh_at:
            return
        self.refresh_at = timestamp + timedelta(seconds=CONFIG["monitoring"]["plan_refresh_seconds"])

        def deadline(*_):
            raise TimeoutError()

        previous = signal.signal(signal.SIGALRM, deadline)
        signal.setitimer(signal.ITIMER_REAL, self.args.timeout_seconds)
        try:
            if self.args.plan_file:
                with self.args.plan_file.open("rb") as response:
                    body = response.read(self.args.max_response_bytes + 1)
                candidate = json.loads(body) if len(body) <= self.args.max_response_bytes else None
                etag, modified = None, None
            else:
                headers = {"Accept": "application/json", "Accept-Encoding": "identity"}
                if self.etag:
                    headers["If-None-Match"] = self.etag
                if self.modified:
                    headers["If-Modified-Since"] = self.modified
                try:
                    response = urlopen(Request(self.args.plan_url, headers=headers), timeout=self.args.timeout_seconds)
                except HTTPError as exc:
                    response = exc
                with response:
                    if response.status == 304:
                        candidate = self.plan
                        etag, modified = self.etag, self.modified
                    elif response.status == 200:
                        body = response.read(self.args.max_response_bytes + 1)
                        candidate = json.loads(body) if len(body) <= self.args.max_response_bytes else None
                        etag, modified = response.headers.get("ETag"), response.headers.get("Last-Modified")
                    else:
                        raise ValueError("Plan request failed")
            validate_plan(candidate, timestamp)
            self.plan, self.etag, self.modified = candidate, etag, modified
            self.error = None
        except (OSError, HTTPException, ValueError, KeyError, TypeError, OverflowError, RecursionError):
            # Failure revokes cached authorization until the next successful refresh.
            self.plan, self.etag, self.modified = None, None, None
            self.error = "plan_unavailable_or_invalid"
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
        atomic_json(self.path, dict(source=self.source, plan=self.plan, etag=self.etag,
                                  modified=self.modified, error=self.error, refresh_at=self.refresh_at.isoformat()))

    def windows(self, timestamp, finals):
        try:
            expires = validate_plan(self.plan, timestamp)
        except (ValueError, KeyError, TypeError, OverflowError):
            self.error = "plan_unavailable_or_invalid"
            return [], None, self.refresh_at
        active, future = [], []
        wake = min(self.refresh_at, expires)
        for game in self.plan["games"]:
            if (not game["monitorable"] or game["final"]
                    or (game["season_id"], game["game_id"]) in finals
                    or (self.args.game_id and game["game_id"] != self.args.game_id)):
                continue
            start = instant(game["starts_at"])
            begin = start - timedelta(minutes=CONFIG["monitoring"]["before_minutes"])
            end = start + timedelta(hours=CONFIG["monitoring"]["after_hours"])
            if begin <= timestamp < end:
                active.append(game["game_id"])
                wake = min(wake, end)
            elif timestamp < begin < expires:
                future.append(begin)
        next_window = min(future) if future else None
        if next_window:
            wake = min(wake, next_window)
        return active, next_window, wake


def final_games(envelope):
    if not envelope or envelope["outcome"] != "ok":
        return set()
    return {(str(row.get("SeasonID")), str(row.get("ID")))
            for row in decode_rows(base64.b64decode(envelope["response_body_base64"]))
            if str(row.get("GameStatus")) == "4"}


def run(journal, args, stopped, clock=lambda: datetime.now(timezone.utc)):
    interval = CONFIG["monitoring"]["active_seconds"]
    if not args.enabled:
        journal.status("disabled", args, interval)
        return
    plan = Plan(args, journal.root, clock())
    # Replay only the tail after a durable final-state checkpoint. A crash after
    # publishing a poll but before checkpointing cannot forget an explicit Final.
    checkpoint = journal.root / "game-final-state.json"
    try:
        state = json.loads(checkpoint.read_text())
        finals = {tuple(game) for game in state["games"]}
        sequence = int(state["sequence"])
    except FileNotFoundError:
        finals, sequence = set(), 0
    for seq, text in journal.db.execute("SELECT sequence,envelope FROM polls WHERE complete=1 AND sequence>? ORDER BY sequence", (sequence,)):
        finals.update(final_games(json.loads(text)))
        sequence = seq
    atomic_json(checkpoint, dict(sequence=sequence, games=sorted(finals)))
    latest = journal.latest()
    next_poll = instant(latest["request_started_at"]) + timedelta(seconds=interval) if latest else clock()
    attempts = 0
    while not stopped.is_set():
        timestamp = clock()
        plan.refresh(timestamp)
        # Refresh may take time: recheck actual time before authorization.
        timestamp = clock()
        active, next_window, wake = plan.windows(timestamp, finals)
        if active and timestamp >= next_poll:
            envelope = poll(journal, interval, args)
            finals.update(final_games(envelope))
            atomic_json(checkpoint, dict(sequence=envelope["sequence"], games=sorted(finals)))
            attempts += 1
            next_poll = clock() + timedelta(seconds=interval)
            if attempts >= args.max_polls:
                break
            continue
        if active:
            wake = min(wake, next_poll)
        journal.status("waiting" if not active else "running", args, interval,
                       active_game_ids=active, next_window_at=next_window.isoformat() if next_window else None,
                       next_plan_refresh_at=plan.refresh_at.isoformat(), plan_error=plan.error,
                       next_attempt_at=next_poll.isoformat() if active else (next_window.isoformat() if next_window else None),
                       next_wake_at=wake.isoformat())
        stopped.wait(max(0, (wake - clock()).total_seconds()))
    journal.status("stopped", args, interval)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storage-root", type=Path, default=Path.home() / ".local/state/my-web-scrapers/hockeytech-live")
    parser.add_argument("--instance-key", help="Optional identity; must match existing storage on restart")
    parser.add_argument("--enabled", action="store_true", help="Explicit opt-in; otherwise only mark disabled")
    parser.add_argument("--game-id", help="Filter central game windows, never force a request")
    parser.add_argument("--max-polls", type=positive, default=1, help="Maximum authorized attempts; waits off-game")
    delivery = parser.add_mutually_exclusive_group()
    delivery.add_argument("--plan-url", default=PLAN_URL)
    delivery.add_argument("--plan-file", type=Path)
    parser.add_argument("--timeout-seconds", type=positive, default=20)
    parser.add_argument("--max-response-bytes", type=positive, default=2 * 1024 * 1024)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    stopped = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopped.set())
    journal = None
    try:
        journal = Journal(args.storage_root, args.instance_key)
        run(journal, args, stopped)
        return 0
    except (RuntimeError, OSError, sqlite3.Error, ValueError, KeyError, TypeError):
        print("Live monitor stopped: storage, configuration or single-writer lease failure", file=sys.stderr)
        return 1
    finally:
        if journal is not None:
            journal.close()


if __name__ == "__main__":
    sys.exit(main())
