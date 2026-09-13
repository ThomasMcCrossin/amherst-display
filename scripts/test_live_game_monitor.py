from __future__ import annotations

import base64
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from scripts import live_game_monitor as monitor
import os
from datetime import timedelta


def body(clock="00:00", status="1", intermission="0"):
    return ("cb(" + json.dumps({"SiteKit": {"Scorebar": [{
        "ID": "4996", "SeasonID": "46", "HomeID": "1", "VisitorID": "12",
        "GameDateISO8601": "2026-09-12T19:00:00-03:00", "GameClock": clock,
        "GameStatus": status, "Intermission": intermission,
    }]}}) + ");\n").encode()


class LiveMonitorTests(unittest.TestCase):
    def test_scripted_http_sequence_preserves_unchanged_errors_and_provider_bytes(self):
        responses = [(200, body()), (200, body()), (200, body("12:34", "2")),
                     (200, body("00:00", "2", "1")), (200, body("00:00", "4")),
                     (503, b"unavailable"), (200, b"invalid JSONP"),
                     (200, b'cb({"SiteKit":{"Scorebar":[]}});'), (200, b"x" * 4097)]
        queue = list(responses)

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                status, payload = queue.pop(0)
                self.send_response(status)
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        with patch.dict(os.environ, {"HOCKEYTECH_API_KEY": "fixture-only"}), tempfile.TemporaryDirectory() as root, patch.object(
                monitor, "BASE_FEED_URL", f"http://127.0.0.1:{server.server_port}/"):
            args = monitor.parse_args(["--max-response-bytes", "4096"])
            journal = monitor.Journal(root)
            try:
                for _ in responses:
                    monitor.poll(journal, 30, args)
                paths = sorted((Path(root) / "polls").glob("*.json"))
                samples = [json.loads(path.read_text()) for path in paths]
                self.assertEqual([sample["sequence"] for sample in samples], list(range(1, 10)))
                self.assertEqual(len({sample["poll_id"] for sample in samples}), 9)
                self.assertEqual([sample["outcome"] for sample in samples],
                                 ["ok"] * 5 + ["error", "error", "ok", "error"])
                for sample, (_, original) in zip(samples[:-1], responses[:-1]):
                    self.assertEqual(base64.b64decode(sample["response_body_base64"]), original)
                    self.assertEqual(sample["response_sha256"], hashlib.sha256(original).hexdigest())
                self.assertEqual(samples[0]["response_sha256"], samples[1]["response_sha256"])
                self.assertEqual(samples[-1]["error_code"], "response_too_large")
                self.assertIsNone(samples[-1]["response_body_base64"])
                journal.status("stopped", args, 300)
                status = json.loads((Path(root) / "status.json").read_text())
                self.assertEqual(status["last_success_at"], samples[-2]["received_at"])
                self.assertEqual(status["last_error_at"], samples[-1]["received_at"])
            finally:
                journal.close()

    def test_restart_recovers_killed_attempt_and_preserves_old_artifacts(self):
        with tempfile.TemporaryDirectory() as root:
            args = monitor.parse_args(["--storage-root", root])
            journal = monitor.Journal(root)
            first = monitor.poll(journal, 300, args, lambda *_: (200, body(), None))
            path = Path(root) / "polls/00000000000000000001.json"
            original = path.read_bytes()
            with self.assertRaises(RuntimeError):
                monitor.Journal(root)
            journal.close()
            # Hard process exit after durable attempt reservation, before response receipt.
            code = "import os,sys; from scripts.live_game_monitor import Journal; j=Journal(sys.argv[1]); j.begin(30); os._exit(19)"
            result = subprocess.run([sys.executable, "-c", code, root], check=False)
            self.assertEqual(result.returncode, 19)
            journal = monitor.Journal(root)
            try:
                recovered = journal.latest()
                self.assertEqual(recovered["sequence"], 2)
                self.assertEqual(recovered["error_code"], "interrupted")
                self.assertIsNone(recovered["response_body_base64"])
                third = monitor.poll(journal, 30, args, lambda *_: (None, None, "network"))
                self.assertEqual(third["sequence"], 3)
                self.assertEqual(third["source_instance_key"], first["source_instance_key"])
                self.assertEqual(path.read_bytes(), original)
            finally:
                journal.close()
            self.assertEqual(monitor.main(["--storage-root", root]), 0)
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(json.loads((Path(root) / "status.json").read_text())["state"], "disabled")
            with self.assertRaises(RuntimeError):
                monitor.Journal(root, "different-instance")

    def test_archive_publication_recovers_without_overwriting_conflicts(self):
        with tempfile.TemporaryDirectory() as root:
            args = monitor.parse_args([])
            journal = monitor.Journal(root)
            # Failure after the ledger commit but before the artifact publish.
            with patch.object(monitor, "atomic_json", side_effect=OSError("disk")):
                with self.assertRaises(OSError):
                    monitor.poll(journal, 30, args, lambda *_: (200, body(), None))
            journal.close()
            journal = monitor.Journal(root)
            journal.close()
            path = Path(root) / "polls/00000000000000000001.json"
            original = path.read_bytes()
            sample = json.loads(original)
            monitor.atomic_json(path, sample, immutable=True)
            sample["outcome"] = "error"
            with self.assertRaises(RuntimeError):
                monitor.atomic_json(path, sample, immutable=True)
            self.assertEqual(path.read_bytes(), original)


class Clock:
    def __init__(self, timestamp, until, on_wait=None):
        self.time = monitor.instant(timestamp)
        self.until = monitor.instant(until)
        self.waits = []
        self.on_wait = on_wait

    def is_set(self):
        return self.time >= self.until

    def wait(self, seconds):
        assert seconds > 0, "Busy loop"
        self.waits.append(seconds)
        if self.on_wait:
            self.on_wait(self.time, seconds)
        self.time = min(self.until, self.time + timedelta(seconds=seconds))
        return self.is_set()


def central_plan():
    return dict(schema_version="amherst.monitor-plan.v1", complete=True,
                generated_at="2026-09-12T00:00:00+00:00",
                **{key: monitor.CONFIG[key] for key in
                   ("client_code", "league_id", "team_id", "season_ids", "monitoring")},
                games=[dict(game_id="4996", season_id="46", starts_at="2026-09-12T19:00:00-03:00",
                            home_team_id="1", away_team_id="12", final=False, monitorable=True),
                       dict(game_id="4997", season_id="46", starts_at="2026-09-13T19:00:00-03:00",
                            home_team_id="1", away_team_id="12", final=False, monitorable=True)])


class CentralWindowTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.plan = central_plan()
        self.plan_status = 200
        self.raw_plan = None
        self.plan_calls, self.upstream = [], []
        self.payload = body()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/plan":
                    outer.plan_calls.append(dict(self.headers))
                    status = outer.plan_status
                    if status == 200 and self.headers.get("If-None-Match") == '"fixture"':
                        status = 304
                    payload = outer.raw_plan if outer.raw_plan is not None else json.dumps(outer.plan).encode()
                    self.send_response(status)
                    self.send_header("ETag", '"fixture"')
                else:
                    outer.upstream.append(outer.clock.time)
                    payload = outer.payload
                    self.send_response(200)
                self.end_headers()
                if self.path != "/plan" or status != 304:
                    self.wfile.write(payload)

            def log_message(self, *_):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.args = monitor.parse_args(["--enabled", "--storage-root", str(self.root),
                                       "--plan-url", self.url + "/plan", "--max-polls", "10000"])

    def run_until(self, start, end, on_wait=None):
        self.clock = Clock(start, end, on_wait)
        with patch.object(monitor, "BASE_FEED_URL", self.url + "/upstream"), patch.dict(
                os.environ, {"HOCKEYTECH_API_KEY": "fixture-only"}), patch.object(
                monitor, "now", lambda: self.clock.time.isoformat()):
            journal = monitor.Journal(self.root, "clarencehub-amherst-mhl")
            try:
                monitor.run(journal, self.args, self.clock, lambda: self.clock.time)
            finally:
                journal.close()
        return self.clock

    def test_cached_restart_exact_wake_between_and_after_games(self):
        self.run_until("2026-09-12T21:00:00+00:00", "2026-09-12T21:30:00+00:00")
        self.assertEqual(self.upstream, [])
        self.assertEqual(len(self.plan_calls), 1)
        observed = []
        def inspect(timestamp, seconds):
            observed.append(json.loads((self.root / "status.json").read_text()))
        self.run_until("2026-09-12T21:30:00+00:00", "2026-09-12T21:56:00+00:00", inspect)
        self.assertEqual(self.upstream, [monitor.instant("2026-09-12T21:55:00+00:00"),
                                         monitor.instant("2026-09-12T21:55:30+00:00")])
        self.assertEqual(len(self.plan_calls), 1)
        self.assertEqual(observed[0]["next_window_at"], "2026-09-12T18:55:00-03:00")
        self.assertEqual(observed[0]["next_plan_refresh_at"], "2026-09-12T22:00:00+00:00")
        before = len(self.upstream)
        self.run_until("2026-09-13T06:00:00+00:00", "2026-09-13T21:55:00+00:00")
        self.assertEqual(len(self.upstream), before)
        self.assertTrue(any(call.get("If-None-Match") == '"fixture"' for call in self.plan_calls))
        self.run_until("2026-09-13T21:55:00+00:00", "2026-09-13T21:55:01+00:00")
        self.assertEqual(self.upstream[-1], monitor.instant("2026-09-13T21:55:00+00:00"))
        before = len(self.upstream)
        self.run_until("2026-09-14T06:00:00+00:00", "2026-09-14T08:00:00+00:00")
        self.assertEqual(len(self.upstream), before)

    def test_explicit_final_persists_and_zero_clock_intermission_does_not_stop(self):
        self.payload = body("00:00", "2", "1")
        self.run_until("2026-09-12T22:00:00+00:00", "2026-09-12T22:01:00+00:00")
        self.assertEqual(len(self.upstream), 2)
        self.payload = body("00:00", "4")
        self.run_until("2026-09-12T22:01:00+00:00", "2026-09-12T23:00:00+00:00")
        self.assertEqual(len(self.upstream), 3)
        self.run_until("2026-09-12T23:00:00+00:00", "2026-09-13T01:00:00+00:00")
        self.assertEqual(len(self.upstream), 3)
        # Lost checkpoint after a durable Final publication must replay that Final.
        (self.root / "game-final-state.json").unlink()
        self.run_until("2026-09-13T01:00:00+00:00", "2026-09-13T02:00:00+00:00")
        self.assertEqual(len(self.upstream), 3)

    def test_failed_refresh_revokes_cached_authority_across_restart(self):
        self.run_until("2026-09-12T21:00:00+00:00", "2026-09-12T21:01:00+00:00")
        self.plan_status = 503
        self.run_until("2026-09-12T22:00:00+00:00", "2026-09-12T22:01:00+00:00")
        self.run_until("2026-09-12T22:01:00+00:00", "2026-09-12T22:59:00+00:00")
        self.assertEqual(self.upstream, [])
        self.assertEqual(len(self.plan_calls), 2)
        self.plan_status = 200
        self.run_until("2026-09-12T23:00:00+00:00", "2026-09-12T23:00:01+00:00")
        self.assertEqual(self.upstream, [monitor.instant("2026-09-12T23:00:00+00:00")])

    def test_invalid_expired_and_wrong_season_never_authorize(self):
        cases = [b"bad-json", json.dumps(dict(central_plan(), complete=False)).encode(),
                 json.dumps(dict(central_plan(), season_ids=["45"])).encode(),
                 json.dumps(dict(central_plan(), generated_at="2026-09-09T00:00:00Z")).encode(),
                 b"x" * (self.args.max_response_bytes + 1)]
        for payload in cases:
            with self.subTest(payload=payload[:40]):
                self.raw_plan = payload
                (self.root / "monitor-plan-cache.json").unlink(missing_ok=True)
                self.run_until("2026-09-12T22:00:00+00:00", "2026-09-12T22:01:00+00:00")
                self.assertEqual(self.upstream, [])

    def test_local_plan_filter_and_plan_final(self):
        self.args.plan_file = self.root / "delivered.json"
        self.args.plan_file.write_text(json.dumps(self.plan))
        self.args.game_id = "absent"
        self.run_until("2026-09-12T22:00:00+00:00", "2026-09-12T23:00:00+00:00")
        self.assertEqual(self.upstream, [])
        self.args.game_id = "4996"
        self.plan["games"][0]["final"] = True
        self.args.plan_file.write_text(json.dumps(self.plan))
        self.run_until("2026-09-12T23:00:00+00:00", "2026-09-13T00:00:00+00:00")
        self.assertEqual(self.upstream, [])
        self.assertEqual(self.plan_calls, [])

    def test_304_does_not_renew_expiry(self):
        self.plan["generated_at"] = "2026-09-11T22:00:00+00:00"
        self.run_until("2026-09-13T20:55:00+00:00", "2026-09-13T22:01:00+00:00")
        self.assertTrue(any(call.get("If-None-Match") == '"fixture"' for call in self.plan_calls))
        self.assertEqual(len(self.upstream), 10)
        self.assertTrue(all(t < monitor.instant("2026-09-13T22:00:00+00:00") for t in self.upstream))
        self.run_until("2026-09-13T22:01:00+00:00", "2026-09-13T23:01:00+00:00")
        self.assertEqual(len(self.upstream), 10)


if __name__ == "__main__":
    unittest.main()
