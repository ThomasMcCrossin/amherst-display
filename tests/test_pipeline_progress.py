"""Live postgame progress in the status JSON (amherst-display#48)."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline_progress import Progress  # noqa: E402


def test_progress_lives_beside_the_status_and_keeps_finished_stages(tmp_path):
    status = tmp_path / "highlights-status.json"
    status.write_text(json.dumps({"state": "running", "game_dir": "G"}))
    p = Progress(status)
    p.stage("extract_timestamps", total=4)
    p.tick()
    p.tick()
    p.stage("create_clips", total=2)  # closes extract_timestamps into history
    p.tick(detail="clip 1/2")
    p._write_locked(force=True)
    doc = json.loads(status.read_text())
    assert doc["state"] == "running" and doc["game_dir"] == "G"
    pr = doc["progress"]
    assert (pr["stage"], pr["done"], pr["total"], pr["detail"]) == ("create_clips", 1, 2, "clip 1/2")
    assert pr["last_activity_at"] and pr["eta_s"] is not None
    assert [h["stage"] for h in pr["history"]] == ["extract_timestamps"]
    assert pr["history"][0]["summary"].startswith("extract_timestamps done in ") and "2/4" in pr["history"][0]["summary"]

    later = Progress(status)  # the next process appends to the same history
    later.stage("review", total=1)
    later.finish("override 1", ok=True)
    pr = json.loads(status.read_text())["progress"]
    assert pr["stage"] is None and [h["stage"] for h in pr["history"]] == ["extract_timestamps", "review"]


def test_without_a_path_nothing_is_written(tmp_path):
    p = Progress(None)
    p.stage("x", total=1)
    p.tick()
    p.finish()
    assert list(tmp_path.iterdir()) == []
