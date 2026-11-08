"""A goal still disputed after the second review goes to the escalation reviewer; held_for_human
only when that escalation fails (amherst-display#47). Fixture: 2026-11-07 Truro clip 09_goal_p3_1-43."""
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from clip_review import review  # noqa: E402

FIX = ROOT / "tests/fixtures/clip_review_escalation"


class Fake:
    """Writes a canned verdict (or nothing) to the requested path, like an agent harness."""

    def __init__(self, name, reviews=(), adversaries=()):
        self.name, self.reviews, self.adversaries, self.calls = name, list(reviews), list(adversaries), []

    def identity(self):
        return "fake|" + self.name

    def _write(self, queue, out_path, kw):
        self.calls.append(kw)
        v = queue.pop(0) if queue else None
        if v is None:
            return {"verdict": None, "stats": {}, "error": "agent wrote no verdict (rc=1)", "log": None}
        out_path.write_text(json.dumps(v))
        return {"verdict": v, "stats": {}, "error": None, "log": None}

    def review(self, packet, out_path, **kw):
        return self._write(self.reviews, out_path, kw)

    def adversary(self, packet, out_path, adv_input, **kw):
        return self._write(self.adversaries, out_path, kw)


def _packet(tmp_path):
    pk = tmp_path / "09_goal_p3_1-43"
    pk.mkdir()
    shutil.copy(FIX / "incident.json", pk / "incident.json")
    return pk


def _run(tmp_path, monkeypatch, escalator):
    monkeypatch.setattr(review, "final_sheets", lambda *a, **k: [])
    r1 = json.loads((FIX / "r1.json").read_text())
    a1 = json.loads((FIX / "a1.json").read_text())
    reviewer = Fake("glm", reviews=[r1, dict(r1, out_t=8.0)])
    adversary = Fake("ds", adversaries=[a1, a1])
    return review.review_incident(_packet(tmp_path), reviewer, adversary, attempts=1, escalator=escalator)


def test_still_disputed_goal_takes_the_escalation_verdict(tmp_path, monkeypatch):
    r1 = json.loads((FIX / "r1.json").read_text())
    esc = Fake("opus", reviews=[dict(r1, in_t=-30.0, out_t=2.0, objection_answer="celebration ends by +2")])
    res = _run(tmp_path, monkeypatch, esc)
    assert res["dispute"] == "resolved_by_escalation"
    assert res["final"]["status"] == "override" and res["final"]["escalated"] is True
    assert (res["final"]["in_t"], res["final"]["out_t"]) == (-30.0, 2.0)
    assert [s["step"] for s in res["steps"]] == ["r1", "a1", "r2", "a2", "r3"]
    record = json.loads(Path(esc.calls[0]["escalation_path"]).read_text())
    assert record["a1"]["agree"] is False and record["r2"]["out_t"] == 8.0


def test_failed_escalation_holds_for_human_with_the_error(tmp_path, monkeypatch):
    res = _run(tmp_path, monkeypatch, Fake("opus", reviews=[None]))
    assert res["dispute"] == "held_for_human"
    assert res["final"]["status"] == "held_for_human"
    assert "no verdict" in res["final"]["escalation_error"]


def test_no_escalator_keeps_the_old_hold(tmp_path, monkeypatch):
    res = _run(tmp_path, monkeypatch, None)
    assert res["final"]["status"] == "held_for_human" and "escalation_error" not in res["final"]
