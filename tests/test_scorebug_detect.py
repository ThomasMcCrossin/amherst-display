import numpy as np

import scorebug_detect


def _patch(monkeypatch, hits, vision_choice=None):
    monkeypatch.setattr(scorebug_detect, "sample_frames", lambda *a, **k: [np.zeros((10, 10, 3), np.uint8)] * 8)
    monkeypatch.setattr(scorebug_detect, "ocr_vote", lambda frames, profiles, engine=None: {
        p.profile_id: hits.get(p.profile_id, 0) for p in profiles})
    calls = []

    def fake_vision(frames, profiles):
        calls.append(True)
        return vision_choice, {"answer": {"choice": vision_choice}}

    monkeypatch.setattr(scorebug_detect, "vision_choose", fake_vision)
    return calls


def test_clear_ocr_winner_skips_vision(monkeypatch):
    calls = _patch(monkeypatch, {"mhl_flo_stacked_topleft": 7, "mhl_yarmouth_home": 1})
    profile, report = scorebug_detect.detect_scorebug_profile("game.mp4")
    assert profile.profile_id == "mhl_flo_stacked_topleft"
    assert report["method"] == "ocr_vote"
    assert not calls


def test_weak_ocr_falls_back_to_vision(monkeypatch):
    calls = _patch(monkeypatch, {"mhl_flo_strip": 2, "flo_legacy_right_banner": 1}, vision_choice="mhl_flo_strip")
    profile, report = scorebug_detect.detect_scorebug_profile("game.mp4")
    assert profile.profile_id == "mhl_flo_strip"
    assert report["method"] == "vision"
    assert calls


def test_undecided_returns_none(monkeypatch):
    _patch(monkeypatch, {}, vision_choice=None)
    profile, report = scorebug_detect.detect_scorebug_profile("game.mp4")
    assert profile is None
    assert report["method"] == "undecided"


def test_every_candidate_layout_has_a_vision_reference():
    for profile in scorebug_detect.candidate_profiles():
        if profile.profile_id.startswith(("mhl_flo_", "flo_corner")):
            assert (scorebug_detect.REFERENCE_DIR / f"{profile.profile_id}.png").exists(), profile.profile_id
