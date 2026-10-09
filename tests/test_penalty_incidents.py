import json
from pathlib import Path

import penalty_incidents as pi


def _key(p):
    return (p["period"], p["secs"])


def test_same_stoppage_penalties_cluster_and_lone_minor_stays_minor():
    pens = [
        {"period": 1, "secs": 100, "inf": "Roughing - Minor", "min": 2},
        {"period": 1, "secs": 100, "inf": "Roughing - Minor", "min": 2},
        {"period": 1, "secs": 104, "inf": "Slashing - Minor", "min": 2},  # within tolerance: chains in
        {"period": 1, "secs": 300, "inf": "Hooking - Minor", "min": 2},
        {"period": 2, "secs": 100, "inf": "Tripping - Minor", "min": 2},  # other period: never joined
    ]
    clusters = pi.cluster_by_stoppage(pens, _key)
    assert [len(c) for c in clusters] == [3, 1, 1]
    kinds = [pi.incident_kind([(p["inf"], p["min"]) for p in c]) for c in clusters]
    assert kinds == ["scrum", "minor", "minor"]


def test_consequential_penalties_and_fight_kind():
    assert pi.incident_kind([("Boarding - Major", 5)]) == "major"
    assert pi.incident_kind([("10 Minute Misconduct", 10)]) == "major"
    assert pi.incident_kind([("Game Misconduct", 10)]) == "major"
    assert pi.incident_kind([("Match Penalty", 5)]) == "major"
    assert pi.incident_kind([("Fighting - Major", 5), ("Fighting - Major", 5)]) == "fight"
    assert pi.incident_kind([("Hooking - Minor", 2)]) == "minor"
    assert pi.incident_kind([("Roughing - Minor", 2), ("Roughing - Minor", 2)]) == "scrum"


def test_scrum_summary_lists_every_penalty():
    text = pi.scrum_summary([
        {"infraction": "Roughing - Minor", "player": "A"},
        {"infraction": "Slashing - Minor", "player": "B"},
        {"infraction": "10 Minute Misconduct", "player": "C"},
    ])
    assert text.startswith("3 penalties: ")
    assert "Slashing - Minor (B)" in text


def test_pipeline_makes_one_scrum_clip_for_penalties_at_one_stoppage(tmp_path: Path):
    import config
    from highlight_extractor.pipeline import HighlightPipeline

    class FakeVideoProcessor:
        duration = 10_000.0

        def create_highlight_clips(self, events, clips_dir: Path, before_seconds=8.0, after_seconds=6.0):
            clips_dir.mkdir(parents=True, exist_ok=True)
            created = []
            for idx, event in enumerate(events, 1):
                clip_path = clips_dir / f"{idx:02d}_{event['type']}.mp4"
                clip_path.write_bytes(b"0")
                created.append((event, clip_path))
            return created

    pipeline = HighlightPipeline(config=config, video_path=tmp_path / "dummy.mp4", video_processor=FakeVideoProcessor())
    game_dir = tmp_path / "game"
    pipeline.game_folders = {k: game_dir / v for k, v in (
        ("game_dir", ""), ("clips_dir", "clips"), ("data_dir", "data"), ("output_dir", "output"), ("logs_dir", "logs"))}
    for path in pipeline.game_folders.values():
        Path(path).mkdir(parents=True, exist_ok=True)
    pipeline.reel_mode = "goals_with_all_penalties"
    pipeline.matched_events = [{"type": "goal", "period": 2, "time": "6:30", "team": "Amherst Ramblers", "scorer": "A",
                                "assist1": "", "assist2": "", "video_time": 1000.0}]
    pipeline.video_timestamps = [
        {"video_time": 600.0, "period": 2, "game_time": "15:00", "game_time_seconds": 900},
        {"video_time": 1500.0, "period": 2, "game_time": "10:00", "game_time_seconds": 600},
    ]

    def pen(time, player, inf, minutes):
        return {"period": 2, "time": time, "team": "Amherst Ramblers", "player": {"name": player, "number": None},
                "infraction": inf, "minutes": minutes}

    pipeline.box_score = {"SiteKit": {"Gamesummary": {"penalties": [
        pen("5:00", "P1", "Roughing - Minor", 2),
        pen("5:00", "P2", "Roughing - Minor", 2),
        pen("5:00", "P3", "10 Minute Misconduct", 10),
        pen("10:00", "P4", "Tripping - Minor", 2),
    ]}}}
    pipeline._step6_create_clips(before_seconds=15.0, after_seconds=4.0)

    clips = json.loads((game_dir / "data" / "clips_manifest.json").read_text())["clips"]
    penalties = [c for c in clips if c["type"] == "penalty"]
    assert len(penalties) == 2  # the 3-penalty scrum is one clip, the lone minor another
    scrum, lone = penalties
    assert scrum["kind"] == "scrum" and scrum["penalty_count"] == 3
    assert [p["player"] for p in scrum["penalties"]] == ["P1", "P2", "P3"]
    assert scrum["infraction"].startswith("3 penalties: ")
    assert scrum["before_seconds"] == config.SCRUM_BEFORE_SECONDS and scrum["after_seconds"] == config.SCRUM_AFTER_SECONDS
    assert "kind" not in lone and lone["after_seconds"] == config.PENALTY_ALL_AFTER_SECONDS


def test_clip_review_groups_a_scrum_into_one_rough_stuff_incident(tmp_path: Path):
    from clip_review.apply import ROUGH_CLASSES
    from clip_review.incidents import build_incidents

    data = tmp_path / "data"
    data.mkdir()

    def pen(time, player, inf, minutes, vt):
        return {"type": "penalty", "period": 1, "time": time, "team": "T", "player": {"name": player},
                "infraction": inf, "minutes": minutes, "video_time": vt}

    (data / "matched_events.json").write_text(json.dumps([
        pen("08:43", "A", "Roughing - Minor", 2, 1000.0),
        pen("08:45", "B", "Roughing - Minor", 2, 1002.0),  # two seconds later: same stoppage
        pen("12:00", "C", "Hooking - Minor", 2, 1400.0),
    ]))
    incidents = build_incidents(tmp_path)
    assert [i["class"] for i in incidents] == ["scrum", "minor"]
    scrum = incidents[0]
    assert len(scrum["rows"]) == 2 and "scrum" in ROUGH_CLASSES
    import config
    assert scrum["engine_out"] - scrum["engine_in"] == config.SCRUM_BEFORE_SECONDS + config.SCRUM_AFTER_SECONDS
