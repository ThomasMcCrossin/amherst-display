import json
from pathlib import Path


def test_pipeline_inserts_contributing_penalty_before_pp_goal(tmp_path: Path):
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

    pipeline = HighlightPipeline(
        config=config,
        video_path=tmp_path / "dummy.mp4",
        video_processor=FakeVideoProcessor(),
    )
    pipeline.reel_mode = "goals_with_pp_penalties"

    game_dir = tmp_path / "game"
    game_folders = {
        "game_dir": game_dir,
        "clips_dir": game_dir / "clips",
        "data_dir": game_dir / "data",
        "output_dir": game_dir / "output",
        "logs_dir": game_dir / "logs",
    }
    for p in game_folders.values():
        Path(p).mkdir(parents=True, exist_ok=True)
    pipeline.game_folders = game_folders

    # PP goal at 6:30 elapsed in P2 -> 13:30 remaining.
    pipeline.matched_events = [
        {
            "type": "goal",
            "period": 2,
            "time": "6:30",
            "team": "Amherst Ramblers",
            "scorer": "A",
            "assist1": "",
            "assist2": "",
            "special": "PP",
            "power_play": True,
            "short_handed": False,
            "empty_net": False,
            "video_time": 1000.0,
        }
    ]

    # Timestamp near the contributing penalty (5:00 elapsed -> 15:00 remaining).
    pipeline.video_timestamps = [
        {"video_time": 500.0, "period": 2, "game_time": "15:00", "game_time_seconds": 900},
        {"video_time": 1000.0, "period": 2, "game_time": "13:30", "game_time_seconds": 810},
    ]

    pipeline.box_score = {
        "SiteKit": {
            "Gamesummary": {
                "penalties": [
                    {
                        "period": 2,
                        "time": "5:00",
                        "team": "Truro Bearcats",
                        "player": {"name": "John Smith", "number": None},
                        "infraction": "Hooking - Minor",
                        "minutes": 2,
                    }
                ]
            }
        }
    }

    pipeline._step6_create_clips(before_seconds=15.0, after_seconds=4.0)

    manifest_path = game_folders["data_dir"] / "clips_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    clips = manifest["clips"]

    assert [c["type"] for c in clips] == ["penalty", "goal"]
    assert clips[0].get("linked_to_goal") == 0


def test_pipeline_estimates_penalty_time_when_no_timestamps(tmp_path: Path):
    import json
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

    pipeline = HighlightPipeline(
        config=config,
        video_path=tmp_path / "dummy.mp4",
        video_processor=FakeVideoProcessor(),
    )
    pipeline.reel_mode = "goals_with_pp_penalties"

    game_dir = tmp_path / "game"
    game_folders = {
        "game_dir": game_dir,
        "clips_dir": game_dir / "clips",
        "data_dir": game_dir / "data",
        "output_dir": game_dir / "output",
        "logs_dir": game_dir / "logs",
    }
    for p in game_folders.values():
        Path(p).mkdir(parents=True, exist_ok=True)
    pipeline.game_folders = game_folders

    # PP goal at 6:30 elapsed in P2 (video matched). No usable OCR timestamps for P2.
    pipeline.matched_events = [
        {
            "type": "goal",
            "period": 2,
            "time": "6:30",
            "team": "Amherst Ramblers",
            "scorer": "A",
            "assist1": "",
            "assist2": "",
            "special": "PP",
            "power_play": True,
            "short_handed": False,
            "empty_net": False,
            "video_time": 1000.0,
        }
    ]
    pipeline.video_timestamps = []

    pipeline.box_score = {
        "SiteKit": {
            "Gamesummary": {
                "penalties": [
                    {
                        "period": 2,
                        "time": "5:00",
                        "team": "Truro Bearcats",
                        "player": {"name": "John Smith", "number": None},
                        "infraction": "Hooking - Minor",
                        "minutes": 2,
                    }
                ]
            }
        }
    }

    pipeline._step6_create_clips(before_seconds=15.0, after_seconds=4.0)

    manifest_path = game_folders["data_dir"] / "clips_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    clips = manifest["clips"]

    assert [c["type"] for c in clips] == ["penalty", "goal"]
    assert clips[0].get("video_time") is not None


def test_pipeline_goals_only_mode_skips_pp_penalty_insertion(tmp_path: Path):
    import json
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

    pipeline = HighlightPipeline(
        config=config,
        video_path=tmp_path / "dummy.mp4",
        video_processor=FakeVideoProcessor(),
    )

    game_dir = tmp_path / "game"
    game_folders = {
        "game_dir": game_dir,
        "clips_dir": game_dir / "clips",
        "data_dir": game_dir / "data",
        "output_dir": game_dir / "output",
        "logs_dir": game_dir / "logs",
    }
    for p in game_folders.values():
        Path(p).mkdir(parents=True, exist_ok=True)
    pipeline.game_folders = game_folders

    pipeline.matched_events = [
        {
            "type": "goal",
            "period": 2,
            "time": "6:30",
            "team": "Amherst Ramblers",
            "scorer": "A",
            "assist1": "",
            "assist2": "",
            "special": "PP",
            "power_play": True,
            "short_handed": False,
            "empty_net": False,
            "video_time": 1000.0,
        }
    ]

    pipeline.video_timestamps = [
        {"video_time": 500.0, "period": 2, "game_time": "15:00", "game_time_seconds": 900},
        {"video_time": 1000.0, "period": 2, "game_time": "13:30", "game_time_seconds": 810},
    ]

    pipeline.box_score = {
        "SiteKit": {
            "Gamesummary": {
                "penalties": [
                    {
                        "period": 2,
                        "time": "5:00",
                        "team": "Truro Bearcats",
                        "player": {"name": "John Smith", "number": None},
                        "infraction": "Hooking - Minor",
                        "minutes": 2,
                    }
                ]
            }
        }
    }

    pipeline._step6_create_clips(before_seconds=15.0, after_seconds=4.0)

    manifest_path = game_folders["data_dir"] / "clips_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    clips = manifest["clips"]

    assert [c["type"] for c in clips] == ["goal"]


def test_pipeline_clock_stop_goals_get_extra_preroll(tmp_path: Path):
    import json
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
                created.append((dict(event), clip_path))
            return created

    pipeline = HighlightPipeline(
        config=config,
        video_path=tmp_path / "dummy.mp4",
        video_processor=FakeVideoProcessor(),
    )

    game_dir = tmp_path / "game"
    game_folders = {
        "game_dir": game_dir,
        "clips_dir": game_dir / "clips",
        "data_dir": game_dir / "data",
        "output_dir": game_dir / "output",
        "logs_dir": game_dir / "logs",
    }
    for p in game_folders.values():
        Path(p).mkdir(parents=True, exist_ok=True)
    pipeline.game_folders = game_folders

    pipeline.matched_events = [
        {
            "type": "goal",
            "period": 3,
            "time": "18:48",
            "team": "Amherst Ramblers",
            "scorer": "Anthony Morin",
            "assist1": "Owen Aura",
            "assist2": "",
            "video_time": 10602.0,
            "refined_by": "clock_stop",
            "match_confidence": 1.0,
        }
    ]
    pipeline.video_timestamps = []
    pipeline.box_score = {"SiteKit": {"Gamesummary": {"penalties": []}}}

    pipeline._step6_create_clips(before_seconds=15.0, after_seconds=4.0)

    manifest_path = game_folders["data_dir"] / "clips_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    clip = manifest["clips"][0]

    assert clip["before_seconds"] == config.GOAL_CLOCK_STOP_BEFORE_SECONDS
    assert clip["after_seconds"] == 4.0
