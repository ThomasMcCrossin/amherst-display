from pathlib import Path


def test_create_game_folder_from_teams_uses_canonical_names(tmp_path):
    import config
    from highlight_extractor.file_manager import FileManager

    fm = FileManager(config)
    folders = fm.create_game_folder_from_teams(
        date="2026-01-31",
        home_team="Amherst Ramblers",
        away_team="Pictou County Weeks Crushers",
        league="MHL",
        filename="Replay- Home - 2026 Pictou County vs Amherst - Jan 31 @ 6 PM.ts",
        home_away="home",
        time_str="6.00pm",
        base_dir=tmp_path,
    )

    assert folders["game_dir"] == tmp_path / "2026-01-31_Amherst Ramblers_vs_Pictou County Weeks Crushers"
    # Sanity: key subfolders exist
    assert (folders["game_dir"] / "data").exists()
    assert (folders["game_dir"] / "clips").exists()
    assert (folders["game_dir"] / "output").exists()
    assert (folders["game_dir"] / "logs").exists()
    assert (folders["game_dir"] / "source").exists()


def test_pipeline_returns_structured_failure_on_video_load(tmp_path):
    import config
    from highlight_extractor.pipeline import HighlightPipeline

    class StubVideoProcessor:
        def __init__(self):
            self.duration = 0.0
            self.fps = 0.0

        def load_video(self):
            return False

        def cleanup(self):
            return None

    class StubBoxScoreFetcher:
        def find_game(self, league, home_team, away_team, game_date):
            return "stub-game-id"

        def fetch_box_score(self, league, game_id):
            return {"SiteKit": {"Gamesummary": {"meta": {"game_id": game_id}}}}

        def extract_events(self, box_score):
            return []

        def get_goals(self, box_score):
            return []

        def get_goal_summary(self, box_score, home_team, away_team):
            return None

    game_dir = tmp_path / "2026-01-31_Test_vs_Test"
    folders = {
        "game_dir": game_dir,
        "output_dir": game_dir / "output",
        "clips_dir": game_dir / "clips",
        "source_dir": game_dir / "source",
        "logs_dir": game_dir / "logs",
        "data_dir": game_dir / "data",
        "folder_name": game_dir.name,
    }
    for p in folders.values():
        if isinstance(p, Path):
            p.mkdir(parents=True, exist_ok=True)

    game_info = {
        "date": "2026-01-31",
        "home_team": "Amherst Ramblers",
        "away_team": "Pictou County Weeks Crushers",
        "league": "MHL",
        "filename": "Replay- Home - 2026 Pictou County vs Amherst - Jan 31 @ 6 PM.ts",
        "home_away": "home",
        "time": "6.00pm",
        "date_formatted": "January 31, 2026",
    }

    pipeline = HighlightPipeline(
        config=config,
        video_path=tmp_path / "dummy.ts",
        video_processor=StubVideoProcessor(),
        box_score_fetcher=StubBoxScoreFetcher(),
        game_info_override=game_info,
        game_folders_override=folders,
        source_game_info_override=game_info,
    )

    result = pipeline.execute(auto_detect_start=False, parallel_ocr=False)
    assert result.success is False
    assert result.failed_step == 3
    assert result.failed_reason == "video_load_failed"
    assert result.exception_type in {"ValueError", "Exception"}


def test_build_debug_bundle_zip_contains_high_signal_files(tmp_path):
    from zipfile import ZipFile

    from scripts.drive_ingest import _build_debug_bundle_zip

    game_dir = tmp_path / "2026-01-31_Test_vs_Test"
    (game_dir / "output").mkdir(parents=True, exist_ok=True)
    (game_dir / "logs").mkdir(parents=True, exist_ok=True)
    (game_dir / "data").mkdir(parents=True, exist_ok=True)
    (game_dir / "source").mkdir(parents=True, exist_ok=True)

    (game_dir / "output" / "preflight.json").write_text("{}", encoding="utf-8")
    (game_dir / "output" / "ingest_status.json").write_text("{}", encoding="utf-8")
    (game_dir / "logs" / "pipeline.log").write_text("pipeline", encoding="utf-8")
    (game_dir / "data" / "video_timestamps.json").write_text("{}", encoding="utf-8")
    (game_dir / "data" / "debug_ocr_frame_0001_1.0s.jpg").write_bytes(b"jpg")  # should be excluded
    (game_dir / "source" / "source_info.json").write_text("{}", encoding="utf-8")

    zip_path = _build_debug_bundle_zip(game_dir)
    assert zip_path is not None
    assert zip_path.exists()

    with ZipFile(zip_path, "r") as zf:
        names = set(zf.namelist())
    assert "output/preflight.json" in names
    assert "output/ingest_status.json" in names
    assert "logs/pipeline.log" in names
    assert "data/video_timestamps.json" in names
    assert "source/source_info.json" in names
    assert "data/debug_ocr_frame_0001_1.0s.jpg" not in names


def test_estimate_audio_delay_explicit_mode_does_not_probe():
    from pathlib import Path

    from scripts.drive_ingest import _estimate_audio_delay_seconds

    delay, auto_applied, debug = _estimate_audio_delay_seconds(
        Path("does_not_exist.ts"),
        explicit_delay_seconds=12.5,
        auto_threshold_seconds=60.0,
    )
    assert delay == 12.5
    assert auto_applied is False
    assert debug.get("mode") == "explicit"
