from pathlib import Path

from local_archive_sync import (
    build_archive_status_payload,
    build_game_drive_folder_name,
    build_review_clip_filename,
    build_source_info_payload,
)


def test_build_game_drive_folder_name_prefers_canonical_info(tmp_path):
    game_dir = tmp_path / "2026-03-14_SWC_vs_AMH"
    game_dir.mkdir()

    name = build_game_drive_folder_name(
        game_dir,
        canonical_game_info={
            "date": "2026-03-14",
            "home_team": "Summerside Western Capitals",
            "away_team": "Amherst Ramblers",
        },
    )

    assert name == "2026-03-14 - Summerside Western Capitals vs Amherst Ramblers"


def test_build_source_info_payload_records_local_source(tmp_path):
    source_video = tmp_path / "game6.mp4"
    source_video.write_bytes(b"video")

    payload = build_source_info_payload(
        source_video=source_video,
        canonical_game_info={
            "date": "2026-03-26",
            "home_team": "Amherst Ramblers",
            "away_team": "Summerside Western Capitals",
            "league": "MHL",
        },
        game={
            "game_id": "4948",
            "home_game": True,
            "opponent": {"team_name": "Summerside Western Capitals"},
            "venue": "Amherst Stadium",
            "attendance": 2044,
            "schedule_notes": "Game # 6, EastLink South Semi-Final 1",
        },
    )

    assert payload["source"]["filename"] == "game6.mp4"
    assert payload["source"]["archive_mode"] == "local_source_sync"
    assert payload["game"]["game_id"] == "4948"
    assert payload["game"]["venue"] == "Amherst Stadium"


def test_build_review_clip_filename_uses_boxscore_fields():
    filename = build_review_clip_filename(
        {
            "type": "goal",
            "period": 1,
            "time": "15:21",
            "team": "Amherst Ramblers",
            "scorer": "Cooper Cormier",
            "assist1": "Anthony Gaudet",
            "assist2": "",
            "special": "PP",
        },
        index=1,
    )

    assert filename == "01 - P1 - 15-21 - Cormier - A1 Gaudet - PP.mp4"
    assert len(filename) < 80


def test_build_archive_status_payload_marks_incomplete_until_source_upload():
    pending = build_archive_status_payload(
        game_folder_id="game123",
        source_folder_id="source123",
        source_file_id="",
        source_file_name="game1.mp4",
        goal_review_folder_id="review123",
        goal_review_folder_url="https://drive.google.com/drive/folders/review123",
        goal_review_uploaded=5,
    )
    complete = build_archive_status_payload(
        game_folder_id="game123",
        source_folder_id="source123",
        source_file_id="file123",
        source_file_name="game1.mp4",
        goal_review_folder_id="review123",
        goal_review_folder_url="https://drive.google.com/drive/folders/review123",
        goal_review_uploaded=5,
    )

    assert pending["archive_complete"] is False
    assert pending["goal_review_uploaded"] == 5
    assert complete["archive_complete"] is True
