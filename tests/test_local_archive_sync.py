from pathlib import Path

import drive_api

from local_archive_sync import (
    build_archive_status_payload,
    build_game_drive_folder_name,
    build_review_clip_filename,
    build_source_info_payload,
)


class _FakeRequest:
    def __init__(self, response=None, chunks=None):
        self.response = response
        self.chunks = list(chunks or [])
        self.next_chunk_calls = []

    def execute(self):
        return self.response

    def next_chunk(self, *, num_retries):
        self.next_chunk_calls.append(num_retries)
        response = self.chunks.pop(0)
        return object(), response


class _FakeFiles:
    def __init__(self, existing=None, *, create_response=None, update_response=None):
        self.existing = list(existing or [])
        self.create_response = create_response or {"id": "created-id"}
        self.update_response = update_response or {"id": "updated-id"}
        self.list_calls = []
        self.create_calls = []
        self.update_calls = []

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        return _FakeRequest({"files": self.existing})

    def create(self, **kwargs):
        self.create_calls.append(kwargs)
        return _FakeRequest(self.create_response)

    def update(self, **kwargs):
        self.update_calls.append(kwargs)
        return _FakeRequest(self.update_response)


class _FakeService:
    def __init__(self, files):
        self._files = files

    def files(self):
        return self._files


class _FakeMedia:
    calls = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.__class__.calls.append(kwargs)


def test_upsert_file_executes_small_create_and_returns_id(tmp_path, monkeypatch):
    local_path = tmp_path / "small.txt"
    local_path.write_text("small")
    files = _FakeFiles()
    monkeypatch.setattr("googleapiclient.http.MediaFileUpload", _FakeMedia)

    file_id = drive_api.upsert_file(
        _FakeService(files), local_path=local_path, parent_id="parent", drive_id="drive"
    )

    assert file_id == "created-id"
    assert files.create_calls[0]["body"] == {"name": "small.txt", "parents": ["parent"]}
    assert _FakeMedia.calls[-1] == {"filename": str(local_path.resolve()), "resumable": False}


def test_upsert_file_completes_resumable_update_after_multiple_chunks(tmp_path, monkeypatch):
    local_path = tmp_path / "large.bin"
    with local_path.open("wb") as handle:
        handle.truncate(drive_api.RESUMABLE_UPLOAD_THRESHOLD_BYTES)
    files = _FakeFiles(existing=[{"id": "existing-id", "name": "large.bin"}])
    request = _FakeRequest(chunks=[None, None, {"id": "ignored-response-id"}])
    files.update = lambda **kwargs: (files.update_calls.append(kwargs) or request)
    monkeypatch.setattr("googleapiclient.http.MediaFileUpload", _FakeMedia)

    file_id = drive_api.upsert_file(
        _FakeService(files), local_path=local_path, parent_id="parent", drive_id="drive"
    )

    assert file_id == "existing-id"
    assert request.next_chunk_calls == [5, 5, 5]
    assert _FakeMedia.calls[-1] == {
        "filename": str(local_path.resolve()),
        "resumable": True,
        "chunksize": drive_api.RESUMABLE_UPLOAD_CHUNK_SIZE,
    }
    assert files.update_calls[0]["fileId"] == "existing-id"


def test_upload_tree_propagates_skips_to_nested_directories(tmp_path, monkeypatch):
    root = tmp_path / "tree"
    nested = root / "keep" / "nested"
    nested.mkdir(parents=True)
    (root / "skip.txt").write_text("skip")
    (root / "keep" / "skip.txt").write_text("skip")
    (nested / "skip-prefix.log").write_text("skip")
    (nested / "upload.txt").write_text("upload")
    uploaded = []
    folders = []

    monkeypatch.setattr(
        drive_api,
        "ensure_folder",
        lambda service, *, parent_id, name, drive_id: (
            folders.append((parent_id, name)) or f"folder-{name}"
        ),
    )
    monkeypatch.setattr(
        drive_api,
        "upsert_file",
        lambda service, *, local_path, parent_id, drive_id, remote_name=None: uploaded.append(
            (Path(local_path).name, parent_id)
        ) or "file-id",
    )

    drive_api.upload_tree(
        _FakeService(_FakeFiles()),
        src_dir=root,
        dst_parent_id="root-id",
        drive_id="drive",
        skip_names={"skip.txt"},
        skip_prefixes={"skip-prefix"},
    )

    assert uploaded == [("upload.txt", "folder-nested")]
    assert folders == [("root-id", "keep"), ("folder-keep", "nested")]


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
