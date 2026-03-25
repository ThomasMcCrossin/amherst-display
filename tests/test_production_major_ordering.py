import json
import runpy
from pathlib import Path


def test_production_builder_inserts_approved_majors_by_video_time(tmp_path: Path):
    mod = runpy.run_path(Path(__file__).resolve().parents[1] / "scripts" / "build_production_highlight_reel.py")
    load_items = mod["_load_clip_items"]

    game_dir = tmp_path / "game"
    clips_dir = game_dir / "clips"
    data_dir = game_dir / "data"
    major_dir = game_dir / "output" / "major_review"
    for p in (clips_dir, data_dir, major_dir):
        p.mkdir(parents=True, exist_ok=True)

    # Dummy clip files referenced by manifests.
    (clips_dir / "01_GOAL.mp4").write_bytes(b"0")
    (clips_dir / "02_GOAL.mp4").write_bytes(b"0")
    (major_dir / "major.mp4").write_bytes(b"0")

    clips_manifest = data_dir / "clips_manifest.json"
    clips_manifest.write_text(
        json.dumps(
            {
                "clips": [
                    {"type": "goal", "video_time": 100.0, "path": "clips/01_GOAL.mp4", "index": 1},
                    {"type": "goal", "video_time": 200.0, "path": "clips/02_GOAL.mp4", "index": 2},
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    major_manifest = data_dir / "major_penalty_approved.json"
    major_manifest.write_text(
        json.dumps(
            {
                "approved": [
                    {"type": "penalty", "video_time": 150.0, "path": "output/major_review/major.mp4"},
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    events_json = data_dir / "matched_events.json"
    events_json.write_text("[]", encoding="utf-8")

    items = load_items(
        game_dir=game_dir,
        clips_dir=clips_dir,
        events_json=events_json,
        clips_manifest=clips_manifest,
        major_approved_json=major_manifest,
    )

    assert [it.clip_path.name for it in items] == ["01_GOAL.mp4", "major.mp4", "02_GOAL.mp4"]

