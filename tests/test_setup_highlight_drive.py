import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "setup_highlight_drive.py"


def test_setup_highlight_drive_dry_run_is_local(tmp_path):
    creds_path = tmp_path / "service-account.json"
    creds_path.write_text("{}", encoding="utf-8")

    manifest_path = tmp_path / "program.json"
    manifest_path.write_text(
        json.dumps(
            {
                "program_id": "mhl-amherst-ramblers-2025-26",
                "league": "MHL",
                "team": "Amherst Ramblers",
                "season": "2025-26",
                "drive_layout": {"root_folder": "Programs"},
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--drive-id",
            "0APnfOBS-rMGtUk9PVA",
            "--creds-path",
            str(creds_path),
            "--program-manifest",
            str(manifest_path),
            "--dry-run",
            "--print-manifest",
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        check=True,
        text=True,
        timeout=5,
    )

    payload = json.loads(result.stdout)
    assert payload["dry_run"] is True
    assert payload["drive"]["id"] == "0APnfOBS-rMGtUk9PVA"
    assert payload["folder_ids"]["games_root_path"].startswith("dryrun::")
