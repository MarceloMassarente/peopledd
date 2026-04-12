from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _env() -> dict[str, str]:
    return {**os.environ, "PYTHONPATH": str(_repo_root() / "src")}


def test_cli_describe_run_exits_zero() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "peopledd.cli", "--describe-run"],
        cwd=_repo_root(),
        env=_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "pipeline_stages" in json.loads(proc.stdout)


def test_cli_invalid_output_dir_exits_2(tmp_path: Path) -> None:
    blocker = tmp_path / "not_a_dir"
    blocker.write_text("x", encoding="utf-8")
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "peopledd.cli",
            "--company-name",
            "X",
            "--dry-run",
            "--output-dir",
            str(blocker),
        ],
        cwd=_repo_root(),
        env=_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 2
    assert proc.stdout == "" or "peopledd dry-run" not in proc.stdout


def test_cli_input_json_roundtrip(tmp_path: Path) -> None:
    sample = _repo_root() / "examples" / "input.sample.json"
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "peopledd.cli",
            "--input-json",
            str(sample),
            "--dry-run",
            "--output-dir",
            str(tmp_path),
        ],
        cwd=_repo_root(),
        env=_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "Ita" in proc.stdout or "Itaú" in proc.stdout
