from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any


class OutputDirectoryError(RuntimeError):
    """Raised when the pipeline output base directory is missing or not writable."""


def validate_output_base_dir(output_dir: str) -> Path:
    """
    Ensure output_dir exists and is writable. Fails fast before RunContext allocates a run_id.
    """
    base = Path(output_dir).expanduser().resolve()
    try:
        base.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise OutputDirectoryError(
            f"Cannot create output directory {base}: {exc}"
        ) from exc
    probe = base / f".peopledd_write_probe_{uuid.uuid4().hex}"
    try:
        probe.write_text("ok", encoding="utf-8")
    except OSError as exc:
        raise OutputDirectoryError(
            f"Output directory is not writable: {base} ({exc})"
        ) from exc
    try:
        probe.unlink()
    except OSError:
        pass
    return base


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, data: Any) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def write_text(path: Path, content: str) -> None:
    ensure_dir(path.parent)
    path.write_text(content, encoding="utf-8")
