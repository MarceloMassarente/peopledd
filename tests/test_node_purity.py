from __future__ import annotations

import ast
from pathlib import Path


def _forbidden_in_nodes_module(source: str) -> list[str]:
    tree = ast.parse(source)
    bad: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.name
                if name.startswith("peopledd.runtime.context") or name == "RunContext":
                    bad.append(f"import {name}")
                if name.startswith("peopledd.utils.io") or name == "write_json":
                    bad.append(f"import {name}")
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod.startswith("peopledd.runtime.context") or mod == "peopledd.utils.io":
                bad.append(f"from {mod}")
            for alias in node.names:
                if alias.name in ("RunContext", "write_json", "write_text"):
                    if mod.startswith("peopledd.runtime.context") or mod.startswith("peopledd.utils.io"):
                        bad.append(f"from {mod} import {alias.name}")
    return bad


def test_nodes_do_not_import_runcontext_or_write_json() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "peopledd" / "nodes"
    for path in sorted(root.glob("*.py")):
        if path.name == "__init__.py":
            continue
        src = path.read_text(encoding="utf-8")
        hits = _forbidden_in_nodes_module(src)
        assert not hits, f"{path.name}: forbidden imports for node purity contract: {hits}"
