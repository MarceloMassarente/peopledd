from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from peopledd.runtime.source_memory import SourceMemoryStore


def test_source_memory_concurrent_updates_same_key(tmp_path: Path) -> None:
    store = SourceMemoryStore(tmp_path)
    key = "abc123def456"
    n = 40

    def one(i: int) -> None:
        store.update_ri_success(key, f"https://ri.example.com/u{i%5}")

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = [ex.submit(one, i) for i in range(n)]
        for f in as_completed(futs):
            f.result()

    path = tmp_path / f"{key}.json"
    assert path.is_file()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert len(data.get("useful_ri_surfaces", [])) >= 1
