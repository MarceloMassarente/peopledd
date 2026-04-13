from __future__ import annotations

from pathlib import Path

from peopledd.runtime.source_memory import CompanyMemory, SourceMemoryStore, company_key_for
from peopledd.services.ri_surface_discoverer import discover_ri_surfaces


def test_company_key_stable() -> None:
    assert company_key_for("  Acme SA ", "br") == company_key_for("acme sa", "BR")


def test_load_missing_returns_none(tmp_path: Path) -> None:
    store = SourceMemoryStore(tmp_path / "_source_memory")
    assert store.load("abc") is None


def test_save_round_trip(tmp_path: Path) -> None:
    store = SourceMemoryStore(tmp_path / "_source_memory")
    key = company_key_for("X", "BR")
    m = CompanyMemory(company_key=key, useful_ri_surfaces=["https://x/ri/gov"])
    store.save(key, m)
    m2 = store.load(key)
    assert m2 is not None
    assert m2.useful_ri_surfaces == ["https://x/ri/gov"]


def test_corrupt_json_returns_none(tmp_path: Path) -> None:
    store = SourceMemoryStore(tmp_path / "_source_memory")
    store._dir.mkdir(parents=True, exist_ok=True)
    p = store._path(company_key_for("Y", "BR"))
    p.write_text("not json", encoding="utf-8")
    assert store.load(company_key_for("Y", "BR")) is None


def test_update_ri_success_moves_to_front(tmp_path: Path) -> None:
    store = SourceMemoryStore(tmp_path / "_source_memory")
    k = company_key_for("Z", "BR")
    store.save(
        k,
        CompanyMemory(
            company_key=k,
            useful_ri_surfaces=["https://old/", "https://new/"],
        ),
    )
    store.update_ri_success(k, "https://new/")
    m = store.load(k)
    assert m is not None
    assert m.useful_ri_surfaces[0] == "https://new/"


def test_discover_prioritizes_memory_urls() -> None:
    preferred = ["https://corp.example/mem-surface"]
    out = discover_ri_surfaces("https://corp.example/ri", None, preferred_urls=preferred)
    assert out[0] == "https://corp.example/mem-surface"
