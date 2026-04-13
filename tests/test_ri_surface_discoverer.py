from __future__ import annotations

from peopledd.services.ri_surface_discoverer import discover_ri_surfaces


def test_discover_prefers_memory_urls_first() -> None:
    html = '<a href="/gov">other</a>'
    out = discover_ri_surfaces(
        "https://corp.example/ri/",
        html,
        preferred_urls=["https://corp.example/ri/mem"],
    )
    assert out[0] == "https://corp.example/ri/mem"
    assert "https://corp.example/ri/" in out


def test_discover_finds_governance_anchor() -> None:
    html = (
        '<html><a href="/pt/governanca-corporativa/">Governança corporativa</a></html>'
    )
    out = discover_ri_surfaces("https://corp.example/", html)
    assert any("/pt/governanca-corporativa" in u for u in out)


def test_discover_adds_static_suffixes_when_no_html() -> None:
    out = discover_ri_surfaces("https://corp.example/ri", None)
    assert "https://corp.example/ri" in out
    assert any(u.endswith("/governance") for u in out)
