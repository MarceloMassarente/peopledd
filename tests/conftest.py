from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def clear_search_env(monkeypatch):
    """Avoid accidental EXA/SearXNG in unit tests affecting SearchOrchestrator construction."""
    for key in ("EXA_API_KEY", "SEARXNG_URL", "SEARXNG_INSTANCE"):
        monkeypatch.delenv(key, raising=False)
