from __future__ import annotations

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from peopledd.api import _resolve_owner_for_create, app


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("PEOPLEDD_API_KEY", raising=False)
    monkeypatch.delenv("PEOPLEDD_ALLOW_LEGACY_UNAUTH", raising=False)
    return TestClient(app)


def test_health_reports_database_not_configured(client: TestClient) -> None:
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "healthy"
    assert body["database_configured"] is False


def test_post_jobs_returns_503_without_database_url(client: TestClient) -> None:
    r = client.post(
        "/jobs",
        json={"company_name": "Acme", "country": "BR"},
    )
    assert r.status_code == 503


def test_legacy_analyze_hidden_without_allow_legacy(client: TestClient) -> None:
    r = client.post("/analyze", json={"company_name": "Acme", "country": "BR"})
    assert r.status_code == 404


def test_legacy_analyze_requires_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PEOPLEDD_ALLOW_LEGACY_UNAUTH", "true")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with TestClient(app) as c:
        r = c.post("/analyze", json={"company_name": "Acme", "country": "BR"})
        assert r.status_code == 503


def test_resolve_owner_for_create_requires_header_when_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PEOPLEDD_API_KEY", "secret")
    with pytest.raises(HTTPException) as excinfo:
        _resolve_owner_for_create(None, None)
    assert excinfo.value.status_code == 400


def test_resolve_owner_for_create_body_must_match_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PEOPLEDD_API_KEY", "secret")
    with pytest.raises(HTTPException) as excinfo:
        _resolve_owner_for_create("user-b", "user-a")
    assert excinfo.value.status_code == 400
    assert "match" in excinfo.value.detail.lower()


def test_resolve_owner_for_create_allows_matching_body_and_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PEOPLEDD_API_KEY", "secret")
    assert _resolve_owner_for_create("user-a", "user-a") == "user-a"


def test_list_jobs_requires_subject_when_database_without_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.delenv("PEOPLEDD_API_KEY", raising=False)
    with TestClient(app) as c:
        r = c.get("/jobs")
    assert r.status_code == 400
    assert "X-User-Subject" in r.json()["detail"]
