import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from peopledd.models.common import ResolutionStatus
from peopledd.models.contracts import (
    BoardMember,
    GovernanceReconciliation,
    GovernanceSnapshot,
    HarvestRecallMeta,
    MatchedProfile,
    PersonResolution,
)
from peopledd.services.harvest_adapter import (
    HarvestAdapter,
    ProfileSearchOutcome,
    ProfileSearchResult,
    _harvest_canonical_linkedin_url,
    _is_likely_anonymized_linkedin_url,
    _name_similarity,
)
from peopledd.nodes import n2_person_resolution, n3_profile_enrichment


# ─────────────────────────────────────────────────────────────────────────────
# Unit tests — HarvestAdapter utilities
# ─────────────────────────────────────────────────────────────────────────────

def test_canonical_url_strips_slug():
    url = "https://www.linkedin.com/in/jo%C3%A3o-silva?trk=whatever"
    canonical = _harvest_canonical_linkedin_url(url)
    assert "joao-silva" in canonical
    assert "?" not in canonical


def test_canonical_url_no_linkedin():
    assert _harvest_canonical_linkedin_url("https://example.com") == "https://example.com"


def test_anonymized_url_acwaa():
    url = "https://www.linkedin.com/in/acwAABKkMEB496mCXBLRf"
    assert _is_likely_anonymized_linkedin_url(url) is True


def test_anonymized_url_normal():
    url = "https://www.linkedin.com/in/roberto-carlos-ferreira"
    assert _is_likely_anonymized_linkedin_url(url) is False


def test_name_similarity_exact():
    assert _name_similarity("Maria Costa Ferreira", "Maria Costa Ferreira") == 1.0


def test_name_similarity_homonym():
    # High but not exact
    score = _name_similarity("João Silva", "João Alberto Silva")
    assert score > 0.4


def test_name_similarity_different():
    score = _name_similarity("Roberto Lima Souza", "Ana Paula Rodrigues")
    assert score < 0.3


def test_profile_search_result_public_identifier_resolves_anonymized_url():
    data = {
        "linkedinUrl": "https://www.linkedin.com/in/acwAABKkMEB496mCXBLRf",
        "publicIdentifier": "joao-silva-real",
        "firstName": "Joao",
        "lastName": "Silva",
        "currentPositions": [{"companyName": "Acme"}],
    }
    r = ProfileSearchResult(data, "Joao Silva", "Acme")
    assert "joao-silva-real" in r.linkedin_url
    assert r.is_anonymized is False


def test_profile_search_result_anonymized_without_public_identifier_stays_filtered():
    data = {
        "linkedinUrl": "https://www.linkedin.com/in/acwAABKkMEB496mCXBLRf",
        "firstName": "Joao",
        "lastName": "Silva",
    }
    r = ProfileSearchResult(data, "Joao Silva", None)
    assert r.is_anonymized is True


def test_search_by_name_retries_when_primary_all_anonymized(tmp_path):
    db = tmp_path / "pipeline.sqlite"
    adapter = HarvestAdapter(api_key="k", pipeline_cache_db_path=str(db))
    primary = {
        "elements": [
            {
                "linkedinUrl": "https://www.linkedin.com/in/acwAABKkMEB496mCXBLRf",
                "firstName": "Maria",
                "lastName": "Costa",
                "currentPositions": [{"companyName": "Contoso"}],
            }
        ]
    }
    retry = {
        "elements": [
            {
                "linkedinUrl": "https://www.linkedin.com/in/maria-costa",
                "firstName": "Maria",
                "lastName": "Costa",
                "currentPositions": [{"companyName": "Contoso"}],
            }
        ]
    }
    mock_get = AsyncMock(side_effect=[primary, retry])
    with patch("peopledd.services.harvest_adapter._harvest_get", new=mock_get):
        out = adapter.search_by_name("Maria Costa", company="Contoso")
    assert mock_get.await_count == 2
    assert out.recall.profile_search_retry_used is True
    assert len(out.candidates) >= 1
    assert "maria-costa" in out.candidates[0].linkedin_url


# ─────────────────────────────────────────────────────────────────────────────
# Unit tests — HarvestAdapter.compute_profile_quality
# ─────────────────────────────────────────────────────────────────────────────

def test_profile_quality_none():
    adapter = HarvestAdapter(api_key="dummy")
    metrics = adapter.compute_profile_quality(None)
    assert metrics["useful_coverage_score"] == 0.0
    assert metrics["profile_confidence"] == 0.1


def test_profile_quality_rich_profile():
    adapter = HarvestAdapter(api_key="dummy")
    profile = {
        "name": "Roberto Lima",
        "headline": "CEO at Empresa XYZ",
        "about": "Executive with 20 years experience in financial services." * 3,
        "experience": [
            {"position": "CEO", "company": "XYZ", "is_current": True, "description": "Led the strategic transformation " * 5},
            {"position": "CFO", "company": "ABC", "is_current": False, "description": "Financial planning " * 5},
            {"position": "VP Finance", "company": "DEF", "is_current": False, "description": ""},
        ],
        "education": [{"title": "MBA", "degree": "MBA", "school": "FGV"}],
    }
    metrics = adapter.compute_profile_quality(profile)
    assert metrics["useful_coverage_score"] >= 0.6
    assert metrics["evidence_density"] >= 0.5  # 2 of 3 have descriptions
    assert metrics["recency_score"] >= 0.8  # first role is current


def test_build_career_summary_extracts_functions():
    adapter = HarvestAdapter(api_key="dummy")
    profile = {
        "experience": [
            {"position": "CFO", "company": "Banco do Brasil", "is_current": True},
            {"position": "Investor Relations Director", "company": "Petrobras", "is_current": False},
        ],
        "education": [],
    }
    career = adapter.build_career_summary(profile)
    assert "cfo" in career.get("functional_experience", []) or "financial" in career.get("functional_experience", [])
    assert len(career["current_roles"]) == 1
    assert len(career["prior_roles"]) == 1


# ─────────────────────────────────────────────────────────────────────────────
# Integration-style tests — n2 with mocked Harvest
# ─────────────────────────────────────────────────────────────────────────────

def _mock_reconciliation(names: list[str]) -> GovernanceReconciliation:
    board = [BoardMember(person_name=n, role="board-member") for n in names]
    snapshot = GovernanceSnapshot(board_members=board)
    return GovernanceReconciliation(
        reconciliation_status="clean",
        reconciled_governance_snapshot=snapshot,
    )


def test_n2_resolves_with_harvest():
    harvest_mock = MagicMock(spec=HarvestAdapter)

    # Simulate a single high-confidence result
    candidate = MagicMock()
    candidate.linkedin_url = "https://www.linkedin.com/in/roberto-lima"
    candidate.name = "Roberto Lima Carvalho"
    candidate.name_similarity = 0.85
    candidate.company_match = True
    candidate.is_anonymized = False

    harvest_mock.search_by_name.return_value = ProfileSearchOutcome(
        candidates=[candidate],
        recall=HarvestRecallMeta(
            raw_hits_profile_search=1,
            after_filter_count=1,
            resolution_attempted=True,
        ),
    )

    reconciled = _mock_reconciliation(["Roberto Lima Carvalho"])
    results = n2_person_resolution.run(reconciled, harvest_mock, company_name="Itaú")

    assert len(results) == 1
    assert results[0].resolution_status == ResolutionStatus.RESOLVED
    assert results[0].resolution_confidence > 0.8
    assert results[0].matched_profiles[0].provider == "harvest"


def test_n2_no_results_sets_not_found():
    harvest_mock = MagicMock(spec=HarvestAdapter)
    harvest_mock.search_by_name.return_value = ProfileSearchOutcome(
        candidates=[],
        recall=HarvestRecallMeta(
            raw_hits_profile_search=0,
            after_filter_count=0,
            resolution_attempted=True,
        ),
    )

    reconciled = _mock_reconciliation(["Pessoa Inexistente"])
    results = n2_person_resolution.run(reconciled, harvest_mock)

    assert len(results) == 1
    assert results[0].resolution_status == ResolutionStatus.NOT_FOUND
    assert results[0].harvest_recall is not None
    assert results[0].harvest_recall.resolution_attempted is True


def test_n2_secondary_sourcing_when_harvest_empty():
    harvest_mock = MagicMock(spec=HarvestAdapter)
    harvest_mock.search_by_name.return_value = ProfileSearchOutcome(
        candidates=[],
        recall=HarvestRecallMeta(resolution_attempted=True, raw_hits_profile_search=2),
    )
    search_orch = Mock()

    with patch(
        "peopledd.services.person_sourcing.linkedin_profile_urls",
        return_value=["https://www.linkedin.com/in/secondary-user"],
    ):
        reconciled = _mock_reconciliation(["Maria Secondary"])
        results = n2_person_resolution.run(
            reconciled,
            harvest_mock,
            company_name="Contoso",
            search_orchestrator=search_orch,
        )

    assert len(results) == 1
    assert results[0].resolution_status == ResolutionStatus.RESOLVED
    assert results[0].matched_profiles[0].provider == "exa_people"
    assert "linkedin.com/in/secondary-user" in results[0].matched_profiles[0].profile_id_or_url
    assert results[0].harvest_recall is not None
    assert results[0].harvest_recall.secondary_web_sourcing_used is True


def test_n2_ambiguous_when_two_similar():
    harvest_mock = MagicMock(spec=HarvestAdapter)

    c1 = MagicMock()
    c1.linkedin_url = "https://www.linkedin.com/in/joao-silva-1"
    c1.name = "João Silva"
    c1.name_similarity = 0.82
    c1.company_match = False
    c1.is_anonymized = False

    c2 = MagicMock()
    c2.linkedin_url = "https://www.linkedin.com/in/joao-silva-2"
    c2.name = "João Santos Silva"
    c2.name_similarity = 0.78
    c2.company_match = False
    c2.is_anonymized = False

    harvest_mock.search_by_name.return_value = ProfileSearchOutcome(
        candidates=[c1, c2],
        recall=HarvestRecallMeta(after_filter_count=2, resolution_attempted=True),
    )

    reconciled = _mock_reconciliation(["João Silva"])
    results = n2_person_resolution.run(reconciled, harvest_mock)

    assert results[0].resolution_status == ResolutionStatus.AMBIGUOUS
    assert results[0].resolution_confidence <= 0.65


# ─────────────────────────────────────────────────────────────────────────────
# Integration-style tests — n3 with mocked Harvest
# ─────────────────────────────────────────────────────────────────────────────

def _make_person_resolution(name: str, url: str | None = None) -> PersonResolution:
    import uuid
    matched = []
    if url:
        matched = [MatchedProfile(provider="harvest", profile_id_or_url=url, match_confidence=0.9)]
    return PersonResolution(
        person_id=str(uuid.uuid4()),
        observed_name=name,
        resolution_status=ResolutionStatus.RESOLVED if url else ResolutionStatus.NOT_FOUND,
        resolution_confidence=0.9 if url else 0.2,
        matched_profiles=matched,
    )


def test_n3_enriches_resolved_person():
    harvest_mock = MagicMock(spec=HarvestAdapter)
    harvest_mock.get_profile.return_value = {
        "name": "Ana Paula Costa",
        "headline": "CFO at Empresa ABC",
        "about": "Finance executive with 15 years experience.",
        "experience": [
            {"position": "CFO", "company": "Empresa ABC", "is_current": True, "description": "Led financial strategy"},
            {"position": "Controller", "company": "XYZ Corp", "is_current": False, "description": ""},
        ],
        "education": [{"title": "MBA FGV", "degree": "MBA", "school": "FGV"}],
    }
    harvest_mock.compute_profile_quality.return_value = {
        "useful_coverage_score": 0.7,
        "evidence_density": 0.5,
        "recency_score": 0.9,
        "profile_confidence": 0.65,
    }
    harvest_mock.build_career_summary.return_value = {
        "current_roles": ["CFO @ Empresa ABC"],
        "prior_roles": ["Controller @ XYZ Corp"],
        "functional_experience": ["financial"],
        "industry_experience": [],
        "governance_signals": ["executive_track"],
    }

    people = [_make_person_resolution("Ana Paula Costa", "https://www.linkedin.com/in/ana-paula-costa")]
    profiles = n3_profile_enrichment.run(people, harvest_mock)

    assert len(profiles) == 1
    profile = profiles[0]
    assert profile.profile_quality.nominal_hit is True
    assert "education_background" not in profile.blind_spots


def test_n3_exa_web_adds_blind_spot_when_no_harvest_profile():
    harvest_mock = MagicMock(spec=HarvestAdapter)
    harvest_mock.get_profile.return_value = None
    harvest_mock.compute_profile_quality.return_value = {
        "useful_coverage_score": 0.0,
        "evidence_density": 0.0,
        "recency_score": 0.0,
        "profile_confidence": 0.1,
    }
    harvest_mock.build_career_summary.return_value = {
        "current_roles": [],
        "prior_roles": [],
        "functional_experience": [],
        "industry_experience": [],
        "governance_signals": [],
    }
    people = [
        PersonResolution(
            person_id="p1",
            observed_name="X",
            resolution_status=ResolutionStatus.RESOLVED,
            resolution_confidence=0.5,
            matched_profiles=[
                MatchedProfile(
                    provider="exa_web",
                    profile_id_or_url="https://www.linkedin.com/in/x",
                    match_confidence=0.5,
                )
            ],
        )
    ]
    profiles = n3_profile_enrichment.run(people, harvest_mock)
    assert "exa_url_only_no_harvest_profile" in profiles[0].blind_spots


def test_n3_gracefully_handles_missing_profile():
    harvest_mock = MagicMock(spec=HarvestAdapter)
    harvest_mock.get_profile.side_effect = Exception("Connection error")
    harvest_mock.compute_profile_quality.return_value = {
        "useful_coverage_score": 0.0, "evidence_density": 0.0, "recency_score": 0.0, "profile_confidence": 0.1
    }
    harvest_mock.build_career_summary.return_value = {
        "current_roles": [], "prior_roles": [], "functional_experience": [],
        "industry_experience": [], "governance_signals": []
    }

    people = [_make_person_resolution("Carlos Henrique", "https://www.linkedin.com/in/carlos-henrique")]
    profiles = n3_profile_enrichment.run(people, harvest_mock)

    assert len(profiles) == 1
    assert profiles[0].profile_quality.nominal_hit is False
