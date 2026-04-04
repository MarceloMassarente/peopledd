import pytest
from unittest.mock import MagicMock, patch
from peopledd.models.contracts import (
    KeyChallenge,
    PersonProfile,
    ProfileQuality,
    RequiredCapability,
    RequiredCapabilityModel,
    StrategicPriority,
    StrategyChallenges,
)
from peopledd.nodes import n4_strategy_inference, n5_required_capability_model, n6_coverage_scoring
from peopledd.services.strategy_retriever import StrategyRetriever, _empty_strategy_dict


# ─────────────────────────────────────────────────────────────────────────────
# n4 — strategy inference
# ─────────────────────────────────────────────────────────────────────────────

def _mock_retriever(raw: dict) -> StrategyRetriever:
    retriever = MagicMock(spec=StrategyRetriever)
    retriever.retrieve.return_value = raw
    return retriever


def test_n4_extracts_priorities():
    raw = {
        "strategic_priorities": [
            {"priority": "Expansão internacional", "time_horizon": "medium", "confidence": 0.8, "evidence_snippet": "RA 2024"},
            {"priority": "Transformação digital", "time_horizon": "short", "confidence": 0.75, "evidence_snippet": "Digital deck"},
        ],
        "key_challenges": [],
        "recent_triggers": ["Aquisição empresa ABC"],
        "company_phase_hypothesis": {"phase": "expansion", "confidence": 0.7, "rationale": ""},
    }
    result = n4_strategy_inference.run("Empresa X", ri_url="https://ri.empresa-x.com", retriever=_mock_retriever(raw))

    assert len(result.strategic_priorities) == 2
    assert result.strategic_priorities[0].time_horizon == "medium"
    assert result.strategic_priorities[0].confidence == 0.8
    assert result.company_phase_hypothesis["phase"] == "expansion"
    assert "Aquisição empresa ABC" in result.recent_triggers


def test_n4_empty_when_no_content():
    raw = _empty_strategy_dict()
    result = n4_strategy_inference.run("Empresa Vazia", retriever=_mock_retriever(raw))

    assert result.strategic_priorities == []
    assert result.key_challenges == []
    assert result.company_phase_hypothesis["phase"] == "mixed"


def test_n4_maps_external_sonar_briefs():
    raw = {
        "strategic_priorities": [],
        "key_challenges": [],
        "recent_triggers": [],
        "company_phase_hypothesis": {"phase": "mixed", "confidence": 0.5},
        "external_sonar_briefs": [
            {
                "role": "recent_company_facts",
                "body": "M&A anunciado.",
                "source_refs": [
                    {
                        "source_type": "perplexity_sonar_pro",
                        "label": "Fatos recentes (web)",
                        "url_or_ref": "https://example.com/news",
                    },
                ],
            },
        ],
    }
    result = n4_strategy_inference.run("Empresa Sonar", retriever=_mock_retriever(raw))
    assert len(result.external_sonar_briefs) == 1
    assert result.external_sonar_briefs[0].body == "M&A anunciado."
    assert result.external_sonar_briefs[0].source_refs[0].url_or_ref == "https://example.com/news"


def test_n4_skips_empty_priority_strings():
    raw = {
        "strategic_priorities": [
            {"priority": "", "time_horizon": "short", "confidence": 0.5},
            {"priority": "   ", "time_horizon": "medium", "confidence": 0.5},
            {"priority": "Crescimento sustentável", "time_horizon": "long", "confidence": 0.9},
        ],
        "key_challenges": [],
        "recent_triggers": [],
        "company_phase_hypothesis": {"phase": "mature", "confidence": 0.6},
    }
    result = n4_strategy_inference.run("Empresa Filtro", retriever=_mock_retriever(raw))
    assert len(result.strategic_priorities) == 1
    assert result.strategic_priorities[0].priority == "Crescimento sustentável"


# ─────────────────────────────────────────────────────────────────────────────
# n5 — capability model
# ─────────────────────────────────────────────────────────────────────────────

def _make_strategy(priorities=None, challenges=None) -> StrategyChallenges:
    return StrategyChallenges(
        strategic_priorities=priorities or [],
        key_challenges=challenges or [],
        recent_triggers=[],
        company_phase_hypothesis={"phase": "mixed", "confidence": 0.5},
    )


def test_n5_creates_baseline_when_no_strategy():
    strategy = _make_strategy()
    result = n5_required_capability_model.run("financeiro", strategy)

    # Should have at least the 3 baseline dimensions
    assert len(result.board_required_capabilities) >= 3


def test_n5_general_unknown_sector_uses_inline_fallback_baseline():
    strategy = _make_strategy()
    result = n5_required_capability_model.run("general", strategy)
    assert len(result.board_required_capabilities) >= 3


def test_n5_regulatory_challenge_bumps_grc():
    challenges = [KeyChallenge(
        challenge="Pressão regulatória crescente",
        challenge_type="regulatory",
        severity="high",
        confidence=0.85,
    )]
    strategy = _make_strategy(challenges=challenges)
    result = n5_required_capability_model.run("financeiro", strategy)

    grc_caps = [c for c in result.board_required_capabilities if c.dimension == "governanca_risco_compliance"]
    assert grc_caps, "GRC dimension should be present"
    assert grc_caps[0].required_level >= 4
    assert grc_caps[0].origin in ("challenge_overlay", "sector_baseline")


def test_n5_digital_priority_adds_tech_dimension():
    priorities = [StrategicPriority(
        priority="Transformação digital acelerada",
        time_horizon="short",
        confidence=0.9,
    )]
    strategy = _make_strategy(priorities=priorities)
    result = n5_required_capability_model.run("varejo", strategy)

    tech_caps = [c for c in result.board_required_capabilities if c.dimension == "transformacao_tecnologia"]
    assert tech_caps, "Tech dimension must be present for digital priority"


def test_n5_people_high_challenge_adds_to_executive():
    challenges = [KeyChallenge(
        challenge="Alta rotatividade de líderes",
        challenge_type="people",
        severity="high",
        confidence=0.7,
    )]
    strategy = _make_strategy(challenges=challenges)
    result = n5_required_capability_model.run("tecnologia", strategy)

    exec_dims = [c.dimension for c in result.executive_required_capabilities]
    assert "lideranca_pessoas" in exec_dims


def test_n5_weights_normalize_to_one():
    challenges = [
        KeyChallenge(challenge="Risco financeiro", challenge_type="financial", severity="high", confidence=0.8),
        KeyChallenge(challenge="Risco regulatório", challenge_type="regulatory", severity="medium", confidence=0.7),
    ]
    strategy = _make_strategy(challenges=challenges)
    result = n5_required_capability_model.run("financeiro", strategy)

    total_weight = sum(c.importance_weight for c in result.board_required_capabilities)
    assert abs(total_weight - 1.0) < 0.01, f"Board weights should sum to 1.0, got {total_weight}"


# ─────────────────────────────────────────────────────────────────────────────
# n6 — coverage scoring
# ─────────────────────────────────────────────────────────────────────────────

def _make_profile(person_id: str, functional: list[str], gov_signals: list[str], confidence: float) -> PersonProfile:
    return PersonProfile(
        person_id=person_id,
        career_summary={
            "current_roles": [],
            "prior_roles": [],
            "functional_experience": functional,
            "industry_experience": [],
            "governance_signals": gov_signals,
        },
        profile_quality=ProfileQuality(
            nominal_hit=True,
            useful_coverage_score=0.7,
            evidence_density=0.6,
            recency_score=0.8,
            profile_confidence=confidence,
        ),
    )


def _make_required(dims: list[str]) -> RequiredCapabilityModel:
    board = [
        RequiredCapability(
            dimension=d, required_level=4, importance_weight=1.0 / len(dims),
            origin="sector_baseline", rationale="test"
        )
        for d in dims
    ]
    return RequiredCapabilityModel(board_required_capabilities=board, executive_required_capabilities=[])


def test_n6_coverage_with_real_functional_experience():
    profiles = [
        _make_profile("p1", functional=["financial", "cfo"], gov_signals=["audit_committee"], confidence=0.85),
        _make_profile("p2", functional=["general_management", "ceo"], gov_signals=["board_experience"], confidence=0.75),
    ]
    required = _make_required(["capital_allocation", "governanca_risco_compliance", "execucao_operacional"])
    result = n6_coverage_scoring.run(required, profiles, board_size=5, executive_size=3)

    dim_names = {item.dimension for item in result.board_coverage}
    assert "capital_allocation" in dim_names
    assert "governanca_risco_compliance" in dim_names

    # Capital allocation should have evidence (from financial/cfo)
    cap_alloc = next(i for i in result.board_coverage if i.dimension == "capital_allocation")
    assert cap_alloc.observed_level > 0
    assert "sem evidência" not in cap_alloc.rationale.lower() or cap_alloc.observed_level > 0


def test_n6_spof_when_single_person_covers_critical():
    # Only one person covers capital_allocation
    profiles = [
        _make_profile("p1", functional=["financial"], gov_signals=[], confidence=0.80),
    ]
    required = _make_required(["capital_allocation"])
    result = n6_coverage_scoring.run(required, profiles, board_size=4, executive_size=2)

    cap_alloc = next(i for i in result.board_coverage if i.dimension == "capital_allocation")
    assert cap_alloc.single_point_of_failure is True
    assert "spof_in_critical_dimension" in result.organ_level_flags


def test_n6_low_confidence_flag():
    profiles = [
        _make_profile("p1", functional=[], gov_signals=[], confidence=0.3),
        _make_profile("p2", functional=[], gov_signals=[], confidence=0.4),
    ]
    required = _make_required(["capital_allocation"])
    result = n6_coverage_scoring.run(required, profiles, board_size=2, executive_size=1)

    assert "low_confidence_dimension" in result.organ_level_flags


def test_n6_empty_profiles_fallback():
    required = _make_required(["capital_allocation", "transformacao_tecnologia"])
    result = n6_coverage_scoring.run(required, [], board_size=0, executive_size=0)

    assert len(result.board_coverage) == 2
    # All should use proxy rationale
    for item in result.board_coverage:
        assert item.coverage_ratio >= 0.0
