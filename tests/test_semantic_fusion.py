from __future__ import annotations

from unittest.mock import patch

from peopledd.models.common import CompanyMode, ResolutionStatus, ServiceLevel, SourceRef
from peopledd.models.contracts import (
    BoardMember,
    CanonicalEntity,
    CommitteeMember,
    ConfidencePolicy,
    CoverageScoring,
    DegradationProfile,
    EvidencePack,
    FinalReport,
    GovernanceFusionDecision,
    GovernanceFusionQuality,
    GovernanceIngestion,
    GovernanceObservation,
    GovernanceReconciliation,
    GovernanceSeed,
    SeedMember,
    GovernanceSnapshot,
    InputPayload,
    RequiredCapabilityModel,
    SemanticGovernanceFusion,
    StrategyChallenges,
)
from peopledd.nodes import n1c_semantic_fusion, n8_evidence_pack
from peopledd.services.governance_fusion_judge import (
    GovernanceCandidate,
    build_resolved_snapshot,
    cluster_observations,
    rule_based_fusion,
)
from peopledd.services.governance_observation_builder import build_governance_observations


def test_build_governance_observations_counts_tracks():
    ingestion = GovernanceIngestion(
        formal_governance_snapshot=GovernanceSnapshot(
            board_members=[
                BoardMember(
                    person_name="Maria Costa",
                    source_refs=[
                        SourceRef(source_type="cvm_fre_structured", label="FRE", url_or_ref="https://fre.example/zip")
                    ],
                )
            ],
        ),
        current_governance_snapshot=GovernanceSnapshot(
            board_members=[
                BoardMember(
                    person_name="Maria Costa Silva",
                    source_refs=[
                        SourceRef(source_type="ri", label="RI", url_or_ref="https://ri.example/gov")
                    ],
                )
            ],
        ),
        ingestion_metadata={"ri_scrape_url": "https://ri.example"},
    )
    obs = build_governance_observations(ingestion)
    assert len(obs) == 2
    tracks = {o.source_track for o in obs}
    assert "formal_fre" in tracks
    assert "current_ri" in tracks


def test_build_governance_observations_includes_seed_track():
    ingestion = GovernanceIngestion(
        formal_governance_snapshot=GovernanceSnapshot(),
        current_governance_snapshot=GovernanceSnapshot(),
    )
    seed = GovernanceSeed(
        company_name_queried="Sabesp",
        ri_url_candidate="https://ri.sabesp.com.br",
        board_members=[SeedMember(person_name="A Conselheira", role_or_title="Conselheira")],
        executive_members=[SeedMember(person_name="B CEO", role_or_title="CEO")],
        confidence=0.7,
        provider="perplexity_sonar",
        generated_at="2026-04-23T00:00:00Z",
    )
    obs = build_governance_observations(ingestion, governance_seed=seed)
    tracks = [o.source_track for o in obs]
    assert tracks.count("seed_sonar") == 2


def test_cluster_observations_merges_similar_names():
    obs = [
        GovernanceObservation(
            observation_id="o1",
            observed_name="Joao Silva",
            organ="board",
            source_track="formal_fre",
            source_ref=SourceRef(source_type="x", label="a", url_or_ref="u1"),
        ),
        GovernanceObservation(
            observation_id="o2",
            observed_name="Joao da Silva",
            organ="board",
            source_track="current_ri",
            source_ref=SourceRef(source_type="x", label="b", url_or_ref="u2"),
        ),
    ]
    cands = cluster_observations(obs)
    assert len(cands) == 1
    assert set(cands[0].observation_ids) == {"o1", "o2"}


def test_rule_based_fusion_prefers_more_recent_observation():
    obs = [
        GovernanceObservation(
            observation_id="o1",
            observed_name="Maria Silva",
            organ="board",
            source_track="current_ri",
            source_confidence=0.8,
            as_of_date="2019-01-01",
            source_ref=SourceRef(source_type="x", label="a", url_or_ref="u1"),
        ),
        GovernanceObservation(
            observation_id="o2",
            observed_name="Maria Silva",
            organ="board",
            source_track="current_ri",
            source_confidence=0.8,
            as_of_date="2024-06-01",
            source_ref=SourceRef(source_type="x", label="b", url_or_ref="u2"),
        ),
    ]
    cand = cluster_observations(obs)
    decs = rule_based_fusion(obs, cand)
    assert len(decs) == 1
    assert decs[0].supporting_observation_ids[0] == "o2"


def test_build_resolved_snapshot_materializes_committees():
    obs = [
        GovernanceObservation(
            observation_id="o1",
            observed_name="Pat",
            observed_role="chair",
            organ="committee",
            source_track="formal_fre",
            source_confidence=0.9,
            source_ref=SourceRef(source_type="x", label="c", url_or_ref="https://ri/c"),
            raw_attributes={"committee_name": "Audit", "committee_type": "audit"},
        ),
    ]
    cand = [GovernanceCandidate(candidate_id="c1", observation_ids=["o1"])]
    decs = rule_based_fusion(obs, cand)
    recon = GovernanceReconciliation(reconciled_governance_snapshot=GovernanceSnapshot())
    snap = build_resolved_snapshot(decs, obs, recon)
    assert len(snap.committees) == 1
    assert snap.committees[0].committee_name == "Audit"
    assert snap.committees[0].committee_type == "audit"
    assert isinstance(snap.committees[0].members[0], CommitteeMember)


def test_rule_based_fusion_single_decision():
    obs = [
        GovernanceObservation(
            observation_id="a1",
            observed_name="Pedro",
            observed_role="CEO",
            organ="executive",
            source_track="formal_fre",
            source_confidence=0.9,
            source_ref=SourceRef(source_type="x", label="f", url_or_ref="u"),
        )
    ]
    cand = [GovernanceCandidate(candidate_id="c1", observation_ids=["a1"])]
    decs = rule_based_fusion(obs, cand)
    assert len(decs) == 1
    assert decs[0].canonical_name == "Pedro"
    assert decs[0].organ == "executive"


def test_n1c_semantic_fusion_runs_without_llm():
    ingestion = GovernanceIngestion(
        formal_governance_snapshot=GovernanceSnapshot(
            board_members=[
                BoardMember(
                    person_name="Ana",
                    source_refs=[SourceRef(source_type="fre", label="f", url_or_ref="u1")],
                )
            ],
        ),
        current_governance_snapshot=GovernanceSnapshot(),
    )
    recon = GovernanceReconciliation(
        reconciled_governance_snapshot=GovernanceSnapshot(
            board_members=[
                BoardMember(
                    person_name="Ana",
                    source_refs=[SourceRef(source_type="fre", label="f", url_or_ref="u1")],
                )
            ],
        ),
    )
    with patch("peopledd.services.governance_fusion_judge.llm_judge_fusion", return_value=None):
        out = n1c_semantic_fusion.run(
            ingestion,
            recon,
            company_name="Acme",
            harvest=None,
            search_orchestrator=None,
            use_harvest=False,
            prefer_llm=True,
        )
    assert out.fusion_quality.observation_count >= 1
    assert out.fusion_decisions
    assert out.resolved_snapshot.board_members


def test_n8_emits_fusion_claims_when_semantic_present():
    entity = CanonicalEntity(
        entity_id="e",
        input_company_name="X",
        resolution_status=ResolutionStatus.RESOLVED,
        company_mode=CompanyMode.LISTED_BR,
        resolution_evidence=[
            SourceRef(source_type="cvm_cad", label="cad", url_or_ref="https://cvm.example/cad.csv")
        ],
    )
    ingestion = GovernanceIngestion(
        formal_governance_snapshot=GovernanceSnapshot(),
        current_governance_snapshot=GovernanceSnapshot(),
    )
    recon = GovernanceReconciliation(
        reconciled_governance_snapshot=GovernanceSnapshot(),
    )
    sem = SemanticGovernanceFusion(
        fusion_decisions=[
            GovernanceFusionDecision(
                decision_id="dec1",
                candidate_id="c1",
                canonical_name="Test Person",
                organ="board",
                decision_status="resolved",
                confidence=0.88,
                supporting_observation_ids=["obs1"],
                discarded_observation_ids=[],
                decision_rationale_code="rule_fallback",
            )
        ],
        fusion_quality=GovernanceFusionQuality(
            observation_count=1,
            candidate_count=1,
            llm_judge_used=False,
            judge_passes=1,
        ),
    )
    report = FinalReport(
        input_payload=InputPayload(company_name="Co"),
        entity_resolution=entity,
        governance=ingestion,
        governance_reconciliation=recon,
        semantic_governance_fusion=sem,
        people_resolution=[],
        people_profiles=[],
        strategy_and_challenges=StrategyChallenges(),
        required_capability_model=RequiredCapabilityModel(),
        coverage_scoring=CoverageScoring(),
        improvement_hypotheses=[],
        evidence_pack=EvidencePack(),
        degradation_profile=DegradationProfile(service_level=ServiceLevel.SL1),
        confidence_policy=ConfidencePolicy(),
    )
    pack = n8_evidence_pack.run(partial_report=report, run_id="t1")
    ids = {c.claim_id for c in pack.claims}
    assert "C_FUSION_DEC_1" in ids
    fusion_claim = next(c for c in pack.claims if c.claim_id == "C_FUSION_DEC_1")
    assert fusion_claim.observation_ids == ["obs1"]
    assert fusion_claim.fusion_decision_id == "dec1"
