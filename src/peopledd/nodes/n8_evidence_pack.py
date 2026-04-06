from __future__ import annotations

"""
n8_evidence_pack — monta EvidencePack a partir do FinalReport (proveniência auditável).

Documentos e claims são derivados deterministicamente; o orquestrador não precisa injetar dicts manuais.
"""

import re
from datetime import datetime, timezone

from peopledd.models.common import SourceRef
from peopledd.models.contracts import (
    EvidenceClaim,
    EvidenceDocument,
    EvidencePack,
    FinalReport,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slug(s: str, max_len: int = 48) -> str:
    t = re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")
    return (t[:max_len] or "x").rstrip("_")


def _collect_snapshot_source_refs(report: FinalReport) -> list[SourceRef]:
    out: list[SourceRef] = []
    for snap in (
        report.governance.formal_governance_snapshot,
        report.governance.current_governance_snapshot,
    ):
        for m in snap.board_members:
            out.extend(m.source_refs)
        for e in snap.executive_members:
            out.extend(e.source_refs)
        for c in snap.committees:
            out.extend(c.source_refs)
        for f in snap.fiscal_council:
            out.extend(f.source_refs)
    return out


def run(
    partial_report: FinalReport | None,
    docs: list[dict] | None = None,
    claims: list[dict] | None = None,
    run_id: str | None = None,
) -> EvidencePack:
    """
    Build evidence pack from partial_report. Optional legacy docs/claims merge for tests/overrides.

    Args:
        partial_report: FinalReport with improvement_hypotheses possibly empty.
        docs: Optional extra document dicts (merged after built-in docs, deduped by doc_id).
        claims: Optional extra claim dicts (appended).
        run_id: Pipeline run UUID for C_PIPELINE_RUN claim.
    """
    now = _now_iso()
    documents: list[EvidenceDocument] = []
    evidence_claims: list[EvidenceClaim] = []
    seen_doc_ids: set[str] = set()

    def add_doc(
        doc_id: str,
        source_type: str,
        title: str,
        url_or_ref: str,
        date: str | None = None,
    ) -> None:
        if doc_id in seen_doc_ids:
            return
        seen_doc_ids.add(doc_id)
        documents.append(
            EvidenceDocument(
                doc_id=doc_id,
                source_type=source_type,
                title=title,
                date=date,
                url_or_ref=url_or_ref,
                retrieval_timestamp=now,
            )
        )

    if partial_report is None:
        merged_docs = list(docs or [])
        merged_claims = [EvidenceClaim(**c) for c in (claims or [])]
        return EvidencePack(documents=[EvidenceDocument(**d) for d in merged_docs], claims=merged_claims)

    entity = partial_report.entity_resolution
    gov = partial_report.governance
    recon = partial_report.governance_reconciliation
    strategy = partial_report.strategy_and_challenges
    coverage = partial_report.coverage_scoring
    deg = partial_report.degradation_profile
    conf = partial_report.confidence_policy
    meta = gov.ingestion_metadata or {}

    cad_url = (
        entity.resolution_evidence[0].url_or_ref
        if entity.resolution_evidence
        else "degraded://cvm_cad_unknown"
    )
    add_doc("D_CVM_CAD", "cvm_cad", "Referência cadastro CVM (dados abertos)", cad_url)

    fre_url = meta.get("fre_source_url") or "degraded://fre_not_available"
    fre_year = meta.get("fre_year")
    fre_title = f"Formulário de Referência (FRE) — ano {fre_year}" if fre_year else "FRE formal — indisponível ou sem membros parseados"
    add_doc("D_FRE", "cvm_fre_structured", fre_title, fre_url, date=fre_year)

    ri_url = entity.ri_url or meta.get("ri_scrape_url") or "degraded://ri_url_unknown"
    add_doc("D_RI_GOV", "ri", "Governança observada (Relações com Investidores)", ri_url)

    run_ref = run_id or "unknown"
    add_doc(
        "D_RECON",
        "reconciliation",
        "Reconciliação dual-track (formal vs current)",
        f"internal://reconciliation/{run_ref}",
    )

    url_to_doc_id: dict[str, str] = {}
    strat_idx = 0
    for prio in strategy.strategic_priorities:
        for sr in prio.source_refs:
            u = (sr.url_or_ref or "").strip()
            if not u or u in url_to_doc_id:
                continue
            did = f"D_STRATEGY_{strat_idx}"
            strat_idx += 1
            url_to_doc_id[u] = did
            add_doc(
                did,
                sr.source_type or "web",
                sr.label or "Fonte estratégia",
                u,
                date=sr.date,
            )
    for ch in strategy.key_challenges:
        for sr in ch.source_refs:
            u = (sr.url_or_ref or "").strip()
            if not u or u in url_to_doc_id:
                continue
            did = f"D_STRATEGY_{strat_idx}"
            strat_idx += 1
            url_to_doc_id[u] = did
            add_doc(
                did,
                sr.source_type or "web",
                sr.label or "Fonte desafio",
                u,
                date=sr.date,
            )

    for brief in strategy.external_sonar_briefs:
        for sr in brief.source_refs:
            u = (sr.url_or_ref or "").strip()
            if not u or u in url_to_doc_id:
                continue
            did = f"D_SONAR_{strat_idx}"
            strat_idx += 1
            url_to_doc_id[u] = did
            add_doc(
                did,
                sr.source_type or "perplexity_sonar_pro",
                sr.label or f"Sonar ({brief.role})",
                u,
                date=sr.date,
            )

    mp = partial_report.market_pulse
    add_doc(
        "D_MARKET_PULSE",
        "market_pulse",
        "Midia publica agregada (Exa news + SearXNG, pos-n4)",
        f"internal://market_pulse/{run_ref}",
    )
    mkt_idx = 0
    for hit in mp.source_hits:
        u = (hit.url or "").strip()
        if not u or u in url_to_doc_id:
            continue
        did = f"D_MKT_{mkt_idx}"
        mkt_idx += 1
        url_to_doc_id[u] = did
        add_doc(
            did,
            hit.provider,
            (hit.title or "Midia")[:200],
            u,
            date=hit.published_date,
        )

    for i, sr in enumerate(_collect_snapshot_source_refs(partial_report)):
        u = (sr.url_or_ref or "").strip()
        if not u or u in url_to_doc_id:
            continue
        did = f"D_SNAP_{i}"
        url_to_doc_id[u] = did
        add_doc(did, sr.source_type or "governance_snapshot", sr.label or "Snapshot governança", u, date=sr.date)

    recon_snap = recon.reconciled_governance_snapshot

    evidence_claims.append(
        EvidenceClaim(
            claim_id="C_ENTITY_SCOPE",
            claim_text=(
                f"Escopo de análise: {entity.analysis_scope_entity or entity.input_company_name} "
                f"(status={entity.resolution_status.value}, confiança={entity.resolution_confidence:.2f})"
            ),
            claim_type="fact",
            source_refs=["D_CVM_CAD", "D_RI_GOV"],
            confidence=entity.resolution_confidence,
        )
    )

    evidence_claims.append(
        EvidenceClaim(
            claim_id="C_BOARD_RECON_COUNT",
            claim_text=f"Conselho reconciliado: {len(recon_snap.board_members)} membros.",
            claim_type="fact",
            source_refs=["D_FRE", "D_RI_GOV", "D_RECON"],
            confidence=0.85 if recon.reconciliation_status == "clean" else 0.65,
        )
    )

    evidence_claims.append(
        EvidenceClaim(
            claim_id="C_EXEC_RECON_COUNT",
            claim_text=f"Diretoria reconciliada: {len(recon_snap.executive_members)} membros.",
            claim_type="fact",
            source_refs=["D_FRE", "D_RI_GOV", "D_RECON"],
            confidence=0.85 if recon.reconciliation_status == "clean" else 0.65,
        )
    )

    evidence_claims.append(
        EvidenceClaim(
            claim_id="C_RECON_STATUS",
            claim_text=f"Status de reconciliação: {recon.reconciliation_status}; conflitos={len(recon.conflict_items)}.",
            claim_type="fact",
            source_refs=["D_RECON"],
            confidence=0.9,
        )
    )

    for i, ci in enumerate(recon.conflict_items, start=1):
        cid = f"C_CONFLICT_{i}"
        evidence_claims.append(
            EvidenceClaim(
                claim_id=cid,
                claim_text=(
                    f"Conflito {ci.conflict_type} "
                    f"(pessoa={ci.person_name or 'n/d'}): formal={ci.formal_value!r} vs current={ci.current_value!r}."
                ),
                claim_type="inference",
                source_refs=["D_RECON", "D_FRE", "D_RI_GOV"],
                confidence=ci.confidence,
            )
        )

    for mi, mc in enumerate(mp.claims, start=1):
        doc_refs: list[str] = []
        for u in mc.source_urls:
            u = (u or "").strip()
            rid = url_to_doc_id.get(u)
            if rid:
                doc_refs.append(rid)
        if not doc_refs:
            doc_refs = ["D_MARKET_PULSE"]
        evidence_claims.append(
            EvidenceClaim(
                claim_id=f"C_MARKET_{mi}",
                claim_text=(
                    f"[{mc.topic}] {mc.statement} "
                    f"(sentimento={mc.sentiment}, alinhamento_RI={mc.alignment_with_ri})"
                ),
                claim_type="market_pulse",
                source_refs=doc_refs,
                confidence=mc.confidence,
            )
        )

    sem = partial_report.semantic_governance_fusion
    if sem is not None:
        add_doc(
            "D_FUSION_SEM",
            "semantic_fusion",
            "Fusao semantica multi-fonte (n1c)",
            f"internal://semantic_fusion/{run_ref}",
        )
        for idx, d in enumerate(sem.fusion_decisions, start=1):
            evidence_claims.append(
                EvidenceClaim(
                    claim_id=f"C_FUSION_DEC_{idx}",
                    claim_text=(
                        f"Fusao semantica: {d.canonical_name} orgao={d.organ} "
                        f"status={d.decision_status} conf={d.confidence:.2f} "
                        f"rationale={d.decision_rationale_code}"
                    ),
                    claim_type="semantic_fusion",
                    source_refs=["D_FUSION_SEM"],
                    confidence=d.confidence,
                    observation_ids=list(d.supporting_observation_ids),
                    fusion_decision_id=d.decision_id,
                )
            )

    for item in coverage.board_coverage:
        if item.gap_severity in ("high", "medium"):
            dim_slug = _slug(item.dimension)
            evidence_claims.append(
                EvidenceClaim(
                    claim_id=f"C_GAP_BOARD_{dim_slug}",
                    claim_text=(
                        f"Board — dimensão {item.dimension}: cobertura {item.coverage_ratio:.2f}, "
                        f"gap={item.gap_severity}, SPOF={item.single_point_of_failure}."
                    ),
                    claim_type="score_input",
                    source_refs=["D_RECON"],
                    confidence=min(1.0, max(0.3, item.confidence_adjusted_level / 5.0)),
                )
            )

    for item in coverage.executive_coverage:
        if item.gap_severity in ("high", "medium"):
            dim_slug = _slug(item.dimension)
            evidence_claims.append(
                EvidenceClaim(
                    claim_id=f"C_GAP_EXEC_{dim_slug}",
                    claim_text=(
                        f"Diretoria — dimensão {item.dimension}: cobertura {item.coverage_ratio:.2f}, "
                        f"gap={item.gap_severity}, SPOF={item.single_point_of_failure}."
                    ),
                    claim_type="score_input",
                    source_refs=["D_RECON"],
                    confidence=min(1.0, max(0.3, item.confidence_adjusted_level / 5.0)),
                )
            )

    sl = deg.service_level.value if hasattr(deg.service_level, "value") else str(deg.service_level)
    evidence_claims.append(
        EvidenceClaim(
            claim_id="C_PIPELINE_RUN",
            claim_text=(
                f"Pipeline n0–n9 executado (run_id={run_ref}); service_level={sl}; "
                f"degradações={deg.degradations}; "
                f"data_completeness={conf.data_completeness_score:.2f}, "
                f"evidence_quality={conf.evidence_quality_score:.2f}, "
                f"analytical_confidence={conf.analytical_confidence_score:.2f}."
            ),
            claim_type="fact",
            source_refs=["D_RECON"],
            confidence=0.85,
        )
    )

    for d in docs or []:
        did = d.get("doc_id", f"EXTRA-{len(documents)}")
        if did in seen_doc_ids:
            continue
        seen_doc_ids.add(did)
        documents.append(
            EvidenceDocument(
                doc_id=did,
                source_type=d.get("source_type", "web"),
                title=d.get("title", "Untitled"),
                date=d.get("date"),
                url_or_ref=d.get("url_or_ref", "stub://none"),
                retrieval_timestamp=d.get("retrieval_timestamp", now),
            )
        )

    for c in claims or []:
        evidence_claims.append(EvidenceClaim(**c))

    return EvidencePack(documents=documents, claims=evidence_claims)
