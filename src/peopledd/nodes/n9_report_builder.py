from __future__ import annotations

from peopledd.models.contracts import FinalReport


def _section_executive_summary(report: FinalReport) -> list[str]:
    entity = report.entity_resolution
    deg = report.degradation_profile
    cov = report.coverage_scoring
    high_gaps = [x.dimension for x in cov.board_coverage if x.gap_severity == "high"][:3]
    lines = [
        "## 1. Sumário executivo",
        "",
        f"Empresa em foco: **{entity.resolved_name or report.input_payload.company_name}**. "
        f"Service level **{deg.service_level}** com {len(deg.degradations)} sinal(is) de degradação registrados.",
    ]
    if high_gaps:
        lines.append(f"Principais dimensões com gap alto no conselho: {', '.join(high_gaps)}.")
    else:
        lines.append("Nenhum gap classificado como alto no conselho nas dimensões avaliadas.")
    lines.append("")
    return lines


def _section_entity(report: FinalReport) -> list[str]:
    e = report.entity_resolution
    lines = [
        "## 2. Entidade e escopo",
        "",
        f"- Nome de entrada: `{e.input_company_name}`",
        f"- Nome resolvido: `{e.resolved_name or 'n/d'}`",
        f"- CNPJ: `{e.cnpj or 'n/d'}`",
        f"- URL RI: `{e.ri_url or 'n/d'}`",
        f"- Status de resolução: **{e.resolution_status.value}** (confiança {e.resolution_confidence:.2f})",
        f"- Modo: **{e.company_mode.value}**",
        f"- Escopo de análise: `{e.analysis_scope_entity or 'n/d'}`",
        "",
    ]
    if e.candidate_entities:
        lines.append(f"- Candidatos (ambíguo): {', '.join(e.candidate_entities[:8])}")
        lines.append("")
    return lines


def _section_governance(report: FinalReport) -> list[str]:
    g = report.governance
    rb = report.governance_reconciliation.reporting_basis
    formal = g.formal_governance_snapshot
    current = g.current_governance_snapshot
    lines = [
        "## 3. Governança observada (dual-track)",
        "",
        f"- Qualidade formal: completude {g.governance_data_quality.formal_completeness:.2f}; "
        f"current: {g.governance_data_quality.current_completeness:.2f}; "
        f"freshness {g.governance_data_quality.freshness_score:.2f}.",
        f"- FRE `as_of`: `{formal.as_of_date or 'n/d'}` | RI `as_of`: `{current.as_of_date or 'n/d'}`.",
        f"- Base de reporte preferida: `{rb.get('preferred_view_for_reporting', 'reconciled')}`.",
        f"- Board (formal): {len(formal.board_members)} | Board (current): {len(current.board_members)}",
        f"- Diretoria (formal): {len(formal.executive_members)} | Diretoria (current): {len(current.executive_members)}",
        "",
    ]
    meta = g.ingestion_metadata or {}
    if meta.get("fre_source_url"):
        lines.append(f"- Fonte FRE: `{meta.get('fre_source_url')}` (ano {meta.get('fre_year', 'n/d')}).")
    if meta.get("ri_scrape_url"):
        lines.append(f"- RI usado no scrape: `{meta.get('ri_scrape_url')}`.")
    lines.append("")
    return lines


def _section_semantic_fusion(report: FinalReport) -> list[str]:
    s = report.semantic_governance_fusion
    if s is None:
        return []
    fq = s.fusion_quality
    lines = [
        "## 4b. Fusao semantica multi-fonte (n1c)",
        "",
        f"- Qualidade geral: **{fq.overall_status}**",
        f"- Observacoes: {fq.observation_count} | Candidatos: {fq.candidate_count} | "
        f"Juiz LLM: {fq.llm_judge_used} | Passes: {fq.judge_passes} | "
        f"Evidencia de perfil auxiliar: {fq.profile_evidence_rounds}",
        f"- Decisoes: {len(s.fusion_decisions)} | Itens nao resolvidos: {len(s.unresolved_items)}",
        "",
    ]
    for d in s.fusion_decisions[:24]:
        lines.append(
            f"  - `{d.canonical_name}` | {d.organ} | {d.decision_status} | conf={d.confidence:.2f}"
        )
    if len(s.fusion_decisions) > 24:
        lines.append(f"  - ... (+{len(s.fusion_decisions) - 24} demais)")
    lines.append("")
    return lines


def _section_reconciliation(report: FinalReport) -> list[str]:
    r = report.governance_reconciliation
    lines = [
        "## 4. Reconciliação CVM vs RI",
        "",
        f"- Status: **{r.reconciliation_status}**",
        f"- Conflitos: {len(r.conflict_items)}",
        "",
    ]
    for c in r.conflict_items[:20]:
        lines.append(
            f"  - `{c.conflict_type}` — {c.person_name or 'n/d'}: "
            f"formal={c.formal_value!r} vs current={c.current_value!r}"
        )
    if len(r.conflict_items) > 20:
        lines.append(f"  - … (+{len(r.conflict_items) - 20} demais)")
    lines.append("")
    lines.append(
        f"- Reconciliado: board={len(r.reconciled_governance_snapshot.board_members)}, "
        f"exec={len(r.reconciled_governance_snapshot.executive_members)}",
    )
    lines.append("")
    return lines


def _section_strategy(report: FinalReport) -> list[str]:
    s = report.strategy_and_challenges
    lines = ["## 5. Estratégia e desafios (n4)", ""]
    if not s.strategic_priorities and not s.key_challenges:
        lines.append("- Nenhuma prioridade ou desafio estruturado extraído (fonte fraca ou degradação).")
    for p in s.strategic_priorities[:12]:
        lines.append(f"- Prioridade ({p.time_horizon}): {p.priority} (conf. {p.confidence:.2f})")
    for k in s.key_challenges[:12]:
        lines.append(
            f"- Desafio [{k.challenge_type}, {k.severity}]: {k.challenge} (conf. {k.confidence:.2f})"
        )
    if s.recent_triggers:
        lines.append(f"- Gatilhos recentes: {', '.join(s.recent_triggers[:6])}")
    ph = s.company_phase_hypothesis
    lines.append(
        f"- Hipótese de fase: **{ph.get('phase', 'n/d')}** (conf. {ph.get('confidence', 0):.2f})",
    )
    if s.external_sonar_briefs:
        lines.append("")
        lines.append("### Evidência web (Perplexity Sonar Pro)")
        for b in s.external_sonar_briefs:
            role_label = "Fatos recentes" if b.role == "recent_company_facts" else "Contexto setorial"
            snippet = (b.body or "").strip().replace("\n", " ")
            if len(snippet) > 320:
                snippet = snippet[:317] + "..."
            lines.append(f"- **{role_label}:** {snippet or '(sem texto)'}")
            for sr in b.source_refs[:6]:
                lines.append(f"  - {sr.url_or_ref}")
    lines.append("")
    return lines


def _section_people(report: FinalReport) -> list[str]:
    lines = ["## 6. Pessoas (resolução e perfis)", ""]
    lines.append("| Nome | Status | Provedor | Cobertura útil | Blind spots |")
    lines.append("|------|--------|----------|----------------|-------------|")
    prof_by_id = {p.person_id: p for p in report.people_profiles}
    for pr in report.people_resolution:
        p = prof_by_id.get(pr.person_id)
        prov = pr.matched_profiles[0].provider if pr.matched_profiles else "n/d"
        cov = f"{p.profile_quality.useful_coverage_score:.2f}" if p else "n/d"
        bs = ", ".join(p.blind_spots[:4]) if p else "n/d"
        lines.append(
            f"| {pr.observed_name} | {pr.resolution_status.value} | {prov} | {cov} | {bs} |"
        )
    lines.append("")
    return lines


def _section_harvest_recall_totals(report: FinalReport) -> list[str]:
    tel = report.pipeline_telemetry
    if not tel or not tel.harvest_recall_totals:
        return []
    t = tel.harvest_recall_totals
    if not any(t.values()):
        return []
    lines = [
        "### Harvest recall (agregado do run)",
        "",
        f"- Hits brutos profile-search (soma): **{t.get('raw_hits_profile_search_sum', 0)}**",
        f"- Candidatos após filtro (soma): **{t.get('after_filter_count_sum', 0)}**",
        f"- Descartados como anonimizados (soma): **{t.get('anonymized_dropped_count_sum', 0)}**",
        f"- Pessoas com retry de profile-search: **{t.get('people_with_profile_search_retry', 0)}**",
        f"- Pessoas com sourcing web secundário: **{t.get('people_with_secondary_web_sourcing', 0)}**",
        f"- Pessoas com tentativa Harvest registada: **{t.get('people_with_resolution_attempted', 0)}**",
        "",
    ]
    return lines


def _section_coverage(report: FinalReport) -> list[str]:
    c = report.coverage_scoring
    lines = [
        "## 7. Cobertura de capacidades",
        "",
        "### Conselho",
    ]
    for item in c.board_coverage:
        lines.append(
            f"- **{item.dimension}**: ratio={item.coverage_ratio:.2f}, gap={item.gap_severity}, "
            f"SPOF={item.single_point_of_failure}, conf_adj={item.confidence_adjusted_level:.2f}"
        )
    lines += ["", "### Diretoria"]
    for item in c.executive_coverage:
        lines.append(
            f"- **{item.dimension}**: ratio={item.coverage_ratio:.2f}, gap={item.gap_severity}, "
            f"SPOF={item.single_point_of_failure}, conf_adj={item.confidence_adjusted_level:.2f}"
        )
    if c.organ_level_flags:
        lines += ["", f"Flags: {', '.join(c.organ_level_flags)}"]
    lines.append("")
    return lines


def _section_hypotheses(report: FinalReport) -> list[str]:
    lines = ["## 8. Hipóteses de melhoria", ""]
    if not report.improvement_hypotheses:
        lines.append("- Sem hipóteses materiais no nível atual de confiança ou por política de degradação.")
    else:
        for h in report.improvement_hypotheses:
            refs = ", ".join(h.evidence_claim_refs) if h.evidence_claim_refs else "n/d"
            lines.append(f"### {h.hypothesis_id} [{h.category}] — {h.title}")
            lines.append(f"- Urgência: **{h.urgency}** | Confiança: {h.confidence:.2f}")
            lines.append(f"- Problema: {h.problem_statement}")
            lines.append(f"- Ação proposta: {h.proposed_action}")
            lines.append(f"- Benefício esperado: {h.expected_benefit}")
            lines.append(f"- Claims: `{refs}`")
            lines.append("")
    return lines


def _section_evidence(report: FinalReport) -> list[str]:
    pack = report.evidence_pack
    lines = [
        "## 9. Pacote de evidências",
        "",
        f"Documentos: **{len(pack.documents)}** | Claims: **{len(pack.claims)}**",
        "",
        "### Documentos",
    ]
    for d in pack.documents[:40]:
        lines.append(f"- `{d.doc_id}` ({d.source_type}): {d.title} — `{d.url_or_ref}`")
    if len(pack.documents) > 40:
        lines.append(f"- … (+{len(pack.documents) - 40} demais)")
    lines += ["", "### Claims (amostra)"]
    for cl in pack.claims[:15]:
        lines.append(f"- `{cl.claim_id}` [{cl.claim_type}]: {cl.claim_text[:160]}…")
    if len(pack.claims) > 15:
        lines.append(f"- … (+{len(pack.claims) - 15} demais)")
    lines.append("")
    return lines


def _section_uncertainty_heatmap(report: FinalReport) -> list[str]:
    deg = report.degradation_profile
    sl_map = deg.sl_by_dimension
    stale_map = deg.staleness_by_dimension
    if not sl_map and not stale_map:
        return []
    dims = sorted(set(sl_map.keys()) | set(stale_map.keys()))
    lines = [
        "## 11. Mapa de incerteza por dimensão",
        "",
        f"Service level global: **{deg.service_level.value}**.",
        "",
        "| Dimensao | SL dimensao | Dados desatualizados / fracos |",
        "|----------|-------------|--------------------------------|",
    ]
    for d in dims:
        slv = sl_map.get(d, "n/d")
        st = "sim" if stale_map.get(d) else "nao"
        lines.append(f"| {d} | {slv} | {st} |")
    lines.append("")
    cp = report.confidence_policy
    lines.append(
        f"Referencias agregadas: completude {cp.data_completeness_score:.2f}, "
        f"evidencia {cp.evidence_quality_score:.2f}, analitica {cp.analytical_confidence_score:.2f}.",
    )
    lines.append("")
    return lines


def _section_confidence(report: FinalReport) -> list[str]:
    cp = report.confidence_policy
    deg = report.degradation_profile
    lines = [
        "## 10. Confiança, disclaimers e limitações",
        "",
        f"- Completude de dados: **{cp.data_completeness_score:.2f}**",
        f"- Qualidade de evidência: **{cp.evidence_quality_score:.2f}**",
        f"- Confiança analítica: **{cp.analytical_confidence_score:.2f}**",
        "",
        "### Degradações",
    ]
    for d in deg.degradations:
        lines.append(f"- {d}")
    lines += ["", "### Disclaimers obrigatórios"]
    for m in deg.mandatory_disclaimers:
        lines.append(f"- {m}")
    if deg.omitted_sections:
        lines.append("")
        lines.append(f"Seções omitidas: {', '.join(deg.omitted_sections)}")
    lines.append("")
    return lines


def to_markdown(report: FinalReport) -> str:
    lines = [
        "# Company Organization & Governance X-ray",
        "",
    ]
    lines += _section_executive_summary(report)
    lines += _section_entity(report)
    lines += _section_governance(report)
    lines += _section_reconciliation(report)
    lines += _section_semantic_fusion(report)
    lines += _section_strategy(report)
    lines += _section_people(report)
    lines += _section_harvest_recall_totals(report)
    lines += _section_coverage(report)
    lines += _section_hypotheses(report)
    lines += _section_evidence(report)
    lines += _section_confidence(report)
    lines += _section_uncertainty_heatmap(report)
    return "\n".join(lines) + "\n"
