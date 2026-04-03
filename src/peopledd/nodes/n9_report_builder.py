from __future__ import annotations

from peopledd.models.contracts import FinalReport


def to_markdown(report: FinalReport) -> str:
    lines = [
        "# Company Organization & Governance X-ray",
        "",
        "## 1. Visão executiva",
        f"- Empresa: **{report.entity_resolution.resolved_name or report.input_payload.company_name}**",
        f"- Service level: **{report.degradation_profile.service_level}**",
        f"- Modo: **{report.entity_resolution.company_mode}**",
        "",
        "## 2. Escopo e resolução da entidade",
        f"- relation_type: `{report.entity_resolution.entity_relation_type}`",
        f"- analysis_scope_entity: `{report.entity_resolution.analysis_scope_entity}`",
        "",
        "## 3. Governança observada",
        f"- Board (reconciled): {len(report.governance_reconciliation.reconciled_governance_snapshot.board_members)}",
        f"- Diretoria (reconciled): {len(report.governance_reconciliation.reconciled_governance_snapshot.executive_members)}",
        "",
        "## 4. Reconciliação CVM vs RI",
        f"- Status: **{report.governance_reconciliation.reconciliation_status}**",
        f"- Conflitos: {len(report.governance_reconciliation.conflict_items)}",
        "",
        "## 5. Cobertura e gaps",
    ]

    for item in report.coverage_scoring.board_coverage:
        lines.append(
            f"- {item.dimension}: ratio={item.coverage_ratio}, gap={item.gap_severity}, confidence_adj={item.confidence_adjusted_level}"
        )

    lines += ["", "## 6. Hipóteses de melhoria"]
    if not report.improvement_hypotheses:
        lines.append("- Sem hipóteses materiais no nível atual de confiança.")
    else:
        for h in report.improvement_hypotheses:
            lines.append(f"- [{h.urgency}] {h.title} (confiança={h.confidence})")

    lines += ["", "## 7. Limitações e degradações"]
    for d in report.degradation_profile.degradations:
        lines.append(f"- {d}")

    return "\n".join(lines) + "\n"
