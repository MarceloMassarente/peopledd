from __future__ import annotations

from peopledd.models.contracts import (
    BoardMember,
    ExecutiveMember,
    GovernanceDataQuality,
    GovernanceIngestion,
    GovernanceSnapshot,
)
from peopledd.models.common import SourceRef


def run(company_name: str) -> GovernanceIngestion:
    src_formal = SourceRef(source_type="cvm_fre_structured", label="FRE", url_or_ref="cvm://fre/stub")
    src_current = SourceRef(source_type="ri", label="Governança RI", url_or_ref="ri://governanca/stub")

    formal = GovernanceSnapshot(
        as_of_date=None,
        board_members=[
            BoardMember(person_name="Conselheiro Exemplo", role="chair", source_refs=[src_formal]),
        ],
        executive_members=[
            ExecutiveMember(
                person_name="Executivo Exemplo",
                formal_title="Diretor Presidente",
                normalized_role="ceo",
                source_refs=[src_formal],
            )
        ],
    )

    current = GovernanceSnapshot(
        as_of_date=None,
        board_members=[
            BoardMember(person_name="Conselheiro Exemplo", role="chair", source_refs=[src_current]),
        ],
        executive_members=[
            ExecutiveMember(
                person_name="Executivo Exemplo",
                formal_title="CEO",
                normalized_role="ceo",
                source_refs=[src_current],
            )
        ],
    )

    return GovernanceIngestion(
        formal_governance_snapshot=formal,
        current_governance_snapshot=current,
        governance_data_quality=GovernanceDataQuality(
            formal_completeness=0.7,
            current_completeness=0.6,
            freshness_score=0.6,
        ),
    )
