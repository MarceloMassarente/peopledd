from __future__ import annotations

from unittest.mock import MagicMock, patch

from peopledd.models.contracts import BoardMember, ExecutiveMember, GovernanceSnapshot
from peopledd.nodes import n1_governance_ingestion


def test_n1_sets_private_web_metadata_when_discovery_returns_snapshot() -> None:
    orch = MagicMock()
    orch.exa = MagicMock(api_key="k")

    discovered = GovernanceSnapshot(
        board_members=[BoardMember(person_name="Ana Costa", source_refs=[])],
    )

    with (
        patch(
            "peopledd.nodes.n1_governance_ingestion._ingest_formal",
            return_value=(GovernanceSnapshot(), {}),
        ),
        patch(
            "peopledd.nodes.n1_governance_ingestion._ingest_current",
            return_value=(GovernanceSnapshot(), {}),
        ),
        patch(
            "peopledd.nodes.n1_governance_ingestion.discover_governance",
            return_value=(
                discovered,
                {
                    "anchor_website": "https://corp.example.com",
                    "official_host": "corp.example.com",
                    "source_count": "4",
                    "reason": "ok",
                },
            ),
        ),
    ):
        out = n1_governance_ingestion.run(
            "Example Co",
            cnpj=None,
            ri_url=None,
            search_orchestrator=orch,
            company_mode="private_or_unresolved",
        )

    assert out.ingestion_metadata.get("private_web_discovery") == "1"
    assert out.ingestion_metadata.get("private_web_reason") == "ok"
    assert len(out.current_governance_snapshot.board_members) == 1
    assert out.current_governance_snapshot.board_members[0].person_name == "Ana Costa"


def test_n1_skips_discovery_when_ri_track_has_board() -> None:
    orch = MagicMock()
    orch.exa = MagicMock(api_key="k")

    with (
        patch(
            "peopledd.nodes.n1_governance_ingestion._ingest_formal",
            return_value=(GovernanceSnapshot(), {}),
        ),
        patch(
            "peopledd.nodes.n1_governance_ingestion._ingest_current",
            return_value=(
                GovernanceSnapshot(
                    board_members=[
                        BoardMember(person_name="Existing", source_refs=[]),
                        BoardMember(person_name="Other", source_refs=[]),
                    ],
                ),
                {},
            ),
        ),
        patch(
            "peopledd.nodes.n1_governance_ingestion.discover_governance",
        ) as dg,
    ):
        out = n1_governance_ingestion.run(
            "Listed Co",
            ri_url="https://ri.example.com",
            search_orchestrator=orch,
        )

    dg.assert_not_called()
    assert out.ingestion_metadata.get("private_web_discovery") is None
    assert out.current_governance_snapshot.board_members[0].person_name == "Existing"


def test_n1_merges_private_discovery_when_ri_had_executives_only() -> None:
    orch = MagicMock()
    orch.exa = MagicMock(api_key="k")

    discovered = GovernanceSnapshot(
        board_members=[BoardMember(person_name="Board From Web", source_refs=[])],
    )

    with (
        patch(
            "peopledd.nodes.n1_governance_ingestion._ingest_formal",
            return_value=(GovernanceSnapshot(), {}),
        ),
        patch(
            "peopledd.nodes.n1_governance_ingestion._ingest_current",
            return_value=(
                GovernanceSnapshot(
                    executive_members=[
                        ExecutiveMember(
                            person_name="RI CEO",
                            formal_title="CEO",
                            normalized_role="ceo",
                        ),
                    ],
                ),
                {},
            ),
        ),
        patch(
            "peopledd.nodes.n1_governance_ingestion.discover_governance",
            return_value=(
                discovered,
                {"anchor_website": "https://x.com", "reason": "ok", "source_count": "2"},
            ),
        ),
    ):
        out = n1_governance_ingestion.run(
            "Example Co",
            cnpj=None,
            ri_url="https://ri.example.com",
            search_orchestrator=orch,
        )

    assert out.ingestion_metadata.get("private_web_discovery") == "1"
    names_board = {m.person_name for m in out.current_governance_snapshot.board_members}
    names_exec = {e.person_name for e in out.current_governance_snapshot.executive_members}
    assert "Board From Web" in names_board
    assert "RI CEO" in names_exec
