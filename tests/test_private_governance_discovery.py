from __future__ import annotations

from unittest.mock import MagicMock

from peopledd.models.contracts import BoardMember, ExecutiveMember, GovernanceSnapshot
from peopledd.pipeline_helpers import assign_service_level
from peopledd.services.private_governance_discovery import (
    build_snapshot_from_llm_data,
    eligible_for_private_web_discovery,
)


def test_eligible_when_empty_current_and_exa_configured() -> None:
    orch = MagicMock()
    orch.exa = MagicMock(api_key="test-key")
    assert eligible_for_private_web_discovery(
        current_snapshot=GovernanceSnapshot(),
        search_orchestrator=orch,
        enabled=True,
    )


def test_not_eligible_when_board_present() -> None:
    orch = MagicMock()
    orch.exa = MagicMock(api_key="test-key")
    snap = GovernanceSnapshot(board_members=[BoardMember(person_name="A")])
    assert not eligible_for_private_web_discovery(
        current_snapshot=snap,
        search_orchestrator=orch,
        enabled=True,
    )


def test_eligible_when_only_executives_sparse_completeness() -> None:
    orch = MagicMock()
    orch.exa = MagicMock(api_key="test-key")
    snap = GovernanceSnapshot(
        executive_members=[
            ExecutiveMember(person_name="CEO", formal_title="CEO", normalized_role="ceo"),
        ],
    )
    assert eligible_for_private_web_discovery(
        current_snapshot=snap,
        search_orchestrator=orch,
        enabled=True,
    )


def test_eligible_listed_when_formal_has_board_but_current_missing_board() -> None:
    orch = MagicMock()
    orch.exa = MagicMock(api_key="test-key")
    formal = GovernanceSnapshot(
        board_members=[BoardMember(person_name="Formal Only", source_refs=[])],
    )
    current = GovernanceSnapshot(
        executive_members=[
            ExecutiveMember(person_name="CEO", formal_title="CEO", normalized_role="ceo"),
        ],
    )
    assert eligible_for_private_web_discovery(
        current_snapshot=current,
        search_orchestrator=orch,
        enabled=True,
        company_mode="listed_br",
        formal_snapshot=formal,
    )


def test_not_eligible_without_exa_key() -> None:
    orch = MagicMock()
    orch.exa = MagicMock(api_key="")
    assert not eligible_for_private_web_discovery(
        current_snapshot=GovernanceSnapshot(),
        search_orchestrator=orch,
        enabled=True,
    )


def test_advisory_board_routed_to_committee_not_ca() -> None:
    meta = [
        {
            "url": "https://news.example.com/a",
            "title": "t",
            "text": "Maria Souza integra o conselho consultivo da Acme Brasil.",
        }
    ]
    data = {
        "board_members": [
            {
                "person_name": "Maria Souza",
                "role": "board-member",
                "independence_status": "unknown",
                "evidence_org": "advisory_board",
                "source_index": 1,
            }
        ],
        "executive_members": [],
        "committee_rows": [],
        "as_of_date": None,
    }
    snap = build_snapshot_from_llm_data(
        data, meta, official_host=None, company_name="Acme Brasil", validated_names=set()
    )
    assert not snap.board_members
    assert len(snap.committees) == 1
    assert snap.committees[0].committee_name == "Conselho consultivo"
    assert len(snap.committees[0].members) == 1
    assert snap.committees[0].members[0].person_name == "Maria Souza"


def test_administrative_board_kept_when_official_domain() -> None:
    meta = [
        {
            "url": "https://corp.acme.com/governanca",
            "title": "gov",
            "text": "Conselho de Administracao: Pedro Alves.",
        }
    ]
    data = {
        "board_members": [
            {
                "person_name": "Pedro Alves",
                "role": "chair",
                "independence_status": "unknown",
                "evidence_org": "administrative_board",
                "source_index": 1,
            }
        ],
        "executive_members": [],
        "committee_rows": [],
        "as_of_date": None,
    }
    snap = build_snapshot_from_llm_data(
        data,
        meta,
        official_host="corp.acme.com",
        company_name="Acme SA",
        validated_names=set(),
    )
    assert len(snap.board_members) == 1
    assert snap.board_members[0].person_name == "Pedro Alves"


def test_administrative_board_kept_when_exa_people_validated() -> None:
    meta = [
        {
            "url": "https://random-blog.com/x",
            "title": "x",
            "text": "Pedro Alves no conselho.",
        }
    ]
    data = {
        "board_members": [
            {
                "person_name": "Pedro Alves",
                "role": "board-member",
                "independence_status": "unknown",
                "evidence_org": "administrative_board",
                "source_index": 1,
            }
        ],
        "executive_members": [],
        "committee_rows": [],
        "as_of_date": None,
    }
    snap = build_snapshot_from_llm_data(
        data,
        meta,
        official_host=None,
        company_name="Acme SA",
        validated_names={"Pedro Alves"},
    )
    assert len(snap.board_members) == 1


def test_administrative_board_dropped_without_evidence_and_no_linkedin() -> None:
    meta = [
        {
            "url": "https://random-blog.com/x",
            "title": "x",
            "text": "Pedro Alves foi mencionado em reuniao sem cargo claro.",
        }
    ]
    data = {
        "board_members": [
            {
                "person_name": "Pedro Alves",
                "role": "board-member",
                "independence_status": "unknown",
                "evidence_org": "administrative_board",
                "source_index": 1,
            }
        ],
        "executive_members": [],
        "committee_rows": [],
        "as_of_date": None,
    }
    snap = build_snapshot_from_llm_data(
        data,
        meta,
        official_host="corp.acme.com",
        company_name="Acme SA",
        validated_names=set(),
    )
    assert not snap.board_members


def test_administrative_board_kept_when_text_mentions_company() -> None:
    meta = [
        {
            "url": "https://news.example.com/p",
            "title": "p",
            "text": "Pedro Alves e conselheiro de administracao da Acme Holdings.",
        }
    ]
    data = {
        "board_members": [
            {
                "person_name": "Pedro Alves",
                "role": "board-member",
                "independence_status": "unknown",
                "evidence_org": "administrative_board",
                "source_index": 1,
            }
        ],
        "executive_members": [],
        "committee_rows": [],
        "as_of_date": None,
    }
    snap = build_snapshot_from_llm_data(
        data,
        meta,
        official_host=None,
        company_name="Acme Holdings",
        validated_names=set(),
    )
    assert len(snap.board_members) == 1


def test_assign_service_level_private_web_disclaimer() -> None:
    sl, _, disclaimers = assign_service_level(
        formal_completeness=0.0,
        current_completeness=0.5,
        useful_coverage_board=0.0,
        entity_resolved=True,
        mode="private_or_unresolved",
        private_web_governance_used=True,
    )
    assert sl.value == "SL4"
    assert any("Exa" in d for d in disclaimers)
