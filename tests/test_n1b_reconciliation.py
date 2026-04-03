import pytest
from unittest.mock import MagicMock, patch
from peopledd.models.contracts import (
    BoardMember,
    ConflictItem,
    ExecutiveMember,
    GovernanceIngestion,
    GovernanceSnapshot,
    GovernanceDataQuality,
)
from peopledd.nodes.n1b_reconciliation import (
    run,
    _fuzzy_match,
    _find_fuzzy,
    _shingles,
    _jaccard,
    _date_delta_days,
    FUZZY_MATCH_THRESHOLD,
)


# ─────────────────────────────────────────────────────────────────────────────
# Unit tests for fuzzy name matching
# ─────────────────────────────────────────────────────────────────────────────

def test_fuzzy_match_exact():
    assert _fuzzy_match("João Silva Santos", "João Silva Santos") == 1.0


def test_fuzzy_match_accents_stripped():
    # Our normalizer lowercases, so accent handling comes from shingles
    score = _fuzzy_match("Joao Silva Santos", "João Silva Santos")
    # May not be 1.0 due to accent, but should be high
    assert score > 0.5


def test_fuzzy_match_partial_name():
    # "João da Silva" vs "João Silva" — same person, short vs long form
    score = _fuzzy_match("João da Silva", "João Silva")
    assert score >= FUZZY_MATCH_THRESHOLD


def test_fuzzy_match_different_person():
    score = _fuzzy_match("Carlos Eduardo Moura", "Maria Aparecida Costa")
    assert score < FUZZY_MATCH_THRESHOLD


def test_find_fuzzy_finds_match():
    names = ["Carlos Eduardo Moura", "Maria Aparecida Costa", "Pedro Augusto Ferreira"]
    result = _find_fuzzy("Carlos Eduardo Moura", names)
    assert result == "Carlos Eduardo Moura"


def test_find_fuzzy_no_match():
    names = ["Maria Aparecida Costa", "Pedro Augusto Ferreira"]
    result = _find_fuzzy("Xing Zhao Fang", names)
    assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# Integration tests for n1b reconciliation
# ─────────────────────────────────────────────────────────────────────────────

def _ingestion(formal_board=None, formal_exec=None, current_board=None, current_exec=None):
    return GovernanceIngestion(
        formal_governance_snapshot=GovernanceSnapshot(
            board_members=formal_board or [],
            executive_members=formal_exec or [],
        ),
        current_governance_snapshot=GovernanceSnapshot(
            board_members=current_board or [],
            executive_members=current_exec or [],
        ),
    )


def test_n1b_clean_reconciliation():
    board = [BoardMember(person_name="João Silva Santos", role="chair", independence_status="independent")]
    exec_ = [ExecutiveMember(person_name="Maria Costa Ferreira", formal_title="Diretora Presidente", normalized_role="ceo")]
    result = run(_ingestion(formal_board=board, formal_exec=exec_, current_board=board, current_exec=exec_))
    assert result.reconciliation_status == "clean"
    assert len(result.conflict_items) == 0


def test_n1b_title_mismatch():
    formal_exec = [ExecutiveMember(person_name="Maria Costa Ferreira", formal_title="Diretora Presidente", normalized_role="ceo")]
    current_exec = [ExecutiveMember(person_name="Maria Costa Ferreira", formal_title="CEO", normalized_role="ceo")]
    board = [BoardMember(person_name="Carlos Henrique Souza", role="chair")]

    result = run(_ingestion(
        formal_board=board, formal_exec=formal_exec,
        current_board=board, current_exec=current_exec
    ))

    title_conflicts = [c for c in result.conflict_items if c.conflict_type == "title_mismatch"]
    assert len(title_conflicts) == 1
    assert title_conflicts[0].person_name == "Maria Costa Ferreira"
    assert result.reconciliation_status == "minor_conflicts"


def test_n1b_organ_mismatch_escalates_to_major():
    # Person appears in formal board but in current executivo (role flip — major conflict)
    formal_board = [BoardMember(person_name="Roberto Augusto Lima", role="board-member")]
    formal_exec = [ExecutiveMember(person_name="Ana Paula Ribeiro", formal_title="CFO", normalized_role="cfo")]
    current_board = [BoardMember(person_name="Ana Paula Ribeiro", role="board-member")]  # wrong!
    current_exec = [ExecutiveMember(person_name="Roberto Augusto Lima", formal_title="CEO", normalized_role="ceo")]  # wrong!

    result = run(_ingestion(
        formal_board=formal_board, formal_exec=formal_exec,
        current_board=current_board, current_exec=current_exec
    ))

    organ_conflicts = [c for c in result.conflict_items if c.conflict_type == "organ_mismatch"]
    assert len(organ_conflicts) >= 1
    assert result.reconciliation_status == "major_conflicts"


def test_n1b_missing_person():
    formal_board = [
        BoardMember(person_name="Helena Costa Marques", role="chair"),
        BoardMember(person_name="Paulo Sérgio Almeida", role="board-member"),
    ]
    current_board = [BoardMember(person_name="Helena Costa Marques", role="chair")]
    # Paulo Sérgio Almeida is missing in current

    result = run(_ingestion(formal_board=formal_board, current_board=current_board))

    missing = [c for c in result.conflict_items if c.conflict_type == "missing_person"]
    names = [c.person_name for c in missing]
    assert "Paulo Sérgio Almeida" in names


def test_n1b_independence_mismatch():
    formal_board = [BoardMember(person_name="Lucas Teixeira Neto", role="board-member", independence_status="independent")]
    current_board = [BoardMember(person_name="Lucas Teixeira Neto", role="board-member", independence_status="non_independent")]

    result = run(_ingestion(formal_board=formal_board, current_board=current_board))

    indep_conflicts = [c for c in result.conflict_items if c.conflict_type == "independence_mismatch"]
    assert len(indep_conflicts) == 1
    assert indep_conflicts[0].formal_value == "independent"
    assert indep_conflicts[0].current_value == "non_independent"


def test_n1b_current_only():
    current_board = [BoardMember(person_name="Fernanda Lima Barbosa", role="chair")]
    result = run(_ingestion(current_board=current_board))
    assert result.reconciliation_status == "current_only"


def test_n1b_formal_only():
    formal_board = [BoardMember(person_name="Fernanda Lima Barbosa", role="chair")]
    result = run(_ingestion(formal_board=formal_board))
    assert result.reconciliation_status == "formal_only"


def test_n1b_term_mismatch():
    formal_board = [BoardMember(person_name="Igor Mendes Carvalho", role="board-member", term_start="2022-04-01")]
    current_board = [BoardMember(person_name="Igor Mendes Carvalho", role="board-member", term_start="2021-01-01")]

    result = run(_ingestion(formal_board=formal_board, current_board=current_board))

    term_conflicts = [c for c in result.conflict_items if c.conflict_type == "term_mismatch"]
    assert len(term_conflicts) == 1
