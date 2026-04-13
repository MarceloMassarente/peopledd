from __future__ import annotations

from peopledd.models.contracts import BoardMember, GovernanceReconciliation, GovernanceSnapshot
from peopledd.nodes import n2_person_resolution
from peopledd.runtime.identity_models import resolve_profile_candidates


class _P:
    def __init__(
        self,
        *,
        name_similarity: float,
        company_match: bool = False,
        current_company: str = "",
        headline: str = "",
        linkedin_url: str = "",
        name: str = "X",
    ) -> None:
        self.name_similarity = name_similarity
        self.company_match = company_match
        self.current_company = current_company
        self.headline = headline
        self.linkedin_url = linkedin_url
        self.name = name


def test_resolve_single_candidate_round_one() -> None:
    c = [_P(name_similarity=0.91, company_match=True, linkedin_url="https://li/a")]
    pool, rnd, neg = resolve_profile_candidates("Alice", "Acme SA", "board", c)
    assert pool == c
    assert rnd == 1
    assert "linkedin_homonym_ambiguous" not in neg


def test_resolve_company_disambiguation_round_two() -> None:
    pool_in = [
        _P(name_similarity=0.88, company_match=False, current_company="Other", linkedin_url="u1"),
        _P(name_similarity=0.86, company_match=True, current_company="Acme SA", linkedin_url="u2"),
    ]
    pool, rnd, neg = resolve_profile_candidates("Bob", "Acme SA", "board", pool_in)
    assert len(pool) == 1
    assert pool[0].linkedin_url == "u2"
    assert rnd == 2


def test_n2_sets_resolution_round_on_mock_harvest() -> None:
    snap = GovernanceSnapshot(
        board_members=[BoardMember(person_name="Zed", source_refs=[])],
    )
    recon = GovernanceReconciliation(reconciled_governance_snapshot=snap)

    def fake_search(*, name: str, company: str | None):
        from peopledd.services.harvest_adapter import HarvestRecallMeta, ProfileSearchOutcome

        c = _P(
            name_similarity=0.93,
            company_match=True,
            current_company=company or "",
            linkedin_url="https://linkedin.com/in/zed",
            name="Zed",
        )
        return ProfileSearchOutcome(candidates=[c], recall=HarvestRecallMeta(resolution_attempted=True))

    class H:
        def search_by_name(self, **kw):
            return fake_search(**kw)

    out = n2_person_resolution.run(recon, harvest=H(), company_name="Contoso", use_harvest=True)
    assert len(out) == 1
    assert out[0].resolution_round >= 1
    assert out[0].resolution_status.value in ("resolved", "ambiguous", "not_found")
