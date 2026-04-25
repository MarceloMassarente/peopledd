from __future__ import annotations

from peopledd.models.common import CompanyMode, ResolutionStatus
from peopledd.models.contracts import CanonicalEntity, GovernanceSeed
from peopledd.runtime.pipeline_merge import effective_ri_url_for_pipeline


def test_effective_ri_url_prefers_entity_over_seed() -> None:
    ent = CanonicalEntity(
        entity_id="e1",
        input_company_name="A",
        ri_url="https://entity-ri.test/",
        company_mode=CompanyMode.PRIVATE_OR_UNRESOLVED,
        resolution_status=ResolutionStatus.RESOLVED,
    )
    seed = GovernanceSeed(ri_url_candidate="https://seed.test/")
    assert effective_ri_url_for_pipeline(ent, seed) == "https://entity-ri.test/"


def test_effective_ri_url_uses_seed_when_entity_missing() -> None:
    ent = CanonicalEntity(
        entity_id="e1",
        input_company_name="A",
        ri_url=None,
        company_mode=CompanyMode.PRIVATE_OR_UNRESOLVED,
        resolution_status=ResolutionStatus.PARTIAL,
    )
    seed = GovernanceSeed(ri_url_candidate="https://seed-only.test/ri")
    assert effective_ri_url_for_pipeline(ent, seed) == "https://seed-only.test/ri"


def test_effective_ri_url_none_when_both_empty() -> None:
    ent = CanonicalEntity(
        entity_id="e1",
        input_company_name="A",
        ri_url=None,
        company_mode=CompanyMode.PRIVATE_OR_UNRESOLVED,
        resolution_status=ResolutionStatus.NOT_FOUND,
    )
    assert effective_ri_url_for_pipeline(ent, None) is None
    assert effective_ri_url_for_pipeline(ent, GovernanceSeed(ri_url_candidate=None)) is None
