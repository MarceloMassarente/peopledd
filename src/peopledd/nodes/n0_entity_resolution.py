from __future__ import annotations

import uuid

from peopledd.models.contracts import CanonicalEntity, InputPayload
from peopledd.models.common import CompanyMode, EntityRelationType, ResolutionStatus, SourceRef
from peopledd.services.connectors import CVMConnector, RIConnector
from peopledd.utils.text import normalize_company_name


def classify_relation_type(name: str) -> EntityRelationType:
    lowered = normalize_company_name(name)
    if "holding" in lowered or lowered.endswith("sa"):
        return EntityRelationType.HOLDING
    if "banco" in lowered or "energia" in lowered:
        return EntityRelationType.OPCO
    return EntityRelationType.UNKNOWN


def run(input_payload: InputPayload, cvm: CVMConnector, ri: RIConnector) -> CanonicalEntity:
    lookup = cvm.lookup_company(input_payload.company_name)
    ri_result = ri.resolve_ri_url(input_payload.company_name)

    mode = CompanyMode.LISTED_BR if lookup.payload.get("listed") else CompanyMode.PRIVATE_OR_UNRESOLVED
    rel_type = classify_relation_type(input_payload.company_name)

    return CanonicalEntity(
        entity_id=str(uuid.uuid4()),
        input_company_name=input_payload.company_name,
        resolved_name=lookup.payload.get("resolved_name"),
        legal_name=lookup.payload.get("legal_name"),
        company_mode=mode,
        cnpj=lookup.payload.get("cnpj"),
        cod_cvm=lookup.payload.get("cod_cvm"),
        tickers=lookup.payload.get("tickers", []),
        ri_url=ri_result.payload.get("ri_url"),
        entity_relation_type=rel_type,
        analysis_scope_entity=lookup.payload.get("resolved_name"),
        resolution_confidence=0.6 if lookup.ok else 0.2,
        resolution_status=ResolutionStatus.RESOLVED if lookup.ok else ResolutionStatus.PARTIAL,
        resolution_evidence=[
            SourceRef(source_type="cvm_cad", label="CVM lookup", url_or_ref="cvm://stub"),
            SourceRef(source_type="ri", label="RI resolve", url_or_ref=ri_result.payload.get("ri_url", "ri://none")),
        ],
    )
