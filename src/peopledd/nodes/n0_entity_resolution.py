from __future__ import annotations

import uuid

from peopledd.models.contracts import CanonicalEntity, InputPayload
from peopledd.models.common import CompanyMode, EntityRelationType, ResolutionStatus, SourceRef
from peopledd.services.connectors import CVMConnector, RIConnector
from peopledd.utils.text import normalize_company_name


def classify_relation_type(name: str, cvm_tipo: str | None = None) -> EntityRelationType:
    if cvm_tipo and "CIA ABERTA" in cvm_tipo:
        return EntityRelationType.HOLDING # Often listeds are holdings, but this is a heuristic
        
    lowered = normalize_company_name(name)
    if "holding" in lowered or lowered.endswith("sa"):
        return EntityRelationType.HOLDING
    if "banco" in lowered or "energia" in lowered:
        return EntityRelationType.OPCO
    return EntityRelationType.UNKNOWN


def run(input_payload: InputPayload, cvm: CVMConnector, ri: RIConnector) -> CanonicalEntity:
    lookup = cvm.lookup_company(
        name=input_payload.company_name,
        cnpj_hint=input_payload.cnpj_hint,
        ticker_hint=input_payload.ticker_hint
    )
    
    cvm_payload = lookup.payload
    is_ambiguous = cvm_payload.get("ambiguous", False)
    
    # Calculate Confidence
    confidence = 0.2
    if lookup.ok and not is_ambiguous:
        if input_payload.cnpj_hint and cvm_payload.get("cnpj"):
            confidence = 0.95
        elif input_payload.ticker_hint and cvm_payload.get("tickers"):
            confidence = 0.80
        else:
            confidence = 0.65
            
    # Resolution Status
    if not lookup.ok:
        status = ResolutionStatus.NOT_FOUND
    elif is_ambiguous:
        status = ResolutionStatus.AMBIGUOUS
    else:
        status = ResolutionStatus.RESOLVED

    # If resolved, use real data. Else fallback to input.
    resolved_name = cvm_payload.get("resolved_name") if status == ResolutionStatus.RESOLVED else None
    legal_name = cvm_payload.get("legal_name") if status == ResolutionStatus.RESOLVED else None
    
    candidates = cvm_payload.get("candidates", []) if status == ResolutionStatus.AMBIGUOUS else []
    
    # RI Resolve
    search_name = resolved_name or input_payload.company_name
    ri_result = ri.resolve_ri_url(search_name)

    mode = CompanyMode.LISTED_BR if cvm_payload.get("listed") else CompanyMode.PRIVATE_OR_UNRESOLVED
    rel_type = classify_relation_type(input_payload.company_name, cvm_tipo="CIA ABERTA" if cvm_payload.get("listed") else None)

    return CanonicalEntity(
        entity_id=str(uuid.uuid4()),
        input_company_name=input_payload.company_name,
        resolved_name=resolved_name,
        legal_name=legal_name,
        company_mode=mode,
        cnpj=cvm_payload.get("cnpj"),
        cod_cvm=cvm_payload.get("cod_cvm"),
        tickers=cvm_payload.get("tickers", []),
        ri_url=cvm_payload.get("site_ri") or ri_result.payload.get("ri_url"),
        entity_relation_type=rel_type,
        analysis_scope_entity=resolved_name or input_payload.company_name,
        resolution_confidence=confidence,
        resolution_status=status,
        candidate_entities=[c.get("legal_name") for c in candidates] if candidates else [],
        resolution_evidence=[
            SourceRef(source_type="cvm_cad", label="CVM lookup", url_or_ref="https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"),
            SourceRef(source_type="ri", label="RI resolve", url_or_ref=cvm_payload.get("site_ri") or ri_result.payload.get("ri_url", "ri://none")),
        ],
    )
