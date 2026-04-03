from __future__ import annotations

from datetime import datetime

from peopledd.models.contracts import EvidenceClaim, EvidenceDocument, EvidencePack, FinalReport


def run(partial_report: FinalReport | None, docs: list[dict], claims: list[dict]) -> EvidencePack:
    now = datetime.utcnow().isoformat()
    documents = [
        EvidenceDocument(
            doc_id=d.get("doc_id", f"DOC-{i}"),
            source_type=d.get("source_type", "web"),
            title=d.get("title", "Untitled"),
            date=d.get("date"),
            url_or_ref=d.get("url_or_ref", "stub://none"),
            retrieval_timestamp=d.get("retrieval_timestamp", now),
        )
        for i, d in enumerate(docs, start=1)
    ]
    evidence_claims = [EvidenceClaim(**c) for c in claims]

    if partial_report is not None:
        evidence_claims.append(
            EvidenceClaim(
                claim_id="C-PIPELINE",
                claim_text="Pipeline executado de n0 a n9 com degradações explícitas",
                claim_type="fact",
                source_refs=["run_log"],
                confidence=0.8,
            )
        )

    return EvidencePack(documents=documents, claims=evidence_claims)
