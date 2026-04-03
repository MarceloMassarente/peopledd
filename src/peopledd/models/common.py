from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class CompanyMode(str, Enum):
    LISTED_BR = "listed_br"
    PRIVATE_OR_UNRESOLVED = "private_or_unresolved"


class ResolutionStatus(str, Enum):
    RESOLVED = "resolved"
    PARTIAL = "partial"
    AMBIGUOUS = "ambiguous"
    NOT_FOUND = "not_found"


class EntityRelationType(str, Enum):
    HOLDING = "holding"
    OPCO = "opco"
    SUBSIDIARY = "subsidiary"
    MIXED = "mixed"
    UNKNOWN = "unknown"


class ServiceLevel(str, Enum):
    SL1 = "SL1"
    SL2 = "SL2"
    SL3 = "SL3"
    SL4 = "SL4"
    SL5 = "SL5"


class SourceRef(BaseModel):
    source_type: str
    label: str | None = None
    url_or_ref: str
    date: str | None = None


class BaseArtifact(BaseModel):
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = Field(default_factory=dict)
