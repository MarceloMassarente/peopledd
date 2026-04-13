from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def company_key_for(company_name: str, country: str) -> str:
    norm = " ".join(company_name.strip().lower().split())
    raw = f"{norm}::{country.strip().upper()}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:12]


@dataclass
class CompanyMemory:
    company_key: str
    useful_ri_surfaces: list[str] = field(default_factory=list)
    failed_ri_strategies: list[str] = field(default_factory=list)
    company_aliases: list[str] = field(default_factory=list)
    person_observations: list[dict[str, Any]] = field(default_factory=list)
    last_updated: str = ""

    def to_json_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json_dict(cls, data: dict[str, Any]) -> CompanyMemory | None:
        try:
            return cls(
                company_key=str(data["company_key"]),
                useful_ri_surfaces=list(data.get("useful_ri_surfaces") or []),
                failed_ri_strategies=list(data.get("failed_ri_strategies") or []),
                company_aliases=list(data.get("company_aliases") or []),
                person_observations=list(data.get("person_observations") or []),
                last_updated=str(data.get("last_updated") or ""),
            )
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("source_memory: invalid JSON shape: %s", e)
            return None


class SourceMemoryStore:
    """Per-output-dir JSON files keyed by company hash (cross-run persistence)."""

    def __init__(self, base_dir: Path) -> None:
        self._dir = Path(base_dir)

    def _path(self, company_key: str) -> Path:
        safe = "".join(c for c in company_key if c.isalnum())
        return self._dir / f"{safe}.json"

    def load(self, company_key: str) -> CompanyMemory | None:
        path = self._path(company_key)
        if not path.is_file():
            return None
        try:
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw)
            if not isinstance(data, dict):
                return None
            return CompanyMemory.from_json_dict(data)
        except (OSError, json.JSONDecodeError, UnicodeError) as e:
            logger.warning("source_memory: load failed for %s: %s", path, e)
            return None

    def save(self, company_key: str, memory: CompanyMemory) -> None:
        memory.company_key = company_key
        try:
            self._dir.mkdir(parents=True, exist_ok=True)
            path = self._path(company_key)
            tmp = path.with_suffix(".json.tmp")
            payload = json.dumps(memory.to_json_dict(), ensure_ascii=False, indent=2)
            tmp.write_text(payload, encoding="utf-8")
            tmp.replace(path)
        except OSError as e:
            logger.warning("source_memory: save failed for %s: %s", company_key, e)

    def update_ri_success(self, company_key: str, surface_url: str) -> None:
        u = surface_url.strip()
        if not u:
            return
        mem = self.load(company_key) or CompanyMemory(company_key=company_key)
        rest = [x for x in mem.useful_ri_surfaces if x != u]
        mem.useful_ri_surfaces = [u] + rest
        mem.last_updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.save(company_key, mem)

    def update_ri_failure(self, company_key: str, strategy: str) -> None:
        s = strategy.strip()
        if not s:
            return
        mem = self.load(company_key) or CompanyMemory(company_key=company_key)
        if s not in mem.failed_ri_strategies:
            mem.failed_ri_strategies.append(s)
        mem.last_updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.save(company_key, mem)
