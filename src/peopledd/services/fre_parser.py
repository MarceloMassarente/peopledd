from __future__ import annotations

"""
FREParser — CVM Formulário de Referência structured data extractor.
Supports both v6 (up to ~2022) and v7 (2023+) CSV schemas.

Data source:
  https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/fre_cia_aberta_{year}.zip

ZIP structure:
  fre_cia_aberta_adm_{year}.csv          — Seção 12 (administração, diretoria, CA)
  fre_cia_aberta_adm_member_{year}.csv  — (v7 variant)
  fre_cia_aberta_conselho_admin_{year}.csv
  fre_cia_aberta_org_admin_{year}.csv   — comitês
"""

import csv
import io
import logging
import zipfile
from datetime import datetime, timedelta
from typing import Literal

from peopledd.models.contracts import (
    BoardMember,
    Committee,
    CommitteeMember,
    ExecutiveMember,
    GovernanceSnapshot,
)
from peopledd.models.common import SourceRef

logger = logging.getLogger(__name__)

# CVM column mappings — columns differ between v6/v7 but core fields are stable
_ROLE_NORMALIZATION: dict[str, str] = {
    "presidente": "chair",
    "vice presidente": "vice-chair",
    "vice-presidente": "vice-chair",
    "membro": "board-member",
    "conselheiro": "board-member",
    "conselheira": "board-member",
    "diretor presidente": "ceo",
    "ceo": "ceo",
    "diretor financeiro": "cfo",
    "cfo": "cfo",
    "diretor operacional": "coo",
    "coo": "coo",
    "diretor de recursos humanos": "chro",
    "chro": "chro",
    "diretor de tecnologia": "cto",
    "cto": "cto",
    "diretor de ti": "cio",
    "cio": "cio",
    "diretor comercial": "cmo",
    "cmo": "cmo",
    "diretor juridico": "legal",
    "diretor juridico e de relacoes": "legal",
}

_EXEC_ROLE_MAP: dict[str, Literal["ceo", "cfo", "coo", "chro", "cto", "cio", "cmo", "legal", "other"]] = {
    "ceo": "ceo",
    "diretor presidente": "ceo",
    "cfo": "cfo",
    "diretor financeiro": "cfo",
    "coo": "coo",
    "diretor operacional": "coo",
    "chro": "chro",
    "diretor de recursos humanos": "chro",
    "cto": "cto",
    "diretor de tecnologia": "cto",
    "cio": "cio",
    "diretor de ti": "cio",
    "cmo": "cmo",
    "diretor comercial": "cmo",
    "legal": "legal",
    "diretor juridico": "legal",
}

_COMMITTEE_MAP: dict[str, Literal["audit", "people", "finance", "strategy", "risk", "esg", "other"]] = {
    "auditoria": "audit",
    "auditagem": "audit",
    "fiscal": "audit",
    "pessoas": "people",
    "remuneracao": "people",
    "remuneração": "people",
    "rh": "people",
    "financas": "finance",
    "financeiro": "finance",
    "investimentos": "finance",
    "estrategia": "strategy",
    "estratégico": "strategy",
    "risco": "risk",
    "riscos": "risk",
    "compliance": "risk",
    "esg": "esg",
    "sustentabilidade": "esg",
    "ambiental": "esg",
}


def _normalize_name(raw: str) -> str:
    """Normalize a person name: strip, title-case, remove double spaces."""
    return " ".join(raw.strip().title().split())


def _normalize_role_key(raw: str) -> str:
    return raw.strip().lower().replace("ç", "c").replace("ã", "a").replace("ê", "e").replace("ú", "u")


def _detect_schema_version(headers: list[str]) -> str:
    """Detect FRE CSV v6 vs v7 by looking for known v7-only fields."""
    h_lower = {h.lower() for h in headers}
    if "nm_orgao" in h_lower or "nm_cargo_eletivo" in h_lower:
        return "v7"
    return "v6"


def _freshness_score(as_of_date: str | None) -> float:
    """Score freshness: 1.0 = within 6 months, 0.5 = within 18 months, 0.2 = older."""
    if not as_of_date:
        return 0.0
    try:
        dt = datetime.strptime(as_of_date[:10], "%Y-%m-%d")
        delta = datetime.now() - dt
        if delta < timedelta(days=180):
            return 1.0
        if delta < timedelta(days=540):
            return 0.5
        return 0.2
    except ValueError:
        return 0.0


class FREParser:
    """
    Parses CVM FRE ZIP files into GovernanceSnapshot.
    Supports v6 and v7 CSV schemas, with defensive fallback between them.
    """

    def __init__(self, cnpj: str, source_url: str):
        self.cnpj = cnpj.replace(".", "").replace("-", "").replace("/", "")
        self.source_url = source_url
        self._src = SourceRef(
            source_type="cvm_fre_structured",
            label="FRE estruturado",
            url_or_ref=source_url,
        )

    def parse(self, zip_bytes: bytes) -> GovernanceSnapshot:
        """
        Main entry point. Returns a GovernanceSnapshot from the ZIP content.
        Falls back to empty snapshot on any structural error.
        """
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                names = zf.namelist()
                logger.info(f"[FREParser] ZIP contains {len(names)} files: {names[:10]}")

                board_members = self._parse_board(zf, names)
                executive_members = self._parse_executives(zf, names)
                committees = self._parse_committees(zf, names)
                as_of_date = self._extract_as_of_date(zf, names)

                return GovernanceSnapshot(
                    as_of_date=as_of_date,
                    board_members=board_members,
                    executive_members=executive_members,
                    committees=committees,
                )
        except zipfile.BadZipFile:
            logger.warning(f"[FREParser] Invalid ZIP for CNPJ {self.cnpj}")
            return GovernanceSnapshot()
        except Exception as e:
            logger.error(f"[FREParser] Unexpected error: {e}")
            return GovernanceSnapshot()

    def _read_csv(self, zf: zipfile.ZipFile, filename: str) -> list[dict]:
        """Read a single CSV from the ZIP, returning list of row dicts."""
        try:
            with zf.open(filename) as f:
                content = f.read().decode("iso-8859-1", errors="replace")
                reader = csv.DictReader(io.StringIO(content), delimiter=";")
                return list(reader)
        except Exception as e:
            logger.warning(f"[FREParser] Failed to read {filename}: {e}")
            return []

    def _find_file(self, names: list[str], *keywords: str) -> str | None:
        """Find the first filename in ZIP that contains all keywords (case-insensitive)."""
        for name in names:
            nl = name.lower()
            if all(kw in nl for kw in keywords):
                return name
        return None

    def _filter_by_cnpj(self, rows: list[dict]) -> list[dict]:
        """Filter rows to only those matching our CNPJ."""
        result = []
        for row in rows:
            # Common CVM field names for CNPJ
            cnpj_raw = (
                row.get("CNPJ_CIA", "") or
                row.get("cnpj_cia", "") or
                row.get("CD_CNPJ", "")
            )
            cnpj_clean = cnpj_raw.replace(".", "").replace("-", "").replace("/", "").strip()
            if cnpj_clean == self.cnpj:
                result.append(row)
        return result

    def _parse_board(self, zf: zipfile.ZipFile, names: list[str]) -> list[BoardMember]:
        """
        Parse conselho de administração from FRE.
        v7: fre_cia_aberta_administracaoConselhoAdm_*.csv
        v6: fre_cia_aberta_adm_*.csv (shared file with exec rows)
        """
        board_members: list[BoardMember] = []

        # Try v7 first
        filename = self._find_file(names, "conselhoadm") or self._find_file(names, "conselho_adm")
        schema = "v7"
        if not filename:
            # v6 fallback — shared admin file
            filename = self._find_file(names, "adm")
            schema = "v6"

        if not filename:
            logger.warning("[FREParser] No board CSV found in ZIP")
            return []

        rows = self._read_csv(zf, filename)
        rows = self._filter_by_cnpj(rows)

        for row in rows:
            version = _detect_schema_version(list(row.keys()))

            if version == "v7":
                # v7 fields
                nm_pessoa = row.get("NM_PESSOA", "") or row.get("nm_pessoa", "")
                cargo = row.get("NM_CARGO_ELETIVO", "") or row.get("CD_CARGO", "")
                tipo_membro = row.get("TP_MEMBRO", "") or row.get("TP_PARTICIP", "")
                dt_inicio = row.get("DT_INICIO_CARGO", "")
                dt_fim = row.get("DT_FIM_CARGO", "")
                independente = row.get("IND_INDEPENDENTE", "") or row.get("ST_INDEPENDENTE", "")
            else:
                # v6 fields
                nm_pessoa = row.get("Nm_Pessoa", "") or row.get("NM_PESSOA", "")
                cargo = row.get("Ds_Cargo", "") or row.get("CD_CARGO", "")
                tipo_membro = row.get("Tp_Particip", "") or row.get("TP_PARTICIP", "")
                dt_inicio = row.get("Dt_Inicio_Cargo", "") or row.get("DT_INICIO_CARGO", "")
                dt_fim = row.get("Dt_Fim_Cargo", "") or row.get("DT_FIM_CARGO", "")
                independente = row.get("St_Independente", "") or row.get("IND_INDEPENDENTE", "")

            if not nm_pessoa:
                continue

            # Map role
            cargo_key = _normalize_role_key(cargo)
            role = "board-member"
            if "presidente" in cargo_key and "vice" not in cargo_key:
                role = "chair"
            elif "vice" in cargo_key and "presidente" in cargo_key:
                role = "vice-chair"

            # Determine independence
            indep_str = independente.strip().lower()
            if indep_str in ("s", "sim", "yes", "1", "true", "independente"):
                independence = "independent"
            elif indep_str in ("n", "nao", "não", "no", "0", "false", "nao independente"):
                independence = "non_independent"
            else:
                independence = "unknown"

            board_members.append(BoardMember(
                person_name=_normalize_name(nm_pessoa),
                role=role,
                independence_status=independence,
                term_start=dt_inicio or None,
                term_end=dt_fim or None,
                source_refs=[self._src],
            ))

        logger.info(f"[FREParser] Parsed {len(board_members)} board members (schema: {schema})")
        return board_members

    def _parse_executives(self, zf: zipfile.ZipFile, names: list[str]) -> list[ExecutiveMember]:
        """Parse diretoria estatutária from FRE."""
        filename = (
            self._find_file(names, "diretoria") or
            self._find_file(names, "administracaoDiretoria") or
            self._find_file(names, "adm")  # v6 fallback
        )

        if not filename:
            logger.warning("[FREParser] No executive CSV found in ZIP")
            return []

        rows = self._read_csv(zf, filename)
        rows = self._filter_by_cnpj(rows)

        executives: list[ExecutiveMember] = []

        for row in rows:
            # Try to detect if this row is an executive (not board)
            tipo = (
                row.get("TP_PARTICIP", "") or
                row.get("Tp_Particip", "") or
                row.get("NM_ORGAO", "")
            ).strip().lower()

            # Skip board rows in shared files
            if "conselho" in tipo:
                continue

            nm_pessoa = (
                row.get("NM_PESSOA", "") or
                row.get("Nm_Pessoa", "")
            ).strip()
            if not nm_pessoa:
                continue

            cargo = (
                row.get("NM_CARGO_ELETIVO", "") or
                row.get("Ds_Cargo", "") or
                row.get("CD_CARGO", "")
            ).strip()

            dt_inicio = (
                row.get("DT_INICIO_CARGO", "") or
                row.get("Dt_Inicio_Cargo", "")
            )

            cargo_key = _normalize_role_key(cargo)
            normalized_role: Literal["ceo", "cfo", "coo", "chro", "cto", "cio", "cmo", "legal", "other"] = "other"
            for key, val in _EXEC_ROLE_MAP.items():
                if key in cargo_key:
                    normalized_role = val
                    break

            executives.append(ExecutiveMember(
                person_name=_normalize_name(nm_pessoa),
                formal_title=cargo,
                normalized_role=normalized_role,
                term_start=dt_inicio or None,
                source_refs=[self._src],
            ))

        logger.info(f"[FREParser] Parsed {len(executives)} executive members")
        return executives

    def _parse_committees(self, zf: zipfile.ZipFile, names: list[str]) -> list[Committee]:
        """Parse comitês (seção 12-B) from FRE."""
        filename = (
            self._find_file(names, "comite") or
            self._find_file(names, "orgao_admin") or
            self._find_file(names, "org_adm")
        )

        if not filename:
            return []

        rows = self._read_csv(zf, filename)
        rows = self._filter_by_cnpj(rows)

        committees_raw: dict[str, list[CommitteeMember]] = {}

        for row in rows:
            nm_comite = (
                row.get("NM_ORGAO", "") or
                row.get("Ds_Orgao", "") or
                row.get("nm_comite", "")
            ).strip()

            nm_pessoa = (
                row.get("NM_PESSOA", "") or
                row.get("Nm_Pessoa", "")
            ).strip()

            cargo = (
                row.get("CD_CARGO", "") or
                row.get("Ds_Cargo", "")
            ).strip().lower()

            position: Literal["chair", "member", "unknown"] = "member"
            if "presidente" in cargo or "coordenador" in cargo:
                position = "chair"

            if nm_comite and nm_pessoa:
                if nm_comite not in committees_raw:
                    committees_raw[nm_comite] = []
                committees_raw[nm_comite].append(
                    CommitteeMember(
                        person_name=_normalize_name(nm_pessoa),
                        position_in_committee=position,
                    )
                )

        committees: list[Committee] = []
        for nm_comite, members in committees_raw.items():
            comite_lower = nm_comite.lower()
            committee_type: Literal["audit", "people", "finance", "strategy", "risk", "esg", "other"] = "other"
            for keyword, ctype in _COMMITTEE_MAP.items():
                if keyword in comite_lower:
                    committee_type = ctype
                    break

            committees.append(Committee(
                committee_name=nm_comite,
                committee_type=committee_type,
                members=members,
                source_refs=[self._src],
            ))

        logger.info(f"[FREParser] Parsed {len(committees)} committees")
        return committees

    def _extract_as_of_date(self, zf: zipfile.ZipFile, names: list[str]) -> str | None:
        """Try to read the DT_REFER or DT_RECEBIMENTO from any admin CSV."""
        # Try to find date field from first admin row
        filename = self._find_file(names, "adm") or (names[0] if names else None)
        if not filename:
            return None

        rows = self._read_csv(zf, filename)
        rows = self._filter_by_cnpj(rows)

        if rows:
            row = rows[0]
            date = (
                row.get("DT_REFER", "") or
                row.get("Dt_Refer", "") or
                row.get("DT_RECEBIMENTO", "") or
                row.get("Dt_Recebimento", "")
            )
            if date:
                return date[:10]  # yyyy-mm-dd

        return None

    def completeness_score(self, snapshot: GovernanceSnapshot) -> float:
        """
        Crude completeness proxy.
        0.0 = empty, 1.0 = board + executives + committees populated.
        """
        score = 0.0
        if snapshot.board_members:
            score += 0.5
        if snapshot.executive_members:
            score += 0.35
        if snapshot.committees:
            score += 0.15
        return round(score, 2)

    def freshness_score(self, snapshot: GovernanceSnapshot) -> float:
        return _freshness_score(snapshot.as_of_date)
