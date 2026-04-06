from __future__ import annotations

import asyncio
import io
import logging
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import httpx

logger = logging.getLogger(__name__)


def _norm_header_key(raw: str) -> str:
    s = raw.strip().lstrip("\ufeff").strip()
    return s.casefold().replace(" ", "_")


def _header_index_map(headers: list[str]) -> dict[str, int]:
    m: dict[str, int] = {}
    for i, raw in enumerate(headers):
        key = _norm_header_key(raw)
        if key and key not in m:
            m[key] = i
    return m


def _col_index(name_map: dict[str, int], *candidates: str) -> int | None:
    for name in candidates:
        k = _norm_header_key(name)
        if k in name_map:
            return name_map[k]
    return None


def _site_ri_column_index(headers: list[str]) -> int | None:
    for i, raw in enumerate(headers):
        hn = _norm_header_key(raw)
        if "site" not in hn:
            continue
        if any(x in hn for x in ("ri", "invest", "relac", "relacao")):
            return i
    return None


def parse_cad_cia_aberta_lines(lines: list[str], name_or_cnpj: str) -> list[CVMCandidate]:
    """
    Parse CVM cad_cia_aberta CSV lines (header + data). Used by search_company and unit tests.
    """
    if not lines or not lines[0].strip():
        return []

    header_cells = lines[0].strip().split(";")
    name_map = _header_index_map(header_cells)

    cnpj_i = _col_index(name_map, "CNPJ_CIA", "CNPJ CIA", "CNPJ")
    razao_i = _col_index(name_map, "DENOM_SOCIAL")
    pregao_i = _col_index(name_map, "DENOM_COMERC")
    sit_i = _col_index(name_map, "SIT")
    cod_i = _col_index(name_map, "CD_CVM", "CD CVM")
    setor_i = _col_index(name_map, "SETOR_ATIV", "SETOR")
    site_i = _site_ri_column_index(header_cells)

    use_legacy_positions = cnpj_i is None
    if use_legacy_positions:
        logger.warning(
            "[CVMClient] cad_cia_aberta: missing CNPJ_CIA in header; using legacy column positions"
        )
        cnpj_i, razao_i, pregao_i, sit_i = 0, 1, 2, 7
        cod_i = setor_i = site_i = None

    search_query = name_or_cnpj.lower().strip()
    search_cnpj = search_query.replace(".", "").replace("-", "").replace("/", "")

    candidates: list[CVMCandidate] = []
    for line in lines[1:]:
        if not line.strip():
            continue
        cols = line.split(";")
        idx_needed = [i for i in (cnpj_i, razao_i, pregao_i, sit_i, cod_i, setor_i, site_i) if i is not None]
        min_len = max(idx_needed) + 1 if idx_needed else 1
        if len(cols) < min_len:
            continue

        def cell(idx: int | None) -> str:
            if idx is None or idx >= len(cols):
                return ""
            return cols[idx].strip()

        cnpj = cell(cnpj_i)
        razao_social = cell(razao_i)
        nome_pregao = cell(pregao_i) or None
        situacao = cell(sit_i)
        cod_cvm = cell(cod_i) if cod_i is not None else ""
        setor_val = cell(setor_i) if setor_i is not None else None
        site_raw = cell(site_i) if site_i is not None else ""
        site_ri = site_raw if site_raw else None
        setor = setor_val if setor_val else None

        cnpj_clean = cnpj.replace(".", "").replace("-", "").replace("/", "")
        match = False
        if search_cnpj and search_cnpj == cnpj_clean:
            match = True
        elif search_query in razao_social.lower() or (nome_pregao and search_query in nome_pregao.lower()):
            match = True

        if match:
            candidates.append(
                CVMCandidate(
                    cod_cvm=cod_cvm,
                    cnpj=cnpj,
                    nome_pregao=nome_pregao,
                    nome_razao_social=razao_social,
                    situacao=situacao,
                    tipo="CIA ABERTA",
                    site_ri=site_ri,
                    setor=setor,
                )
            )

    return candidates


# Models

@dataclass
class CVMCandidate:
    cod_cvm: str
    cnpj: str
    nome_pregao: str | None
    nome_razao_social: str
    situacao: str
    tipo: str
    site_ri: str | None = None
    setor: str | None = None
    tickers: list[str] = field(default_factory=list)


@dataclass
class FREMetadata:
    cod_cvm: str
    ano: int
    data_recebimento: str
    url_zip: str


@dataclass
class IPEEvent:
    cod_cvm: str
    data_referencia: str
    categoria: str
    tipo: str
    especie: str
    assunto: str
    url_documento: str


class CVMClient:
    """
    HTTP client for CVM Public Data APIs.
    Based on the deepsearch LLMClient pattern with resilient async requests.
    """

    # URLs based on CVM CKAN endpoints and static data
    BASE_URL = "https://dados.cvm.gov.br"

    def __init__(self, timeout: float = 30.0, max_retries: int = 3):
        self.timeout = timeout
        self.max_retries = max_retries
        self._limiter = asyncio.Semaphore(5)  # Max 5 concurrent requests to CVM

    async def _get_with_retry(self, url: str) -> httpx.Response:
        """Helper for resilient GET requests."""
        async with self._limiter:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                for attempt in range(self.max_retries):
                    try:
                        response = await client.get(url)
                        response.raise_for_status()
                        return response
                    except (httpx.TimeoutException, httpx.HTTPError) as e:
                        if attempt == self.max_retries - 1:
                            logger.error(f"[CVMClient] Request failed after {self.max_retries} attempts to {url}: {e}")
                            raise
                        await asyncio.sleep(2 ** attempt)

    async def search_company(self, name_or_cnpj: str) -> list[CVMCandidate]:
        """
        Searches for a listed company in the CVM registry.
        Uses the CKAN API for the `cia_aberta_cad` datastore if possible,
        or fetches the daily CSV: https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv
        """
        # For MVP, we will fetch the static CSV and filter it in memory
        # since the CKAN search can be unreliable on the CVM portal.
        url = f"{self.BASE_URL}/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"
        try:
            response = await self._get_with_retry(url)
            content = response.content.decode("iso-8859-1")  # CVM CSVs are often iso-8859-1
            lines = content.split("\n")
            return parse_cad_cia_aberta_lines(lines, name_or_cnpj)
            
        except Exception as e:
            logger.error(f"[CVMClient] Failed to search company: {e}")
            return []

    async def get_fre_metadata(self, cnpj_clean: str, year: int) -> FREMetadata | None:
        """
        Locates the FRE zip file for a given company and year.
        FRE static files are at https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/fre_cia_aberta_{year}.zip
        """
        url = f"{self.BASE_URL}/dados/CIA_ABERTA/DOC/FRE/DADOS/fre_cia_aberta_{year}.zip"
        
        # We don't download it here, just return the metadata indicating it's accessible.
        try:
            # Send HEAD request to verify file exists
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.head(url)
                if resp.status_code == 200:
                    return FREMetadata(
                        cod_cvm="", # Unused when looking by CNPJ via URL 
                        ano=year,
                        data_recebimento=resp.headers.get("Last-Modified", ""),
                        url_zip=url
                    )
        except Exception as e:
            logger.warning(f"[CVMClient] Error checking FRE for year {year}: {e}")
            
        return None

    async def download_fre_zip(self, url: str) -> bytes:
        """Downloads the FRE ZIP file payload."""
        response = await self._get_with_retry(url)
        return response.content

    async def get_ipe_events(self, cnpj: str, days_back: int = 180) -> list[IPEEvent]:
        """
        Retrieves recent IPE events (Fatos Relevantes, Comunicados).
        Uses https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{year}.csv
        """
        now = datetime.now()
        years_to_check = {now.year, (now - timedelta(days=days_back)).year}
        
        events = []
        cnpj_clean = cnpj.replace(".", "").replace("-", "").replace("/", "")
        
        for year in sorted(years_to_check, reverse=True):
            try:
                url = f"{self.BASE_URL}/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{year}.csv"
                response = await self._get_with_retry(url)
                content = response.content.decode("iso-8859-1")
                
                lines = content.split('\n')
                if not lines:
                    continue
                    
                for line in lines[1:]:
                    if not line.strip():
                        continue
                        
                    cols = line.split(';')
                    if len(cols) < 14:
                        continue
                        
                    row_cnpj = cols[0].strip().replace(".", "").replace("-", "").replace("/", "")
                    
                    if row_cnpj == cnpj_clean:
                        events.append(IPEEvent(
                            cod_cvm=cols[2].strip() if len(cols) > 2 else "",
                            data_referencia=cols[7].strip() if len(cols) > 7 else "",
                            categoria=cols[9].strip() if len(cols) > 9 else "",
                            tipo=cols[10].strip() if len(cols) > 10 else "",
                            especie=cols[11].strip() if len(cols) > 11 else "",
                            assunto=cols[12].strip() if len(cols) > 12 else "",
                            url_documento=cols[14].strip() if len(cols) > 14 else ""
                        ))
            except Exception as e:
                logger.warning(f"[CVMClient] Failed to fetch IPE for {year}: {e}")
                
        # Filter for recent events based on days_back
        cutoff_date = now - timedelta(days=days_back)
        recent_events = []
        
        for event in events:
            try:
                # Typical format: 2024-03-15
                if event.data_referencia:
                    # simplistic check
                    evt_date = datetime.strptime(event.data_referencia[:10], "%Y-%m-%d")
                    if evt_date >= cutoff_date:
                        recent_events.append(event)
            except ValueError:
                recent_events.append(event) # If parsing fails, include it
                
        return recent_events
