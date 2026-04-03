from __future__ import annotations

import re
import unicodedata


def normalize_company_name(name: str) -> str:
    txt = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    txt = re.sub(r"[^a-zA-Z0-9 ]+", " ", txt).lower()
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt
