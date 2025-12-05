import json
import re
from pathlib import Path
from typing import Optional, Dict

DEFAULT_ROOTS = [Path("."), Path("./data"), Path("/mnt/data")]
LOOKUP_JSON = "company_lookup.json"

EMAIL_DOMAIN_RE = re.compile(r"@(.+)$")

def _find(path_name: str) -> Path:
    for root in DEFAULT_ROOTS:
        cand = (root / path_name).resolve()
        if cand.exists():
            return cand
    return Path(path_name).resolve()

def normalize_ein(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    digits = re.sub(r"\D", "", s)
    if len(digits) == 9:
        return f"{digits[:2]}-{digits[2:]}"
    return s

def extract_domain(email: Optional[str]) -> Optional[str]:
    if not isinstance(email, str):
        return None
    m = EMAIL_DOMAIN_RE.search(email.strip().lower())
    return m.group(1) if m else None

def load_lookup() -> Dict[str, str]:
    p = _find(LOOKUP_JSON)
    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)

    norm = {}
    for dom, ein in data.items():
        norm[str(dom).strip().lower()] = normalize_ein(ein)
    return norm

def infer_ein(email: Optional[str], explicit_ein: Optional[str], lookup: Dict[str, str]) -> Optional[str]:
    if explicit_ein:
        return normalize_ein(explicit_ein)
    dom = extract_domain(email)
    if dom and dom in lookup:
        return normalize_ein(lookup[dom])
    return None