import json
from pathlib import Path
from typing import Dict, Optional
import time

DEFAULT_ROOTS = [Path("."), Path("./data"), Path("/mnt/data")]
API_MOCK_JSON = "api_mock.json"

def _find(path_name: str) -> Path:
    for root in DEFAULT_ROOTS:
        cand = (root / path_name).resolve()
        if cand.exists():
            return cand
    return Path(path_name).resolve()

class MockEnricher:
    def __init__(self) -> None:
        p = _find(API_MOCK_JSON)
        with open(p, "r", encoding="utf-8") as f:
            self.api_mock = json.load(f)
        self.template = self.api_mock.get("sample_response", {"industry": "Unknown", "revenue": None, "headcount": None})
        self.cache: Dict[str, Dict] = {}

    def _payload_from_domain(self, domain: str) -> Dict:
        seed = sum(ord(c) for c in domain)
        industries = ["Technology", "Healthcare", "Finance", "Manufacturing", "Retail"]
        revenue_bands = ["10M", "25M", "50M", "100M", "250M", "500M", "1B+"]
        out = dict(self.template)
        out.update({
            "industry": industries[seed % len(industries)],
            "revenue": revenue_bands[seed % len(revenue_bands)],
            "headcount": str(50 + (seed % 950))
        })
        return out

    def enrich_domain(self, domain: Optional[str]) -> Dict:
        if not domain:
            return dict(self.template)
        if domain in self.cache:
            return self.cache[domain]
        for attempt in range(3):
            try:
                payload = self._payload_from_domain(domain)
                self.cache[domain] = payload
                return payload
            except Exception:
                time.sleep((2 ** attempt) * 0.2)
        return dict(self.template)