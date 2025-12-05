from dataclasses import dataclass
from typing import List
import pandas as pd

@dataclass
class VError:
    row_id: str
    field: str
    error_reason: str

class Validator:
    def __init__(self) -> None:
        self._errors: List[VError] = []

    def add(self, row_id: str, field: str, reason: str) -> None:
        self._errors.append(VError(row_id=row_id, field=field, error_reason=reason))

    def any(self) -> bool:
        return len(self._errors) > 0

    def to_frame(self) -> pd.DataFrame:
        if not self._errors:
            return pd.DataFrame(columns=["row_id", "field", "error_reason"])
        return pd.DataFrame([e.__dict__ for e in self._errors])