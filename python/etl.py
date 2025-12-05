import json
import re
from pathlib import Path
from typing import Optional, Tuple, Dict, List

import pandas as pd

from utils.validator import Validator
from utils.lookup import load_lookup, infer_ein, extract_domain, normalize_ein
from utils.enrichment import MockEnricher


DEFAULT_ROOTS = [Path("."), Path("./data"), Path("/mnt/data")]
OUTPUTS = Path("outputs")
STATE_FILE = OUTPUTS / "state.json"

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

FILES = {
    "employees": "employees_raw.csv",
    "plans": "plans_raw.csv",
    "claims": "claims_raw.csv",
}

DATE_HINTS = {
    "employees": ["last_updated", "updated_at", "created_at", "hire_date", "start_date"],
    "plans": ["last_updated", "updated_at", "start_date", "end_date"],
    "claims": ["last_updated", "updated_at", "service_date", "claim_date", "posted_date"],
}

def _find(path_name: str) -> Path:
    for root in DEFAULT_ROOTS:
        cand = (root / path_name).resolve()
        if cand.exists():
            return cand
    return Path(path_name).resolve()

def _read_csv(name: str) -> pd.DataFrame:
    p = _find(name)
    return pd.read_csv(p)

def _ensure_outputs():
    OUTPUTS.mkdir(parents=True, exist_ok=True)

def _load_state() -> Dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_state(state: Dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")

def _pick_date_col(df: pd.DataFrame, source: str) -> Optional[str]:
    for col in DATE_HINTS.get(source, []):
        if col in df.columns:
            return col
    for col in df.columns:
        lc = col.lower()
        if "date" in lc or "time" in lc or lc.endswith("_at"):
            return col
    return None

def _to_dt(sr: pd.Series) -> pd.Series:
    return pd.to_datetime(sr, errors="coerce", utc=True)

def _incremental_filter(df: pd.DataFrame, source: str, state: Dict) -> Tuple[pd.DataFrame, Optional[pd.Timestamp], Optional[str]]:
    date_col = _pick_date_col(df, source)
    if not date_col:
        return df, None, None
    df = df.copy()
    df[date_col] = _to_dt(df[date_col])
    last_ts = state.get("high_water", {}).get(source)
    if last_ts:
        last_ts = pd.to_datetime(last_ts, utc=True, errors="coerce")
        df = df[df[date_col] > last_ts]
    new_max = df[date_col].max() if not df.empty else None
    return df, new_max, date_col

def _clean_email(email: Optional[str]) -> Optional[str]:
    if not isinstance(email, str):
        return None
    e = email.strip().lower()
    return e if EMAIL_RE.match(e) else None

def clean_and_validate(source: str, df: pd.DataFrame, lookup: Dict[str, str], enricher: MockEnricher, v: Validator) -> pd.DataFrame:
    df = df.copy()
    df["__source__"] = source
    df["__rowid__"] = df.index.astype(str)

    if "email" in df.columns:
        df["email"] = df["email"].apply(_clean_email)
    if "company_ein" in df.columns:
        df["company_ein"] = df["company_ein"].apply(normalize_ein)

    if "company_ein" not in df.columns:
        df["company_ein"] = None
    df["company_ein"] = df.apply(
        lambda r: infer_ein(r.get("email"), r.get("company_ein"), lookup), axis=1
    )

    if "email" in df.columns:
        df["company_domain"] = df["email"].apply(extract_domain)
    else:
        df["company_domain"] = None
    enrich_cols = MockEnricher().template.keys()
    enricher_instance = enricher
    enr = df["company_domain"].apply(lambda d: enricher_instance.enrich_domain(d) if pd.notna(d) else enricher_instance.enrich_domain(None))
    for col in enrich_cols:
        df[f"enrich_{col}"] = enr.apply(lambda x: (x or {}).get(col))

    for i, row in df.iterrows():
        rid = row["__rowid__"]
        if "email" in df.columns and pd.isna(row.get("email")):
            v.add(rid, "email", "invalid_or_missing_email")
        if pd.isna(row.get("company_ein")):
            v.add(rid, "company_ein", "missing_ein_infer_failed")

    mask_valid = True
    if "email" in df.columns:
        mask_valid = mask_valid & df["email"].notna()
    mask_valid = mask_valid & df["company_ein"].notna()
    df_valid = df.loc[mask_valid].copy()

    df_valid = df_valid.drop_duplicates()

    return df_valid

def main():
    _ensure_outputs()
    state = _load_state()
    state.setdefault("high_water", {})

    lookup = load_lookup()
    enricher = MockEnricher()
    v = Validator()

    cleaned_parts: List[pd.DataFrame] = []
    summary: Dict[str, Dict] = {}

    for source, fname in FILES.items():
        try:
            raw = _read_csv(fname)
        except FileNotFoundError:
            raw = pd.read_csv(_find(f"/mnt/data/{fname}"))

        inc_df, new_max, date_col = _incremental_filter(raw, source, state)

        cleaned = clean_and_validate(source, inc_df, lookup, enricher, v)
        cleaned_parts.append(cleaned)

        summary[source] = {
            "input_rows": int(len(raw)),
            "processed_rows": int(len(inc_df)),
            "valid_rows": int(len(cleaned)),
            "date_col": date_col,
            "new_high_water": str(new_max) if new_max is not None else None,
        }
        if new_max is not None:
            state["high_water"][source] = str(new_max)

    v.to_frame().to_csv(OUTPUTS / "validation_errors.csv", index=False)

    if cleaned_parts:
        final_df = pd.concat(cleaned_parts, ignore_index=True, sort=False)
    else:
        final_df = pd.DataFrame()

    final_path = OUTPUTS / "clean_data.parquet"
    if not final_df.empty:
        try:
            final_df.to_parquet(final_path, index=False)
        except Exception:
            final_df.to_parquet(final_path, index=False, engine="pyarrow")
    else:
        final_df.to_parquet(final_path, index=False)

    _save_state(state)

    print(f"Validation errors written to: {OUTPUTS / 'validation_errors.csv'}")
    print(f"Clean data written to: {final_path}")
    print(f"State saved to: {STATE_FILE}")

if __name__ == '__main__':
    main()