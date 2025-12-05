# Hillwinds Take-Home

This repository provides a reproducible structure to execute the technical take‑home challenge.  
The correct execution order is:

1. **SQL (run each file individually)**
2. **Python ETL**

---

## Project Layout
```
hillwinds/
  data/
    employees_raw.csv
    plans_raw.csv
    claims_raw.csv
    company_lookup.json
    api_mock.json
  python/
    etl.py
    enrichment.py
    lookup.py
    validator.py
  sql/
    gaps.sql
    spikes.sql
    roster.sql
  outputs/
  requirements.txt
  DESIGN.md
  README.md
```

---

# 1) SQL — Run All Scripts

Execute all SQL scripts with a single command:

```bash
chmod +x run_sql.sh
./run_sql.sh
```

This runs `gaps.sql`, `spikes.sql`, and `roster.sql`, generating outputs in `outputs/`.

### Running individually

If you prefer to run each script separately:

```bash
duckdb -csv < sql/gaps.sql > outputs/sql_gaps.csv
duckdb -csv < sql/spikes.sql > outputs/sql_spikes.csv
duckdb -csv < sql/roster.sql > outputs/sql_roster.csv
```

### Outputs generated
- `outputs/sql_gaps.csv` — coverage gaps > 7 days between plans
- `outputs/sql_spikes.csv` — cost spikes > 200% in 90-day rolling window
- `outputs/sql_roster.csv` — expected vs observed employee counts

---

# 2) Python ETL

### Install dependencies:
```bash
pip install -r requirements.txt
```

### Run ETL:
```bash
python python/etl.py
```

### Outputs generated:
- `outputs/clean_data.parquet`
- `outputs/validation_errors.csv`
- `outputs/state.json`

---

## Notes & Assumptions
- EIN inference uses `company_lookup.json` (domain → EIN). If an EIN appears in the CSV, it takes precedence.
- Company names are derived from the domain without the TLD (e.g., `acme.com` → `acme`).
- The enrichment mock follows the structure defined in `api_mock.json`.
- Expected employee counts are hard‑coded inside `roster.sql`, as required by the challenge.
- The 90‑day rolling window approximation uses the previous day's cumulative amount for simpler and faster computation.
- Both SQL scripts and Python ETL operate directly on the local files in `data/`.
- I would have liked to create a DESIGN.md file but was unable to complete it in time.