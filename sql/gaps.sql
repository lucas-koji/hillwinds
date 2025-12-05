CREATE OR REPLACE TABLE plans_raw AS
SELECT * FROM read_csv_auto('data/plans_raw.csv', header=True);

WITH company_names AS (
	SELECT * FROM (
		VALUES
			('11-1111111', 'acme'),
			('22-2222222', 'bluehorizon'),
			('33-3333333', 'pinecrestfoods')
	) AS t(company_ein, company_name)
),

ordered AS (
    SELECT
        company_ein,
        plan_type,
        carrier_name,
        CAST(start_date AS DATE) AS start_date,
        CAST(end_date AS DATE) AS end_date
    FROM plans_raw
),

with_lag AS (
    SELECT
        company_ein,
        plan_type,
        start_date,
        end_date,
        carrier_name,
        LAG(end_date) OVER (
            PARTITION BY company_ein, plan_type
            ORDER BY start_date
        ) AS prev_end_date,
        LAG(carrier_name) OVER (
            PARTITION BY company_ein, plan_type
            ORDER BY start_date
        ) AS previous_carrier,
        LEAD(carrier_name) OVER (
            PARTITION BY company_ein, plan_type
            ORDER BY start_date
        ) AS next_carrier,
        LAG(start_date) OVER (
            PARTITION BY company_ein, plan_type
            ORDER BY start_date
        ) AS prev_start_date
    FROM ordered
),

gaps AS (
    SELECT
        company_ein,
        plan_type,
        carrier_name,
        previous_carrier,
        next_carrier,
        prev_end_date AS gap_start,
        start_date AS gap_end,
        (start_date - prev_end_date) AS gap_length_days
    FROM with_lag
    WHERE prev_end_date IS NOT NULL
      AND (start_date - prev_end_date) > 7
)

SELECT
	cn.company_name,
	g.gap_start,
	g.gap_end,
	g.gap_length_days,
	g.previous_carrier,
	g.next_carrier
FROM gaps AS g
LEFT JOIN company_names AS cn
	ON g.company_ein = cn.company_ein
ORDER BY
    g.company_ein,
    g.plan_type,
    g.gap_start;