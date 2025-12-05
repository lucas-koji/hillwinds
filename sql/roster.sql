CREATE OR REPLACE TABLE employees_raw AS
SELECT * FROM read_csv_auto('data/employees_raw.csv', header=True);

WITH domain_to_ein AS (
    SELECT * FROM (
        VALUES
            ('acme.com', '11-1111111', 'acme'),
            ('bluehorizon.io', '22-2222222', 'bluehorizon'),
            ('pinecrestfoods.com', '33-3333333', 'pinecrestfoods')
    ) AS t(domain, company_ein, company_name)
),

expected_roster AS (
    SELECT * FROM (
        VALUES
            ('11-1111111', 60),
            ('22-2222222', 45),
            ('33-3333333', 40)
    ) AS t(company_ein, expected)
),

employees AS (
    SELECT
        LOWER(TRIM(email)) AS email_norm,
        NULLIF(REGEXP_REPLACE(TRIM(company_ein), '\s+', ''), '') AS company_ein_raw
    FROM employees_raw
),

employees_w_domain AS (
    SELECT
        email_norm,
        CASE
            WHEN REGEXP_MATCHES(email_norm, '.+@.+') THEN LOWER(REGEXP_EXTRACT(email_norm, '@(.+)$', 1))
        END AS domain,
        company_ein_raw
    FROM employees
),

employees_resolved AS (
    SELECT
        COALESCE(d.company_ein, ed.company_ein_raw) AS company_ein,
        ed.domain,
        ed.email_norm
    FROM employees_w_domain AS ed
    LEFT JOIN domain_to_ein d ON d.domain = ed.domain
    WHERE ed.email_norm IS NOT NULL
),

observed_by_ein AS (
    SELECT
        company_ein,
        COUNT(DISTINCT email_norm) AS observed
    FROM employees_resolved
    WHERE company_ein IS NOT NULL
    GROUP BY 1
),

cmp AS (
    SELECT
        e.company_ein,
        e.expected,
        o.observed,
        CASE WHEN e.expected = 0 OR o.observed IS NULL THEN NULL
             ELSE (o.observed - e.expected) * 1.0 / e.expected
        END AS pct_diff
    FROM expected_roster e
    LEFT JOIN observed_by_ein o USING (company_ein)
)

SELECT
    d.company_name,
    c.expected,
    COALESCE(c.observed, 0) AS observed,
    c.pct_diff,
    CASE
		WHEN c.pct_diff IS NULL THEN 'Unknown'
        WHEN ABS(c.pct_diff) < 0.20 THEN 'Low'
        WHEN ABS(c.pct_diff) < 0.50 THEN 'Medium'
        WHEN ABS(c.pct_diff) <= 1.00 THEN 'High'
        ELSE 'Critical'
    END AS severity
FROM cmp c
JOIN domain_to_ein d
	ON c.company_ein = d.company_ein
ORDER BY d.company_name;
