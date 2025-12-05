CREATE OR REPLACE TABLE claims_raw AS
SELECT * FROM read_csv_auto('data/claims_raw.csv', header=True);

WITH company_names AS (
	SELECT * FROM (
		VALUES
			('11-1111111', 'acme'),
			('22-2222222', 'bluehorizon'),
			('33-3333333', 'pinecrestfoods')
	) AS t(company_ein, company_name)
),

claims AS (
	SELECT
		company_ein,
		CAST(service_date AS DATE) AS service_date,
		CAST(amount AS DOUBLE) AS amount
	FROM claims_raw
	WHERE
		company_ein IS NOT NULL
		AND service_date IS NOT NULL
		AND amount IS NOT NULL
),

daily AS (
	SELECT
		company_ein,
		service_date,
		SUM(amount) AS daily_cost
	FROM claims
	GROUP BY
    	company_ein,
    	service_date
),

rolling AS (
	SELECT
		company_ein,
		service_date AS window_end,
		service_date - INTERVAL 89 DAY AS window_start,
		SUM(daily_cost) OVER (
			PARTITION BY company_ein
			ORDER BY service_date
			RANGE BETWEEN INTERVAL 89 DAY PRECEDING AND CURRENT ROW
		) AS current_90d_cost
	FROM daily
),

compare AS (
	SELECT
		company_ein,
		window_start,
		window_end,
		LAG(current_90d_cost) OVER (
			PARTITION BY company_ein
			ORDER BY window_end
		) AS prev_90d_cost,
		current_90d_cost AS current_90d_cost
	FROM rolling
)

SELECT
	cn.company_name,
	c.window_start,
	c.window_end,
	c.prev_90d_cost,
	c.current_90d_cost,
	CASE
		WHEN c.prev_90d_cost IS NULL OR c.prev_90d_cost = 0 THEN NULL
		ELSE (c.current_90d_cost - c.prev_90d_cost) / c.prev_90d_cost
	END AS pct_change
FROM compare AS c
JOIN company_names AS cn
	ON cn.company_ein = c.company_ein
WHERE
	CASE
		WHEN c.prev_90d_cost IS NULL OR c.prev_90d_cost = 0 THEN NULL
		ELSE (c.current_90d_cost - c.prev_90d_cost) / c.prev_90d_cost
	END > 2
ORDER BY cn.company_name, c.window_end;
