--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--
-- Vaccination analysis.
--

WITH downloads AS (
	SELECT
		date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
			AS bulletin_date,
		max(downloaded_at) downloaded_at
	FROM {{ ref('vacunacion') }}
	GROUP BY date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
), vendors AS (
    SELECT DISTINCT
        co_manufacturero vendor,
        nu_dosis dose_number
	FROM {{ ref('vacunacion') }}
), grid AS (
	SELECT
	    downloaded_at,
		bulletin_date,
		date(date_column) AS dose_date,
		city,
		display_name,
		fips,
		vendor,
		dose_number
	FROM {{ ref('covid19datos_v2_vacunacion_city_names') }}
	CROSS JOIN (
		VALUES (SEQUENCE(DATE '2020-12-03', DATE '2021-12-31', INTERVAL '1' DAY))
	) AS date_array (date_array)
	CROSS JOIN UNNEST(date_array) AS t2(date_column)
	INNER JOIN downloads
		ON CAST(date_column AS DATE) < bulletin_date
	CROSS JOIN vendors
)
SELECT
	grid.bulletin_date,
	grid.dose_date,
	grid.display_name AS municipality,
	grid.fips,
	grid.vendor,
	grid.dose_number,
	(grid.vendor = 'JSN' AND grid.dose_number = 1)
	    OR (grid.vendor != 'JSN' AND grid.dose_number = 2)
		AS is_complete,
	(grid.vendor = 'JSN' AND grid.dose_number > 1)
	    OR (grid.vendor != 'JSN' AND grid.dose_number > 2)
		AS is_booster,
	count(vax.downloaded_at) doses,
	sum(count(vax.downloaded_at)) OVER (
		PARTITION BY
			grid.bulletin_date,
			grid.fips,
			grid.vendor,
			grid.dose_number
		ORDER BY grid.dose_date
	) AS cumulative_doses,
	count(vax.downloaded_at)
	    - lag(count(vax.downloaded_at)) OVER (
	        PARTITION BY
                grid.dose_date,
                grid.fips,
                grid.vendor,
                grid.dose_number
	        ORDER BY grid.bulletin_date
	    ) AS delta_doses
FROM {{ ref('vacunacion') }} vax
RIGHT OUTER JOIN grid
	ON grid.city = vax.co_municipio
	AND grid.dose_date = vax.fe_vacuna
	AND grid.vendor = vax.co_manufacturero
	AND grid.dose_number = vax.nu_dosis
	AND grid.downloaded_at = vax.downloaded_at
GROUP BY
	grid.bulletin_date,
	grid.dose_date,
	grid.display_name,
	grid.fips,
	grid.vendor,
	grid.dose_number
ORDER BY
	grid.bulletin_date,
	grid.dose_date,
	grid.display_name,
	grid.vendor,
	grid.dose_number;
