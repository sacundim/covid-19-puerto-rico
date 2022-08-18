------------------------------------------------------------------------
------------------------------------------------------------------------
--
-- Vaccinations summarized by municipality.
--
{{ config(enabled=false) }}
WITH prdoh AS (
	SELECT
		bulletin_date local_date,
		fips,
		-- Persons with at least one dose
		sum(doses) FILTER (
			WHERE dose_number = 1
		) total_dosis1,
		-- Persons with complete regime
		sum(doses) FILTER (
			WHERE is_complete
		) total_dosis2,
		-- Persons with booster
		sum(doses) FILTER (
			WHERE is_booster
		) total_dosis3,
		-- Number of doses administered
		sum(doses) AS total_dosis
	FROM {{ ref('vacunacion_cube') }}
	WHERE bulletin_date >= DATE '2021-07-23'
	GROUP BY bulletin_date, fips
	UNION
	SELECT
		local_date,
		fips,
		-- Persons with at least one dose
		total_dosis1,
		-- Persons with complete regime
		total_dosis2,
		-- Persons with booster. This old data source didn't
		-- have any.
		0 AS total_dosis3,
		-- Incorrect approximation of total number of doses.
		-- The problem is that single-dose vaccines, when
		-- administered, increment both dosis1 and dosis2.
		-- But this sum is the best we can do...
		total_dosis1 + total_dosis2
			AS total_dosis
	FROM covid19datos_sources.vacunaciones_municipios_totales_daily prdoh
	INNER JOIN {{ ref('municipal_population') }} pop
		ON pop.name = prdoh.municipio
	-- data in this source goes bad on the 22nd...
	WHERE local_date <= DATE '2021-07-21'
)
SELECT
	local_date,
	pop.name AS municipio,
	fips AS fips_code,
	pop2020,
	total_dosis1 AS salud_total_dosis1,
	CAST(total_dosis1 AS DOUBLE)
		/ pop2020
		AS salud_total_dosis1_pct,
	total_dosis1 - lag(total_dosis1) OVER (
		PARTITION BY fips
		ORDER BY local_date
	) AS salud_dosis1,
	total_dosis2 AS salud_total_dosis2,
	CAST(total_dosis2 AS DOUBLE)
		/ pop2020
		AS salud_total_dosis2_pct,
	total_dosis2 - lag(total_dosis2) OVER (
		PARTITION BY fips
		ORDER BY local_date
	) AS salud_dosis2,
	total_dosis3 AS salud_total_dosis3,
	CAST(total_dosis3 AS DOUBLE)
		/ pop2020
		AS salud_total_dosis3_pct,
	total_dosis3 - lag(total_dosis3) OVER (
		PARTITION BY fips
		ORDER BY local_date
	) AS salud_dosis3,
	total_dosis AS salud_total_dosis,
	100.0 * (total_dosis) / pop2020
		AS salud_total_dosis_per_100,
	total_dosis - lag(total_dosis) OVER (
		PARTITION BY fips
		ORDER BY local_date
	) AS salud_dosis
FROM prdoh
INNER JOIN {{ ref('municipal_population') }} pop
	USING (fips)
ORDER BY local_date, municipio;