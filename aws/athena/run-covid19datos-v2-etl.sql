----------------------------------------------------------
----------------------------------------------------------
--
-- Rebuild the whole schema from scratch from the raw tables.
--

DROP DATABASE IF EXISTS covid19datos_v2_etl CASCADE;

CREATE DATABASE covid19datos_v2_etl
LOCATION 's3://covid-19-puerto-rico-athena/';


MSCK REPAIR TABLE covid19datos_v2_sources.casos_parquet;

CREATE TABLE covid19datos_v2_etl.casos WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['downloaded_at'],
    bucket_count = 1
) AS
SELECT
	CAST(from_iso8601_timestamp(
		regexp_extract("$path", '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z'))
		AS TIMESTAMP)
		AS downloaded_at,
	cast(NULLIF(id_number, '') AS BIGINT) id_number,
	cast(NULLIF(age, '') AS BIGINT) age,
	NULLIF(sex, '') sex,
	NULLIF(city, '') city,
	NULLIF(region, '') region,
	NULLIF(class, '') class,
 	date(date_parse(NULLIF(sample_date, ''), '%Y-%m-%d %H:%i:%s'))
		AS sample_date,
 	date(date_parse(NULLIF(fe_reporte, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_reporte,
	from_iso8601_date(downloaded_date)
		AS downloaded_date
FROM covid19datos_v2_sources.casos_parquet
ORDER BY downloaded_at, sample_date, fe_reporte;



MSCK REPAIR TABLE covid19datos_v2_sources.defunciones_parquet;

CREATE TABLE covid19datos_v2_etl.defunciones WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['downloaded_at'],
    bucket_count = 1
) AS
SELECT
	CAST(from_iso8601_timestamp(
		regexp_extract("$path", '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z'))
		AS TIMESTAMP)
		AS downloaded_at,
	from_iso8601_date(downloaded_date)
		AS downloaded_date,
	NULLIF(id_muerte, '') id_muerte,
	NULLIF(co_sexo, '') co_sexo,
	NULLIF(co_region, '') co_region,
	NULLIF(co_clasificacion, '') co_clasificacion,
 	date(date_parse(NULLIF(fe_muerte, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_muerte,
 	date(date_parse(NULLIF(fe_reporte, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_reporte,
 	date(date_parse(NULLIF(fe_bioportal, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_bioportal,
 	date(date_parse(NULLIF(fe_registro, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_registro,
	NULLIF(tx_grupo_edad, '') tx_grupo_edad
FROM covid19datos_v2_sources.defunciones_parquet
ORDER BY downloaded_at, fe_muerte, fe_reporte;


MSCK REPAIR TABLE covid19datos_v2_sources.sistemas_salud_parquet;

CREATE TABLE covid19datos_v2_etl.sistemas_salud WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['downloaded_at'],
    bucket_count = 1
) AS
SELECT
	from_iso8601_date(downloaded_date)
		AS downloaded_date,
	CAST(from_iso8601_timestamp(
		regexp_extract("$path", '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z'))
		AS TIMESTAMP)
		AS downloaded_at,
 	date(date_parse(NULLIF(fe_reporte, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_reporte,
    CAST(nullif(camas_adultos_covid, '') AS INT) camas_adultos_covid,
    CAST(nullif(camas_adultos_nocovid, '') AS INT) camas_adultos_nocovid,
    CAST(nullif(camas_adultos_occ, '') AS INT) camas_adultos_occ,
    CAST(nullif(camas_adultos_disp, '') AS INT) camas_adultos_disp,
    CAST(nullif(camas_adultos_total, '') AS INT) camas_adultos_total,
    CAST(nullif(camas_icu_covid, '') AS INT) camas_icu_covid,
    CAST(nullif(camas_icu_nocovid, '') AS INT) camas_icu_nocovid,
    CAST(nullif(camas_icu_occ, '') AS INT) camas_icu_occ,
    CAST(nullif(camas_icu_disp, '') AS INT) camas_icu_disp,
    CAST(nullif(camas_icu_total, '') AS INT) camas_icu_total,
    CAST(nullif(camas_ped_covid, '') AS INT) camas_ped_covid,
    CAST(nullif(camas_ped_nocovid, '') AS INT) camas_ped_nocovid,
    CAST(nullif(camas_ped_occ, '') AS INT) camas_ped_occ,
    CAST(nullif(camas_ped_disp, '') AS INT) camas_ped_disp,
    CAST(nullif(camas_ped_total, '') AS INT) camas_ped_total,
    CAST(nullif(camas_picu_covid, '') AS INT) camas_picu_covid,
    CAST(nullif(camas_picu_nocovid, '') AS INT) camas_picu_nocovid,
    CAST(nullif(camas_picu_occ, '') AS INT) camas_picu_occ,
    CAST(nullif(camas_picu_disp, '') AS INT) camas_picu_disp,
    CAST(nullif(camas_picu_total, '') AS INT) camas_picu_total,
    CAST(nullif(vent_adultos_covid, '') AS INT) vent_adultos_covid,
    CAST(nullif(vent_adultos_nocovid, '') AS INT) vent_adultos_nocovid,
    CAST(nullif(vent_adultos_occ, '') AS INT) vent_adultos_occ,
    CAST(nullif(vent_adultos_disp, '') AS INT) vent_adultos_disp,
    CAST(nullif(vent_adultos_total, '') AS INT) vent_adultos_total,
    CAST(nullif(vent_ped_covid, '') AS INT) vent_ped_covid,
    CAST(nullif(vent_ped_nocovid, '') AS INT) vent_ped_nocovid,
    CAST(nullif(vent_ped_occ, '') AS INT) vent_ped_occ,
    CAST(nullif(vent_ped_disp, '') AS INT) vent_ped_disp,
    CAST(nullif(vent_ped_total, '') AS INT) vent_ped_total,
    CAST(nullif(cuartos_presneg_occ, '') AS INT) cuartos_presneg_occ,
    CAST(nullif(cuartos_presneg_disp, '') AS INT) cuartos_presneg_disp,
    CAST(nullif(cuartos_presneg_total, '') AS INT) cuartos_presneg_total,
    CAST(nullif(vent_ord, '') AS INT) vent_ord,
    CAST(nullif(vent_rec, '') AS INT) vent_rec,
    CAST(nullif(vent_entr, '') AS INT) vent_entr,
    CAST(nullif(convalecientes, '') AS INT) convalecientes
FROM covid19datos_v2_sources.sistemas_salud_parquet
ORDER BY downloaded_at, fe_reporte;


MSCK REPAIR TABLE covid19datos_v2_sources.vacunacion_parquet;

CREATE TABLE covid19datos_v2_etl.vacunacion WITH (
    format = 'PARQUET'
) AS
SELECT
	from_iso8601_date(downloaded_date)
		AS downloaded_date,
	CAST(from_iso8601_timestamp(
		regexp_extract("$path", '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z'))
		AS TIMESTAMP)
		AS downloaded_at,
    CAST(nullif(nu_edad, '') AS INT) nu_edad,
    nullif(co_municipio, '') co_municipio,
    nullif(co_region, '') co_region,
 	date(date_parse(NULLIF(fe_vacuna, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_vacuna,
    CAST(nullif(nu_dosis, '') AS INT) nu_dosis,
    nullif(co_manufacturero, '') co_manufacturero,
 	date(date_parse(NULLIF(fe_reporte, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_reporte
FROM covid19datos_v2_sources.vacunacion_parquet
ORDER BY downloaded_at, fe_vacuna;


MSCK REPAIR TABLE covid19datos_v2_sources.pruebas_parquet;

CREATE TABLE covid19datos_v2_etl.pruebas WITH (
    format = 'PARQUET'
) AS
SELECT
	from_iso8601_date(downloaded_date)
		AS downloaded_date,
	CAST(from_iso8601_timestamp(
		regexp_extract("$path", '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z'))
		AS TIMESTAMP)
		AS downloaded_at,
    nullif(id_orden, '') id_orden,
    nullif(co_tipo, '') co_tipo,
    nullif(tx_grupo_edad, '') tx_grupo_edad,
    nullif(co_resultado, '') co_resultado,
    nullif(co_sexo, '') co_sexo,
    nullif(co_region, '') co_region,
 	date(date_parse(NULLIF(fe_prueba, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_prueba,
 	date(date_parse(NULLIF(fe_reporte, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_reporte,
 	date(date_parse(NULLIF(fe_registro, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_registro
FROM covid19datos_v2_sources.pruebas_parquet
ORDER BY downloaded_at, fe_prueba;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--
-- Recreate the old bulletin_cases table that I used to make from the daily
-- PDFs.  In fact, we're still pulling here from my old CSV.
--

CREATE TABLE covid19datos_v2_etl.bulletin_cases WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
WITH casos AS (
	WITH downloads AS (
		SELECT
			date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
				AS bulletin_date,
			max(downloaded_at) downloaded_at
		FROM covid19datos_v2_etl.casos
		GROUP BY date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
	)
	SELECT
		bulletin_date,
		sample_date AS datum_date,
		count(*) FILTER (WHERE class = 'CONFIRMADO')
			AS confirmed_cases,
		count(*) FILTER (WHERE class = 'PROBABLE')
			AS probable_cases
	FROM covid19datos_v2_etl.casos casos
	INNER JOIN downloads
		USING (downloaded_at)
	GROUP BY bulletin_date, sample_date
), defunciones AS (
	WITH downloads AS (
		SELECT
			date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
				AS bulletin_date,
			max(downloaded_at) downloaded_at
		FROM covid19datos_v2_etl.casos
		GROUP BY date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
	)
	SELECT
		bulletin_date,
		fe_muerte AS datum_date,
		count(*) deaths
	FROM covid19datos_v2_etl.defunciones defunciones
	INNER JOIN downloads
		USING (downloaded_at)
	GROUP BY bulletin_date, fe_muerte
), joined AS (
	SELECT
		bulletin_date,
		datum_date,
		confirmed_cases,
		probable_cases,
		COALESCE(deaths, 0) deaths
	FROM casos
	FULL OUTER JOIN defunciones
		USING (bulletin_date, datum_date)
	UNION
	-- Merge with the old hand-curated data set:
    SELECT
        from_iso8601_date(bulletin_date) AS bulletin_date,
        from_iso8601_date(datum_date) AS datum_date,
        CAST(nullif(confirmed_cases, '') AS INTEGER) AS confirmed_cases,
        CAST(nullif(probable_cases, '') AS INTEGER) AS probable_cases,
        CAST(nullif(deaths, '') AS INTEGER) AS deaths
    FROM covid_pr_sources.bulletin_cases_csv
)
SELECT
	bulletin_date,
	datum_date,
	date_diff('day', datum_date, bulletin_date)
		AS age,
	confirmed_cases,
    sum(confirmed_cases) OVER (
        PARTITION BY bulletin_date
        ORDER BY datum_date
    ) AS cumulative_confirmed_cases,
    COALESCE(confirmed_cases, 0)
        - COALESCE(lag(confirmed_cases) OVER (
            PARTITION BY datum_date
            ORDER BY bulletin_date
        ), 0) AS delta_confirmed_cases,
    probable_cases,
    sum(probable_cases) OVER (
        PARTITION BY bulletin_date
        ORDER BY datum_date
    ) AS cumulative_probable_cases,
    COALESCE(probable_cases, 0)
        - COALESCE(lag(probable_cases) OVER (
            PARTITION BY datum_date
            ORDER BY bulletin_date
        ), 0) AS delta_probable_cases,
	deaths,
    sum(deaths) OVER (
        PARTITION BY bulletin_date
        ORDER BY datum_date
    ) AS cumulative_deaths,
    COALESCE(deaths, 0)
        - COALESCE(lag(deaths) OVER (
            PARTITION BY datum_date
            ORDER BY bulletin_date
        ), 0) AS delta_deaths
FROM joined
ORDER BY bulletin_date, datum_date;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--
-- Daily hospitalization data
--

CREATE VIEW covid19datos_v2_etl.hospitales_daily AS
WITH downloads AS (
    SELECT
        date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
            AS bulletin_date,
        max(downloaded_at) downloaded_at
    FROM covid19datos_v2_etl.sistemas_salud
    GROUP BY date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
)
SELECT *
FROM covid19datos_v2_etl.sistemas_salud
INNER JOIN downloads
    USING (downloaded_at)
ORDER BY bulletin_date, fe_reporte;


--
-- Hospital bed availabilty and ICU occupancy, using PRDoH data
--
CREATE OR REPLACE VIEW covid19datos_v2_etl.prdoh_hospitalizations AS
SELECT
	bulletin_date,
	fe_reporte date,
	'Adultos' age,
	'Camas' resource,
	camas_adultos_total total,
	camas_adultos_covid covid,
	camas_adultos_nocovid nocovid,
	camas_adultos_disp disp
FROM covid19datos_v2_etl.hospitales_daily
WHERE fe_reporte >= date_add('day', -41, bulletin_date)
UNION ALL
SELECT
	bulletin_date,
	fe_reporte date,
	'Adultos' age,
	'UCI' resource,
	camas_icu_total total,
	camas_icu_covid covid,
	camas_icu_nocovid nocovid,
	camas_icu_disp disp
FROM covid19datos_v2_etl.hospitales_daily
WHERE fe_reporte >= date_add('day', -41, bulletin_date)
UNION ALL
SELECT
	bulletin_date,
	fe_reporte date,
	'Pediátricos' age,
	'Camas' resource,
	camas_ped_total total,
	camas_ped_covid covid,
	camas_ped_nocovid nocovid,
	camas_ped_disp disp
FROM covid19datos_v2_etl.hospitales_daily
WHERE fe_reporte >= date_add('day', -42, bulletin_date)
UNION ALL
SELECT
	bulletin_date,
	fe_reporte date,
	'Pediátricos' age,
	'UCI' resource,
	camas_picu_total total,
	camas_picu_covid covid,
	camas_picu_nocovid nocovid,
	camas_picu_disp disp
FROM covid19datos_v2_etl.hospitales_daily
WHERE fe_reporte >= date_add('day', -41, bulletin_date)
ORDER BY date DESC, age, resource;



--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--
-- Cases by municipality.
--

CREATE TABLE covid19datos_v2_etl.cases_municipal_agg WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
SELECT
	date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
		AS bulletin_date,
	sample_date,
	display_name municipality,
	fips,
	count(*) new_cases,
	count(*) FILTER (
		WHERE class = 'CONFIRMADO'
	) AS new_confirmed,
	count(*) FILTER (
		WHERE class = 'PROBABLE'
	) AS new_probable
FROM covid19datos_v2_etl.casos
INNER JOIN covid19datos_v2_sources.casos_city_names names
	USING (city)
WHERE sample_date >= date_add('day', -15, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
GROUP BY downloaded_at, sample_date, fips, display_name
ORDER BY downloaded_at, sample_date, display_name;



--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--
-- Vaccination analysis.
--

-- FIXME: there are missing dose_date/municipality pairs
CREATE TABLE covid19datos_v2_etl.vacunacion_agg WITH (
    format = 'PARQUET'
) AS
WITH downloads AS (
	SELECT
		date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
			AS bulletin_date,
		max(downloaded_at) downloaded_at
	FROM covid19datos_v2_etl.vacunacion
	GROUP BY date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
)
SELECT
	bulletin_date,
	fe_vacuna AS dose_date,
	munis.display_name AS municipality,
	munis.fips,
	co_manufacturero AS vendor,
	nu_dosis AS dose_number,
	co_manufacturero = 'JSN' OR nu_dosis = 2
		AS is_complete,
	count(*) doses
FROM covid19datos_v2_etl.vacunacion vax
INNER JOIN downloads
	USING (downloaded_at)
LEFT OUTER JOIN covid19datos_v2_sources.casos_city_names munis
	ON munis.city = vax.co_municipio
GROUP BY
	bulletin_date,
	fe_vacuna,
	nu_dosis,
	co_manufacturero,
	munis.display_name,
	munis.fips
ORDER BY
	bulletin_date,
	fe_vacuna,
	munis.display_name,
	vendor,
	dose_number;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--
-- Views for diverse reports.
--

CREATE VIEW covid19datos_v2_etl.daily_deltas AS
SELECT
    bulletin_date,
	datum_date,
	delta_confirmed_cases,
	delta_probable_cases,
	delta_deaths
FROM covid19datos_v2_etl.bulletin_cases
-- We exclude the earliest bulletin date because it's artificially big
WHERE bulletin_date > (
	SELECT min(bulletin_date)
	FROM covid19datos_v2_etl.bulletin_cases
);


--
-- For the LatenessTiers chart
--
CREATE VIEW covid19datos_v2_etl.lateness_tiers AS
SELECT
	bulletin_date,
	ranges.tier,
	ranges.lo AS tier_order,
	COALESCE(sum(delta_confirmed_cases) FILTER (
		WHERE delta_confirmed_cases > 0
	), 0) AS count
FROM covid19datos_v2_etl.bulletin_cases ba
INNER JOIN (VALUES (0, 3, '0-3'),
				   (4, 7, '4-7'),
				   (8, 14, '8-14'),
				   (14, NULL, '> 14')) AS ranges (lo, hi, tier)
	ON ranges.lo <= age AND age <= COALESCE(ranges.hi, 2147483647)
WHERE bulletin_date > DATE '2020-04-24'
GROUP BY bulletin_date, ranges.lo, ranges.hi, ranges.tier
ORDER BY bulletin_date DESC, ranges.lo ASC;


--
-- For the WeekdayBias chart
--
CREATE VIEW covid19datos_v2_etl.weekday_bias AS
SELECT
	ba.bulletin_date,
	ba.datum_date,
	ba.delta_confirmed_cases,
	ba.delta_probable_cases,
	ba.delta_deaths
FROM covid19datos_v2_etl.bulletin_cases ba
WHERE ba.datum_date >= ba.bulletin_date - INTERVAL '14' DAY
AND ba.bulletin_date > (
	SELECT min(bulletin_date)
	FROM covid19datos_v2_etl.bulletin_cases
	WHERE delta_confirmed_cases IS NOT NULL
	AND delta_probable_cases IS NOT NULL
	AND delta_deaths IS NOT NULL)
ORDER BY bulletin_date, datum_date;


--
-- For VaccinationMap.
--
CREATE VIEW covid19datos_v2_etl.municipal_vaccinations AS
SELECT
	bulletin_date,
	dose_date,
	municipality,
	fips,
	popest2019,
	sum(doses) AS doses,
	sum(sum(doses) FILTER (
		WHERE is_complete
	)) OVER (
		PARTITION BY bulletin_date, municipality
		ORDER BY dose_date
	) AS complete
FROM covid19datos_v2_etl.vacunacion_agg
LEFT OUTER JOIN covid19datos_v2_sources.population_estimates_2019
	USING (fips)
GROUP BY
	bulletin_date,
	dose_date,
	municipality,
	fips,
	popest2019
ORDER BY
	bulletin_date DESC,
	dose_date DESC,
	municipality;