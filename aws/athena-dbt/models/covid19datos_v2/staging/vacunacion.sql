{{ config(pre_hook=["MSCK REPAIR TABLE {{ source('covid19datos_v2', 'vacunacion') }}"]) }}

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
FROM {{ source('covid19datos_v2', 'vacunacion') }}
ORDER BY downloaded_at, fe_vacuna;