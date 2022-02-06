{{ config(pre_hook=["MSCK REPAIR TABLE {{ source('covid19datos_v2', 'vacunacion') }}"]) }}

SELECT
	from_iso8601_date(downloaded_date)
		AS downloaded_date,
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
    nullif(co_municipio, '') co_municipio,
 	date(date_parse(NULLIF(fe_vacuna, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_vacuna,
    CAST(nullif(nu_dosis, '') AS INT) nu_dosis,
    nullif(co_manufacturero, '') co_manufacturero
FROM {{ source('covid19datos_v2', 'vacunacion') }}
ORDER BY downloaded_at, fe_vacuna;