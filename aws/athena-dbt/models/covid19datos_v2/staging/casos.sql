----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------
--
-- Simple cleanup of the cases dataset.
--

{{ config(pre_hook=["MSCK REPAIR TABLE {{ source('covid19datos_v2', 'casos') }}"]) }}

SELECT
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
	cast(NULLIF(id_number, '') AS BIGINT) id_number,
	cast(NULLIF(age, '') AS BIGINT) age,
	NULLIF(sex, '') sex,
	NULLIF(city, '') city,
	NULLIF(region, '') region,
	NULLIF(class, '') class,
 	date(date_parse(NULLIF(sample_date, ''), '%Y-%m-%d %H:%i:%s'))
		AS sample_date,
	from_iso8601_date(downloaded_date)
		AS downloaded_date
FROM {{ source('covid19datos_v2', 'casos') }}
ORDER BY downloaded_at, sample_date, fe_reporte;