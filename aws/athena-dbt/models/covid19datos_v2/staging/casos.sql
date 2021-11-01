----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------
--
-- Simple cleanup of the cases dataset.
--

{{ config(pre_hook=["MSCK REPAIR TABLE {{ source('covid19datos_v2', 'casos') }}"]) }}

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
FROM {{ source('covid19datos_v2', 'casos') }}
ORDER BY downloaded_at, sample_date, fe_reporte;