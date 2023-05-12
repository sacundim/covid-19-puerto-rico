----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------
--
-- Simple cleanup of the cases dataset.
--

{{
    config(pre_hook=[
        "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'casos_v1').render_hive() }}",
        "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'casos_v2').render_hive() }}",
        "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'casos_v3').render_hive() }}"
    ])
}}

SELECT
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
	NULLIF(id_number, '') id_number,
	NULLIF(age, '') age,
	NULLIF(sex, '') sex,
	NULLIF(city, '') city,
	NULLIF(region, '') region,
	NULLIF(class, '') class,
 	date(date_parse(NULLIF(sample_date, ''), '%Y-%m-%d %H:%i:%s'))
		AS sample_date,
	from_iso8601_date(downloaded_date)
		AS downloaded_date
FROM {{ source('covid19datos_v2', 'casos_v1') }}
-- IMPORTANT: This prunes partitions
WHERE downloaded_date >= cast(date_add('day', -32, current_date) AS VARCHAR)

UNION ALL

SELECT
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
	CAST(id_number AS VARCHAR) id_number,
	CAST(age AS VARCHAR) age,
	NULLIF(sex, '') sex,
	NULLIF(city, '') city,
	NULLIF(region, '') region,
	NULLIF(class, '') class,
 	date(date_parse(NULLIF("Sample Date", ''), '%Y-%m-%d %H:%i:%s'))
		AS sample_date,
	from_iso8601_date(downloaded_date)
		AS downloaded_date
FROM {{ source('covid19datos_v2', 'casos_v2') }}
-- IMPORTANT: This prunes partitions
WHERE downloaded_date >= cast(date_add('day', -32, current_date) AS VARCHAR)

UNION ALL

SELECT
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
	NULLIF(id_number, '') id_number,
	NULLIF(age, '') age,
	NULLIF(sex, '') sex,
	NULLIF(city, '') city,
	NULLIF(region, '') region,
	NULLIF(class, '') class,
 	date(date_parse(NULLIF("Sample Date", ''), '%Y-%m-%d %H:%i:%s'))
		AS sample_date,
	from_iso8601_date(downloaded_date)
		AS downloaded_date
FROM {{ source('covid19datos_v2', 'casos_v3') }}
-- IMPORTANT: This prunes partitions
WHERE downloaded_date >= cast(date_add('day', -32, current_date) AS VARCHAR)

ORDER BY downloaded_at, sample_date;