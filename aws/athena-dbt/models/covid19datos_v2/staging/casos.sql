----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------
--
-- Simple cleanup of the cases dataset.
--
{{
    config(
        pre_hook=[
            "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'casos_v1').render_hive() }}",
            "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'casos_v2').render_hive() }}",
            "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'casos_v3').render_hive() }}",
            "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'casos_v4').render_hive() }}"
        ],
        table_type='iceberg',
        partitioned_by=['month(downloaded_date)'],
        materialized='incremental',
        incremental_strategy='append',
        post_hook = [
            'VACUUM {{ this.render_pure() }};'
        ]
    )
}}
{% if is_incremental() %}
WITH incremental AS (
    SELECT
        max(downloaded_at) max_downloaded_at,
        CAST(max(downloaded_date) AS VARCHAR) max_downloaded_date
    FROM {{ this }}
)
{% endif %}

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
{% if is_incremental() %}
INNER JOIN incremental
  ON {{ parse_filename_timestamp('"$path"') }} > max_downloaded_at
  -- IMPORTANT: prunes partitions
  AND downloaded_date >= max_downloaded_date
{% endif %}

UNION ALL

SELECT
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
	CAST("ID number" AS VARCHAR) id_number,
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
{% if is_incremental() %}
INNER JOIN incremental
  ON {{ parse_filename_timestamp('"$path"') }} > max_downloaded_at
  -- IMPORTANT: prunes partitions
  AND downloaded_date >= max_downloaded_date
{% endif %}

UNION ALL

SELECT
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
	NULLIF("ID number", '') id_number,
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
{% if is_incremental() %}
INNER JOIN incremental
  ON {{ parse_filename_timestamp('"$path"') }} > max_downloaded_at
  -- IMPORTANT: prunes partitions
  AND downloaded_date >= max_downloaded_date
{% endif %}

UNION ALL

SELECT
    downloaded_at,
	NULLIF("ID number", '') id_number,
	NULLIF(age, '') age,
	NULLIF(sex, '') sex,
	NULLIF(city, '') city,
	NULLIF(region, '') region,
	NULLIF(class, '') class,
 	"Sample Date" AS sample_date,
	from_iso8601_date(downloaded_date)
		AS downloaded_date
FROM {{ source('covid19datos_v2', 'casos_v4') }}
{% if is_incremental() %}
INNER JOIN incremental
  ON downloaded_at > max_downloaded_at
  -- IMPORTANT: prunes partitions
  AND downloaded_date >= max_downloaded_date
{% endif %}
