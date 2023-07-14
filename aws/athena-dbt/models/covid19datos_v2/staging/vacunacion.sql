{{
    config(
        pre_hook=[
            "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'vacunacion_v1').render_hive() }}",
            "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'vacunacion_v2').render_hive() }}",
            "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'vacunacion_v3').render_hive() }}",
            "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'vacunacion_v4').render_hive() }}",
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
	from_iso8601_date(downloaded_date)
		AS downloaded_date,
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
    nullif(co_municipio, '') co_municipio,
 	date(date_parse(NULLIF(fe_vacuna, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_vacuna,
    nullif(nu_dosis, '') nu_dosis,
    nullif(co_manufacturero, '') co_manufacturero
FROM {{ source('covid19datos_v2', 'vacunacion_v1') }}
{% if is_incremental() %}
INNER JOIN incremental
  ON {{ parse_filename_timestamp('"$path"') }} > max_downloaded_at
  -- IMPORTANT: prunes partitions
  AND downloaded_date >= max_downloaded_date
{% endif %}

UNION ALL

SELECT
	from_iso8601_date(downloaded_date)
		AS downloaded_date,
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
    nullif(co_municipio, '') co_municipio,
 	date(date_parse(NULLIF(fe_vacuna, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_vacuna,
    CAST(nu_dosis AS VARCHAR) nu_dosis,
    nullif(co_manufacturero, '') co_manufacturero
FROM {{ source('covid19datos_v2', 'vacunacion_v2') }}
{% if is_incremental() %}
INNER JOIN incremental
  ON {{ parse_filename_timestamp('"$path"') }} > max_downloaded_at
  -- IMPORTANT: prunes partitions
  AND downloaded_date >= max_downloaded_date
{% endif %}

UNION ALL

SELECT
	from_iso8601_date(downloaded_date)
		AS downloaded_date,
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
    nullif(co_municipio, '') co_municipio,
 	date(date_parse(NULLIF(fe_vacuna, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_vacuna,
    nullif(nu_dosis, '') nu_dosis,
    nullif(co_manufacturero, '') co_manufacturero
FROM {{ source('covid19datos_v2', 'vacunacion_v3') }}
{% if is_incremental() %}
INNER JOIN incremental
  ON {{ parse_filename_timestamp('"$path"') }} > max_downloaded_at
  -- IMPORTANT: prunes partitions
  AND downloaded_date >= max_downloaded_date
{% endif %}

UNION ALL

SELECT
	from_iso8601_date(downloaded_date)
		AS downloaded_date,
    downloaded_at,
    nullif(co_municipio, '') co_municipio,
 	date(fe_vacuna) AS fe_vacuna,
    nullif(nu_dosis, '') nu_dosis,
    nullif(co_manufacturero, '') co_manufacturero
FROM {{ source('covid19datos_v2', 'vacunacion_v4') }}
{% if is_incremental() %}
INNER JOIN incremental
  ON downloaded_at > max_downloaded_at
  -- IMPORTANT: prunes partitions
  AND downloaded_date >= max_downloaded_date
{% endif %}
