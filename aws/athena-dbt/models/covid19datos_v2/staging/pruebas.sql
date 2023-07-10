{{
    config(
        pre_hook=[
            "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'pruebas_v3').render_hive() }}"
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

 /* The initial full refresh times out in Athena when I try to do v1, v2 and v3
SELECT
	from_iso8601_date(downloaded_date)
		AS downloaded_date,
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
    nullif(id_orden, '') id_orden,
    nullif(co_tipo, '') co_tipo,
    nullif(tx_grupo_edad, '') tx_grupo_edad,
    nullif(co_resultado, '') co_resultado,
    nullif(co_sexo, '') co_sexo,
    nullif(co_region, '') co_region,
 	date(date_parse(NULLIF(fe_prueba, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_prueba
FROM {{ source('covid19datos_v2', 'pruebas_v1') }}
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
    nullif(id_orden, '') id_orden,
    nullif(co_tipo, '') co_tipo,
    nullif(tx_grupo_edad, '') tx_grupo_edad,
    nullif(co_resultado, '') co_resultado,
    nullif(co_sexo, '') co_sexo,
    nullif(co_region, '') co_region,
 	date(date_parse(NULLIF(fe_prueba, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_prueba
FROM {{ source('covid19datos_v2', 'pruebas_v2') }}
{% if is_incremental() %}
INNER JOIN incremental
  ON {{ parse_filename_timestamp('"$path"') }} > max_downloaded_at
  -- IMPORTANT: prunes partitions
  AND downloaded_date >= max_downloaded_date
{% endif %}

UNION ALL
*/

SELECT
	from_iso8601_date(downloaded_date)
		AS downloaded_date,
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
    nullif(id_orden, '') id_orden,
    nullif(co_tipo, '') co_tipo,
    nullif(tx_grupo_edad, '') tx_grupo_edad,
    nullif(co_resultado, '') co_resultado,
    nullif(co_sexo, '') co_sexo,
    nullif(co_region, '') co_region,
 	date(date_parse(NULLIF(fe_prueba, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_prueba
FROM {{ source('covid19datos_v2', 'pruebas_v3') }}
{% if is_incremental() %}
INNER JOIN incremental
  ON {{ parse_filename_timestamp('"$path"') }} > max_downloaded_at
  -- IMPORTANT: prunes partitions
  AND downloaded_date >= max_downloaded_date
{% endif %}
