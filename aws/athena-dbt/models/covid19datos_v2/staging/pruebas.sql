{{
    config(pre_hook=[
        "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'pruebas_v3').render_hive() }}"
    ])
}}

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
-- IMPORTANT: This prunes partitions
WHERE downloaded_date >= cast(date_add('day', -17, current_date) AS VARCHAR)

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
-- IMPORTANT: This prunes partitions
WHERE downloaded_date >= cast(date_add('day', -17, current_date) AS VARCHAR)

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
FROM {{ source('covid19datos_v2', 'pruebas_v3') }}
-- IMPORTANT: This prunes partitions
WHERE downloaded_date >= cast(date_add('day', -17, current_date) AS VARCHAR)

ORDER BY downloaded_at, fe_prueba;

