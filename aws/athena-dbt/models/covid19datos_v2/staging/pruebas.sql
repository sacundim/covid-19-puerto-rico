{{ config(pre_hook=["MSCK REPAIR TABLE covid19datos_v2_sources.pruebas_parquet"]) }}

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
-- IMPORTANT: This prunes partitions
WHERE downloaded_date >= cast(date_add('day', -32, current_date) AS VARCHAR)
ORDER BY downloaded_at, fe_prueba;
