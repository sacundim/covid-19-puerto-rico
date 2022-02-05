{{ config(pre_hook=["MSCK REPAIR TABLE {{ source('covid19datos_v2', 'defunciones') }}"]) }}

SELECT
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
	from_iso8601_date(downloaded_date)
		AS downloaded_date,
	NULLIF(id_muerte, '') id_muerte,
	NULLIF(co_sexo, '') co_sexo,
	NULLIF(co_region, '') co_region,
	NULLIF(co_clasificacion, '') co_clasificacion,
 	date(date_parse(NULLIF(fe_muerte, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_muerte,
 	date(date_parse(NULLIF(fe_reporte, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_reporte,
 	date(date_parse(NULLIF(fe_bioportal, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_bioportal,
 	date(date_parse(NULLIF(fe_registro, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_registro,
	NULLIF(tx_grupo_edad, '') tx_grupo_edad
FROM {{ source('covid19datos_v2', 'defunciones') }}
ORDER BY downloaded_at, fe_muerte, fe_reporte;