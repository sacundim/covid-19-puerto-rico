DROP DATABASE IF EXISTS covid19datos_sources CASCADE;

CREATE DATABASE covid19datos_sources
LOCATION 's3://covid-19-puerto-rico-data/covid19datos.salud.gov.pr/';


---------------------------------------------------------------------------
---------------------------------------------------------------------------
--
-- Hospitales nivel Puerto Rico
--

CREATE EXTERNAL TABLE covid19datos_sources.hospitales_json (
	FE_HOSPITALARIO DATE,
	CAMAS_ADULTOS_COVID INT,
	CAMAS_ADULTOS_NOCOVID INT,
	CAMAS_ADULTOS_DISP INT,
	CAMAS_ADULTOS_TOTAL INT,
	CAMAS_ICU_COVID INT,
	CAMAS_ICU_NOCOVID INT,
	CAMAS_ICU_DISP INT,
	CAMAS_ICU_TOTAL INT,
	CAMAS_PED_COVID INT,
	CAMAS_PED_NOCOVID INT,
	CAMAS_PED_DISP INT,
	CAMAS_PED_TOTAL INT,
	CAMAS_PICU_COVID INT,
	CAMAS_PICU_NOCOVID INT,
	CAMAS_PICU_DISP INT,
	CAMAS_PICU_TOTAL INT,
	VENT_ADULTOS_COVID INT,
	VENT_ADULTOS_NOCOVID INT,
	VENT_ADULTOS_DISP INT,
	VENT_ADULTOS_TOTAL INT,
	VENT_PED_COVID INT,
	VENT_PED_NOCOVID INT,
	VENT_PED_DISP INT,
	VENT_PED_TOTAL INT,
	CUARTOS_PRESNEG_OCC INT,
	CUARTOS_PRESNEG_DISP INT,
	CUARTOS_PRESNEG_TOTAL INT,
	VENT_ORD INT,
	VENT_REC INT,
	VENT_ENTR INT,
	CONVALECIENTES INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://covid-19-puerto-rico-data/covid19datos.salud.gov.pr/hospitales/';

--
-- Limpieza para exponer la fecha de las descargas, que suelen ser múltiples por día
--
CREATE OR REPLACE VIEW covid19datos_sources.hospitales_downloads AS
SELECT
	CAST(from_iso8601_timestamp(
		regexp_extract("$path", '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{6})[+-](\d{2}):(\d{2})'))
		AS TIMESTAMP)
		AS downloaded_at,
	*
FROM covid19datos_sources.hospitales_json
ORDER BY downloaded_at;

--
-- Limpieza para exponer una sola descarga por fecha.
--
CREATE OR REPLACE VIEW covid19datos_sources.hospitales_daily AS
WITH downloads AS (
	SELECT
		fe_hospitalario,
		max(downloaded_at) AS downloaded_at
	FROM covid19datos_sources.hospitales_downloads
	GROUP BY fe_hospitalario
)
SELECT *
FROM covid19datos_sources.hospitales_downloads
INNER JOIN downloads
	USING (fe_hospitalario, downloaded_at);


---------------------------------------------------------------------------
---------------------------------------------------------------------------
--
-- Vacunaciones nivel Puerto Rico
--

--
-- Los datos JSON crudos.  No consulten esta tabla directo, sino usen
-- uno de los resúmenes abajo.
--
CREATE EXTERNAL TABLE covid19datos_sources.vacunaciones_json (
	FE_DISTRIB_MODERNA DATE,
	FE_DISTRIB_PFIZER DATE,
	FE_DISTRIB_JOHNSON DATE,
	FE_DISTRIB_MUN_D1 DATE,
	FE_DISTRIB_MUN_D2 DATE,
	FE_POBLACION_INTERES DATE,
	DATA_DOSIS STRUCT<
		DISTRIBUIDAS: STRUCT<
			TX_REGISTRO: INT,
			FE_ACTUALIZADO: DATE
		>,
		RECIBIDAS: STRUCT<
			TX_REGISTRO: INT,
			FE_ACTUALIZADO: DATE
		>
	>,
	POBLACION_INTERES ARRAY<
		struct<
			POBLACION: STRING,
			DOSISTOTAL: INT
		>
	>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://covid-19-puerto-rico-data/covid19datos.salud.gov.pr/vacunaciones/';

--
-- Limpieza para exponer la fecha de las descargas, que pueden ser múltiples por día
--
CREATE OR REPLACE VIEW covid19datos_sources.vacunaciones_poblacion_interes_downloads AS
SELECT
	CAST(from_iso8601_timestamp(
		regexp_extract("$path", '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{6})[+-](\d{2}):(\d{2})'))
		AS TIMESTAMP)
		AS downloaded_at,
	t.poblacion_interes.poblacion,
	t.poblacion_interes.dosistotal
FROM covid19datos_sources.vacunaciones_json
CROSS JOIN UNNEST(poblacion_interes) AS t(poblacion_interes)
ORDER BY downloaded_at, poblacion;

--
-- Limpieza para exponer una sola descarga por fecha y cambio diario.
--
CREATE OR REPLACE VIEW covid19datos_sources.vacunaciones_poblacion_interes_daily AS
WITH downloads AS (
	SELECT
		date(downloaded_at AT TIME ZONE 'America/Puerto_Rico') local_date,
		max(downloaded_at) AS max_downloaded_at
	FROM covid19datos_sources.vacunaciones_poblacion_interes_downloads
	GROUP BY date(downloaded_at AT TIME ZONE 'America/Puerto_Rico')
)
SELECT
	local_date,
	poblacion,
	dosistotal,
	dosistotal - lag(dosistotal, 1) OVER (
		PARTITION BY poblacion
		ORDER BY local_date
	) dosis
FROM covid19datos_sources.vacunaciones_poblacion_interes_downloads
INNER JOIN downloads
	ON max_downloaded_at = downloaded_at;


---------------------------------------------------------------------------
---------------------------------------------------------------------------
--
-- Vacunaciones nivel municipios
--

--
-- Los datos JSON crudos.  No consulten esta tabla directo, sino usen
-- uno de los resúmenes abajo.
--
CREATE EXTERNAL TABLE covid19datos_sources.vacunaciones_municipios_json (
	TOTAL_DOSIS1 INT,
	TOTAL_DOSIS2 INT,
	DOSIS1_EDAD ARRAY<
		STRUCT<
			EDAD: STRING,
			TOTALDOSIS: INT
		>
	>,
	DOSIS1_SEXO ARRAY<
		STRUCT<
			SEXO: STRING,
			TOTALDOSIS: INT
		>
	>,
	DOSIS_UNADOSIS ARRAY<
		STRUCT<
			MAKER: STRING,
			TOTALDOSIS: INT
		>
	>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://covid-19-puerto-rico-data/covid19datos.salud.gov.pr/vacunaciones-municipios/';


CREATE OR REPLACE VIEW covid19datos_sources.vacunaciones_municipios_totales_downloads AS
SELECT
	CAST(from_iso8601_timestamp(
		regexp_extract("$path", '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{6})[+-](\d{2}):(\d{2})'))
		AS TIMESTAMP)
		AS downloaded_at,
	regexp_extract("$path", 'vacunaciones-([\p{Alpha} ]+)_', 1) municipio,
	total_dosis1,
	total_dosis2
FROM covid19datos_sources.vacunaciones_municipios_json
ORDER BY downloaded_at, municipio;

CREATE OR REPLACE VIEW covid19datos_sources.vacunaciones_municipios_totales_daily AS
WITH downloads AS (
	SELECT
		date(downloaded_at AT TIME ZONE 'America/Puerto_Rico') local_date,
		max(downloaded_at) AS max_downloaded_at
	FROM covid19datos_sources.vacunaciones_municipios_totales_downloads
	GROUP BY date(downloaded_at AT TIME ZONE 'America/Puerto_Rico')
)
SELECT
	local_date,
	municipio,
	total_dosis1,
	total_dosis1 - lag(total_dosis1, 1) OVER (
		PARTITION BY municipio
		ORDER BY local_date
	) dosis1,
	total_dosis2,
	total_dosis2 - lag(total_dosis2, 1) OVER (
		PARTITION BY municipio
		ORDER BY local_date
	) dosis2
FROM covid19datos_sources.vacunaciones_municipios_totales_downloads
INNER JOIN downloads
	ON max_downloaded_at = downloaded_at;


CREATE OR REPLACE VIEW covid19datos_sources.vacunaciones_municipios_dosis1_edad_downloads AS
SELECT
	CAST(from_iso8601_timestamp(
		regexp_extract("$path", '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{6})[+-](\d{2}):(\d{2})'))
		AS TIMESTAMP)
		AS downloaded_at,
	regexp_extract("$path", 'vacunaciones-([\p{Alpha} ]+)_', 1) municipio,
	t.dosis1_edad.edad,
	t.dosis1_edad.totaldosis
FROM covid19datos_sources.vacunaciones_municipios_json
CROSS JOIN UNNEST(dosis1_edad) AS t(dosis1_edad)
ORDER BY downloaded_at, municipio, edad;


CREATE OR REPLACE VIEW covid19datos_sources.vacunaciones_municipios_dosis1_sexo_downloads AS
SELECT
	CAST(from_iso8601_timestamp(
		regexp_extract("$path", '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{6})[+-](\d{2}):(\d{2})'))
		AS TIMESTAMP)
		AS downloaded_at,
	regexp_extract("$path", 'vacunaciones-([\p{Alpha} ]+)_', 1) municipio,
	t.dosis1_sexo.sexo,
	t.dosis1_sexo.totaldosis
FROM covid19datos_sources.vacunaciones_municipios_json
CROSS JOIN UNNEST(dosis1_sexo) AS t(dosis1_sexo)
ORDER BY downloaded_at, municipio, sexo;



CREATE OR REPLACE VIEW covid19datos_sources.vacunaciones_municipios_dosis_unadosis_downloads AS
SELECT
	CAST(from_iso8601_timestamp(
		regexp_extract("$path", '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{6})[+-](\d{2}):(\d{2})'))
		AS TIMESTAMP)
		AS downloaded_at,
	regexp_extract("$path", 'vacunaciones-([\p{Alpha} ]+)_', 1) municipio,
	t.dosis_unadosis.maker,
	t.dosis_unadosis.totaldosis
FROM covid19datos_sources.vacunaciones_municipios_json
CROSS JOIN UNNEST(dosis_unadosis) AS t(dosis_unadosis)
ORDER BY downloaded_at, municipio, maker;
