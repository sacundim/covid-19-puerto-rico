DROP DATABASE IF EXISTS covid19datos_v2_sources CASCADE;

CREATE DATABASE covid19datos_v2_sources
LOCATION 's3://covid-19-puerto-rico-data/covid19datos-v2/';


---------------------------------------------------------------------------
---------------------------------------------------------------------------
--
-- Casos
--

CREATE EXTERNAL TABLE covid19datos_v2_sources.casos_parquet_v1 (
	id_number STRING,
	age STRING,
	sex STRING,
	city STRING,
	region STRING,
	class STRING,
	sample_date STRING,
	fe_reporte STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/covid19datos-v2/casos/parquet_v1/';

CREATE EXTERNAL TABLE covid19datos_v2_sources.casos_parquet_v2 (
	id_number BIGINT,
	age BIGINT,
	sex STRING,
	city STRING,
	region STRING,
	class STRING,
	sample_date STRING,
	fe_reporte STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/covid19datos-v2/casos/parquet_v2/';


---------------------------------------------------------------------------
---------------------------------------------------------------------------
--
-- Defunciones
--

CREATE EXTERNAL TABLE covid19datos_v2_sources.defunciones_parquet_v1 (
	id_muerte STRING,
	co_sexo STRING,
	co_region STRING,
	co_clasificacion STRING,
	fe_muerte STRING,
	fe_reporte STRING,
	fe_bioportal STRING,
	fe_registro STRING,
	tx_grupo_edad STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/covid19datos-v2/defunciones/parquet_v1/';

CREATE EXTERNAL TABLE covid19datos_v2_sources.defunciones_parquet_v2 (
    -- Yes, this one is identical in v2
	id_muerte STRING,
	co_sexo STRING,
	co_region STRING,
	co_clasificacion STRING,
	fe_muerte STRING,
	tx_grupo_edad STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/covid19datos-v2/defunciones/parquet_v2/';


---------------------------------------------------------------------------
---------------------------------------------------------------------------
--
-- Sistemas de salud
--

CREATE EXTERNAL TABLE covid19datos_v2_sources.sistemas_salud_parquet_v1 (
    fe_reporte STRING,
    camas_adultos_covid STRING,
    camas_adultos_nocovid STRING,
    camas_adultos_occ STRING,
    camas_adultos_disp STRING,
    camas_adultos_total STRING,
    camas_icu_covid STRING,
    camas_icu_nocovid STRING,
    camas_icu_occ STRING,
    camas_icu_disp STRING,
    camas_icu_total STRING,
    camas_ped_covid STRING,
    camas_ped_nocovid STRING,
    camas_ped_occ STRING,
    camas_ped_disp STRING,
    camas_ped_total STRING,
    camas_picu_covid STRING,
    camas_picu_nocovid STRING,
    camas_picu_occ STRING,
    camas_picu_disp STRING,
    camas_picu_total STRING,
    vent_adultos_covid STRING,
    vent_adultos_nocovid STRING,
    vent_adultos_occ STRING,
    vent_adultos_disp STRING,
    vent_adultos_total STRING,
    vent_ped_covid STRING,
    vent_ped_nocovid STRING,
    vent_ped_occ STRING,
    vent_ped_disp STRING,
    vent_ped_total STRING,
    cuartos_presneg_occ STRING,
    cuartos_presneg_disp STRING,
    cuartos_presneg_total STRING,
    vent_ord STRING,
    vent_rec STRING,
    vent_entr STRING,
    convalecientes STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/covid19datos-v2/sistemas_salud/parquet_v1/';

CREATE EXTERNAL TABLE covid19datos_v2_sources.sistemas_salud_parquet_v2 (
    fe_reporte STRING,
    camas_adultos_covid BIGINT,
    camas_adultos_nocovid BIGINT,
    camas_adultos_occ BIGINT,
    camas_adultos_disp BIGINT,
    camas_adultos_total BIGINT,
    camas_icu_covid BIGINT,
    camas_icu_nocovid BIGINT,
    camas_icu_occ BIGINT,
    camas_icu_disp BIGINT,
    camas_icu_total BIGINT,
    camas_ped_covid BIGINT,
    camas_ped_nocovid BIGINT,
    camas_ped_occ BIGINT,
    camas_ped_disp BIGINT,
    camas_ped_total BIGINT,
    camas_picu_covid BIGINT,
    camas_picu_nocovid BIGINT,
    camas_picu_occ BIGINT,
    camas_picu_disp BIGINT,
    camas_picu_total BIGINT,
    vent_adultos_covid BIGINT,
    vent_adultos_nocovid BIGINT,
    vent_adultos_occ BIGINT,
    vent_adultos_disp BIGINT,
    vent_adultos_total BIGINT,
    vent_ped_covid BIGINT,
    vent_ped_nocovid BIGINT,
    vent_ped_occ BIGINT,
    vent_ped_disp BIGINT,
    vent_ped_total BIGINT,
    cuartos_presneg_occ BIGINT,
    cuartos_presneg_disp BIGINT,
    cuartos_presneg_total BIGINT,
    vent_ord BIGINT,
    vent_rec BIGINT,
    vent_entr BIGINT,
    convalecientes BIGINT
    -- TODO: There's regional fields lately
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/covid19datos-v2/sistemas_salud/parquet_v2/';


---------------------------------------------------------------------------
---------------------------------------------------------------------------
--
-- Vacunación
--

CREATE EXTERNAL TABLE covid19datos_v2_sources.vacunacion_parquet_v1 (
	nu_edad STRING,
	co_municipio STRING,
	co_region STRING,
	fe_vacuna STRING,
	nu_dosis STRING,
	co_manufacturero STRING,
	fe_reporte STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/covid19datos-v2/vacunacion/parquet_v1/';

CREATE EXTERNAL TABLE covid19datos_v2_sources.vacunacion_parquet_v2 (
    co_sexo STRING,
	co_municipio STRING,
	co_region STRING,
	fe_vacuna STRING,
	nu_dosis BIGINT,
	co_manufacturero STRING,
	tx_grupo_edad STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/covid19datos-v2/vacunacion/parquet_v2/';


---------------------------------------------------------------------------
---------------------------------------------------------------------------
--
-- Pruebas
--

CREATE EXTERNAL TABLE covid19datos_v2_sources.pruebas_parquet_v1 (
	id_orden STRING,
	co_tipo STRING,
	tx_grupo_edad STRING,
	co_resultado STRING,
	co_sexo STRING,
	co_region STRING,
	fe_prueba STRING,
	fe_reporte STRING,
	fe_registro STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/covid19datos-v2/pruebas/parquet_v1/';

CREATE EXTERNAL TABLE covid19datos_v2_sources.pruebas_parquet_v2 (
	id_orden STRING,
	co_tipo STRING,
	tx_grupo_edad STRING,
	co_resultado STRING,
	co_sexo STRING,
	co_region STRING,
	fe_prueba STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/covid19datos-v2/pruebas/parquet_v2/';
