DROP DATABASE IF EXISTS covid19datos_v2_sources CASCADE;

CREATE DATABASE covid19datos_v2_sources
LOCATION 's3://covid-19-puerto-rico-data/covid19datos-v2/';


---------------------------------------------------------------------------
---------------------------------------------------------------------------
--
-- Casos
--

CREATE EXTERNAL TABLE covid19datos_v2_sources.casos_parquet (
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


---------------------------------------------------------------------------
---------------------------------------------------------------------------
--
-- Defunciones
--

CREATE EXTERNAL TABLE covid19datos_v2_sources.defunciones_parquet (
	id_muerte STRING,
	id_paciente STRING,
	fe_nacimiento STRING,
	co_sexo STRING,
	co_municipio STRING,
	co_region STRING,
	co_clasificacion STRING,
	fe_muerte STRING,
	fe_reporte STRING,
	fe_bioportal STRING,
	fe_registro STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/covid19datos-v2/defunciones/parquet_v1/';


---------------------------------------------------------------------------
---------------------------------------------------------------------------
--
-- Sistemas de salud
--

CREATE EXTERNAL TABLE covid19datos_v2_sources.sistemas_salud_parquet (
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


---------------------------------------------------------------------------
---------------------------------------------------------------------------
--
-- Vacunación
--

CREATE EXTERNAL TABLE covid19datos_v2_sources.vacunacion_parquet (
	id_paciente STRING,
	fe_nacimiento STRING,
	nu_edad STRING,
	co_sexo STRING,
	co_municipio STRING,
	co_region STRING,
	fe_vacuna STRING,
	nu_dosis STRING,
	co_manufacturero STRING,
	fe_registro_preis STRING,
	fe_reporte STRING,
	fe_registro STRING,
	in_ignore STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/covid19datos-v2/vacunacion/parquet_v1/';


---------------------------------------------------------------------------
---------------------------------------------------------------------------
--
-- Pruebas
--

CREATE EXTERNAL TABLE covid19datos_v2_sources.pruebas_parquet (
	id_orden STRING,
	id_paciente STRING,
	co_tipo STRING,
	co_resultado STRING,
	fe_nacimiento STRING,
	co_sexo STRING,
	co_municipio STRING,
	co_region STRING,
	fe_prueba STRING,
	fe_reporte STRING,
	fe_registro STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/covid19datos-v2/pruebas/parquet_v1/';
