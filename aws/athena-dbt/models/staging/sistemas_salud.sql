SELECT
	from_iso8601_date(downloaded_date)
		AS downloaded_date,
	CAST(from_iso8601_timestamp(
		regexp_extract("$path", '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z'))
		AS TIMESTAMP)
		AS downloaded_at,
 	date(date_parse(NULLIF(fe_reporte, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_reporte,
    CAST(nullif(camas_adultos_covid, '') AS INT) camas_adultos_covid,
    CAST(nullif(camas_adultos_nocovid, '') AS INT) camas_adultos_nocovid,
    CAST(nullif(camas_adultos_occ, '') AS INT) camas_adultos_occ,
    CAST(nullif(camas_adultos_disp, '') AS INT) camas_adultos_disp,
    CAST(nullif(camas_adultos_total, '') AS INT) camas_adultos_total,
    CAST(nullif(camas_icu_covid, '') AS INT) camas_icu_covid,
    CAST(nullif(camas_icu_nocovid, '') AS INT) camas_icu_nocovid,
    CAST(nullif(camas_icu_occ, '') AS INT) camas_icu_occ,
    CAST(nullif(camas_icu_disp, '') AS INT) camas_icu_disp,
    CAST(nullif(camas_icu_total, '') AS INT) camas_icu_total,
    CAST(nullif(camas_ped_covid, '') AS INT) camas_ped_covid,
    CAST(nullif(camas_ped_nocovid, '') AS INT) camas_ped_nocovid,
    CAST(nullif(camas_ped_occ, '') AS INT) camas_ped_occ,
    CAST(nullif(camas_ped_disp, '') AS INT) camas_ped_disp,
    CAST(nullif(camas_ped_total, '') AS INT) camas_ped_total,
    CAST(nullif(camas_picu_covid, '') AS INT) camas_picu_covid,
    CAST(nullif(camas_picu_nocovid, '') AS INT) camas_picu_nocovid,
    CAST(nullif(camas_picu_occ, '') AS INT) camas_picu_occ,
    CAST(nullif(camas_picu_disp, '') AS INT) camas_picu_disp,
    CAST(nullif(camas_picu_total, '') AS INT) camas_picu_total,
    CAST(nullif(vent_adultos_covid, '') AS INT) vent_adultos_covid,
    CAST(nullif(vent_adultos_nocovid, '') AS INT) vent_adultos_nocovid,
    CAST(nullif(vent_adultos_occ, '') AS INT) vent_adultos_occ,
    CAST(nullif(vent_adultos_disp, '') AS INT) vent_adultos_disp,
    CAST(nullif(vent_adultos_total, '') AS INT) vent_adultos_total,
    CAST(nullif(vent_ped_covid, '') AS INT) vent_ped_covid,
    CAST(nullif(vent_ped_nocovid, '') AS INT) vent_ped_nocovid,
    CAST(nullif(vent_ped_occ, '') AS INT) vent_ped_occ,
    CAST(nullif(vent_ped_disp, '') AS INT) vent_ped_disp,
    CAST(nullif(vent_ped_total, '') AS INT) vent_ped_total,
    CAST(nullif(cuartos_presneg_occ, '') AS INT) cuartos_presneg_occ,
    CAST(nullif(cuartos_presneg_disp, '') AS INT) cuartos_presneg_disp,
    CAST(nullif(cuartos_presneg_total, '') AS INT) cuartos_presneg_total,
    CAST(nullif(vent_ord, '') AS INT) vent_ord,
    CAST(nullif(vent_rec, '') AS INT) vent_rec,
    CAST(nullif(vent_entr, '') AS INT) vent_entr,
    CAST(nullif(convalecientes, '') AS INT) convalecientes
FROM covid19datos_v2_sources.sistemas_salud_parquet
ORDER BY downloaded_at, fe_reporte;
