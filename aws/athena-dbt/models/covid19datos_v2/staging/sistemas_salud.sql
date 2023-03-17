{{
    config(pre_hook=[
        "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'sistemas_salud_v1').render_hive() }}",
        "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'sistemas_salud_v2').render_hive() }}"
    ])
}}

SELECT
	from_iso8601_date(downloaded_date)
		AS downloaded_date,
    {{ parse_filename_timestamp('"$path"') }}
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
FROM {{ source('covid19datos_v2', 'sistemas_salud_v1') }}

UNION ALL

SELECT
	from_iso8601_date(downloaded_date)
		AS downloaded_date,
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
 	date(date_parse(NULLIF(fe_reporte, ''), '%Y-%m-%d %H:%i:%s'))
		AS fe_reporte,
    camas_adultos_covid,
    camas_adultos_nocovid,
    camas_adultos_occ,
    camas_adultos_disp,
    camas_adultos_total,
    camas_icu_covid,
    camas_icu_nocovid,
    camas_icu_occ,
    camas_icu_disp,
    camas_icu_total,
    camas_ped_covid,
    camas_ped_nocovid,
    camas_ped_occ,
    camas_ped_disp,
    camas_ped_total,
    camas_picu_covid,
    camas_picu_nocovid,
    camas_picu_occ,
    camas_picu_disp,
    camas_picu_total,
    vent_adultos_covid,
    vent_adultos_nocovid,
    vent_adultos_occ,
    vent_adultos_disp,
    vent_adultos_total,
    vent_ped_covid,
    vent_ped_nocovid,
    vent_ped_occ,
    vent_ped_disp,
    vent_ped_total,
    cuartos_presneg_occ,
    cuartos_presneg_disp,
    cuartos_presneg_total,
    vent_ord,
    vent_rec,
    vent_entr,
    convalecientes
FROM {{ source('covid19datos_v2', 'sistemas_salud_v2') }}

ORDER BY downloaded_at, fe_reporte;
