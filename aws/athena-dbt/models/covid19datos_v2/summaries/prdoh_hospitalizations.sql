--
-- Hospital bed availabilty and ICU occupancy, using PRDoH data
--

SELECT
	bulletin_date,
	fe_reporte date,
	'Adultos' age,
	'Camas' resource,
	camas_adultos_total total,
	camas_adultos_covid covid,
	camas_adultos_nocovid nocovid,
	camas_adultos_disp disp
FROM {{ ref('hospitales_daily') }}
WHERE fe_reporte >= date_add('day', -42, bulletin_date)
UNION ALL
SELECT
	bulletin_date,
	fe_reporte date,
	'Adultos' age,
	'UCI' resource,
	camas_icu_total total,
	camas_icu_covid covid,
	camas_icu_nocovid nocovid,
	camas_icu_disp disp
FROM {{ ref('hospitales_daily') }}
WHERE fe_reporte >= date_add('day', -42, bulletin_date)
UNION ALL
SELECT
	bulletin_date,
	fe_reporte date,
	'Pediátricos' age,
	'Camas' resource,
	camas_ped_total total,
	camas_ped_covid covid,
	camas_ped_nocovid nocovid,
	camas_ped_disp disp
FROM {{ ref('hospitales_daily') }}
WHERE fe_reporte >= date_add('day', -42, bulletin_date)
UNION ALL
SELECT
	bulletin_date,
	fe_reporte date,
	'Pediátricos' age,
	'UCI' resource,
	camas_picu_total total,
	camas_picu_covid covid,
	camas_picu_nocovid nocovid,
	camas_picu_disp disp
FROM {{ ref('hospitales_daily') }}
WHERE fe_reporte >= date_add('day', -42, bulletin_date)
ORDER BY date DESC, age, resource;
