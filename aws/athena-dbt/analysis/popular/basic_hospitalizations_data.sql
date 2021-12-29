--
-- Datos básicos de hospitalizaciones
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM {{ ref('hospitalizations') }}
)
SELECT
	bulletin_date AS "Datos",
	date AS "Fecha",
	hospitalized_currently AS "Camas ocupadas por COVID",
	previous_day_admission_adult_covid
		+ previous_day_admission_pediatric_covid
		AS "Ingresos por COVID",
	in_icu_currently
		AS "Camas UCI ocupadas por COVID"
FROM {{ ref('hospitalizations') }} hosp
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = hosp.bulletin_date
ORDER BY date DESC;