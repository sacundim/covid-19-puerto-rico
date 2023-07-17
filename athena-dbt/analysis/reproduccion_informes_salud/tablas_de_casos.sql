--
-- Reproducir las tablas de casos confirmados y probables por fecha de muestra
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS bulletin_date
	FROM {{ ref('bulletin_cases') }}
)
SELECT
	bulletin_date "Fecha de actualización de datos",
	datum_date "Fecha de muestra",
	confirmed_cases "Casos confirmados",
	cumulative_confirmed_cases "Acumulados",
	probable_cases "Casos probables",
	cumulative_probable_cases "Casos probables"
FROM {{ ref('bulletin_cases') }}
INNER JOIN bulletins USING (bulletin_date)
ORDER BY datum_date DESC;
