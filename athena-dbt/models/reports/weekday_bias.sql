--
-- For the WeekdayBias chart
--

SELECT
	ba.bulletin_date,
	ba.datum_date,
	ba.delta_confirmed_cases,
	ba.delta_probable_cases,
	ba.delta_deaths
FROM {{ ref('bulletin_cases') }} ba
WHERE ba.datum_date >= ba.bulletin_date - INTERVAL '14' DAY
AND ba.bulletin_date > (
	SELECT min(bulletin_date)
	FROM {{ ref('bulletin_cases') }}
	WHERE delta_confirmed_cases IS NOT NULL
	AND delta_probable_cases IS NOT NULL
	AND delta_deaths IS NOT NULL)
ORDER BY bulletin_date, datum_date;
