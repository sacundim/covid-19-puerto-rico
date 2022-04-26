--
-- Illustrate how the hospital admission rate in the HHS dataset
-- changes with later downloads of the same file.
--
WITH puerto_rico AS (
	SELECT 3193694 AS popest2019
), bitemporal AS (
	SELECT
		file_timestamp,
		bulletin_date,
		date,
		1e5 * sum(admission_covid_confirmed) OVER (
			PARTITION BY bulletin_date
			ORDER BY date
			ROWS 6 PRECEDING
		) / puerto_rico.popest2019 AS admission_rate
	FROM {{ ref('hhs_hospitals_bitemporal') }}
	CROSS JOIN puerto_rico
)
SELECT
	perspectival.file_timestamp "HHS file version timestamp",
	date_add('day', 1, date) AS "Record date in file",
	perspectival.admission_rate "Admission rate with data from that file",
	newest.admission_rate "Admission rate with freshest file",
	newest.admission_rate - perspectival.admission_rate AS "Difference"
FROM bitemporal perspectival
INNER JOIN bitemporal newest
	USING (date)
WHERE perspectival.bulletin_date = date
AND newest.file_timestamp = (
	SELECT max(file_timestamp)
	FROM {{ ref('hhs_hospitals_bitemporal') }}
)
ORDER BY "date" DESC;
