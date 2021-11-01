--
-- RecentAgeGroups chart
--

SELECT
	bulletin_date,
	collected_date,
	age_gte AS youngest,
	age_lt - 1 AS oldest,
	population,
	encounters,
	antigens,
	molecular,
	cases,
	antigens_cases,
	molecular_cases,
	deaths,
	positive_antigens,
	positive_molecular
FROM {{ ref('bioportal_acs_age_curve') }}
WHERE collected_date >= date_add('day', -175, bulletin_date)
ORDER BY
	bulletin_date DESC,
	collected_date DESC,
	youngest;
