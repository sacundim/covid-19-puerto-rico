----------------------------------------------------------------------------------
----------------------------------------------------------------------------------
--
-- Deaths according to Bioportal, which I don't think is as reliable
-- as the daily report.
--

SELECT
	downloaded_at,
	bulletin_date,
	death_date,
   	age_range,
   	count(*) deaths,
   	sum(count(*)) OVER (
   		PARTITION BY downloaded_at, bulletin_date, age_range
   		ORDER BY death_date
   	) AS cumulative_deaths
FROM {{ ref('bioportal_deaths') }}
GROUP BY downloaded_at, bulletin_date, death_date, age_range
ORDER BY downloaded_at, bulletin_date, death_date, age_range;