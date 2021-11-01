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
   	count(*) deaths,
   	sum(count(*)) OVER (
   		PARTITION BY downloaded_at, bulletin_date
   		ORDER BY death_date
   	) AS cumulative_deaths
FROM {{ ref('bioportal_deaths') }}
GROUP BY downloaded_at, bulletin_date, death_date
ORDER BY downloaded_at, bulletin_date, death_date;
