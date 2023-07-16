WITH downloads AS (
	SELECT
		date,
		max(runid) runid,
		max(downloaded_at) downloaded_at
	FROM {{ ref('vaccination_county_condensed_data_downloads') }}
	GROUP BY date
)
SELECT *
FROM {{ ref('vaccination_county_condensed_data_downloads') }} vax
INNER JOIN downloads
	USING (date, runid, downloaded_at)
WHERE StateAbbr LIKE 'PR%'