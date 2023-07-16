--
-- The "hub" cube table for the model.  This has only one
-- download per bulletin_date (the most recent download),
-- which means that e.g. windowed LAGs tell you changes
-- between days, not file downloads.
--
WITH bulletins AS (
	SELECT max(downloaded_at) downloaded_at
	FROM {{ ref('biostatistics_cases_downloads_cube') }}
	GROUP BY bulletin_date
)
SELECT *
FROM {{ ref('biostatistics_cases_downloads_cube') }}
INNER JOIN bulletins
	USING (downloaded_at)