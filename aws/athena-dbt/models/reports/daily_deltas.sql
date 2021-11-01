-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
--
-- The daily deltas report says how much the case counts for the same
-- datum_date changed from one bulletin_date to the next
--

SELECT
    bulletin_date,
	datum_date,
	delta_confirmed_cases,
	delta_probable_cases,
	delta_deaths
FROM {{ ref('bulletin_cases') }}
-- We exclude the earliest bulletin date because it's artificially big
WHERE bulletin_date > (
	SELECT min(bulletin_date)
	FROM {{ ref('bulletin_cases') }}
);
