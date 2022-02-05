--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--
-- Recreate the old bulletin_cases table that I used to make from the daily
-- PDFs.  In fact, we're still pulling here from my old CSV.
--

WITH casos AS (
	WITH downloads AS (
		SELECT
			date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
				AS bulletin_date,
			max(downloaded_at) downloaded_at
		FROM {{ ref('casos') }}
		GROUP BY date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
	)
	SELECT
		bulletin_date,
		sample_date AS datum_date,
		count(*) FILTER (WHERE class = 'CONFIRMADO')
			AS confirmed_cases,
		count(*) FILTER (WHERE class = 'PROBABLE')
			AS probable_cases
	FROM {{ ref('casos') }} casos
	INNER JOIN downloads
		USING (downloaded_at)
	GROUP BY bulletin_date, sample_date
), defunciones AS (
	WITH downloads AS (
		SELECT
			date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
				AS bulletin_date,
			max(downloaded_at) downloaded_at
		FROM {{ ref('casos') }}
		GROUP BY date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
	)
	SELECT
		bulletin_date,
		fe_muerte AS datum_date,
		count(*) deaths
	FROM {{ ref('defunciones') }} defunciones
	INNER JOIN downloads
		USING (downloaded_at)
	GROUP BY bulletin_date, fe_muerte
), joined AS (
	SELECT
		bulletin_date,
		datum_date,
		confirmed_cases,
		probable_cases,
		COALESCE(deaths, 0) deaths
	FROM casos
	FULL OUTER JOIN defunciones
		USING (bulletin_date, datum_date)
	UNION
	-- Merge with the old hand-curated data set:
    SELECT
        from_iso8601_date(bulletin_date) AS bulletin_date,
        from_iso8601_date(datum_date) AS datum_date,
        CAST(nullif(confirmed_cases, '') AS INTEGER) AS confirmed_cases,
        CAST(nullif(probable_cases, '') AS INTEGER) AS probable_cases,
        CAST(nullif(deaths, '') AS INTEGER) AS deaths
    FROM {{ source('bulletin', 'bulletin_cases_csv')}}
)
SELECT
	bulletin_date,
	datum_date,
	date_diff('day', datum_date, bulletin_date)
		AS age,
	confirmed_cases,
    sum(confirmed_cases) OVER (
        PARTITION BY bulletin_date
        ORDER BY datum_date
    ) AS cumulative_confirmed_cases,
    COALESCE(confirmed_cases, 0)
        - COALESCE(lag(confirmed_cases) OVER (
            PARTITION BY datum_date
            ORDER BY bulletin_date
        ), 0) AS delta_confirmed_cases,
    probable_cases,
    sum(probable_cases) OVER (
        PARTITION BY bulletin_date
        ORDER BY datum_date
    ) AS cumulative_probable_cases,
    COALESCE(probable_cases, 0)
        - COALESCE(lag(probable_cases) OVER (
            PARTITION BY datum_date
            ORDER BY bulletin_date
        ), 0) AS delta_probable_cases,
	deaths,
    sum(deaths) OVER (
        PARTITION BY bulletin_date
        ORDER BY datum_date
    ) AS cumulative_deaths,
    COALESCE(deaths, 0)
        - COALESCE(lag(deaths) OVER (
            PARTITION BY datum_date
            ORDER BY bulletin_date
        ), 0) AS delta_deaths
FROM joined
ORDER BY bulletin_date, datum_date;