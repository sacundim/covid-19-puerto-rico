--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--
-- Daily hospitalization data
--

WITH downloads AS (
    SELECT
        date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
            AS bulletin_date,
        max(downloaded_at) downloaded_at
    FROM {{ ref('sistemas_salud') }}
    GROUP BY date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
)
SELECT *
FROM {{ ref('sistemas_salud') }}
INNER JOIN downloads
    USING (downloaded_at)
ORDER BY bulletin_date, fe_reporte;