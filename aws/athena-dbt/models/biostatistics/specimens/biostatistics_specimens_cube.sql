WITH bulletins AS (
    SELECT max(downloaded_at) downloaded_at
    FROM {{ ref('biostatistics_tests') }}
    WHERE downloaded_date >= CURRENT_DATE - INTERVAL '17' DAY
    GROUP BY date(downloaded_at AT TIME ZONE 'America/Puerto_Rico')
)
SELECT
    downloaded_at,
    downloaded_date,
    bulletin_date,
    collected_date,
    reported_date,
    received_date,
    test_type,
    municipality,
    age_range,
    count(*) specimens,
    count(*) FILTER (WHERE positive)
        AS positive_specimens
FROM {{ ref('biostatistics_tests') }} specimens
INNER JOIN bulletins
    USING (downloaded_at)
WHERE downloaded_date >= CURRENT_DATE - INTERVAL '17' DAY
AND test_type IN ('Molecular', 'Antígeno')
AND DATE '2020-03-01' <= collected_date
AND collected_date <= received_date
AND DATE '2020-03-01' <= reported_date
AND reported_date <= received_date
AND received_date <= bulletin_date
GROUP BY
    downloaded_at,
    downloaded_date,
    bulletin_date,
    collected_date,
    reported_date,
    received_date,
    test_type,
    municipality,
    age_range
ORDER BY
    downloaded_at,
    downloaded_date,
    bulletin_date,
    collected_date,
    reported_date,
    received_date,
    test_type,
    municipality,
    age_range
;