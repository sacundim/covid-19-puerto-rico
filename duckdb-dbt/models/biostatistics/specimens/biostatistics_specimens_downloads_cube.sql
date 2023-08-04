--
-- The "downloads cube" has aggregates for all the file downloads,
-- which may be more than one for each bulletin_date.  This is not
-- what we usually want but the upside is we can update it incrementally
--
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
    age_gte,
    age_lt,
    count(*) specimens,
    count(*) FILTER (WHERE positive)
        AS positive_specimens
FROM {{ ref('biostatistics_tests') }} specimens
INNER JOIN {{ ref('bioportal_age_ranges') }}
    USING (age_range)
WHERE test_type IN ('Molecular', 'Antígeno')
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
    age_range,
    age_gte,
    age_lt