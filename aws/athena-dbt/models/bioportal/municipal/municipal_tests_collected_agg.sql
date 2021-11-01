SELECT
    downloaded_at,
    bulletin_date,
    collected_date,
    municipality,
    test_type,
    sum(specimens) AS specimens,
    sum(positives) AS positives,
    sum(sum(specimens)) OVER (
        PARTITION BY downloaded_at, bulletin_date, municipality, test_type
        ORDER BY collected_date
    ) AS cumulative_specimens,
    sum(sum(positives)) OVER (
        PARTITION BY downloaded_at, bulletin_date, municipality, test_type
        ORDER BY collected_date
    ) AS cumulative_positives,
    sum(specimens) - coalesce(lag(sum(specimens), 1, 0) OVER (
        PARTITION BY downloaded_at, bulletin_date, municipality, test_type
        ORDER BY collected_date
    ), 0) AS delta_specimens,
    sum(positives) - coalesce(lag(sum(positives), 1, 0) OVER (
            PARTITION BY downloaded_at, bulletin_date, municipality, test_type
            ORDER BY collected_date
        ), 0) AS delta_positives
FROM {{ ref('municipal_tests_cube') }}
GROUP BY
    downloaded_at,
    bulletin_date,
    collected_date,
    municipality,
    test_type;
