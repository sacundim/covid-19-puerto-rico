SELECT
    downloaded_at,
    bulletin_date,
    collected_date,
    sum(cases) cases,
    sum(probable_cases) probable_cases,
    sum(confirmed_cases) confirmed_cases,
    sum(initial_cases) initial_cases,
    sum(reinfections) reinfections
FROM {{ ref('biostatistics_cases_cube') }} cases
GROUP BY
    downloaded_at,
    bulletin_date,
    collected_date
ORDER BY
    downloaded_at,
    bulletin_date,
    collected_date
