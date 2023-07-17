--
-- Municipal cases aggregated by `bulletin_date`
--
SELECT
    bulletin_date,
    region,
    fips,
    municipality,
    popest2019,
    pop2020,
    sum(delta_cases) AS delta_cases,
    sum(delta_confirmed) AS delta_confirmed,
    sum(delta_probable) AS delta_probable,
    sum(sum(delta_cases)) OVER (
        ORDER BY bulletin_date
    ) AS cumulative_cases,
    sum(sum(delta_confirmed)) OVER (
        ORDER BY bulletin_date
    ) AS cumulative_confirmed,
    sum(sum(delta_probable)) OVER (
        ORDER BY bulletin_date
    ) AS cumulative_probable
FROM {{ ref('cases_municipal_agg') }}
GROUP BY
    bulletin_date,
    region,
    fips,
    municipality,
    popest2019,
    pop2020
ORDER BY
    bulletin_date,
    municipality;