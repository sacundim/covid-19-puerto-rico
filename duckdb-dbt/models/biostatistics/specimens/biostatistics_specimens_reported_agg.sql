SELECT
	bulletin_date,
	reported_date,
	test_type,
	sum(specimens) AS specimens,
	sum(sum(specimens)) OVER cumulative
	    AS cumulative_specimens,
	sum(specimens)
	    - lag(sum(specimens), 1, 0) OVER delta
	    AS delta_specimens,
	sum(positive_specimens) AS positive_specimens,
	sum(sum(positive_specimens)) OVER cumulative
	    AS cumulative_positives,
	sum(positive_specimens)
	    - lag(sum(positive_specimens), 1, 0) OVER delta
	    AS delta_positive_specimens
FROM {{ ref('biostatistics_specimens_cube') }}
GROUP BY 
    bulletin_date,
    reported_date,
    test_type
WINDOW cumulative AS (
    PARTITION BY test_type, bulletin_date
    ORDER BY reported_date
), delta AS (
    PARTITION BY test_type, reported_date
    ORDER BY bulletin_date
)
ORDER BY
    bulletin_date,
    reported_date,
    test_type