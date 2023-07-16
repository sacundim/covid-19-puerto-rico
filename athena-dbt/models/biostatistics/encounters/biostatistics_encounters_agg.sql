SELECT
    bulletin_date,
	collected_date,
	date_diff('day', collected_date, bulletin_date) age,
	sum(encounters) encounters,
	sum(cases) cases,
	sum(cases_strict) cases_strict,
	sum(first_infections) first_infections,
	sum(possible_reinfections) possible_reinfections,
	sum(rejections) rejections,
	sum(antigens) antigens,
	sum(molecular) molecular,
	sum(positive_antigens) positive_antigens,
	sum(positive_molecular) positive_molecular,
	sum(antigens_cases) antigens_cases,
	sum(molecular_cases) molecular_cases,
	sum(initial_molecular) initial_molecular,
	sum(initial_positive_molecular) initial_positive_molecular,

	sum(sum(encounters)) OVER cumulative
	    AS cumulative_encounters,
	sum(sum(cases)) OVER cumulative
	    AS cumulative_cases,
	sum(sum(cases_strict)) OVER cumulative
	    AS cumulative_cases_strict,
	sum(sum(first_infections)) OVER cumulative
	    AS cumulative_first_infections,
	sum(sum(possible_reinfections)) OVER cumulative
	    AS cumulative_possible_reinfections,
	sum(sum(rejections)) OVER cumulative
	    AS cumulative_rejections,
	sum(sum(antigens)) OVER cumulative
	    AS cumulative_antigens,
	sum(sum(molecular)) OVER cumulative
	    AS cumulative_molecular,
	sum(sum(positive_antigens)) OVER cumulative
	    AS cumulative_positive_antigens,
	sum(sum(positive_molecular)) OVER cumulative
	    AS cumulative_positive_molecular,
	sum(sum(antigens_cases)) OVER cumulative
	    AS cumulative_antigens_cases,
	sum(sum(molecular_cases)) OVER cumulative
	    AS cumulative_molecular_cases,
	sum(sum(initial_molecular)) OVER cumulative
	    AS cumulative_initial_molecular,
	sum(sum(initial_positive_molecular)) OVER cumulative
	    AS cumulative_initial_positive_molecular,

	sum(encounters)
	    - lag(sum(encounters), 1, 0) OVER delta
	    AS delta_encounters,
	sum(cases)
	    - lag(sum(cases), 1, 0) OVER delta
	    AS delta_cases,
	sum(cases_strict)
	    - lag(sum(cases_strict), 1, 0) OVER delta
	    AS delta_cases_strict,
	sum(first_infections)
	    - lag(sum(first_infections), 1, 0) OVER delta
	    AS delta_first_infections,
	sum(possible_reinfections)
	    - lag(sum(possible_reinfections), 1, 0) OVER delta
	        AS delta_possible_reinfections,
	sum(rejections)
	    - lag(sum(rejections), 1, 0) OVER delta
        AS delta_rejections,
	sum(antigens)
	    - lag(sum(antigens), 1, 0) OVER delta
	    AS delta_antigens,
	sum(molecular)
	    - lag(sum(molecular), 1, 0) OVER delta
	    AS delta_molecular,
	sum(positive_antigens)
	    - lag(sum(positive_antigens), 1, 0) OVER delta
	    AS delta_positive_antigens,
	sum(positive_molecular)
	    - lag(sum(positive_molecular), 1, 0) OVER delta
	    AS delta_positive_molecular,
	sum(antigens_cases)
	    - lag(sum(antigens_cases), 1, 0) OVER delta
	    AS delta_antigens_cases,
	sum(molecular_cases)
	    - lag(sum(molecular_cases), 1, 0) OVER delta
	    AS delta_molecular_cases,
	sum(initial_molecular)
	    - lag(sum(initial_molecular), 1, 0) OVER delta
	    AS delta_initial_molecular,
	sum(initial_positive_molecular)
	    - lag(sum(initial_positive_molecular), 1, 0) OVER delta
	    AS delta_initial_positive_molecular
FROM {{ ref('biostatistics_encounters_cube') }}
GROUP BY
	bulletin_date,
	collected_date
WINDOW cumulative AS (
    PARTITION BY bulletin_date
    ORDER BY collected_date
), delta AS (
    PARTITION BY collected_date
    ORDER BY bulletin_date
)
ORDER BY
	bulletin_date,
	collected_date;