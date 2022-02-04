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
	sum(sum(encounters)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_encounters,
	sum(sum(cases)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_cases,
	sum(sum(cases_strict)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_cases_strict,
	sum(sum(first_infections)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_first_infections,
	sum(sum(possible_reinfections)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_possible_reinfections,
	sum(sum(rejections)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_rejections,
	sum(sum(antigens)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_antigens,
	sum(sum(molecular)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_molecular,
	sum(sum(positive_antigens)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_positive_antigens,
	sum(sum(positive_molecular)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_positive_molecular,
	sum(sum(antigens_cases)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_antigens_cases,
	sum(sum(molecular_cases)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_molecular_cases,
	sum(sum(initial_molecular)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_initial_molecular,
	sum(sum(initial_positive_molecular)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_initial_positive_molecular,
	sum(encounters) - lag(sum(encounters), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_encounters,
	sum(cases) - lag(sum(cases), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_cases,
	sum(rejections) - lag(sum(rejections), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_rejections,
	sum(antigens) - lag(sum(antigens), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_antigens,
	sum(molecular) - lag(sum(molecular), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_molecular,
	sum(positive_antigens) - lag(sum(positive_antigens), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_positive_antigens,
	sum(positive_molecular) - lag(sum(positive_molecular), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_positive_molecular,
	sum(antigens_cases) - lag(sum(antigens_cases), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_antigens_cases,
	sum(molecular_cases) - lag(sum(molecular_cases), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_molecular_cases,
	sum(initial_molecular) - lag(sum(initial_molecular), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_initial_molecular,
	sum(initial_positive_molecular) - lag(sum(initial_positive_molecular), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_initial_positive_molecular
FROM {{ ref('bioportal_encounters_cube') }}
GROUP BY
	bulletin_date,
	collected_date;
