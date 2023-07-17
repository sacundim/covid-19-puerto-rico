--
-- View for a map and/or scatterplot chart of antigen vs. molecular volume
--

SELECT
	bulletin_date,
	municipality,
	abbreviation,
	population,
	sum(specimens) / 21.0 daily_specimens,
	1e3 * sum(specimens) / population / 21.0
		AS daily_specimens_1k,
	sum(antigens) / 21.0 daily_antigens,
	1e3 * sum(antigens) / population / 21.0
		AS daily_antigens_1k,
	1.0 * sum(positive_antigens) / sum(antigens)
	    AS antigens_positivity,
	sum(molecular) / 21.0 daily_molecular,
	1e3 * sum(molecular) / population / 21.0
		AS daily_molecular_1k,
	1.0 * sum(positive_molecular) / sum(molecular)
	    AS molecular_positivity
FROM {{ ref('biostatistics_specimens_municipal_agg') }}
INNER JOIN {{ ref('municipal_abbreviations') }}
	USING (municipality)
WHERE date_add('day', -21, bulletin_date) < collected_date
AND collected_date <= bulletin_date
GROUP BY bulletin_date, municipality, abbreviation, population
ORDER BY bulletin_date, municipality;
