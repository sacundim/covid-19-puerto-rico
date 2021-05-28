COPY announcement
FROM '/data/cases/PuertoRico-bulletin.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY bitemporal
FROM '/data/cases/PuertoRico-bitemporal.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';
ANALYZE VERBOSE bitemporal;
REFRESH MATERIALIZED VIEW bitemporal_agg;
ANALYZE VERBOSE bitemporal_agg;


COPY age_groups_population
FROM '/data/cases/AgeGroups-population.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY age_groups_molecular
FROM '/data/cases/AgeGroups-molecular.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY age_groups_antigens
FROM '/data/cases/AgeGroups-antigens.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

REFRESH MATERIALIZED VIEW age_groups;
ANALYZE VERBOSE age_groups;


COPY canonical_municipal_names
FROM '/data/cases/Municipalities-canonical_names.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY municipal_hex_grid
FROM '/data/Census/municipal_hex_grid/municipal_hex_grid.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY municipal_molecular
FROM '/data/cases/Municipalities-molecular.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY municipal_antigens
FROM '/data/cases/Municipalities-antigens.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

REFRESH MATERIALIZED VIEW municipal;
ANALYZE VERBOSE municipal;
