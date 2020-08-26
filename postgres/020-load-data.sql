COPY announcement
FROM '/data/cases/PuertoRico-bulletin.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY bitemporal
FROM '/data/cases/PuertoRico-bitemporal.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';
ANALYZE VERBOSE bitemporal;

COPY canonical_municipal_names
FROM '/data/cases/Municipalities-canonical_names.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY municipal
FROM '/data/cases/Municipalities-molecular.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY age_groups_molecular
FROM '/data/cases/AgeGroups-molecular.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY age_groups_population
FROM '/data/cases/AgeGroups-population.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY bioportal
FROM '/data/cases/PuertoRico-bioportal.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY hospitalizations
FROM '/data/cases/PuertoRico-hospitalizations.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY prpht_molecular_raw
FROM '/data/cases/PRPHT-molecular.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';
