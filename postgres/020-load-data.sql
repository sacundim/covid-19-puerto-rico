COPY announcement
FROM '/data/PuertoRico-bulletin.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY bitemporal
FROM '/data/PuertoRico-bitemporal.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY canonical_municipal_names
FROM '/data/Municipalities-canonical_names.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY municipal
FROM '/data/Municipalities-molecular.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY bioportal
FROM '/data/PuertoRico-bioportal.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY bioportal_bitemporal
FROM '/data/PuertoRico-bioportal-bitemporal.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY hospitalizations
FROM '/data/PuertoRico-hospitalizations.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY prpht_molecular_raw
FROM '/data/PRPHT-molecular.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

