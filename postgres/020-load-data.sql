COPY announcement
FROM '/data/PuertoRico-bulletin.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY bitemporal
FROM '/data/PuertoRico-bitemporal.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY municipal
FROM '/data/Municipalities-molecular.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';
