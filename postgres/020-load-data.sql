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

COPY hospitalizations
FROM '/data/PuertoRico-hospitalizations.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY prpht_molecular_raw
FROM '/data/PRPHT-molecular.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

---------------------------------------------------------
CREATE TEMPORARY TABLE bioportal_raw (
    id BIGINT NOT NULL,
    collectedDate DATE,
    reportedDate DATE,
    ageRange TEXT,
    testType TEXT,
    result TEXT,
    patientCity TEXT,
    createdAt TIMESTAMP WITHOUT TIME ZONE
);

COPY bioportal_raw
FROM '/data/raw/bioportal.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

INSERT INTO bioportal_tests
SELECT
    id,
    collectedDate AS datum_date,
    reportedDate AS bulletin_date,
    CASE patientCity
        WHEN 'Rio Grande' THEN 'Río Grande'
        ELSE patientCity
    END AS municipality,
    COALESCE(result, '') LIKE '%Positive%'
        AS positive
FROM bioportal_raw
WHERE collectedDate >= '2020-01-01'
AND reportedDate >= '2020-03-13'
AND testType = 'Molecular';

CREATE INDEX ON bioportal_tests (datum_date, bulletin_date, municipality, positive);
CREATE INDEX ON bioportal_tests (datum_date, municipality, positive);
CREATE INDEX ON bioportal_tests (bulletin_date, municipality, positive);