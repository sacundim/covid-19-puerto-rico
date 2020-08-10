CREATE FUNCTION log2(x NUMERIC)
RETURNS NUMERIC AS $$
    SELECT log(2.0, x);
$$ LANGUAGE SQL;

CREATE FUNCTION safe_log2(x NUMERIC)
RETURNS NUMERIC AS $$
    SELECT CASE WHEN x > 0.0 THEN log(2.0, x) END;
$$ LANGUAGE SQL;


CREATE TABLE bitemporal (
    bulletin_date DATE NOT NULL,
    datum_date DATE NOT NULL,
    confirmed_and_probable_cases INTEGER,
    confirmed_cases INTEGER,
    probable_cases INTEGER,
    deaths INTEGER,
    PRIMARY KEY (bulletin_date, datum_date)
);

COMMENT ON TABLE bitemporal IS
'Data from graphs in Department of Health bulletins since April 25,
organized in a bitemporal schema that records datums by both the date
as of which each bulletin reports data for and the dates that each
bulletin attributes values to.  Read this as: "As of [bulletin_date],
the Department of Health had attributed [metric] incidences to
[datum_date]."  Data points are counts for the datum day (not
cumulative sums).';

COMMENT ON COLUMN bitemporal.bulletin_date IS
'Date that the bulletin has data up to.  Note that bulletins are
published on the next day, and many collections use that publication
date instead of this date.';

COMMENT ON COLUMN bitemporal.datum_date IS
'Date that the data items are attributed to.  For cases this
is the data that the test sample was taken.  For deaths this is
the date of the actual death.';


CREATE TABLE announcement (
    bulletin_date DATE NOT NULL,
    cumulative_positive_results INTEGER,
    cumulative_negative_results INTEGER,
    cumulative_pending_results INTEGER,
    new_cases INTEGER,
    new_confirmed_cases INTEGER,
    new_probable_cases INTEGER,
    adjusted_confirmed_cases INTEGER,
    adjusted_probable_cases INTEGER,
    cumulative_cases INTEGER,
    cumulative_confirmed_cases INTEGER,
    cumulative_probable_cases INTEGER,
    cumulative_deaths INTEGER,
    cumulative_certified_deaths INTEGER,
    cumulative_confirmed_deaths INTEGER,
    PRIMARY KEY (bulletin_date)
);

COMMENT ON TABLE announcement IS
'Daily "headline" values announced in the bulletins, the ones that
normally make the news.  These are generally attributed to the date
that the Department of Health recorded them and not the date each
death happened, test administered, etc.';

COMMENT ON COLUMN announcement.cumulative_positive_results IS
'Positive test results.  No deduplication done by person.
Publication stopped on April 25.';

COMMENT ON COLUMN announcement.cumulative_negative_results IS
'Negative test results.  No deduplication done by person.
Publication stopped on April 22.';

COMMENT ON COLUMN announcement.cumulative_pending_results IS
'Pending test results.  No deduplication done by person.
Publication stopped on April 22.';

COMMENT ON COLUMN announcement.new_cases IS
'Unique confirmed or probable cases (deduplicated by person),
by date that they were announced (not date of test sample).
Publication stopped on July 10.';

COMMENT ON COLUMN announcement.new_confirmed_cases IS
'Unique confirmed cases (molecular test, deduplicated by person),
by date that they were announced (not date of test sample).';

COMMENT ON COLUMN announcement.new_probable_cases IS
'Unique probable cases (antibody test, deduplicated by person),
by date that they were announced (not date of test sample).';

COMMENT ON COLUMN announcement.adjusted_confirmed_cases IS
'Irregular retroactive adjustment applied to cumulative_confirmed_cases,
that was not counted toward new_confirmed_cases.  When this is done it
usually means either old cases that were belatedly added to the count,
or subtracted cases.  Adjustments to confirmed cases became a not uncommon
occurrence in July 2020 but the earliest one is from 2020-05-19.  These
are reported in footnotes that give free-form textual reports of cases
added and subtracted, which we add up manually to get the net adjustment
we record in this column.';

COMMENT ON COLUMN announcement.adjusted_probable_cases IS
'Irregular retroactive adjustment applied to cumulative_probable_cases,
that was not counted toward new_probable_cases.  When this is done it
usually means either old cases that were belatedly added to the count.
Adjustments to probable cases became a common occurrence since 2020-06-03.
These are reported in footnotes that give free-form textual reports of cases
added and subtracted, which we add up manually to get the net adjustment we
record in this column.';

COMMENT ON COLUMN announcement.cumulative_confirmed_deaths IS
'Deaths confirmed by a positive lab test, by date that they
were announced (not date of actual death).';

COMMENT ON COLUMN announcement.cumulative_certified_deaths IS
'Deaths not confirmed by a positive lab test, but for which a
doctor or coroner indicated COVID-19 as cause of death in the
death certificate.  Given by date that they were announced (not
date of actual death).  First reported April 8.';


CREATE TABLE bioportal (
    bulletin_date DATE NOT NULL,
    cumulative_tests INTEGER,
    cumulative_molecular_tests INTEGER,
    cumulative_positive_molecular_tests INTEGER,
    cumulative_negative_molecular_tests INTEGER,
    cumulative_inconclusive_molecular_tests INTEGER,
    cumulative_serological_tests INTEGER,
    cumulative_positive_serological_tests INTEGER,
    cumulative_negative_serological_tests INTEGER,
    new_tests INTEGER,
    new_molecular_tests INTEGER,
    new_positive_molecular_tests INTEGER,
    new_negative_molecular_tests INTEGER,
    new_inconclusive_molecular_tests INTEGER,
    new_serological_tests INTEGER,
    new_positive_serological_tests INTEGER,
    new_negative_serological_tests INTEGER,
    PRIMARY KEY (bulletin_date)
);

COMMENT ON TABLE bioportal IS
'Weekly (?) report on number of test results counted by the Department of Health.
Publication began with 2020-05-21 report.';

CREATE UNLOGGED TABLE bioportal_tests (
    id SERIAL,
    downloaded_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    raw_collected_date DATE,
    raw_reported_date DATE,
    created_at TIMESTAMP,

    collected_date DATE NOT NULL
        GENERATED ALWAYS AS (
            CASE
                WHEN raw_collected_date >= '2020-01-01'
                THEN raw_collected_date
                WHEN raw_reported_date >= '2020-03-13'
                -- Suggested by @rafalab. He uses two days as the value and says
                -- that's the average, but my spot check says 2.8 days.
                THEN raw_reported_date - 3
                ELSE date(created_at - INTERVAL '4 hour') - 3
            END
        ) STORED,
    reported_date DATE NOT NULL
        GENERATED ALWAYS AS (
            CASE
                WHEN raw_reported_date >= '2020-03-13'
                THEN raw_reported_date
                ELSE date(created_at - INTERVAL '4 hour')
            END
        ) STORED,
    created_date DATE NOT NULL
        GENERATED ALWAYS AS (
            date(created_at - INTERVAL '4 hour')
        ) STORED,
    downloaded_date DATE NOT NULL
        GENERATED ALWAYS AS (
            date(downloaded_at - INTERVAL '4 hour')
        ) STORED,

    patient_id UUID,
    age_range TEXT,
    municipality TEXT,
    result TEXT,
    positive BOOLEAN NOT NULL
        GENERATED ALWAYS AS (
            COALESCE(result, '') LIKE '%Positive%'
        ) STORED,
    PRIMARY KEY (id)
);

CREATE VIEW bioportal_bitemporal AS
SELECT
    downloaded_at,
    collected_date,
    reported_date,
    count(*) molecular_tests,
    count(*) FILTER (WHERE positive)
        AS positive_molecular_tests
FROM bioportal_tests
GROUP BY downloaded_at, collected_date, reported_date;

CREATE VIEW bioportal_bitemporal_agg AS
WITH downloads AS (
    SELECT DISTINCT downloaded_at
    FROM bioportal_tests
), reported_dates AS (
	SELECT DISTINCT reported_date
	FROM bioportal_tests
), dates AS (
	SELECT DISTINCT collected_date
	FROM bioportal_tests
	WHERE '2020-03-01' <= collected_date
	AND collected_date <= '2020-12-01'
	UNION
	SELECT DISTINCT datum_date AS collected_date
	FROM bitemporal
), grouped AS (
	SELECT
	    downloads.downloaded_at,
		reported_dates.reported_date,
		dates.collected_date,
		sum(molecular_tests)
			AS molecular_tests,
		sum(positive_molecular_tests)
			AS positive_molecular_tests
	FROM downloads
	INNER JOIN reported_dates
	    ON reported_dates.reported_date < downloads.downloaded_at - INTERVAL '4 hour'
	INNER JOIN dates
		ON dates.collected_date <= reported_dates.reported_date
	LEFT OUTER JOIN bioportal_bitemporal tests
		ON tests.collected_date = dates.collected_date
		AND tests.reported_date <= reported_dates.reported_date
	GROUP BY downloads.downloaded_at, dates.collected_date, reported_dates.reported_date
)
SELECT
	downloaded_at,
	reported_date,
	collected_date,
	reported_date - collected_date AS age,
	molecular_tests,
	positive_molecular_tests,
	sum(molecular_tests) OVER cumulative
		AS cumulative_molecular_tests,
	sum(positive_molecular_tests) OVER cumulative
		AS cumulative_positive_molecular_tests,
	molecular_tests
		- lag(molecular_tests, 1, 0 :: NUMERIC) OVER bulletin
		AS delta_molecular_tests,
	positive_molecular_tests
		- lag(positive_molecular_tests, 1, 0 :: NUMERIC) OVER bulletin
		AS delta_positive_molecular_tests
FROM grouped
WINDOW cumulative AS (
	PARTITION BY downloaded_at, reported_date
	ORDER BY collected_date
), bulletin AS (
	PARTITION BY downloaded_at, collected_date
	ORDER BY reported_date
)
ORDER BY downloaded_at DESC, reported_date DESC, collected_date;

CREATE TABLE hospitalizations (
    datum_date DATE,
    "Arecibo" INTEGER,
    "Bayamón" INTEGER,
    "Caguas" INTEGER,
    "Fajardo" INTEGER,
    "Mayagüez" INTEGER,
    "Metro" INTEGER,
    "Ponce" INTEGER,
    "Total" INTEGER NOT NULL,
    PRIMARY KEY (datum_date)
);

COMMENT ON TABLE hospitalizations IS
'Total # of patients hospitalized for COVID-19 by date and region.';


CREATE TABLE canonical_municipal_names (
    name TEXT NOT NULL,
    popest2019 INTEGER,
    PRIMARY KEY (name)
);

COMMENT ON TABLE canonical_municipal_names IS
'A list of the canonical form of municipal names, meant to be used in constraints
to catch mispelled names in the municipal tables.';

COMMENT ON COLUMN canonical_municipal_names.popest2019 IS
'Estimated population as of July 1, 2019 according the US Census Bureau''s
Population Estimates Program.';


CREATE TABLE municipal (
    bulletin_date DATE NOT NULL,
    municipality TEXT NOT NULL
        REFERENCES canonical_municipal_names (name),
    confirmed_cases INTEGER,
    confirmed_cases_percent DOUBLE PRECISION,
    PRIMARY KEY (bulletin_date, municipality)
);


CREATE TABLE age_groups_population (
    age_range TEXT NOT NULL,
    total2019 INTEGER NOT NULL,
    female2019 INTEGER NOT NULL,
    male2019 INTEGER NOT NULL,
    PRIMARY KEY (age_range)
);

COMMENT ON TABLE age_groups_population IS
'Populations for age groups in Puerto Rico, according to 2019 Census Bureau
Population Estimates Program.';


CREATE TABLE age_groups_molecular (
    bulletin_date DATE NOT NULL,
    age_range TEXT NOT NULL,
    female INTEGER,
    female_pct DOUBLE PRECISION,
    male INTEGER,
    male_pct DOUBLE PRECISION,
    cases INTEGER,
    cases_pct DOUBLE PRECISION,
    PRIMARY KEY (bulletin_date, age_range)
);


CREATE TABLE prpht_molecular_raw (
    bulletin_date DATE NOT NULL,
    laboratory TEXT NOT NULL,
    cumulative_molecular_tests INTEGER,
    cumulative_positive_molecular_tests INTEGER,
    new_molecular_tests INTEGER,
    new_positive_molecular_tests INTEGER,
    current_molecular_capacity_per_week INTEGER,
    PRIMARY KEY (bulletin_date, laboratory)
);

CREATE OR REPLACE VIEW prpht_molecular_cleaned AS
SELECT
	bulletin_date,
	laboratory,
	bulletin_date - lag(bulletin_date) OVER previous
		AS days_since_last,
	COALESCE(cumulative_positive_molecular_tests,
		     LAG(cumulative_positive_molecular_tests) OVER previous + new_positive_molecular_tests,
		     SUM(new_positive_molecular_tests) OVER previous)
		AS cumulative_positive_molecular_tests,
	COALESCE(new_positive_molecular_tests,
	         cumulative_positive_molecular_tests - LAG(cumulative_positive_molecular_tests) OVER previous,
	         0)
	    AS new_positive_molecular_tests,
	COALESCE(cumulative_molecular_tests,
		     LAG(cumulative_molecular_tests) OVER previous + new_molecular_tests,
		     SUM(new_molecular_tests) OVER previous)
		AS cumulative_molecular_tests,
	COALESCE(new_molecular_tests,
	         cumulative_molecular_tests - LAG(cumulative_molecular_tests) OVER previous,
	         0)
	    AS new_molecular_tests
FROM prpht_molecular_raw
WINDOW previous AS (
	PARTITION BY laboratory ORDER BY bulletin_date ROWS UNBOUNDED PRECEDING
)
ORDER BY laboratory, bulletin_date;


CREATE VIEW bitemporal_agg AS
SELECT
    bulletin_date,
    datum_date,

    confirmed_and_probable_cases,
    sum(confirmed_and_probable_cases) OVER bulletin
        AS cumulative_confirmed_and_probable_cases,
    COALESCE(confirmed_and_probable_cases, 0)
    		- COALESCE(lag(confirmed_and_probable_cases) OVER datum, 0)
        AS delta_confirmed_and_probable_cases,
    (COALESCE(confirmed_and_probable_cases, 0)
    		- COALESCE(lag(confirmed_and_probable_cases) OVER datum, 0))
        * (bulletin_date - datum_date)
        AS lateness_confirmed_and_probable_cases,

    confirmed_cases,
    sum(confirmed_cases) OVER bulletin
        AS cumulative_confirmed_cases,
    COALESCE(confirmed_cases, 0)
        - COALESCE(lag(confirmed_cases) OVER datum, 0)
        AS delta_confirmed_cases,
    (COALESCE(confirmed_cases, 0)
        - COALESCE(lag(confirmed_cases) OVER datum, 0))
        * (bulletin_date - datum_date)
        AS lateness_confirmed_cases,

    probable_cases,
    sum(probable_cases) OVER bulletin
        AS cumulative_probable_cases,
    COALESCE(probable_cases, 0)
        - COALESCE(lag(probable_cases) OVER datum, 0)
        AS delta_probable_cases,
    (COALESCE(probable_cases, 0)
        - COALESCE(lag(probable_cases) OVER datum, 0))
        * (bulletin_date - datum_date)
        AS lateness_probable_cases,

    deaths,
    sum(deaths) OVER bulletin
        AS cumulative_deaths,
    COALESCE(deaths, 0)
        - COALESCE(lag(deaths) OVER datum, 0)
        AS delta_deaths,
    (COALESCE(deaths, 0)
        - COALESCE(lag(deaths) OVER datum, 0))
        * (bulletin_date - datum_date)
        AS lateness_deaths
FROM bitemporal
WINDOW bulletin AS (
	PARTITION BY bulletin_date
	ORDER BY datum_date
), datum AS (
	PARTITION BY datum_date
	ORDER BY bulletin_date
	RANGE '1 day' PRECEDING
)
ORDER BY bulletin_date DESC, datum_date ASC;

COMMENT ON VIEW bitemporal_agg IS
'Useful aggregations/windows over the bitemporal table:

- Cumulative: Cumulative sums, partitioned by bulletin date;
- Delta: Current bulletin_date''s value for current datum_date
  minus previous bulletin_date''s value for previous datum_date;
- Lateness score: Delta * (bulletin_date - datum_date).';


CREATE VIEW announcement_consolidated AS
SELECT
    bulletin_date,
    coalesce(cumulative_tests,
             cumulative_positive_results + cumulative_negative_results + cumulative_pending_results)
        AS cumulative_tests,
    coalesce(cumulative_positive_results,
             cumulative_positive_molecular_tests + cumulative_positive_serological_tests)
        AS cumulative_positive_results,
    coalesce(cumulative_negative_results,
             cumulative_negative_molecular_tests + cumulative_negative_serological_tests)
        AS cumulative_negative_results,
    cumulative_pending_results,
    cumulative_molecular_tests,
    cumulative_positive_molecular_tests,
    cumulative_negative_molecular_tests,
    cumulative_inconclusive_molecular_tests,
    cumulative_serological_tests,
    cumulative_positive_serological_tests,
    cumulative_negative_serological_tests,
    new_cases,
    new_confirmed_cases,
    new_probable_cases,
    cumulative_cases,
    cumulative_confirmed_cases,
    cumulative_probable_cases,
    cumulative_deaths,
    cumulative_certified_deaths,
    cumulative_confirmed_deaths
FROM announcement
LEFT OUTER JOIN bioportal USING (bulletin_date);


CREATE VIEW hospitalizations_delta AS
SELECT
    datum_date,
    "Arecibo" - lag("Arecibo") OVER datum AS "Arecibo",
    "Bayamón" - lag("Bayamón") OVER datum AS "Bayamón",
    "Caguas" - lag("Caguas") OVER datum AS "Caguas",
    "Fajardo" - lag("Fajardo") OVER datum AS "Fajardo",
    "Mayagüez" - lag("Mayagüez") OVER datum AS "Mayagüez",
    "Metro" - lag("Metro") OVER datum AS "Metro",
    "Ponce" - lag("Ponce") OVER datum AS "Ponce",
    "Total" - lag("Total") OVER datum AS "Total"
FROM hospitalizations
WINDOW datum AS (ORDER BY datum_date);

CREATE VIEW municipal_agg AS
WITH first_bulletin AS (
	SELECT min(bulletin_date) min_bulletin_date
	FROM municipal
), new_cases AS (
	SELECT
		bulletin_date,
		municipality,
		confirmed_cases AS cumulative_confirmed_cases,
        CASE WHEN bulletin_date > first_bulletin.min_bulletin_date
		THEN COALESCE(confirmed_cases - lag(confirmed_cases) OVER bulletin,
    				  confirmed_cases)
		END AS new_confirmed_cases
	FROM municipal m
	CROSS JOIN first_bulletin
	WINDOW bulletin AS (
		PARTITION BY municipality ORDER BY bulletin_date
	)
)SELECT
	bulletin_date,
	municipality,
	cumulative_confirmed_cases,
	new_confirmed_cases,
	sum(new_confirmed_cases) OVER seven_most_recent
		AS new_7day_confirmed_cases,
	sum(new_confirmed_cases) OVER seven_before
		AS previous_7day_confirmed_cases,
	sum(new_confirmed_cases) OVER fourteen_most_recent
		AS new_14day_confirmed_cases,
	sum(new_confirmed_cases) OVER fourteen_before
		AS previous_14day_confirmed_cases,
	sum(new_confirmed_cases) OVER twentyone_most_recent
		AS new_21day_confirmed_cases,
	sum(new_confirmed_cases) OVER twentyone_before
		AS previous_21day_confirmed_cases
FROM new_cases
WINDOW seven_most_recent AS (
	PARTITION BY municipality
	ORDER BY bulletin_date
	RANGE BETWEEN '6 days' PRECEDING AND CURRENT ROW
), seven_before AS (
	PARTITION BY municipality
	ORDER BY bulletin_date
	RANGE BETWEEN '7 days' PRECEDING AND CURRENT ROW
	EXCLUDE CURRENT ROW
), fourteen_most_recent AS (
	PARTITION BY municipality
	ORDER BY bulletin_date
	RANGE BETWEEN '13 days' PRECEDING AND CURRENT ROW
), fourteen_before AS (
	PARTITION BY municipality
	ORDER BY bulletin_date
	RANGE BETWEEN '14 days' PRECEDING AND CURRENT ROW
	EXCLUDE CURRENT ROW
), twentyone_most_recent AS (
	PARTITION BY municipality
	ORDER BY bulletin_date
	RANGE BETWEEN '20 days' PRECEDING AND CURRENT ROW
), twentyone_before AS (
	PARTITION BY municipality
	ORDER BY bulletin_date
	RANGE BETWEEN '21 days' PRECEDING AND CURRENT ROW
	EXCLUDE CURRENT ROW
)
ORDER BY municipality, bulletin_date;


CREATE VIEW age_groups_molecular_agg AS
SELECT
	bulletin_date,
	age_range,
	female AS cumulative_female,
	female - lag(female) OVER seven AS new_female,
	male AS cumulative_male,
	male - lag(male) OVER seven AS new_male,
	cases AS cumulative_cases,
	cases - lag(cases) OVER seven AS new_cases,
	(cases - lag(cases, 7) OVER seven) / 7.0
		AS smoothed_daily_cases
FROM age_groups_molecular agm
WINDOW seven AS (
	PARTITION BY age_range
	ORDER BY bulletin_date
	RANGE BETWEEN '6 days' PRECEDING AND CURRENT ROW
)
ORDER BY bulletin_date, age_range;

CREATE VIEW prpht_molecular_deltas AS
SELECT
	laboratory,
	bulletin_date,
	days_since_last,
	cumulative_molecular_tests,
	cumulative_molecular_tests
		- LAG(cumulative_molecular_tests, 1, 0::BIGINT) OVER previous
		AS delta_molecular_tests,
	cumulative_positive_molecular_tests,
	cumulative_positive_molecular_tests
		- LAG(cumulative_positive_molecular_tests, 1, 0::BIGINT) OVER previous
		AS delta_positive_molecular_tests
FROM prpht_molecular_cleaned
WINDOW previous AS (
	PARTITION BY laboratory ORDER BY bulletin_date ROWS 1 PRECEDING
)
ORDER BY laboratory, bulletin_date;


-------------------------------------------------------------------------------
CREATE SCHEMA quality;

COMMENT ON SCHEMA quality IS
'Views to explore data quality issues in the data';


CREATE VIEW quality.mismatched_announcement_aggregates AS
SELECT
	bulletin_date,
	cumulative_confirmed_cases,
	lag(cumulative_confirmed_cases) OVER bulletin
		+ COALESCE(new_confirmed_cases, 0)
		+ COALESCE(adjusted_confirmed_cases, 0)
		AS computed_cumulative_confirmed_cases,
	cumulative_probable_cases,
	lag(cumulative_probable_cases) OVER bulletin
		+ COALESCE(new_probable_cases, 0)
		+ COALESCE(adjusted_probable_cases, 0)
		AS computed_cumulative_probable_cases,
	cumulative_deaths,
	COALESCE(cumulative_certified_deaths , 0)
		+ COALESCE(cumulative_confirmed_deaths , 0)
		AS computed_cumulative_deaths
FROM announcement a
WINDOW bulletin AS (ORDER BY bulletin_date)
ORDER BY bulletin_date;

COMMENT ON VIEW quality.mismatched_announcement_aggregates IS
'Check whether daily new cases plus previous cumulative cases
matches the cumulative cases figure.';


CREATE VIEW quality.mismatched_announcement_and_chart AS
SELECT
	bulletin_date,
	a.cumulative_confirmed_cases,
	sum(bi.confirmed_cases) sum_confirmed_cases,
	a.cumulative_probable_cases,
	sum(bi.probable_cases) sum_probable_cases,
	a.cumulative_deaths,
	sum(bi.deaths) sum_deaths
FROM announcement a
INNER JOIN bitemporal bi
	USING (bulletin_date)
GROUP BY bulletin_date
ORDER BY bulletin_date;

COMMENT ON VIEW quality.mismatched_announcement_and_chart IS
'Check whether the announced cumulative figures and the sample date
charts match in each bulletin.';


-------------------------------------------------------------------------------
CREATE SCHEMA products;

COMMENT ON SCHEMA products IS
'Views with queries that are intended to be presentations of the data';

CREATE VIEW products.cumulative_data AS
-- This is harder to write than it looks. You can't FULL OUTER JOIN
-- the whole bitemporal table with just the announcements because
-- you need to union a copy of the latter to each bulletin_date.
-- So you end up building a grid of all the date combinations
-- and outer joining with that as its base.
WITH dates AS (
	SELECT DISTINCT bulletin_date date
	FROM announcement
	UNION
	SELECT DISTINCT bulletin_date date
	FROM bitemporal
	UNION
	SELECT DISTINCT datum_date date
	FROM bitemporal
), bulletin_dates AS (
	SELECT DISTINCT bulletin_date
	FROM bitemporal
)
SELECT
	bulletin_dates.bulletin_date,
	dates.date datum_date,
	announcement.cumulative_positive_results AS positive_results,
	announcement.cumulative_negative_results AS negative_results,
	announcement.cumulative_pending_results AS pending_results,
	announcement.cumulative_cases AS announced_cases,
	announcement.cumulative_confirmed_cases AS announced_confirmed_cases,
	announcement.cumulative_probable_cases AS announced_probable_cases,
	announcement.cumulative_deaths AS announced_deaths,
	announcement.cumulative_certified_deaths AS announced_certified_deaths,
	announcement.cumulative_confirmed_deaths AS announced_confirmed_deaths,
	ba.cumulative_confirmed_cases AS confirmed_cases,
	ba.cumulative_probable_cases AS probable_cases,
	ba.cumulative_deaths AS deaths
FROM bulletin_dates
INNER JOIN dates
	ON dates.date <= bulletin_dates.bulletin_date
LEFT OUTER JOIN bitemporal_agg ba
	ON ba.bulletin_date = bulletin_dates.bulletin_date
	AND ba.datum_date = dates.date
LEFT OUTER JOIN announcement
	ON announcement.bulletin_date = dates.date
ORDER BY bulletin_dates.bulletin_date, dates.date;



CREATE VIEW products.daily_deltas AS
SELECT
    bulletin_date,
	datum_date,
	delta_confirmed_cases,
	delta_probable_cases,
	delta_deaths
FROM bitemporal_agg
-- We exclude the earliest bulletin date because it's artificially big
WHERE bulletin_date > (SELECT min(bulletin_date) FROM bitemporal_agg);


CREATE FUNCTION safediv(n NUMERIC, m NUMERIC)
RETURNS DOUBLE PRECISION AS $$
    SELECT cast(n AS DOUBLE PRECISION) / nullif(m, 0);
$$ LANGUAGE SQL;

CREATE VIEW products.lateness_daily AS
SELECT
    bulletin_date,
    safediv(sum(lateness_confirmed_cases) FILTER (WHERE lateness_confirmed_cases > 0),
            sum(delta_confirmed_cases) FILTER (WHERE delta_confirmed_cases > 0))
        AS confirmed_cases_additions,
    safediv(sum(lateness_probable_cases) FILTER (WHERE lateness_probable_cases > 0),
            sum(delta_probable_cases) FILTER (WHERE delta_probable_cases > 0))
        AS probable_cases_additions,
    -- There is a very weird (nondeterminism?) bug in PRDoH's deaths data processing
    -- that causes weird back-and-forth revisions to the same six dates up to 2020-04-18,
    -- so we just filter those out.
    safediv(sum(lateness_deaths) FILTER (WHERE lateness_deaths > 0 AND datum_date > '2020-04-18'),
            sum(delta_deaths) FILTER (WHERE delta_deaths > 0 AND datum_date > '2020-04-18'))
        AS deaths_additions
FROM bitemporal_agg
-- We exclude the earliest bulletin date because it's artificially late
WHERE bulletin_date > (
    SELECT min(bulletin_date)
    FROM bitemporal_agg
    WHERE delta_confirmed_cases IS NOT NULL
    AND delta_probable_cases IS NOT NULL
    AND delta_deaths IS NOT NULL
)
GROUP BY bulletin_date;

COMMENT ON VIEW products.lateness_daily IS
'An estimate of how late on average new data for each bulletin
is, based on the `bitemporal_agg` view.';


CREATE VIEW products.lateness_7day AS
WITH min_date AS (
	SELECT min(bulletin_date) bulletin_date
	FROM bitemporal
	WHERE confirmed_cases IS NOT NULL
	AND probable_cases IS NOT NULL
	AND deaths IS NOT NULL
), bulletin_sums AS (
	SELECT
		ba.bulletin_date,
		sum(lateness_confirmed_cases)
			FILTER (WHERE lateness_confirmed_cases > 0)
			AS lateness_added_confirmed_cases,
		sum(delta_confirmed_cases)
			FILTER (WHERE delta_confirmed_cases > 0)
			AS delta_added_confirmed_cases,
		sum(lateness_probable_cases)
			FILTER (WHERE lateness_probable_cases > 0)
			AS lateness_added_probable_cases,
		sum(delta_probable_cases)
			FILTER (WHERE delta_probable_cases > 0)
			AS delta_added_probable_cases,
        -- There is a very weird (nondeterminism?) bug in PRDoH's deaths data processing
        -- that causes weird back-and-forth revisions to the same six dates up to 2020-04-18,
        -- so we just filter those out.
		sum(lateness_deaths)
			FILTER (WHERE lateness_deaths > 0 AND datum_date > '2020-04-18')
			AS lateness_added_deaths,
		sum(delta_deaths)
			FILTER (WHERE delta_deaths > 0 AND datum_date > '2020-04-18')
			AS delta_added_deaths
	FROM bitemporal_agg ba, min_date md
	WHERE ba.bulletin_date > md.bulletin_date
	GROUP BY ba.bulletin_date
), windowed_sums AS (
	SELECT
		bulletin_date,
        safediv(SUM(lateness_added_confirmed_cases) OVER bulletin,
	        	SUM(delta_added_confirmed_cases) OVER bulletin)
	        AS confirmed_cases_additions,
        safediv(SUM(lateness_added_probable_cases) OVER bulletin,
	        	SUM(delta_added_probable_cases) OVER bulletin)
	        AS probable_cases_additions,
        safediv(SUM(lateness_added_deaths) OVER bulletin,
	        	SUM(delta_added_deaths) OVER bulletin)
	        AS deaths_additions
	FROM bulletin_sums bs
	WINDOW bulletin AS (ORDER BY bulletin_date ROWS 6 PRECEDING)
)
SELECT
	ws.bulletin_date,
	confirmed_cases_additions,
	probable_cases_additions,
	deaths_additions
FROM windowed_sums ws, min_date m
WHERE ws.bulletin_date >= m.bulletin_date + INTERVAL '7' DAY
ORDER BY bulletin_date;

COMMENT ON VIEW products.lateness_7day IS
'An estimate of how late on average new data is, based on the
`bitemporal_agg` view.  Averages over 7-day windows';


CREATE VIEW products.tests_by_collected_date AS
SELECT
    downloaded_at,
	reported_date,
	collected_date,
	'Salud (moleculares)' source,
	cumulative_molecular_tests AS cumulative_tests,
	cumulative_positive_molecular_tests AS cumulative_positive_tests,
	cumulative_confirmed_cases AS cumulative_cases,
	sum(molecular_tests) OVER seven / 7.0
		AS smoothed_daily_tests,
	sum(positive_molecular_tests) OVER seven / 7.0
		AS smoothed_daily_positive_tests,
	(cumulative_confirmed_cases
		- LAG(cumulative_confirmed_cases, 7, 0::bigint) OVER seven)
		/ 7.0
		AS smoothed_daily_cases
FROM bioportal_bitemporal_agg tests
INNER JOIN bitemporal_agg cases
	ON cases.bulletin_date = tests.reported_date
	AND cases.datum_date = tests.collected_date
WHERE reported_date > '2020-04-24'
WINDOW seven AS (
	PARTITION BY downloaded_at, reported_date
	ORDER BY collected_date
	RANGE '6 days' PRECEDING
)
ORDER BY downloaded_at DESC, reported_date, collected_date;

CREATE VIEW products.municipal_map AS
SELECT
	municipality,
	popest2019,
	bulletin_date,
	new_confirmed_cases,
	new_7day_confirmed_cases,
	CAST(new_confirmed_cases AS DOUBLE PRECISION)
		/ CASE WHEN previous_7day_confirmed_cases > 0
			THEN previous_7day_confirmed_cases
			ELSE 1.0
			END
		AS pct_increase_1day,
	previous_14day_confirmed_cases - previous_7day_confirmed_cases,
	CAST(new_7day_confirmed_cases AS DOUBLE PRECISION)
		/ CASE WHEN (previous_14day_confirmed_cases - previous_7day_confirmed_cases) > 0
			THEN previous_14day_confirmed_cases - previous_7day_confirmed_cases
			ELSE 1.0
			END
		AS pct_increase_7day
FROM municipal_agg ma
INNER JOIN canonical_municipal_names cmn
    ON cmn.name = ma.municipality
ORDER BY municipality, bulletin_date;


CREATE VIEW products.bitemporal_datum_lateness_agg AS
SELECT
	datum_date,
	bulletin_date,
	bulletin_date - datum_date days_before_bulletin,
	sum(delta_confirmed_cases)
		FILTER (WHERE delta_confirmed_cases > 0)
		OVER bulletins
		AS cumulative_confirmed_cases,
	sum(lateness_confirmed_cases)
		FILTER (WHERE lateness_confirmed_cases > 0)
		OVER bulletins
		AS lateness_confirmed_cases,
	sum(delta_probable_cases)
		FILTER (WHERE delta_probable_cases > 0)
		OVER bulletins
		AS cumulative_probable_cases,
	sum(lateness_probable_cases)
		FILTER (WHERE lateness_probable_cases > 0)
		OVER bulletins
		AS lateness_probable_cases,
    -- There is a very weird (nondeterminism?) bug in PRDoH's deaths data processing
    -- that causes weird back-and-forth revisions to the same six dates up to 2020-04-18,
    -- so we just filter those out.
	sum(delta_deaths)
		FILTER (WHERE delta_deaths > 0 AND datum_date > '2020-04-18')
		OVER bulletins
		AS cumulative_deaths,
	sum(lateness_deaths)
		FILTER (WHERE lateness_deaths > 0 AND datum_date > '2020-04-18')
		OVER bulletins
		AS lateness_deaths
FROM bitemporal_agg ba
-- This is the earliest bulletin from which it makes sense to do this analysis.
-- Which we exclude because it's artificially late.
WHERE bulletin_date > '2020-04-24'
WINDOW bulletins AS (
	PARTITION BY datum_date
	ORDER BY bulletin_date
)
ORDER BY datum_date, bulletin_date;

COMMENT ON VIEW products.bitemporal_datum_lateness_agg IS
'Aggregation of deltas and lateness over datum_date, used for analyzing
lateness not in term of how late each bulletin''s data was, but rather
how long data for a given datum_date took to be reported. (Which changes
with each successive bulletin_date.)';


CREATE VIEW products.molecular_lateness AS
WITH grouped AS (
    SELECT
        downloaded_at,
        reported_date,
        sum(delta_molecular_tests * age)
            AS lateness_molecular_tests,
        sum(delta_molecular_tests)
            AS delta_molecular_tests,
        sum(delta_positive_molecular_tests * age)
            AS lateness_positive_molecular_tests,
        sum(delta_positive_molecular_tests)
            AS delta_positive_molecular_tests
    FROM bioportal_bitemporal_agg
    -- We exclude the earliest bulletin date because it's artificially late
    WHERE reported_date > (
        SELECT min(reported_date)
        FROM bioportal_bitemporal_agg
    )
    GROUP BY downloaded_at, reported_date
)
SELECT
    downloaded_at,
    reported_date,
    safediv(lateness_molecular_tests, delta_molecular_tests)
        AS lateness_molecular_tests,
    safediv(lateness_positive_molecular_tests, delta_positive_molecular_tests)
        AS lateness_positive_molecular_tests,
    safediv(sum(lateness_molecular_tests) OVER seven,
            sum(delta_molecular_tests) OVER seven)
        AS smoothed_lateness_molecular_tests,
    safediv(sum(lateness_positive_molecular_tests) OVER seven,
            sum(delta_positive_molecular_tests) OVER seven)
        AS smoothed_lateness_positive_molecular_tests
FROM grouped
WINDOW seven AS (
    PARTITION BY downloaded_at
	ORDER BY reported_date
	RANGE '6 days' PRECEDING
)
ORDER BY downloaded_at DESC, reported_date DESC;
