CREATE FUNCTION log2(x NUMERIC)
RETURNS NUMERIC AS $$
    SELECT log(2.0, x);
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
by date that they were announced (not date of test sample).';

COMMENT ON COLUMN announcement.new_confirmed_cases IS
'Unique confirmed cases (molecular test, deduplicated by person),
by date that they were announced (not date of test sample).';

COMMENT ON COLUMN announcement.new_probable_cases IS
'Unique probable cases (antibody test, deduplicated by person),
by date that they were announced (not date of test sample).';

COMMENT ON COLUMN announcement.cumulative_confirmed_deaths IS
'Deaths confirmed by a positive lab test, by date that they
were announced (not date of actual death).';

COMMENT ON COLUMN announcement.cumulative_certified_deaths IS
'Deaths not confirmed by a positive lab test, but for which a
doctor or coroner indicated COVID-19 as cause of death in the
death certificate.  Given by date that they were announced (not
date of actual death).  First reported April 8';


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


CREATE TABLE bioportal_bitemporal (
    bulletin_date DATE NOT NULL,
    datum_date DATE NOT NULL,
    positive_molecular_tests INTEGER,
    molecular_tests INTEGER,
    PRIMARY KEY (bulletin_date, datum_date)
);

COMMENT ON TABLE bioportal IS
'Very irregularly published charts on number of tests by sample date.';


CREATE TABLE municipal (
    bulletin_date DATE NOT NULL,
    municipality TEXT NOT NULL,
    confirmed_cases INTEGER,
    confirmed_cases_percent DOUBLE PRECISION,
    PRIMARY KEY (bulletin_date, municipality)
);


CREATE VIEW bitemporal_agg AS
SELECT
    bulletin_date,
    datum_date,

    confirmed_and_probable_cases,
    sum(confirmed_and_probable_cases) OVER bulletin
        AS cumulative_confirmed_and_probable_cases,
    confirmed_and_probable_cases - coalesce(lag(confirmed_and_probable_cases) OVER datum, 0)
        AS delta_confirmed_and_probable_cases,
    (confirmed_and_probable_cases - coalesce(lag(confirmed_and_probable_cases) OVER datum, 0))
        * (bulletin_date - datum_date) AS lateness_confirmed_and_probable_cases,

    confirmed_cases,
    sum(confirmed_cases) OVER bulletin
        AS cumulative_confirmed_cases,
    confirmed_cases - coalesce(lag(confirmed_cases) OVER datum, 0)
        AS delta_confirmed_cases,
    (confirmed_cases - coalesce(lag(confirmed_cases) OVER datum, 0))
        * (bulletin_date - datum_date) AS lateness_confirmed_cases,

    probable_cases,
    sum(probable_cases) OVER bulletin
        AS cumulative_probable_cases,
    probable_cases - coalesce(lag(probable_cases) OVER datum, 0)
        AS delta_probable_cases,
    (probable_cases - coalesce(lag(probable_cases) OVER datum, 0))
        * (bulletin_date - datum_date) AS lateness_probable_cases,

    deaths,
    sum(deaths) OVER bulletin
        AS cumulative_deaths,
    deaths - coalesce(lag(deaths) OVER datum, 0)
        AS delta_deaths,
    (deaths - coalesce(lag(deaths) OVER datum, 0))
        * (bulletin_date - datum_date) AS lateness_deaths
FROM bitemporal
WINDOW
    bulletin AS (PARTITION BY bulletin_date ORDER BY datum_date),
    datum AS (PARTITION BY datum_date ORDER BY bulletin_date);

COMMENT ON VIEW bitemporal_agg IS
'Useful aggregations/windows over the bitemporal table:

- Cumulative: Cumulative sums, partitioned by bulletin date;
- Delta: Current bulletin_date''s value for current datum_date
  minus previous bulletin_date''s value for previous datum_date;
- Lateness score: Delta * (bulletin_date - datum_date).';

CREATE VIEW bioportal_bitemporal_agg AS
SELECT
    bulletin_date,
    datum_date,

    molecular_tests,
    sum(molecular_tests) OVER bulletin
        AS cumulative_molecular_tests,
    molecular_tests - coalesce(lag(molecular_tests) OVER datum, 0)
        AS delta_molecular_tests,
    (molecular_tests - coalesce(lag(molecular_tests) OVER datum, 0))
        * (bulletin_date - datum_date) AS lateness_molecular_tests,

    positive_molecular_tests,
    sum(positive_molecular_tests) OVER bulletin
        AS cumulative_positive_molecular_tests,
    positive_molecular_tests - coalesce(lag(positive_molecular_tests) OVER datum, 0)
        AS delta_positive_molecular_tests,
    (positive_molecular_tests - coalesce(lag(positive_molecular_tests) OVER datum, 0))
        * (bulletin_date - datum_date) AS lateness_positive_molecular_tests
FROM bioportal_bitemporal
WINDOW
    bulletin AS (PARTITION BY bulletin_date ORDER BY datum_date),
    datum AS (PARTITION BY datum_date ORDER BY bulletin_date);


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


CREATE VIEW municipal_agg AS
SELECT
	bulletin_date,
	municipality,
	confirmed_cases AS cumulative_confirmed_cases,
	confirmed_cases - lag(confirmed_cases) OVER bulletin
		AS new_confirmed_cases
FROM municipal m
WINDOW bulletin AS (
	PARTITION BY municipality ORDER BY bulletin_date
)
ORDER BY municipality, bulletin_date;


-------------------------------------------------------------------------------
CREATE SCHEMA quality;

COMMENT ON SCHEMA quality IS
'Views to explore data quality issues in the data';


CREATE VIEW quality.mismatched_announcement_aggregates AS
WITH base AS (
	SELECT
		bulletin_date,
		lag(cumulative_cases) OVER bulletin
			AS previous_cumulative_cases,
		new_cases,
		cumulative_cases - lag(cumulative_cases) OVER bulletin
			AS computed_new_cases,
		cumulative_cases,
		lag(cumulative_cases) OVER bulletin + new_cases
			AS computed_cumulative_cases,

		lag(cumulative_confirmed_cases) OVER bulletin
			AS previous_cumulative_confirmed_cases,
		new_confirmed_cases,
		cumulative_confirmed_cases - lag(cumulative_confirmed_cases) OVER bulletin
			AS computed_new_confirmed_cases,
		cumulative_confirmed_cases,
		lag(cumulative_confirmed_cases) OVER bulletin + new_confirmed_cases
			AS computed_cumulative_confirmed_cases,

		lag(cumulative_probable_cases) OVER bulletin
			AS previous_cumulative_probable_cases,
		new_probable_cases,
		cumulative_probable_cases - lag(cumulative_probable_cases) OVER bulletin
			AS computed_new_probable_cases,
		cumulative_probable_cases,
		lag(cumulative_probable_cases) OVER bulletin + new_probable_cases
			AS computed_cumulative_probable_cases
	FROM announcement a
	WINDOW bulletin AS (ORDER BY bulletin_date))
SELECT *
FROM base
WHERE new_cases != computed_new_cases
OR new_confirmed_cases != computed_new_confirmed_cases
OR new_probable_cases != computed_new_probable_cases;

COMMENT ON VIEW quality.mismatched_announcement_aggregates IS
'Check whether the daily new cases and the cumulative figures
in the bulletins match. Which they don''t always do...';


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
	ba.cumulative_confirmed_and_probable_cases AS confirmed_and_probable_cases,
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
	delta_confirmed_and_probable_cases,
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

    safediv(sum(lateness_confirmed_and_probable_cases),
            sum(delta_confirmed_and_probable_cases))
        AS confirmed_and_probable_cases_total,
    safediv(sum(lateness_confirmed_and_probable_cases)
                FILTER (WHERE lateness_confirmed_and_probable_cases > 0),
            sum(delta_confirmed_and_probable_cases)
                FILTER (WHERE delta_confirmed_and_probable_cases > 0))
        AS confirmed_and_probable_cases_additions,
    -safediv(sum(lateness_confirmed_and_probable_cases)
                 FILTER (WHERE lateness_confirmed_and_probable_cases < 0),
             sum(delta_confirmed_and_probable_cases)
                 FILTER (WHERE delta_confirmed_and_probable_cases < 0))
         AS confirmed_and_probable_cases_removals,

    safediv(sum(lateness_confirmed_cases), sum(delta_confirmed_cases))
        AS confirmed_cases_total,
    safediv(sum(lateness_confirmed_cases) FILTER (WHERE lateness_confirmed_cases > 0),
            sum(delta_confirmed_cases) FILTER (WHERE delta_confirmed_cases > 0))
        AS confirmed_cases_additions,
    -safediv(sum(lateness_confirmed_cases) FILTER (WHERE lateness_confirmed_cases < 0),
             sum(delta_confirmed_cases) FILTER (WHERE delta_confirmed_cases < 0))
         AS confirmed_cases_removals,

    safediv(sum(lateness_probable_cases), sum(delta_probable_cases))
        AS probable_cases_total,
    safediv(sum(lateness_probable_cases) FILTER (WHERE lateness_probable_cases > 0),
            sum(delta_probable_cases) FILTER (WHERE delta_probable_cases > 0))
        AS probable_cases_additions,
    -safediv(sum(lateness_probable_cases) FILTER (WHERE lateness_probable_cases < 0),
             sum(delta_probable_cases) FILTER (WHERE delta_probable_cases < 0))
         AS probable_cases_removals,

    safediv(sum(lateness_deaths), sum(delta_deaths))
        AS deaths_total,
    safediv(sum(lateness_deaths) FILTER (WHERE lateness_deaths > 0),
            sum(delta_deaths) FILTER (WHERE delta_deaths > 0))
        AS deaths_additions,
    -safediv(sum(lateness_deaths) FILTER (WHERE lateness_deaths < 0),
             sum(delta_deaths) FILTER (WHERE delta_deaths < 0))
         AS deaths_removals
FROM bitemporal_agg
-- We exclude the earliest bulletin date because it's artificially late
WHERE bulletin_date > (SELECT min(bulletin_date) FROM bitemporal_agg)
GROUP BY bulletin_date;

COMMENT ON VIEW products.lateness_daily IS
'An estimate of how late on average new data for each bulletin
is, based on the `bitemporal_agg` view.';


CREATE VIEW products.lateness_7day AS
WITH min_date AS (
	SELECT min(bulletin_date) bulletin_date
	FROM bitemporal
	WHERE confirmed_and_probable_cases IS NOT NULL
	AND confirmed_cases IS NOT NULL
	AND probable_cases IS NOT NULL
	AND deaths IS NOT NULL
), bulletin_sums AS (
	SELECT
		ba.bulletin_date,

		sum(lateness_confirmed_and_probable_cases) lateness_confirmed_and_probable_cases,
		sum(delta_confirmed_and_probable_cases) delta_confirmed_and_probable_cases,
		sum(lateness_confirmed_and_probable_cases)
			FILTER (WHERE lateness_confirmed_and_probable_cases > 0)
			AS lateness_added_confirmed_and_probable_cases,
		sum(delta_confirmed_and_probable_cases)
			FILTER (WHERE delta_confirmed_and_probable_cases > 0)
			AS delta_added_confirmed_and_probable_cases,
		sum(lateness_confirmed_and_probable_cases)
			FILTER (WHERE lateness_confirmed_and_probable_cases < 0)
			AS lateness_removed_confirmed_and_probable_cases,
		sum(delta_confirmed_and_probable_cases)
			FILTER (WHERE delta_confirmed_and_probable_cases < 0)
			AS delta_removed_confirmed_and_probable_cases,

		sum(lateness_confirmed_cases) lateness_confirmed_cases,
		sum(delta_confirmed_cases) delta_confirmed_cases,
		sum(lateness_confirmed_cases)
			FILTER (WHERE lateness_confirmed_cases > 0)
			AS lateness_added_confirmed_cases,
		sum(delta_confirmed_cases)
			FILTER (WHERE delta_confirmed_cases > 0)
			AS delta_added_confirmed_cases,
		sum(lateness_confirmed_cases)
			FILTER (WHERE lateness_confirmed_cases < 0)
			AS lateness_removed_confirmed_cases,
		sum(delta_confirmed_cases)
			FILTER (WHERE delta_confirmed_cases < 0)
			AS delta_removed_confirmed_cases,

		sum(lateness_probable_cases) lateness_probable_cases,
		sum(delta_probable_cases) delta_probable_cases,
		sum(lateness_probable_cases)
			FILTER (WHERE lateness_probable_cases > 0)
			AS lateness_added_probable_cases,
		sum(delta_probable_cases)
			FILTER (WHERE delta_probable_cases > 0)
			AS delta_added_probable_cases,
		sum(lateness_probable_cases)
			FILTER (WHERE lateness_probable_cases < 0)
			AS lateness_removed_probable_cases,
		sum(delta_probable_cases)
			FILTER (WHERE delta_probable_cases < 0)
			AS delta_removed_probable_cases,

		sum(lateness_deaths) lateness_deaths,
		sum(delta_deaths) delta_deaths,
		sum(lateness_deaths)
			FILTER (WHERE lateness_deaths > 0)
			AS lateness_added_deaths,
		sum(delta_deaths)
			FILTER (WHERE delta_deaths > 0)
			AS delta_added_deaths,
		sum(lateness_deaths)
			FILTER (WHERE lateness_deaths < 0)
			AS lateness_removed_deaths,
		sum(delta_deaths)
			FILTER (WHERE delta_deaths < 0)
			AS delta_removed_deaths
	FROM bitemporal_agg ba, min_date md
	WHERE ba.bulletin_date > md.bulletin_date
	GROUP BY ba.bulletin_date
), windowed_sums AS (
	SELECT
		bulletin_date,

		safediv(SUM(lateness_confirmed_and_probable_cases) OVER bulletin,
	        	SUM(delta_confirmed_and_probable_cases) OVER bulletin)
	        AS confirmed_and_probable_cases_total,
        safediv(SUM(lateness_added_confirmed_and_probable_cases) OVER bulletin,
	        	SUM(delta_added_confirmed_and_probable_cases) OVER bulletin)
	        AS confirmed_and_probable_cases_additions,
		safediv(SUM(lateness_removed_confirmed_and_probable_cases) OVER bulletin,
	        	SUM(delta_removed_confirmed_and_probable_cases) OVER bulletin)
	        AS confirmed_and_probable_cases_removals,

        safediv(SUM(lateness_confirmed_cases) OVER bulletin,
	        	SUM(delta_confirmed_cases) OVER bulletin)
	        AS confirmed_cases_total,
        safediv(SUM(lateness_added_confirmed_cases) OVER bulletin,
	        	SUM(delta_added_confirmed_cases) OVER bulletin)
	        AS confirmed_cases_additions,
		safediv(SUM(lateness_removed_confirmed_cases) OVER bulletin,
	        	SUM(delta_removed_confirmed_cases) OVER bulletin)
	        AS confirmed_cases_removals,

        safediv(SUM(lateness_probable_cases) OVER bulletin,
	        	SUM(delta_probable_cases) OVER bulletin)
	        AS probable_cases_total,
        safediv(SUM(lateness_added_probable_cases) OVER bulletin,
	        	SUM(delta_added_probable_cases) OVER bulletin)
	        AS probable_cases_additions,
		safediv(SUM(lateness_removed_confirmed_cases) OVER bulletin,
	        	SUM(delta_removed_confirmed_cases) OVER bulletin)
	        AS probable_cases_removals,

        safediv(SUM(lateness_deaths) OVER bulletin,
	        	SUM(delta_deaths) OVER bulletin)
	        AS deaths_total,
        safediv(SUM(lateness_added_deaths) OVER bulletin,
	        	SUM(delta_added_deaths) OVER bulletin)
	        AS deaths_additions,
		safediv(SUM(lateness_removed_deaths) OVER bulletin,
	        	SUM(delta_removed_deaths) OVER bulletin)
	        AS deaths_removals
	FROM bulletin_sums bs
	WINDOW bulletin AS (ORDER BY bulletin_date ROWS 6 PRECEDING)
)
SELECT
	ws.bulletin_date,
	confirmed_and_probable_cases_total,
	confirmed_and_probable_cases_additions,
	-confirmed_and_probable_cases_removals confirmed_and_probable_cases_removals,
	confirmed_cases_total,
	confirmed_cases_additions,
	-confirmed_cases_removals confirmed_cases_removals,
	probable_cases_total,
	probable_cases_additions,
	-probable_cases_removals probable_cases_removals,
	deaths_total,
	deaths_additions,
	-deaths_removals deaths_removals
FROM windowed_sums ws, min_date m
WHERE ws.bulletin_date >= m.bulletin_date + INTERVAL '7' DAY
ORDER BY bulletin_date;

COMMENT ON VIEW products.lateness_7day IS
'An estimate of how late on average new data is, based on the
`bitemporal_agg` view.  Averages over 7-day windows';


CREATE VIEW products.doubling_times AS
SELECT
    bulletin_date,
    datum_date,
    window_size.days window_size_days,
    CAST(window_size.days AS NUMERIC)
    	/ NULLIF(log2(cumulative_confirmed_and_probable_cases)
    				- log2(LAG(cumulative_confirmed_and_probable_cases, window_size.days) OVER datum), 0)
    	AS cumulative_confirmed_and_probable_cases,
    CAST(window_size.days AS NUMERIC)
    	/ NULLIF(log2(cumulative_confirmed_cases) - log2(LAG(cumulative_confirmed_cases, window_size.days) OVER datum), 0)
    	AS cumulative_confirmed_cases,
    CAST(window_size.days AS NUMERIC)
    	/ NULLIF(log2(cumulative_probable_cases) - log2(LAG(cumulative_probable_cases, window_size.days) OVER datum), 0)
    	AS cumulative_probable_cases,
    CAST(window_size.days AS NUMERIC)
    	/ NULLIF(log2(cumulative_deaths) - log2(LAG(cumulative_deaths, window_size.days) OVER datum), 0)
    	AS cumulative_deaths
FROM bitemporal_agg
CROSS JOIN (VALUES (7), (14), (21)) AS window_size (days)
WINDOW datum AS (
	PARTITION BY bulletin_date, window_size.days
	ORDER BY datum_date);

COMMENT ON VIEW products.doubling_times IS
'How long it took values to double, expressed in fractional days.
Computed over windows of 7, 14 and 21 days.';


CREATE VIEW products.animations AS
SELECT
    bulletin_date,
    datum_date,
    COALESCE(confirmed_cases, MAX(confirmed_cases) OVER bulletin) confirmed_cases,
    COALESCE(probable_cases, MAX(probable_cases) OVER bulletin) probable_cases,
    COALESCE(confirmed_and_probable_cases,
             MAX(confirmed_and_probable_cases) OVER bulletin) cases,
	COALESCE(deaths, MAX(deaths) OVER bulletin) deaths,
    announced_confirmed_cases,
    announced_probable_cases,
    announced_cases,
	announced_deaths
FROM products.cumulative_data
WHERE (SELECT min(bulletin_date) FROM bitemporal_agg) <= datum_date
AND datum_date <= bulletin_date
WINDOW bulletin AS (PARTITION BY bulletin_date ROWS UNBOUNDED PRECEDING);


CREATE VIEW products.tests_by_bulletin_date AS
SELECT
	b.bulletin_date,
	b.cumulative_molecular_tests / 3193.694
		AS cumulative_tests_per_thousand,
	CAST(b.cumulative_molecular_tests AS DOUBLE PRECISION)
		/ a.cumulative_confirmed_cases
		AS cumulative_tests_per_confirmed_case,
	(b.new_molecular_tests / 3193.694)
		/ (b.bulletin_date - lag(b.bulletin_date) OVER bulletin)
		AS new_daily_tests_per_thousand,
	CAST(b.new_molecular_tests AS DOUBLE PRECISION)
		/ (a.cumulative_confirmed_cases - lag(a.cumulative_confirmed_cases) OVER bulletin)
		AS new_tests_per_confirmed_case
FROM bioportal b
INNER JOIN announcement a
	USING (bulletin_date)
WINDOW bulletin AS (ORDER BY b.bulletin_date)
ORDER BY bulletin_date;

CREATE VIEW products.tests_by_sample_date AS
SELECT
	bulletin_date,
	datum_date,
	CAST(sum(tests.molecular_tests) OVER seven AS DOUBLE PRECISION)
		/ sum(cases.confirmed_cases) OVER seven
		AS new_tests_per_confirmed_case,
	CAST(sum(tests.molecular_tests) OVER cumulative AS DOUBLE PRECISION)
		/ sum(cases.confirmed_cases) OVER cumulative
		AS cumulative_tests_per_confirmed_case,
	(sum(tests.molecular_tests) OVER seven / 3193.694) / 7.0
		AS new_daily_tests_per_thousand,
	(sum(tests.molecular_tests) OVER cumulative / 3193.694)
		AS cumulative_daily_tests_per_thousand
FROM bitemporal_agg cases
INNER JOIN bioportal_bitemporal_agg tests
	USING (bulletin_date, datum_date)
WINDOW
	cumulative AS (
		PARTITION BY bulletin_date
		ORDER BY datum_date
	),
	seven AS (
		PARTITION BY bulletin_date
		ORDER BY datum_date
		RANGE BETWEEN '6 days' PRECEDING AND CURRENT ROW
	)
ORDER BY bulletin_date, datum_date;


CREATE VIEW products.municipal_map AS
SELECT
	municipality,
	bulletin_date,
	cumulative_confirmed_cases,
	new_confirmed_cases,
	sum(new_confirmed_cases) OVER seven_most_recent
		AS new_7day_confirmed_cases,
	sum(new_confirmed_cases) OVER seven_before
		AS previous_7day_confirmed_cases,
	sum(new_confirmed_cases) OVER fourteen_most_recent
		AS new_14day_confirmed_cases,
	sum(new_confirmed_cases) OVER fourteen_before
		AS previous_14day_confirmed_cases
FROM municipal_agg ma
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
);