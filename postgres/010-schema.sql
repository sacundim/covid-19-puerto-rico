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
    positive_molecular_tests INTEGER,
    molecular_tests INTEGER,
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

    -- These were begun to be reported on 2020-05-22
    cumulative_tests INTEGER,
    cumulative_molecular_tests INTEGER,
    cumulative_positive_molecular_tests INTEGER,
    cumulative_negative_molecular_tests INTEGER,
    cumulative_inconclusive_molecular_tests INTEGER,
    cumulative_serological_tests INTEGER,
    cumulative_positive_serological_tests INTEGER,
    cumulative_negative_serological_tests INTEGER,
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
        * (bulletin_date - datum_date) AS lateness_deaths,

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
FROM announcement;

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
	ba.cumulative_deaths AS deaths,
	ba.cumulative_molecular_tests AS molecular_tests,
	ba.cumulative_positive_molecular_tests AS positive_molecular_tests
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


CREATE VIEW products.lateness_daily AS
SELECT
    bulletin_date,
    cast(sum(lateness_confirmed_and_probable_cases) AS DOUBLE PRECISION)
        / nullif(greatest(0, sum(delta_confirmed_and_probable_cases)), 0)
        AS confirmed_and_probable_cases,
    cast(sum(lateness_confirmed_cases) AS DOUBLE PRECISION)
        / nullif(greatest(0, sum(delta_confirmed_cases)), 0)
        AS confirmed_cases,
    cast(sum(lateness_probable_cases) AS DOUBLE PRECISION)
        / nullif(greatest(0, sum(delta_probable_cases)), 0)
        AS probable_cases,
    cast(sum(lateness_deaths) AS DOUBLE PRECISION)
        / nullif(greatest(0, sum(delta_deaths)), 0)
        AS deaths
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
		sum(lateness_confirmed_cases) lateness_confirmed_cases,
		sum(delta_confirmed_cases) delta_confirmed_cases,
		sum(lateness_probable_cases) lateness_probable_cases,
		sum(delta_probable_cases) delta_probable_cases,
		sum(lateness_deaths) lateness_deaths,
		sum(delta_deaths) delta_deaths
	FROM bitemporal_agg ba, min_date md
	WHERE ba.bulletin_date > md.bulletin_date
	GROUP BY ba.bulletin_date
), windowed_sums AS (
	SELECT
		bulletin_date,
		CAST(SUM(lateness_confirmed_and_probable_cases) OVER bulletin AS DOUBLE PRECISION)
	        / NULLIF(SUM(delta_confirmed_and_probable_cases) OVER bulletin, 0)
	        AS confirmed_and_probable_cases,
		CAST(SUM(lateness_confirmed_cases) OVER bulletin AS DOUBLE PRECISION)
	        / NULLIF(SUM(delta_confirmed_cases) OVER bulletin, 0)
	        AS confirmed_cases,
		CAST(SUM(lateness_probable_cases) OVER bulletin AS DOUBLE PRECISION)
	        / NULLIF(SUM(delta_probable_cases) OVER bulletin, 0)
	        AS probable_cases,
		CAST(SUM(lateness_deaths) OVER bulletin AS DOUBLE PRECISION)
	        / NULLIF(SUM(delta_deaths) OVER bulletin, 0)
	        AS deaths
	FROM bulletin_sums bs
	WINDOW bulletin AS (ORDER BY bulletin_date ROWS 6 PRECEDING)
)
SELECT
	ws.bulletin_date,
	confirmed_and_probable_cases,
	confirmed_cases,
	probable_cases,
	deaths
FROM windowed_sums ws, min_date m
WHERE ws.bulletin_date >= m.bulletin_date + INTERVAL '7' DAY;

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