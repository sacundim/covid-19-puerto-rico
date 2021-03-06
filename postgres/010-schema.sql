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
    confirmed_and_suspect_cases INTEGER,
    confirmed_cases INTEGER,
    probable_cases INTEGER,
    suspect_cases INTEGER,
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
    new_suspect_cases INTEGER,
    adjusted_confirmed_cases INTEGER,
    adjusted_probable_cases INTEGER,
    adjusted_suspect_cases INTEGER,
    cumulative_cases INTEGER,
    cumulative_confirmed_cases INTEGER,
    cumulative_probable_cases INTEGER,
    cumulative_suspect_cases INTEGER,
    cumulative_deaths INTEGER,
    cumulative_confirmed_deaths INTEGER,
    cumulative_probable_deaths INTEGER,
    cumulative_suspect_deaths INTEGER,
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
'Unique confirmed or suspect cases (deduplicated by person),
by date that they were announced (not date of test sample).
Publication stopped on July 10.';

COMMENT ON COLUMN announcement.new_confirmed_cases IS
'Unique confirmed cases (molecular test, deduplicated by person),
by date that they were announced (not date of test sample).';

COMMENT ON COLUMN announcement.new_suspect_cases IS
'Unique suspect cases (antibody test, deduplicated by person),
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

COMMENT ON COLUMN announcement.adjusted_suspect_cases IS
'Irregular retroactive adjustment applied to cumulative_suspect_cases,
that was not counted toward new_suspect_cases.  When this is done it
usually means either old cases that were belatedly added to the count.
Adjustments to suspect cases became a common occurrence since 2020-06-03.
These are reported in footnotes that give free-form textual reports of cases
added and subtracted, which we add up manually to get the net adjustment we
record in this column.';

COMMENT ON COLUMN announcement.cumulative_confirmed_deaths IS
'Deaths confirmed by a positive lab test, by date that they
were announced (not date of actual death).';

COMMENT ON COLUMN announcement.cumulative_suspect_deaths IS
'Deaths not confirmed by a positive lab test, but for which a
doctor or coroner indicated COVID-19 as cause of death in the
death certificate.  Given by date that they were announced (not
date of actual death).  First reported April 8.';


CREATE TABLE canonical_municipal_names (
    name TEXT NOT NULL,
    popest2019 INTEGER,
    fips_code VARCHAR(5),
    region TEXT,
    PRIMARY KEY (name)
);

COMMENT ON TABLE canonical_municipal_names IS
'A list of the canonical form of municipal names, meant to be used in constraints
to catch mispelled names in the municipal tables.';

COMMENT ON COLUMN canonical_municipal_names.popest2019 IS
'Estimated population as of July 1, 2019 according the US Census Bureau''s
Population Estimates Program.';


CREATE TABLE municipal_molecular (
    bulletin_date DATE NOT NULL,
    municipality TEXT NOT NULL
        REFERENCES canonical_municipal_names (name),
    confirmed_cases INTEGER,
    confirmed_cases_percent DOUBLE PRECISION,
    PRIMARY KEY (bulletin_date, municipality)
);

CREATE TABLE municipal_antigens (
    bulletin_date DATE NOT NULL,
    municipality TEXT NOT NULL
        REFERENCES canonical_municipal_names (name),
    probable_cases INTEGER,
    probable_cases_percent DOUBLE PRECISION,
    PRIMARY KEY (bulletin_date, municipality)
);

CREATE MATERIALIZED VIEW municipal AS
SELECT
    bulletin_date,
    municipality,
    confirmed_cases,
    probable_cases,
    confirmed_cases + coalesce(probable_cases, 0)
        AS cases
FROM municipal_molecular
FULL OUTER JOIN municipal_antigens
    USING (bulletin_date, municipality);


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

CREATE TABLE age_groups_antigens (
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

CREATE MATERIALIZED VIEW age_groups AS
SELECT
	agm.bulletin_date,
	agm.age_range,

	agm.female + coalesce(aga.female, 0)
	    AS female,
    agm.female AS female_molecular,
    aga.female AS female_antigens,

	agm.male + coalesce(aga.male, 0)
	    AS male,
    agm.male AS male_molecular,
    aga.male AS male_antigens,

	agm.cases + coalesce(aga.cases, 0)
	    AS cases,
    agm.cases AS cases_molecular,
    aga.cases AS cases_antigens
FROM age_groups_molecular agm
FULL OUTER JOIN age_groups_antigens aga
    USING (bulletin_date, age_range);


CREATE MATERIALIZED VIEW bitemporal_agg AS
SELECT
    bulletin_date,
    datum_date,
    bulletin_date - datum_date AS age,

    confirmed_cases,
    sum(confirmed_cases) OVER bulletin
        AS cumulative_confirmed_cases,
    COALESCE(confirmed_cases, 0)
        - COALESCE(lag(confirmed_cases) OVER datum, 0)
        AS delta_confirmed_cases,

    probable_cases,
    sum(probable_cases) OVER bulletin
        AS cumulative_probable_cases,
    COALESCE(probable_cases, 0)
        - COALESCE(lag(probable_cases) OVER datum, 0)
        AS delta_probable_cases,

    suspect_cases,
    sum(suspect_cases) OVER bulletin
        AS cumulative_suspect_cases,
    COALESCE(suspect_cases, 0)
        - COALESCE(lag(suspect_cases) OVER datum, 0)
        AS delta_suspect_cases,

    deaths,
    sum(deaths) OVER bulletin
        AS cumulative_deaths,
    COALESCE(deaths, 0)
        - COALESCE(lag(deaths) OVER datum, 0)
        AS delta_deaths
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

COMMENT ON MATERIALIZED VIEW bitemporal_agg IS
'Useful aggregations/windows over the bitemporal table:

- Cumulative: Cumulative sums, partitioned by bulletin date;
- Delta: Current bulletin_date''s value for current datum_date
  minus previous bulletin_date''s value for previous datum_date.';


CREATE VIEW municipal_agg AS
WITH first_bulletin AS (
	SELECT min(bulletin_date) min_bulletin_date
	FROM municipal
), new_cases AS (
	SELECT
		bulletin_date,
		municipality,
		cases AS cumulative_cases,
        CASE WHEN bulletin_date > first_bulletin.min_bulletin_date
		THEN COALESCE(cases - lag(cases) OVER bulletin, cases)
		END AS new_cases
	FROM municipal m
	CROSS JOIN first_bulletin
	WINDOW bulletin AS (
		PARTITION BY municipality ORDER BY bulletin_date
	)
)SELECT
	bulletin_date,
	municipality,
	cumulative_cases,
	new_cases,
	sum(new_cases) OVER seven_most_recent
		AS new_7day_cases,
	sum(new_cases) OVER seven_before
		AS previous_7day_cases,
	sum(new_cases) OVER fourteen_most_recent
		AS new_14day_cases,
	sum(new_cases) OVER fourteen_before
		AS previous_14day_cases,
	sum(new_cases) OVER twentyone_most_recent
		AS new_21day_cases,
	sum(new_cases) OVER twentyone_before
		AS previous_21day_cases
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


CREATE VIEW age_groups_agg AS
SELECT
	bulletin_date,
	age_range,
	female AS cumulative_female,
	female - lag(female) OVER seven
	    AS new_female,
	male AS cumulative_male,
	male - lag(male) OVER seven AS new_male,
	cases AS cumulative_cases,
	cases - lag(cases) OVER seven AS new_cases,
	(cases - lag(cases, 7) OVER seven) / 7.0
		AS smoothed_daily_cases
FROM age_groups
WINDOW seven AS (
	PARTITION BY age_range
	ORDER BY bulletin_date
	RANGE BETWEEN '6 days' PRECEDING AND CURRENT ROW
)
ORDER BY bulletin_date, age_range;


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
	cumulative_suspect_cases,
	lag(cumulative_suspect_cases) OVER bulletin
		+ COALESCE(new_suspect_cases, 0)
		+ COALESCE(adjusted_suspect_cases, 0)
		AS computed_cumulative_suspect_cases,
	cumulative_deaths,
	COALESCE(cumulative_confirmed_deaths, 0)
		+ COALESCE(cumulative_probable_deaths, 0)
		+ COALESCE(cumulative_suspect_deaths, 0)
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
	a.cumulative_suspect_cases,
	sum(bi.suspect_cases) sum_suspect_cases,
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


CREATE VIEW products.daily_deltas AS
SELECT
    bulletin_date,
	datum_date,
	delta_confirmed_cases,
	delta_probable_cases,
	delta_suspect_cases,
	delta_deaths
FROM bitemporal_agg
-- We exclude the earliest bulletin date because it's artificially big
WHERE bulletin_date > (SELECT min(bulletin_date) FROM bitemporal_agg);


CREATE FUNCTION safediv(n NUMERIC, m NUMERIC)
RETURNS DOUBLE PRECISION AS $$
    SELECT cast(n AS DOUBLE PRECISION) / nullif(m, 0);
$$ LANGUAGE SQL;

-- Calculating population standard deviation from pre-aggregated count,
-- sum and sum of squares.  See: https://math.stackexchange.com/a/1853685/78028
CREATE FUNCTION aggdev(count NUMERIC, sum NUMERIC, sumsq NUMERIC)
RETURNS DOUBLE PRECISION AS $$
    SELECT sqrt((sumsq :: DOUBLE PRECISION / count) - (sum :: DOUBLE PRECISION / count)^2);
$$ LANGUAGE SQL;

CREATE VIEW products.lateness_daily AS
SELECT
    bulletin_date,
    safediv(sum(delta_confirmed_cases * age) FILTER (WHERE delta_confirmed_cases > 0),
            sum(delta_confirmed_cases) FILTER (WHERE delta_confirmed_cases > 0))
        AS confirmed_cases_additions,
    aggdev(sum(delta_confirmed_cases) FILTER (WHERE delta_confirmed_cases > 0),
    	   sum(delta_confirmed_cases * age) FILTER (WHERE delta_confirmed_cases > 0),
    	   sum(delta_confirmed_cases * age * age) FILTER (WHERE delta_confirmed_cases > 0))
        AS confirmed_cases_additions_stddev,

    safediv(sum(delta_probable_cases * age) FILTER (WHERE delta_probable_cases > 0),
            sum(delta_probable_cases) FILTER (WHERE delta_probable_cases > 0))
        AS probable_cases_additions,
    aggdev(sum(delta_probable_cases) FILTER (WHERE delta_probable_cases > 0),
    	   sum(delta_probable_cases * age) FILTER (WHERE delta_probable_cases > 0),
    	   sum(delta_probable_cases * age * age) FILTER (WHERE delta_probable_cases > 0))
        AS probable_cases_additions_stddev,

    safediv(sum(delta_suspect_cases * age) FILTER (WHERE delta_suspect_cases > 0),
            sum(delta_suspect_cases) FILTER (WHERE delta_suspect_cases > 0))
        AS suspect_cases_additions,
    aggdev(sum(delta_suspect_cases) FILTER (WHERE delta_suspect_cases > 0),
    	   sum(delta_suspect_cases * age) FILTER (WHERE delta_suspect_cases > 0),
    	   sum(delta_suspect_cases * age * age) FILTER (WHERE delta_suspect_cases > 0))
        AS suspect_cases_additions_stddev,

    -- There is a very weird (nondeterminism?) bug in PRDoH's deaths data processing
    -- that causes weird back-and-forth revisions to the same six dates up to 2020-04-18,
    -- so we just filter those out.
    safediv(sum(delta_deaths * age) FILTER (WHERE delta_deaths > 0 AND datum_date > '2020-04-18'),
            sum(delta_deaths) FILTER (WHERE delta_deaths > 0 AND datum_date > '2020-04-18'))
        AS deaths_additions,
    aggdev(sum(delta_deaths) FILTER (WHERE delta_deaths > 0 AND datum_date > '2020-04-18'),
    	   sum(delta_deaths * age) FILTER (WHERE delta_deaths > 0 AND datum_date > '2020-04-18'),
    	   sum(delta_deaths * age * age) FILTER (WHERE delta_deaths > 0 AND datum_date > '2020-04-18'))
        AS deaths_additions_stddev

FROM bitemporal_agg
-- We exclude the earliest bulletin date because it's artificially late
WHERE bulletin_date > (
    SELECT min(bulletin_date)
    FROM bitemporal_agg
    WHERE delta_confirmed_cases IS NOT NULL
--    AND delta_probable_cases IS NOT NULL
    AND delta_suspect_cases IS NOT NULL
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
--    AND delta_probable_cases IS NOT NULL
	AND suspect_cases IS NOT NULL
	AND deaths IS NOT NULL
), windowed_aggregates AS (
	SELECT
		ba.bulletin_date,
		sum(sum(delta_confirmed_cases)
			FILTER (WHERE delta_confirmed_cases > 0)) OVER bulletin
			AS count_confirmed_cases,
		sum(sum(delta_confirmed_cases * age)
			FILTER (WHERE delta_confirmed_cases > 0)) OVER bulletin
			AS sum_age_confirmed_cases,
		sum(sum(delta_confirmed_cases * age * age)
			FILTER (WHERE delta_confirmed_cases > 0)) OVER bulletin
			AS sumsq_age_confirmed_cases,
		sum(sum(delta_probable_cases)
			FILTER (WHERE delta_probable_cases > 0)) OVER bulletin
			AS count_probable_cases,
		sum(sum(delta_probable_cases * age)
			FILTER (WHERE delta_probable_cases > 0)) OVER bulletin
			AS sum_age_probable_cases,
		sum(sum(delta_probable_cases * age * age)
			FILTER (WHERE delta_probable_cases > 0)) OVER bulletin
			AS sumsq_age_probable_cases,
		sum(sum(delta_suspect_cases)
			FILTER (WHERE delta_suspect_cases > 0)) OVER bulletin
			AS count_suspect_cases,
		sum(sum(delta_suspect_cases * age)
			FILTER (WHERE delta_suspect_cases > 0)) OVER bulletin
			AS sum_age_suspect_cases,
		sum(sum(delta_suspect_cases * age * age)
			FILTER (WHERE delta_suspect_cases > 0)) OVER bulletin
			AS sumsq_age_suspect_cases,
		sum(sum(delta_deaths)
			FILTER (WHERE delta_deaths > 0)) OVER bulletin
			AS count_deaths,
		sum(sum(delta_deaths * age)
			FILTER (WHERE delta_deaths > 0)) OVER bulletin
			AS sum_age_deaths,
		sum(sum(delta_deaths * age * age)
			FILTER (WHERE delta_deaths > 0)) OVER bulletin
			AS sumsq_age_deaths
	FROM bitemporal_agg ba, min_date md
	WHERE ba.bulletin_date > md.bulletin_date
	GROUP BY ba.bulletin_date
	WINDOW bulletin AS (
		ORDER BY ba.bulletin_date RANGE '6 day' PRECEDING
	)
)
SELECT
	wa.bulletin_date,
	safediv(sum_age_confirmed_cases, count_confirmed_cases)
		AS confirmed_cases_additions,
	aggdev(count_confirmed_cases, sum_age_confirmed_cases, sumsq_age_confirmed_cases)
		AS confirmed_cases_additions_stdev,
	safediv(sum_age_probable_cases, count_probable_cases)
		AS probable_cases_additions,
	aggdev(count_probable_cases, sum_age_probable_cases, sumsq_age_probable_cases)
		AS probable_cases_additions_stdev,
	safediv(sum_age_suspect_cases, count_suspect_cases)
		AS suspect_cases_additions,
	aggdev(count_suspect_cases, sum_age_suspect_cases, sumsq_age_suspect_cases)
		AS suspect_cases_additions_stdev,
	safediv(sum_age_deaths, count_deaths)
		AS deaths_additions,
	aggdev(count_deaths, sum_age_deaths, sumsq_age_deaths)
		AS deaths_additions_stdev
FROM windowed_aggregates wa, min_date m
WHERE wa.bulletin_date >= m.bulletin_date + INTERVAL '7' DAY
ORDER BY wa.bulletin_date DESC;

COMMENT ON VIEW products.lateness_7day IS
'An estimate of how late on average new data is, based on the
`bitemporal_agg` view.  Averages over 7-day windows';


CREATE VIEW products.municipal_map AS
SELECT
	municipality,
	popest2019,
	bulletin_date,
	new_cases,
	new_7day_cases,
	CAST(new_cases AS DOUBLE PRECISION)
		/ CASE WHEN previous_7day_cases > 0
			THEN previous_7day_cases
			ELSE 1.0
			END
		AS pct_increase_1day,
	CAST(new_7day_cases AS DOUBLE PRECISION)
		/ CASE WHEN (previous_14day_cases - previous_7day_cases) > 0
			THEN previous_14day_cases - previous_7day_cases
			ELSE 1.0
			END
		AS pct_increase_7day
FROM municipal_agg ma
INNER JOIN canonical_municipal_names cmn
    ON cmn.name = ma.municipality
ORDER BY municipality, bulletin_date;


CREATE VIEW products.lateness_tiers AS
SELECT
	bulletin_date,
	ranges.tier,
	ranges.lo AS tier_order,
	COALESCE(sum(delta_confirmed_cases) FILTER (
		WHERE delta_confirmed_cases > 0
	), 0) AS count
FROM bitemporal_agg ba
INNER JOIN (VALUES (0, 3, '0-3'),
				   (4, 7, '4-7'),
				   (8, 14, '8-14'),
				   (14, NULL, '> 14')) AS ranges (lo, hi, tier)
	ON ranges.lo <= age AND age <= COALESCE(ranges.hi, 2147483647)
WHERE bulletin_date > '2020-04-24'
GROUP BY bulletin_date, ranges.lo, ranges.hi, ranges.tier
ORDER BY bulletin_date DESC, ranges.lo ASC;


CREATE VIEW products.lagged_cfr AS
SELECT
	cases.bulletin_date,
	cases.datum_date collected_date,
	deaths.datum_date death_date,
	avg(cases.confirmed_cases) OVER datum
		+ avg(COALESCE(cases.probable_cases, 0)) OVER datum
		AS smoothed_cases,
	avg(deaths.deaths) OVER datum
		AS smoothed_deaths,
	avg(deaths.deaths) OVER datum
		/ (avg(cases.confirmed_cases) OVER datum
			+ avg(COALESCE(cases.probable_cases, 0)) OVER datum)
		AS lagged_cfr
FROM bitemporal_agg cases
INNER JOIN bitemporal_agg deaths
	ON cases.bulletin_date = deaths.bulletin_date
	AND deaths.datum_date = cases.datum_date + 14
WINDOW datum AS (
	PARTITION BY cases.bulletin_date
	ORDER BY cases.datum_date
	RANGE '13 days' PRECEDING
)
ORDER BY cases.bulletin_date DESC, cases.datum_date DESC;

COMMENT ON VIEW products.lagged_cfr IS 'Lagged case fatality rate';


CREATE VIEW products.covimetro AS
WITH terms AS (
	SELECT
		bulletin_date,
		datum_date,
		sum(confirmed_cases) OVER older AS old_sum,
		lag(confirmed_cases, 3) OVER current AS lag3,
		lag(confirmed_cases, 2) OVER current AS lag2,
		lag(confirmed_cases, 1) OVER current AS lag1,
		confirmed_cases,
		lead(confirmed_cases, 1) OVER current AS lead1,
		lead(confirmed_cases, 2) OVER current AS lead2,
		lead(confirmed_cases, 3) OVER current AS lead3
	FROM bitemporal
	WINDOW older AS (
		PARTITION BY bulletin_date
		ORDER BY datum_date
		RANGE BETWEEN '58 day' PRECEDING
			      AND '4 day' PRECEDING
	), current AS (
		PARTITION BY bulletin_date
		ORDER BY datum_date
		RANGE BETWEEN '3 day' PRECEDING
				  AND '3 day' FOLLOWING
	)
), sums AS (
SELECT
	bulletin_date,
	datum_date,
	confirmed_cases,
	(7 * (old_sum + lag3)
		+ 6 * lag2
		+ 5 * lag1
		+ 4 * confirmed_cases
		+ 3 * lead1
		+ 2 * lead2
		+ lead3) / 7.0
		AS sum_a,
	(7 * old_sum
			+ 6 * lag3
			+ 5 * lag2
			+ 4 * lag1
			+ 3 * confirmed_cases
			+ 2 * lead1
			+ lead2) / 7.0
			AS sum_b
	FROM terms
)
SELECT
	bulletin_date,
	datum_date,
	confirmed_cases,
	sum_a,
	sum_b,
	(sum_a - sum_b) / sum_b
		AS covimetro
FROM sums
ORDER BY bulletin_date DESC, datum_date DESC;

COMMENT ON VIEW products.covimetro IS
'Israel Meléndez''s (@tecnocato) "Covímetro" metric, as best as I can make it.';