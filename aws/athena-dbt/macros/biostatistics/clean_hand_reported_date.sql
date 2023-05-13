--
-- Correct dates that look like they have the correct month and day
-- but wrong year, or correct year and day but incorrect month, etc.
--
{% macro clean_hand_reported_date(
    raw_date,
    year_guess_days_off,
    month_guess_days_off,
    alternate_raw_date,
    received_date
)
%}
CASE
    -- Dates that very much look like somebody entered a date of birth
    -- instead of the test sample collection date.
    WHEN DATE '1898-01-01' <= {{raw_date}}
            AND {{raw_date}} < DATE '2000-01-01'
    THEN CASE
            WHEN {{alternate_raw_date}} > DATE '2020-03-01'
            THEN {{alternate_raw_date}}
         END

    -- Dates that very much look like somebody mistyped the year.
    WHEN DATE '2000-01-01' <= {{raw_date}}
            AND {{raw_date}} < DATE '2020-01-01'
    THEN CASE
            -- If the mistyped-year guess falls within three weeks then go for it
            WHEN {{year_guess_days_off}} <= 21
            THEN date(date_add('day', -{{year_guess_days_off}}, {{received_date}}))
            -- Dates that very much look like somebody mistyped the month as well.
            -- Here the year criterion makes it so that we don't go too crazy.
            WHEN {{year_guess_days_off}} <= 42
                    AND {{month_guess_days_off}} <= 21
            THEN date(date_add('day', -{{month_guess_days_off}}, {{received_date}}))
         END
    -- This catches cases where the `{{raw_date}}` is in the future
    -- compared to the `{{received_date}}`
    ELSE least({{raw_date}}, {{received_date}})
END
{% endmacro %}
