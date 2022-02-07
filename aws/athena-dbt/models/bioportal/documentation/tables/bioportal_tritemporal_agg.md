{% docs bioportal_tritemporal_agg %}

# Specimens analysis

These perform fairly straightforward aggregation of Bioportal data, without 
deduplicating test specimens taken during the same test encounter.  For a 
definition of "specimen" vs. "encounter" see:

* https://covidtracking.com/analysis-updates/test-positivity-in-the-us-is-a-mess

This table is counts of tests from Bioportal, classified along four
time axes:

* `bulletin_date`, which is the data as-of date (that
  allows us to "rewind" data to earlier state);

* `collected_date`, which is when test samples were taken

* `reported_date`, which is when the laboratory knew the
  test result (but generally earlier than it communicated
  it to PRDoH).

* `received_date`, which is when Bioportal says they received
  the actual test result.

Yes, I know the name says "tritemporal" and now it's four
time axes.  Not gonna rename it right now.

{% enddocs %}