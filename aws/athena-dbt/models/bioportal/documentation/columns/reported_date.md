{% docs reported_date %}

The date of the lab result, which means whatever the labs mean by that date.  With PCR
this usually is the date that they processed the test sample.  With other test types
it's not at all clear what they mean by it.

We compute this off the raw Bioportal `resultDate` field, with a bit of cleanup.

{% enddocs %}