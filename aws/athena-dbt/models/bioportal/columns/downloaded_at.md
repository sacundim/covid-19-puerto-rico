{% docs downloaded_at %}

An enriched field that our downloader code adds, with the UTC timestamp of 
when we downloaded the file this row comes from from the Bioportal API endpoint.
Usually we compute `downloaded_date` and `bulletin_date` fields from this, though
in other occasions it comes from a partition directory name instead.

{% enddocs %}