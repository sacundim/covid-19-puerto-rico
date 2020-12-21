# AWS Environment

There isn't any infrastructure-as-code as of yet, so this is a
textual description of the setup.


## S3 buckets

1. `covid-19-puerto-rico`: This is expected to become the place
   where we serve web content from.
2. `covid-19-puerto-rico-data`: This is an archive of the external
   data files that we use, mostly the Bioportal downloads.  Rules
   should be set up to maximize protection of data, e.g., set up 
   data versioning so that if we delete stuff by mistake we can 
   get it back. (That's why it's a separate bucket too.)
3. `covid-19-puerto-rico-athena`: A bucket for Athena to dump its 
    result sets and CTAS tables to.  Athena should be configured 
    to use this bucket (using its "workgroups" feature).  To save
    on costs, the bucket itself can/should be set to automatically 
    expire content relatively often.


## Glue Catalog databases

The [Athena SQL scripts](athena/) take care of creating and recreating these
two Glue Catalog databases:

* `covid_pr_sources`: Tables that serve as thin wrappers around the
  external source data files we archive in `s3://covid-19-puerto-rico-data/`.
* `covid_pr_etl`: All tables and views that we build off the external data.


## Lambda (TODO)

Write a Lambda function to do the daily Bioportal downloads and the
Parquet conversion.