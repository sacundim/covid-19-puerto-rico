# AWS Environment

There is a Terraform setup in [the top-level `Terraform` directory](../Terraform)
that manages this infrastructure, but be warned that as of now it was
created after a bunch of it had been created manually so it might not
be 100% reproducible as-is.


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

These are not (as of now) managed by Terraform, so they must be run by hand
with a SQL client (e.g., [DBeaver](https://dbeaver.io/)).


## Automated Bioportal download

The Terraform setup creates an [ECS](https://aws.amazon.com/ecs/) cluster and 
scheduled [Fargate](https://aws.amazon.com/fargate/) task that downloads from 
the Bioportal API endpoints at noon Puerto Rico time every day, converts the
downloads to Parquet, and syncs them to the datalake S3 bucket.  This requires
