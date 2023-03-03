include "root" {
  path = find_in_parent_folders()
}

dependency "batch_infra" {
  config_path = "../batch-infra"
}

dependency "datamart" {
  config_path = "../datamart"
}

dependency "web_infra" {
  config_path = "../web-infra"
}

inputs = {
  main_bucket_rw_arn = dependency.web_infra.main_bucket_rw_arn
  data_bucket_ro_arn = dependency.datamart.data_bucket_ro_arn
  data_bucket_rw_arn = dependency.datamart.data_bucket_rw_arn
  athena_bucket_rw_arn = dependency.datamart.athena_bucket_rw_arn
}