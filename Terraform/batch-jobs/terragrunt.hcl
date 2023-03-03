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
  ec2_queue_name = dependency.batch_infra.ec2_queue_name
  fargate_queue_name = dependency.batch_infra.fargate_queue_name
  athena_workgroup_main_name = dependency.datamart.athena_workgroup_main_name
  athena_workgroup_dbt_name = dependency.datamart.athena_workgroup_dbt_name
  data_bucket_ro_arn = dependency.datamart.data_bucket_ro_arn
  data_bucket_rw_arn = dependency.datamart.data_bucket_rw_arn
  athena_bucket_rw_arn = dependency.datamart.athena_bucket_rw_arn
  main_bucket_rw_arn = dependency.web_infra.main_bucket_rw_arn
}