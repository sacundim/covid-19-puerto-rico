output "bioportal_job_arn" {
  value = aws_batch_job_definition.bioportal_download_and_sync.arn
}

output "covid19datos_job_arn" {
  value = aws_batch_job_definition.covid19datos_v2_download_and_sync.arn
}

output "dbt_job_arn" {
  value = aws_batch_job_definition.dbt_run_models.arn
}

output "hhs_job_arn" {
  value = aws_batch_job_definition.hhs_download_and_sync.arn
}

output "walgreens_job_arn" {
  value = aws_batch_job_definition.walgreens_download_and_sync.arn
}

output "website_job_arn" {
  value = aws_batch_job_definition.website_generator.arn
}

output "cloudwatch_logs_group_arn" {
  value = aws_cloudwatch_log_group.covid_19_puerto_rico.arn
}