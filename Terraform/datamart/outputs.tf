output "data_bucket_arn" {
  value = aws_s3_bucket.data_bucket.arn
}

output "athena_bucket_arn" {
  value = aws_s3_bucket.athena_bucket.arn
}

output "testing_bucket_arn" {
  value = aws_s3_bucket.testing_bucket.arn
}

output "data_bucket_ro_arn" {
  value = aws_iam_policy.data_bucket_ro.arn
}

output "data_bucket_rw_arn" {
  value = aws_iam_policy.data_bucket_rw.arn
}

output "athena_bucket_rw_arn" {
  value = aws_iam_policy.athena_bucket_rw.arn
}

output "athena_workgroup_main_name" {
  value = aws_athena_workgroup.main.name
}

output "athena_workgroup_dbt_name" {
  value = aws_athena_workgroup.dbt.name
}