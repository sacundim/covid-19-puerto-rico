output "cloudfront_domain_name" {
  value = aws_cloudfront_distribution.s3_distribution.domain_name
}

output "main_bucket_arn" {
  value = aws_s3_bucket.main_bucket.arn
}

output "logs_bucket_arn" {
  value = aws_s3_bucket.logs_bucket.arn
}

output "main_bucket_ro_arn" {
  value = aws_iam_policy.main_bucket_ro.arn
}

output "main_bucket_rw_arn" {
  value = aws_iam_policy.main_bucket_rw.arn
}

output "logs_bucket_ro_arn" {
  value = aws_iam_policy.logs_bucket_ro.arn
}

