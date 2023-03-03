data "aws_batch_job_queue" "ec2" {
  name = var.ec2_queue_name
}

data "aws_batch_job_queue" "fargate" {
  name = var.fargate_queue_name
}

data "aws_iam_policy" "main_bucket_rw" {
  arn = var.main_bucket_rw_arn
}

data "aws_iam_policy" "data_bucket_ro" {
  arn = var.data_bucket_ro_arn
}

data "aws_iam_policy" "data_bucket_rw" {
  arn = var.data_bucket_rw_arn
}

data "aws_iam_policy" "athena_bucket_rw" {
  arn = var.athena_bucket_rw_arn
}

data "aws_region" "current" {}
