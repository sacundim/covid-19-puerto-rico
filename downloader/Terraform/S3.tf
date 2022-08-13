data "aws_s3_bucket" "data_bucket" {
  bucket = var.datalake_bucket_name
}

data "aws_s3_bucket" "testing_bucket" {
  bucket = var.testing_bucket_name
}
