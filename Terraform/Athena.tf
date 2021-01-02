resource "aws_athena_workgroup" "main" {
  name = "covid-19-puerto-rico"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_bucket.bucket}/"
    }
  }
}
