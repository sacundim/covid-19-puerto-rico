resource "aws_athena_workgroup" "main" {
  name = var.project_name
  tags = {
    Project = var.project_name
  }

  configuration {
    engine_version {
      selected_engine_version = "AUTO"
    }
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_bucket.bucket}/"
    }
  }
}
