resource "aws_athena_workgroup" "main" {
  name = var.project_name
  tags = {
    Project = var.project_name
  }

  configuration {
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_bucket.bucket}/"
    }
  }
}

resource "aws_athena_workgroup" "dbt" {
  // DBT doesn't like the `output_location` setting in our main
  // workgroup, so we make a whole nother one for it
  name = "${var.project_name}-dbt"
  tags = {
    Project = var.project_name
  }

  configuration {
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }
}