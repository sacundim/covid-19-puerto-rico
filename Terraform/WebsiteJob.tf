#################################################################################
#################################################################################
##
## DBT/Athena model build task
##

resource "aws_batch_job_definition" "website_generator" {
  name = "website-generator"
  tags = {
    Project = var.project_name
  }
  propagate_tags = true
  type = "container"
  platform_capabilities = ["FARGATE"]

  container_properties = jsonencode({
    image = "ghcr.io/sacundim/covid-19-puerto-rico-website:latest"
    command = ["run-and-sync.sh"]
    resourceRequirements = [
      # If these are not strings we get errors
      {"type": "VCPU", "value": "1"},
      {"type": "MEMORY", "value": "4096"}
    ]
    environment = [
      {
        name = "MAIN_BUCKET",
        value = var.main_bucket_name
      },
      {
        name = "AWS_REGION",
        value = data.aws_region.current.name
      },
      {
        name = "ATHENA_S3_STAGING_DIR",
        value = "s3://${var.athena_bucket_name}/"
      },
      {
        name = "ATHENA_SCHEMA_NAME",
        value = "covid19_puerto_rico_model"
      }
    ],
    executionRoleArn = aws_iam_role.ecs_task_role.arn
    jobRoleArn = aws_iam_role.ecs_job_role.arn
    fargatePlatformConfiguration = {
      "platformVersion": "LATEST"
    },
    networkConfiguration = {
      "assignPublicIp": "ENABLED"
    }
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
          "awslogs-group" = aws_cloudwatch_log_group.covid_19_puerto_rico.name,
          "awslogs-region" = var.aws_region,
          "awslogs-stream-prefix" = "website-generator"
      }
    }
  })

  retry_strategy {
    attempts = 1
  }

  timeout {
    # 45 minutes
    attempt_duration_seconds = 2700
  }
}

resource "aws_iam_role_policy_attachment" "ecs_job_role_main_bucket" {
  role       = aws_iam_role.ecs_job_role.name
  policy_arn = aws_iam_policy.main_bucket_rw.arn
}