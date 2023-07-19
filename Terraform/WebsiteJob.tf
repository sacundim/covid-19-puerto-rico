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
    image = "sacundim/covid-19-puerto-rico-website:latest"
    command = [
      "covid19pr",
        "--config-file", "environment.yaml",
        "--output-dir", "output",
        "--rclone-destination", "Ref::rclone_destination"
    ]
    parameters = {
      "rclone_destination": ":s3,provider=AWS,env_auth:${var.main_bucket_name}"
    }
    environment = [
      {
        name = "AWS_REGION",
        value = data.aws_region.current.name
      },
      {
        name = "ATHENA_SCHEMA_NAME",
        value = "covid19_puerto_rico_iceberg"
      },
      {
        name = "ATHENA_WORK_GROUP",
        value = aws_athena_workgroup.main.name
      }
    ],

    resourceRequirements = [
      # If these are not strings we get errors
      {"type": "VCPU", "value": "1"},
      {"type": "MEMORY", "value": "4096"}
    ]
    runtimePlatform = {
      operatingSystemFamily: "LINUX",
      cpuArchitecture: "ARM64"
    }

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