#################################################################################
#################################################################################
##
## DBT/Athena model build task
##

resource "aws_batch_job_definition" "dbt_run_models" {
  name = "dbt-run-models"
  tags = {
    Project = var.project_name
  }
  propagate_tags = true
  type = "container"
  platform_capabilities = ["FARGATE"]

  container_properties = jsonencode({
    image = "sacundim/covid-19-puerto-rico-dbt:latest"

    fargatePlatformConfiguration = {
      "platformVersion": "LATEST"
    },
    runtimePlatform = {
      operatingSystemFamily: "LINUX",
      cpuArchitecture: "ARM64"
    }
    resourceRequirements = [
      {"type": "VCPU", "value": "0.25"},
      {"type": "MEMORY", "value": "1024"}
    ]
    environment = [
      {
        name = "AWS_REGION",
        value = data.aws_region.current.name
      },
      {
        name = "ATHENA_S3_STAGING_DIR",
        value = "s3://${var.athena_bucket_name}/"
      },
      {
        name = "ATHENA_S3_DATA_DIR",
        value = "s3://${var.iceberg_bucket_name}/"
      },
      {
        name = "ATHENA_S3_SCHEMA",
        value = "covid19_puerto_rico_iceberg"
      },
      {
        name = "ATHENA_DATABASE",
        value = "awsdatacatalog"
      },
      {
        name = "ATHENA_WORK_GROUP",
        value = aws_athena_workgroup.dbt.name
      },
      {
        name = "ATHENA_THREADS",
        value = "20"
      }
    ],

    executionRoleArn = aws_iam_role.ecs_task_role.arn
    jobRoleArn = aws_iam_role.ecs_job_role.arn
    networkConfiguration = {
      "assignPublicIp": "ENABLED"
    }
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
          "awslogs-group" = aws_cloudwatch_log_group.covid_19_puerto_rico.name,
          "awslogs-region" = var.aws_region,
          "awslogs-stream-prefix" = "dbt-run-models"
      }
    }
  })

  retry_strategy {
    attempts = 1
  }

  timeout {
    # Half an hour
    attempt_duration_seconds = 1800
  }
}

resource "aws_iam_role_policy_attachment" "ecs_job_role_athena" {
  role       = aws_iam_role.ecs_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
}

resource "aws_iam_role_policy_attachment" "ecs_job_role_athena_bucket" {
  role       = aws_iam_role.ecs_job_role.name
  policy_arn = aws_iam_policy.athena_bucket_rw.arn
}


resource "aws_scheduler_schedule" "dbt_daily_refresh" {
  name        = "dbt-daily-refresh"
  description = "Run the daily DBT refresh."

  schedule_expression_timezone = "America/Puerto_Rico"
  schedule_expression = "cron(05 14 * * ? *)"
  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn = "arn:aws:scheduler:::aws-sdk:batch:submitJob"
    role_arn = aws_iam_role.eventbridge_scheduler_role.arn

    input = jsonencode({
      "JobDefinition": aws_batch_job_definition.dbt_run_models.arn,
      "JobName": "dbt-run-models",
      "JobQueue": aws_batch_job_queue.fargate_amd64.arn
    })
  }
}