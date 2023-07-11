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


resource "aws_cloudwatch_event_rule" "dbt_daily_refresh" {
  name        = "dbt-daily-refresh"
  description = "Run the daily DBT refresh."
  # 10:05am Pacific Standard Time
  schedule_expression = "cron(05 18 * * ? *)"
}

resource "aws_cloudwatch_event_target" "dbt_daily_refresh" {
  target_id = "hhs-daily-download"
  rule = aws_cloudwatch_event_rule.dbt_daily_refresh.name
  arn = aws_batch_job_queue.fargate_amd64.arn
  role_arn = aws_iam_role.ecs_events_role.arn

  batch_target {
    job_definition = aws_batch_job_definition.dbt_run_models.arn
    job_name       = aws_batch_job_definition.dbt_run_models.name
  }
}
