#################################################################################
#################################################################################
##
## Covid19Datos V2 daily download task
##

resource "aws_batch_job_definition" "covid19datos_v2_download_and_sync" {
  name = "covid19datos-v2-download-and-sync"
  tags = {
    Project = var.project_name
  }
  propagate_tags = true
  type = "container"
  platform_capabilities = ["FARGATE"]

  container_properties = jsonencode({
    image = "sacundim/covid-19-puerto-rico-downloader:latest"
    command = ["covid19datos-download"],
    environment = [
      {
        name = "S3_DATA_URL",
        value = "s3://${var.datalake_bucket_name}"
      }
    ],
    executionRoleArn = aws_iam_role.ecs_task_role.arn
    jobRoleArn = aws_iam_role.ecs_job_role.arn
    fargatePlatformConfiguration = {
      "platformVersion": "LATEST"
    },
    resourceRequirements = [
      {"type": "VCPU", "value": "2"},
      {"type": "MEMORY", "value": "6144"}
    ]
    networkConfiguration = {
      "assignPublicIp": "ENABLED"
    }
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
          "awslogs-group" = aws_cloudwatch_log_group.covid_19_puerto_rico.name,
          "awslogs-region" = var.aws_region,
          "awslogs-stream-prefix" = "covid19datos-v2-download-and-sync"
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

resource "aws_cloudwatch_event_rule" "covid19datos_v2_daily_download" {
  name        = "covid19datos-v2-daily-download"
  description = "Run the daily covid19datos-v2 download."
  schedule_expression = "cron(55 16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "covid19datos_v2_daily_download" {
  target_id = "covid19datos-v2-daily-download"
  rule = aws_cloudwatch_event_rule.covid19datos_v2_daily_download.name
  arn = aws_batch_job_queue.fargate_amd64.arn
  role_arn = aws_iam_role.ecs_events_role.arn

  batch_target {
    job_definition = aws_batch_job_definition.covid19datos_v2_download_and_sync.arn
    job_name       = aws_batch_job_definition.covid19datos_v2_download_and_sync.name
  }
}