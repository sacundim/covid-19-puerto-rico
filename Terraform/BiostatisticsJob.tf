#################################################################################
#################################################################################
##
## Biostatistics daily download task
##

resource "aws_batch_job_definition" "biostatistics_download_and_sync" {
  name = "biostatistics-download-and-sync"
  tags = {
    Project = var.project_name
  }
  propagate_tags = true
  type = "container"
  platform_capabilities = ["EC2"]

  container_properties = jsonencode({
    image = "sacundim/covid-19-puerto-rico-downloader:latest"
    command = [
      "biostatistics-download"
    ],
    environment = [
      {
        name = "TARGET_BUCKET",
        value = var.datalake_bucket_name
      },
      {
        name = "ENDPOINT",
        value = var.biostatistics_api_url
      }
    ],
    executionRoleArn = aws_iam_role.ecs_task_role.arn
    jobRoleArn = aws_iam_role.ecs_job_role.arn
    resourceRequirements = [
      {"type": "VCPU", "value": "2"},
      {"type": "MEMORY", "value": "6144"}
    ]
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
          "awslogs-group" = aws_cloudwatch_log_group.covid_19_puerto_rico.name,
          "awslogs-region" = var.aws_region,
          "awslogs-stream-prefix" = "biostatistics-download-and-sync"
      }
    }
  })

  retry_strategy {
    attempts = 1
  }

  timeout {
    # 1 hour
    attempt_duration_seconds = 3600
  }
}

resource "aws_cloudwatch_event_rule" "biostatistics_daily_download" {
  name        = "biostatistics-daily-download"
  description = "Run the daily Biostatistics download."
  schedule_expression = "cron(25 10 * * ? *)"
}

resource "aws_cloudwatch_event_target" "biostatistics_daily_download" {
  target_id = "biostatistics-daily-download"
  rule = aws_cloudwatch_event_rule.biostatistics_daily_download.name
  arn = aws_batch_job_queue.ec2_arm64.arn
  role_arn = aws_iam_role.ecs_events_role.arn

  batch_target {
    job_definition = aws_batch_job_definition.biostatistics_download_and_sync.arn
    job_name       = aws_batch_job_definition.biostatistics_download_and_sync.name
  }
}