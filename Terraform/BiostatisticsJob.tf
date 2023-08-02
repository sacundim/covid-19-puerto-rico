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

  parameters = {
    "rclone_destination": ":s3,provider=AWS,env_auth:${var.datalake_bucket_name}"
  }

  container_properties = jsonencode({
    image = "sacundim/covid-19-puerto-rico-downloader:latest"
    command = [
      "biostatistics-download",
        "--s3-sync-dir", "s3_sync_dir",
        "--rclone-destination", "Ref::rclone_destination"
    ],
    environment = [
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


resource "aws_scheduler_schedule" "biostatistics_daily_download" {
  name        = "biostatistics-daily-download"
  description = "Run the daily Biostatistics download."

  schedule_expression_timezone = "America/Puerto_Rico"
  schedule_expression = "cron(55 5 * * ? *)"
  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn = "arn:aws:scheduler:::aws-sdk:batch:submitJob"
    role_arn = aws_iam_role.eventbridge_scheduler_role.arn

    input = jsonencode({
      "JobDefinition": aws_batch_job_definition.biostatistics_download_and_sync.arn,
      "JobName": "biostatistics-download-and-sync",
      "JobQueue": aws_batch_job_queue.ec2_arm64.arn
    })
  }
}
