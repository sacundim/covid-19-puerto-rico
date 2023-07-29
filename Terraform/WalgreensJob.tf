#################################################################################
#################################################################################
##
## Walgreens/Aegis daily download task
##

resource "aws_batch_job_definition" "walgreens_download_and_sync" {
  name = "walgreens-download-and-sync"
  tags = {
    Project = var.project_name
  }
  propagate_tags = true
  type = "container"
  platform_capabilities = ["FARGATE"]

  parameters = {
    "rclone_destination": ":s3,provider=AWS,env_auth:${var.datalake_bucket_name}"
  }

  container_properties = jsonencode({
    image = "sacundim/covid-19-puerto-rico-downloader:latest"
    command = [
      "walgreens-download",
        "--s3-sync-dir", "s3_sync_dir",
        "--rclone-destination", "Ref::rclone_destination"
    ],

    executionRoleArn = aws_iam_role.ecs_task_role.arn
    jobRoleArn = aws_iam_role.ecs_job_role.arn

    fargatePlatformConfiguration = {
      "platformVersion": "LATEST"
    },
    resourceRequirements = [
      {"type": "VCPU", "value": "0.25"},
      {"type": "MEMORY", "value": "512"}
    ]
    runtimePlatform = {
      operatingSystemFamily: "LINUX",
      cpuArchitecture: "ARM64"
    }

    networkConfiguration = {
      "assignPublicIp": "ENABLED"
    }
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
          "awslogs-group" = aws_cloudwatch_log_group.covid_19_puerto_rico.name,
          "awslogs-region" = var.aws_region,
          "awslogs-stream-prefix" = "walgreens-download-and-sync"
      }
    }
  })

  retry_strategy {
    attempts = 1
  }

  timeout {
    # 10 minutes, which is overkill
    attempt_duration_seconds = 600
  }
}


resource "aws_scheduler_schedule" "walgreens_daily_download" {
  name        = "walgreens-daily-download"
  description = "Run the daily Walgreens download."

  schedule_expression_timezone = "America/Puerto_Rico"
  schedule_expression = "cron(25 12 * * ? *)"
  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn = "arn:aws:scheduler:::aws-sdk:batch:submitJob"
    role_arn = aws_iam_role.eventbridge_scheduler_role.arn

    input = jsonencode({
      "JobDefinition": aws_batch_job_definition.walgreens_download_and_sync.arn,
      "JobName": "walgreens-download-and-sync",
      "JobQueue": aws_batch_job_queue.fargate_amd64.arn
    })
  }
}