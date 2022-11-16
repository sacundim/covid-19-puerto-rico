#################################################################################
#################################################################################
##
## Bioportal daily download task
##

resource "aws_batch_job_definition" "bioportal_download_and_sync" {
  name = "bioportal-download-and-sync"
  tags = {
    Project = var.project_name
  }
  propagate_tags = true
  type = "container"
  platform_capabilities = ["EC2"]

  container_properties = jsonencode({
    image = "${data.aws_ecr_image.downloader.registry_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com/${data.aws_ecr_image.downloader.repository_name}:${data.aws_ecr_image.downloader.image_tag}"
    command = ["bioportal-download.sh"],
    environment = [
      {
        name = "S3_DATA_URL",
        value = "s3://${var.datalake_bucket_name}"
      },
      {
        name = "ENDPOINT",
        value = var.bioportal_api_url
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
          "awslogs-stream-prefix" = "bioportal-download-and-sync"
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

resource "aws_cloudwatch_event_rule" "bioportal_daily_download" {
  name        = "bioportal-daily-download"
  description = "Run the daily Bioportal download."
  schedule_expression = "cron(55 09 * * ? *)"
}

resource "aws_cloudwatch_event_target" "bioportal_daily_download" {
  target_id = "bioportal-daily-download"
  rule = aws_cloudwatch_event_rule.bioportal_daily_download.name
  arn = aws_batch_job_queue.ec2_amd64.arn
  role_arn = aws_iam_role.ecs_events_role.arn

  batch_target {
    job_definition = aws_batch_job_definition.bioportal_download_and_sync.arn
    job_name       = aws_batch_job_definition.bioportal_download_and_sync.name
  }
}