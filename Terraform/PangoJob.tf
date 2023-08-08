#################################################################################
#################################################################################
##
## Pango download task
##

resource "aws_batch_job_definition" "pango_download_and_sync" {
  name = "pango-download-and-sync"
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
      "pango-lineages",
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
      cpuArchitecture: "X86_64" # DuckDB doesn't like ARM64
    }

    networkConfiguration = {
      "assignPublicIp": "ENABLED"
    }
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
          "awslogs-group" = aws_cloudwatch_log_group.covid_19_puerto_rico.name,
          "awslogs-region" = var.aws_region,
          "awslogs-stream-prefix" = "pango-download-and-sync"
      }
    }
  })

  retry_strategy {
    attempts = 1
  }

  timeout {
    # Five minutes
    attempt_duration_seconds = 300
  }
}
