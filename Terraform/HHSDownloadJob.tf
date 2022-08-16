#################################################################################
#################################################################################
##
## HHS daily download task
##

resource "aws_batch_job_definition" "hhs_download_and_sync" {
  name = "hhs-download-and-sync"
  tags = {
    Project = var.project_name
  }
  propagate_tags = true
  type = "container"
  platform_capabilities = ["FARGATE"]

  container_properties = jsonencode({
    image = "${data.aws_ecr_image.downloader.registry_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com/${data.aws_ecr_image.downloader.repository_name}:${data.aws_ecr_image.downloader.image_tag}"
    command = ["hhs-download.sh"]
    environment = [
      {
        name = "S3_DATA_URL",
        value = "s3://${var.datalake_bucket_name}"
      }
    ],
    secrets = [
      {
        name = "SOCRATA_APP_TOKEN"
        valueFrom = aws_secretsmanager_secret.socrata.arn
      }
    ],
    executionRoleArn = aws_iam_role.ecs_task_role.arn
    jobRoleArn = aws_iam_role.ecs_job_role.arn
    fargatePlatformConfiguration = {
      "platformVersion": "LATEST"
    },
    resourceRequirements = [
      {"type": "VCPU", "value": "2"},
      {"type": "MEMORY", "value": "4096"}
    ]
    networkConfiguration = {
      "assignPublicIp": "ENABLED"
    }
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
          "awslogs-group" = aws_cloudwatch_log_group.covid_19_puerto_rico.name,
          "awslogs-region" = var.aws_region,
          "awslogs-stream-prefix" = "hhs-download-and-sync"
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

resource "aws_secretsmanager_secret" "socrata" {
  name = "cdc-app-token"
  description = "Our app token for the CDC and HHS Socrata services. We get throttled without one."
}


resource "aws_cloudwatch_event_rule" "hhs_daily_download" {
  name        = "hhs-daily-download"
  description = "Run the daily HHS download."
  schedule_expression = "cron(25 4,16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "hhs_daily_download" {
  target_id = "hhs-daily-download"
  rule = aws_cloudwatch_event_rule.hhs_daily_download.name
  arn = aws_batch_job_queue.fargate_amd64.arn
  role_arn = aws_iam_role.ecs_events_role.arn

  batch_target {
    job_definition = aws_batch_job_definition.hhs_download_and_sync.arn
    job_name       = aws_batch_job_definition.hhs_download_and_sync.name
  }
}


# We need to grant the Batch service itself (not the task) permission to access
# our secret, which it needs to set up our task
resource "aws_iam_role_policy_attachment" "ecs_task_role_socrata_app_token" {
  role       = aws_iam_role.ecs_task_role.name
  policy_arn = aws_iam_policy.socrata_app_token.arn
}

resource "aws_iam_policy" "socrata_app_token" {
  name        = "${var.project_name}-downloader-socrata-app-token"
  description = "Grant read access to the Socrata app token secret."

  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "secretsmanager:GetSecretValue"
        ],
        "Resource": [
          aws_secretsmanager_secret.socrata.arn
        ]
      }
    ]
  })
}
