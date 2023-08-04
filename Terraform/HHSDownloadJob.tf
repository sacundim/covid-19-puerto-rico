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

  parameters = {
    "rclone_destination": ":s3,provider=AWS,env_auth:${var.datalake_bucket_name}"
  }

  container_properties = jsonencode({
    image = "sacundim/covid-19-puerto-rico-downloader:latest"
    command = [
      "hhs-socrata-download",
        "--socrata-app-token-env-var", "SOCRATA_APP_TOKEN",
        "--s3-sync-dir", "s3_sync_dir",
        "--rclone-destination", "Ref::rclone_destination"
    ]
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
      {"type": "MEMORY", "value": "8192"}
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
