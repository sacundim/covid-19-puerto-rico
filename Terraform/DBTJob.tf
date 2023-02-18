#################################################################################
#################################################################################
##
## DBT/Athena model build task
##

locals {
  # If these are not strings we get errors
  cores = "0.25"
  mem_mb = "1024"

  registry = "sacundim"
  repository = "covid-19-puerto-rico-dbt"
  tag = "latest"
}

resource "aws_batch_job_definition" "dbt_run_models" {
  name = "dbt-run-models"
  tags = {
    Project = var.project_name
  }
  propagate_tags = true
  type = "container"
  platform_capabilities = ["FARGATE"]

  container_properties = jsonencode({
    image = "${local.registry}/${local.repository}:${local.tag}"
    environment = [
      {
        name = "AWS_REGION",
        value = data.aws_region.current.name
      },
      {
        name = "ATHENA_S3_STAGING_DIR",
        value = "s3:/${var.athena_bucket_name}/"
      },
      {
        name = "ATHENA_S3_SCHEMA",
        value = "covid19_puerto_rico_model"
      },
      {
        name = "ATHENA_DATABASE",
        value = "awsdatacatalog"
      },
      {
        name = "ATHENA_WORK_GROUP",
        value = aws_athena_workgroup.main.name
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
    resourceRequirements = [
      {"type": "VCPU", "value": local.cores},
      {"type": "MEMORY", "value": local.mem_mb}
    ]
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
