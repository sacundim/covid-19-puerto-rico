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
  platform_capabilities = ["FARGATE"]

  container_properties = jsonencode({
    image = "${data.aws_ecr_image.downloader.registry_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com/${data.aws_ecr_image.downloader.repository_name}:${data.aws_ecr_image.downloader.image_tag}"
    command = ["bioportal-download.sh"],
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
      {"type": "MEMORY", "value": "16384"}
    ]
    networkConfiguration = {
      "assignPublicIp": "ENABLED"
    }
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
  arn = aws_batch_job_queue.batch_queue.arn
  role_arn = aws_iam_role.ecs_events_role.arn

  batch_target {
    job_definition = aws_batch_job_definition.bioportal_download_and_sync.arn
    job_name       = aws_batch_job_definition.bioportal_download_and_sync.name
  }
}



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
    image = "${data.aws_ecr_image.downloader.registry_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com/${data.aws_ecr_image.downloader.repository_name}:${data.aws_ecr_image.downloader.image_tag}"
    command = ["covid19datos-v2.sh"],
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
      {"type": "MEMORY", "value": "12288"}
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
  schedule_expression = "cron(25 16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "covid19datos_v2_daily_download" {
  target_id = "covid19datos-v2-daily-download"
  rule = aws_cloudwatch_event_rule.covid19datos_v2_daily_download.name
  arn = aws_batch_job_queue.batch_queue.arn
  role_arn = aws_iam_role.ecs_events_role.arn

  batch_target {
    job_definition = aws_batch_job_definition.covid19datos_v2_download_and_sync.arn
    job_name       = aws_batch_job_definition.covid19datos_v2_download_and_sync.name
  }
}


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
    command = ["hhs-download"],
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
      {"type": "VCPU", "value": "1"},
      {"type": "MEMORY", "value": "2048"}
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

resource "aws_cloudwatch_event_rule" "hhs_daily_download" {
  name        = "hhs-daily-download"
  description = "Run the daily HHS download."
  schedule_expression = "cron(55 3,15 * * ? *)"
}

resource "aws_cloudwatch_event_target" "hhs_daily_download" {
  target_id = "hhs-daily-download"
  rule = aws_cloudwatch_event_rule.hhs_daily_download.name
  arn = aws_batch_job_queue.batch_queue.arn
  role_arn = aws_iam_role.ecs_events_role.arn

  batch_target {
    job_definition = aws_batch_job_definition.hhs_download_and_sync.arn
    job_name       = aws_batch_job_definition.hhs_download_and_sync.name
  }
}


##############################################################################
##############################################################################
##
## Compute environment
##

resource "aws_batch_compute_environment" "batch_compute" {
  compute_environment_name = "${var.project_name}-compute-environment"
  tags = {
    Project = var.project_name
  }

  compute_resources {
    max_vcpus = 16

    security_group_ids = [
      aws_security_group.outbound_only.id
    ]

    subnets = aws_subnet.subnet.*.id

    type = "FARGATE"
  }

  service_role = aws_iam_role.batch_service_role.arn
  type         = "MANAGED"
  depends_on   = [aws_iam_role_policy_attachment.batch_service_role]
}

resource "aws_batch_job_queue" "batch_queue" {
  name     = "${var.project_name}-job-queue"
  tags = {
    Project = var.project_name
  }
  state    = "ENABLED"
  priority = 1
  compute_environments = [
    aws_batch_compute_environment.batch_compute.arn
  ]
}


##############################################################################
##############################################################################
##
## IAM permissions that we need to grant to our job containers.
##

resource "aws_iam_role" "ecs_job_role" {
  name = "${var.project_name}-batch-job-role"
  description = "Grants permissions to the job containers."
  tags = {
    Project = var.project_name
  }

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_job_role_bucket_rw" {
  role       = aws_iam_role.ecs_job_role.name
  policy_arn = aws_iam_policy.data_bucket_rw.arn
}


##############################################################################
##############################################################################
##
## IAM permissions that we need to grant to AWS services (Batch, ECS,
## Cloudwatch).  These are generally formulaic (AWS-managed policies).
##

resource "aws_iam_role" "ecs_task_role" {
  name = "${var.project_name}-ecs-task-role"
  description = "Allows ECS tasks to call AWS services on your behalf."
  tags = {
    Project = var.project_name
  }

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "ecs_task_role_policy" {
  role       = aws_iam_role.ecs_task_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}


resource "aws_iam_role" "batch_service_role" {
  name = "${var.project_name}-batch-service-role"
  description = "Grants permissions to the Batch service."
  tags = {
    Project = var.project_name
  }

  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Action": "sts:AssumeRole",
        "Effect": "Allow",
        "Principal": {
          "Service": "batch.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "batch_service_role" {
  role       = aws_iam_role.batch_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}


resource "aws_iam_role" "ecs_events_role" {
  name = "${var.project_name}-batch-events-role"
  description = "Used by CloudWatch Events to launch scheduled tasks in Batch."
  path = "/"
  tags = {
    Project = var.project_name
  }

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "batch_events_role_attach" {
  role       = aws_iam_role.ecs_events_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceEventTargetRole"
}


##############################################################################
##############################################################################
##
## Logging
##

resource "aws_cloudwatch_log_group" "covid_19_puerto_rico" {
  name = "covid-19-puerto-rico"
  retention_in_days = 30
  tags = {
    Project = var.project_name
  }
}


