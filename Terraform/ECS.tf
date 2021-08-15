resource "aws_ecs_cluster" "main" {
  name = "${var.project_name}-ecs"
  tags = {
    "Project" = var.project_name
  }
  capacity_providers = ["FARGATE", "FARGATE_SPOT"]
  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    base = 0
    weight = 1
  }
}

resource "aws_cloudwatch_log_group" "covid_19_puerto_rico" {
  name = "covid-19-puerto-rico"
  retention_in_days = 30
  tags = {
    Project = var.project_name
  }
}


#################################################################################
#################################################################################
##
## Bioportal daily download task
##

resource "aws_ecs_task_definition" "bioportal_download_and_sync" {
  family = "bioportal-download-and-sync"
  tags = {
    Project = var.project_name
  }
  requires_compatibilities = ["FARGATE"]
  task_role_arn = aws_iam_role.ecs_task_role.arn
  execution_role_arn = aws_iam_role.ecs_task_execution_role.arn
  cpu = 2048
  memory = 12288
  network_mode = "awsvpc"
  container_definitions = jsonencode([
    {
      name = "bioportal-downloader",
      image = "${data.aws_ecr_image.downloader.registry_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com/${data.aws_ecr_image.downloader.repository_name}:${data.aws_ecr_image.downloader.image_tag}"
      cpu = 2048,
      memoryReservation = 12288,
      essential = true,
      command = ["bioportal-download.sh"],
      environment = [
        {
          name = "S3_DATA_URL",
          value = "s3://${var.datalake_bucket_name}"
        }
      ],
      logConfiguration = {
        "logDriver" = "awslogs",
        "options" = {
          "awslogs-group" = aws_cloudwatch_log_group.covid_19_puerto_rico.name,
          "awslogs-region" = "us-west-2",
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])
}

resource "aws_cloudwatch_event_rule" "bioportal_daily_download" {
  name        = "bioportal-daily-download"
  description = "Run the daily Bioportal download."
  schedule_expression = "cron(55 09 * * ? *)"
}

resource "aws_cloudwatch_event_target" "bioportal_daily_download" {
  target_id = "bioportal-daily-download"
  rule = aws_cloudwatch_event_rule.bioportal_daily_download.name
  arn = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events_role.arn

  ecs_target {
    launch_type         = "FARGATE"
    platform_version    = "LATEST"
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.bioportal_download_and_sync.arn

    network_configuration {
      assign_public_ip = true
      subnets = aws_subnet.subnet.*.id
      security_groups = [aws_security_group.outbound_only.id]
    }
  }
}


#################################################################################
#################################################################################
##
## HHS daily download task
##

resource "aws_ecs_task_definition" "hhs_download_and_sync" {
  family = "hhs-download-and-sync"
  tags = {
    Project = var.project_name
  }
  requires_compatibilities = ["FARGATE"]
  task_role_arn = aws_iam_role.ecs_task_role.arn
  execution_role_arn = aws_iam_role.ecs_task_execution_role.arn
  cpu = 1024
  memory = 2048
  network_mode = "awsvpc"
  container_definitions = jsonencode([
    {
      name = "hhs-downloader",
      image = "${data.aws_ecr_image.downloader.registry_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com/${data.aws_ecr_image.downloader.repository_name}:${data.aws_ecr_image.downloader.image_tag}"
      cpu = 1024,
      memoryReservation = 2048,
      essential = true,
      command = ["hhs-download"],
      environment = [
        {
          name = "S3_DATA_URL",
          value = "s3://${var.datalake_bucket_name}"
        }
      ],
      logConfiguration = {
        "logDriver" = "awslogs",
        "options" = {
          "awslogs-group" = aws_cloudwatch_log_group.covid_19_puerto_rico.name,
          "awslogs-region" = "us-west-2",
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])
}

resource "aws_cloudwatch_event_rule" "hhs_daily_download" {
  name        = "hhs-daily-download"
  description = "Run the daily HHS download."
  schedule_expression = "cron(55 3,15 * * ? *)"
}

resource "aws_cloudwatch_event_rule" "hhs_weekend_download" {
  name        = "hhs-weekend-download"
  description = "Run the additional weekend HHS downloads (since data is often late)."
  schedule_expression = "cron(00 19,21 ? * SUN,SAT *)"
}

resource "aws_cloudwatch_event_target" "hhs_daily_download" {
  for_each = toset([
    aws_cloudwatch_event_rule.hhs_daily_download.name,
    aws_cloudwatch_event_rule.hhs_weekend_download.name])

  target_id = "hhs-daily-download"
  rule = each.key
  arn = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events_role.arn

  ecs_target {
    launch_type         = "FARGATE"
    platform_version    = "LATEST"
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.hhs_download_and_sync.arn

    network_configuration {
      assign_public_ip = true
      subnets = aws_subnet.subnet.*.id
      security_groups = [aws_security_group.outbound_only.id]
    }
  }
}


#################################################################################
#################################################################################
##
## Covid19Datos V2 daily download task
##

resource "aws_ecs_task_definition" "covid19datos_v2_download_and_sync" {
  family = "covid19datos-v2-download-and-sync"
  tags = {
    Project = var.project_name
  }
  requires_compatibilities = ["FARGATE"]
  task_role_arn = aws_iam_role.ecs_task_role.arn
  execution_role_arn = aws_iam_role.ecs_task_execution_role.arn
  cpu = 2048
  memory = 12288
  network_mode = "awsvpc"
  container_definitions = jsonencode([
    {
      name = "covid19datos-v2-downloader",
      image = "${data.aws_ecr_image.downloader.registry_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com/${data.aws_ecr_image.downloader.repository_name}:${data.aws_ecr_image.downloader.image_tag}"
      cpu = 2048,
      memoryReservation = 12288,
      essential = true,
      command = ["covid19datos-v2.sh"],
      environment = [
        {
          name = "S3_DATA_URL",
          value = "s3://${var.datalake_bucket_name}"
        }
      ],
      logConfiguration = {
        "logDriver" = "awslogs",
        "options" = {
          "awslogs-group" = aws_cloudwatch_log_group.covid_19_puerto_rico.name,
          "awslogs-region" = "us-west-2",
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])
}

resource "aws_cloudwatch_event_rule" "covid19datos_v2_daily_download" {
  name        = "covid19datos-v2-daily-download"
  description = "Run the daily covid19datos-v2 download."
  schedule_expression = "cron(55 09 * * ? *)"
}

resource "aws_cloudwatch_event_target" "covid19datos_v2_daily_download" {
  target_id = "covid19datos-v2-daily-download"
  rule = aws_cloudwatch_event_rule.covid19datos_v2_daily_download.name
  arn = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events_role.arn

  ecs_target {
    launch_type         = "FARGATE"
    platform_version    = "LATEST"
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.covid19datos_v2_download_and_sync.arn

    network_configuration {
      assign_public_ip = true
      subnets = aws_subnet.subnet.*.id
      security_groups = [aws_security_group.outbound_only.id]
    }
  }
}



#################################################################################
#################################################################################
##
## IAM Roles
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

resource "aws_iam_role_policy_attachment" "ecs_task_role_ecr_ro" {
  role       = aws_iam_role.ecs_task_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_iam_role_policy_attachment" "ecs_task_role_s3_rw" {
  role       = aws_iam_role.ecs_task_role.name
  policy_arn = aws_iam_policy.data_bucket_rw.arn
}



resource "aws_iam_role" "ecs_task_execution_role" {
  name = "${var.project_name}-ecs-task-execution-role"
  path = "/"
  tags = {
    Project = var.project_name
  }

  assume_role_policy = <<EOF
{
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_role_attach" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}



resource "aws_iam_role" "ecs_events_role" {
  name = "${var.project_name}-ecs-events-role"
  description = "Used by CloudWatch Events to launch scheduled tasks in our ECS cluster."
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

resource "aws_iam_role_policy_attachment" "ecs_events_role_attach" {
  role       = aws_iam_role.ecs_events_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceEventsRole"
}
