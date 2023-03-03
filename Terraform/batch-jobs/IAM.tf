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
  policy_arn = data.aws_iam_policy.data_bucket_rw.arn
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
## IAM permissions that we need to grant to AWS services (Batch, ECS,
## Cloudwatch).  These are generally either:
##
## 1. Formulaic (AWS-managed policies);
## 2. Permissions that are needed not by our container code per-se, but
##    rather by Batch or ECS to set up our container.  For example, if
##    our container needs a secret from Secrets Manager, then ECS needs
##    permission to read that secret in order to wire it in.
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