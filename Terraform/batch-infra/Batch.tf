##############################################################################
##############################################################################
##
## Fargate compute environment
##

resource "aws_batch_compute_environment" "fargate_amd64" {
  compute_environment_name = "${var.project_name}-fargate-compute-environment"
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

resource "aws_batch_job_queue" "fargate_amd64" {
  name     = "${var.project_name}-fargate-queue"
  tags = {
    Project = var.project_name
  }
  state    = "ENABLED"
  priority = 1
  compute_environments = [
    aws_batch_compute_environment.fargate_amd64.arn
  ]
}


##############################################################################
##############################################################################
##
## EC2 AMD64 compute environment
##

resource "aws_batch_compute_environment" "ec2_amd64" {
  # If we don't do this prefix/create_before_destroy business
  # we get errors when we try to destroy a compute environment.
  # See:
  #
  # * https://github.com/hashicorp/terraform-provider-aws/issues/2044
  # * https://discuss.hashicorp.com/t/error-error-deleting-batch-compute-environment-cannot-delete-found-existing-jobqueue-relationship/5408
  #
  compute_environment_name_prefix = "${var.project_name}-ec2-amd64-"
  lifecycle {
    create_before_destroy = true
  }

  tags = {
    Project = var.project_name
  }

  compute_resources {
    # These tags get applied to the compute resources created:
    tags = {
      Project = var.project_name
    }

    instance_role = aws_iam_instance_profile.ecs_instance_role.arn

    # These have reasonably recent, high-performance processors, and
    # provide a very wide range of memory/cores combinations.
    instance_type = [
      "c6i", "m6i", "r6i",
      # As of 2022-05-09, Batch doesn't yet support these:
      # "c6a", "m6a", "r6a",
      "c5",  "m5",  "r5"
      # The single-threaded performance of these is so much
      # slower that it actually takes longer and costs us more
      # "c5a", "m5a", "r5a"
    ]

    launch_template {
      # We are dangerously close to running out of the default 20GB
      # ephemeral that we get with Fargate, and Batch doesn't as of
      # today (2022-08-14) allow
      launch_template_id = aws_launch_template.ecs_ec2_launch.id
    }

    max_vcpus = 16
    # Important: 0 = compute environment scales down to nothing
    min_vcpus = 0

    security_group_ids = [
      aws_security_group.outbound_only.id,
    ]

    subnets = aws_subnet.subnet.*.id

    type = "EC2"
    allocation_strategy = "BEST_FIT"
  }

  service_role = aws_iam_role.batch_service_role.arn
  type         = "MANAGED"
  depends_on   = [aws_iam_role_policy_attachment.batch_service_role]
}


resource "aws_batch_job_queue" "ec2_amd64" {
  name     = "${var.project_name}-ec2-amd64-queue"
  tags = {
    Project = var.project_name
  }
  state    = "ENABLED"
  priority = 1
  compute_environments = [
    aws_batch_compute_environment.ec2_amd64.arn
  ]
}


resource "aws_launch_template" "ecs_ec2_launch" {
  tags = {
    Project = var.project_name
  }

  block_device_mappings {
    # This must match the AWS ECS image's expectation
    # See: https://aws.amazon.com/premiumsupport/knowledge-center/batch-job-failure-disk-space/
    # See: https://docs.aws.amazon.com/batch/latest/userguide/launch-templates.html
    device_name = "/dev/xvda"

    ebs {
      volume_size = 30
    }
  }
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


resource "aws_iam_instance_profile" "ecs_instance_role" {
  name = "${var.project_name}-ecs-instance-role"
  role = aws_iam_role.ecs_instance_role.name
}

resource "aws_iam_role_policy_attachment" "ecs_instance_role" {
  role       = aws_iam_role.ecs_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_role" "ecs_instance_role" {
  name = "${var.project_name}-ecs-instance-role"

  assume_role_policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
    {
        "Action": "sts:AssumeRole",
        "Effect": "Allow",
        "Principal": {
            "Service": "ec2.amazonaws.com"
        }
    }
    ]
}
EOF
}