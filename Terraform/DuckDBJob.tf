#################################################################################
#################################################################################
##
## Biostatistics daily download task
##

resource "aws_batch_job_definition" "duckdb_job" {
  name = "${var.project_name}-duckdb"
  tags = {
    Project = var.project_name
  }
  propagate_tags = true
  type = "container"
  platform_capabilities = ["EC2"]

  container_properties = jsonencode({
    image = "sacundim/covid-19-puerto-rico-duckdb:latest"
    command = [
      "./run-models.sh", "--target", "aws"
    ],
    environment = [
      {
        name = "DATA_LAKE_ROOT",
        value = "s3://${var.datalake_bucket_name}"
      },
      {
        name = "OUTPUT_ROOT",
        value = "s3://${var.testing_bucket_name}/duckdb"
      },
      {
        name = "EARLIEST_DOWNLOADED_DATE",
        value = "2023-07-15"
      },
      {
        name = "LATEST_DOWNLOADED_DATE",
        value = "2023-07-31"
      },
      {
        name = "DBT_THREADS",
        # Has to be a string
        value = "4"
      },
      {
        name = "S3_REGION",
        value = data.aws_region.current.name
      }
    ],

    executionRoleArn = aws_iam_role.ecs_task_role.arn
    jobRoleArn = aws_iam_role.ecs_job_role.arn
    resourceRequirements = [
      {"type": "VCPU", "value": "16"},
      {"type": "MEMORY", "value": "122880"}
    ]
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
          "awslogs-group" = aws_cloudwatch_log_group.covid_19_puerto_rico.name,
          "awslogs-region" = var.aws_region,
          "awslogs-stream-prefix" = "${var.project_name}-duckdb"
      }
    }
  })

  timeout {
    # half hour
    attempt_duration_seconds = 1800
  }
}


#########################################################################################
#########################################################################################
##
## Since we want to play with really fancy instances in this job, we set up its own
## compute environment
##

resource "aws_batch_compute_environment" "duckdb_ec2_amd64" {
  # If we don't do this prefix/create_before_destroy business
  # we get errors when we try to destroy a compute environment.
  # See:
  #
  # * https://github.com/hashicorp/terraform-provider-aws/issues/2044
  # * https://discuss.hashicorp.com/t/error-error-deleting-batch-compute-environment-cannot-delete-found-existing-jobqueue-relationship/5408
  #
  compute_environment_name_prefix = "${var.project_name}-duckdb-ec2-amd64-"
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

    instance_type = [
      "c5", "m5", "r5",
      "c5n", "m5n", "r5n",
      "c6i", "m6i", "r6i",
      "c6in", "m6in", "r6in",
    ]

    launch_template {
      launch_template_id = aws_launch_template.duckdb_ec2.id
    }

    max_vcpus = 128
    # Important: 0 = compute environment scales down to nothing
    min_vcpus = 0

    security_group_ids = [
      aws_security_group.outbound_only.id,
    ]

    subnets = aws_subnet.subnet.*.id

    type = "SPOT"
    allocation_strategy = "BEST_FIT"

    # If not set, it will bid up to 100% of On-Demand:
    bid_percentage = 67

    spot_iam_fleet_role = aws_iam_role.spot_fleet_tagging_role.arn
  }

  service_role = aws_iam_role.batch_service_role.arn
  type         = "MANAGED"
  depends_on   = [aws_iam_role_policy_attachment.batch_service_role]
}


resource "aws_batch_job_queue" "duckdb_ec2_amd64" {
  name     = "${var.project_name}-duckdb-amd64-queue"
  tags = {
    Project = var.project_name
  }
  state    = "ENABLED"
  priority = 1
  compute_environments = [
    aws_batch_compute_environment.duckdb_ec2_amd64.arn
  ]
}


resource "aws_launch_template" "duckdb_ec2" {
  tags = {
    Project = var.project_name
  }

  block_device_mappings {
    # This must match the AWS ECS image's expectation
    # See: https://aws.amazon.com/premiumsupport/knowledge-center/batch-job-failure-disk-space/
    # See: https://docs.aws.amazon.com/batch/latest/userguide/launch-templates.html
    device_name = "/dev/xvda"

    ebs {
      volume_size = 200
    }
  }
}


#########################################################################################
#########################################################################################
##
## EC2 Spot stuff
##

resource "aws_iam_role" "spot_fleet_tagging_role" {
  name = "${var.project_name}-spot-fleet-tagging-role"
  description = "Used by AWS Batch to tag EC2 Spot Fleets."
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
        "Service": "spotfleet.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "spot_fleet_tagging_role_attach" {
  role       = aws_iam_role.spot_fleet_tagging_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole"
}

/*
resource "aws_iam_service_linked_role" "spot" {
  custom_suffix = "-duckdb"
  description      = "Default EC2 Spot Service Linked Role"
  aws_service_name = "spot.amazonaws.com"
}

resource "aws_iam_service_linked_role" "spotfleet" {
  aws_service_name = "spotfleet.amazonaws.com"
}
*/