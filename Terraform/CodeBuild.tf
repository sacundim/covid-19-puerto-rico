resource "aws_codebuild_project" "docker_image" {
  name          = "${var.project_name}-downloader"
  description   = "Build the Docker image for the downloader app"
  build_timeout = "20"
  service_role  = aws_iam_role.codebuild_service.arn

  artifacts {
    type = "NO_ARTIFACTS"
  }

  build_batch_config {
    service_role = aws_iam_role.codebuild_batch_service.arn
    restrictions {
      compute_types_allowed = ["BUILD_GENERAL1_SMALL"]
      maximum_builds_allowed = 8
    }

    # If we don't set this, Terraform keeps toggling it back and
    # forth between null and 2,160
    timeout_in_mins = 480
  }

  environment {
    compute_type                = "BUILD_GENERAL1_SMALL"
    image                       = "aws/codebuild/amazonlinux2-aarch64-standard:2.0"
    type                        = "ARM_CONTAINER"
    privileged_mode             = true

    environment_variable {
      name  = "AWS_DEFAULT_REGION"
      value = data.aws_region.current.name
    }

    environment_variable {
      name  = "AWS_ACCOUNT_ID"
      value = data.aws_caller_identity.current.account_id
    }

    environment_variable {
      name  = "IMAGE_REPO_NAME"
      value = data.aws_ecr_image.downloader.repository_name
    }
  }

  source {
    buildspec = "downloader/buildspec.yml"
    type            = "GITHUB"
    location        = var.github_url
    git_clone_depth = 1
    report_build_status = false # TODO
    git_submodules_config {
      fetch_submodules = false
    }
  }

  source_version = var.github_branch

  logs_config {
    cloudwatch_logs {
      group_name = aws_cloudwatch_log_group.codebuild.name
      stream_name = "downloader"
    }
  }

  tags = {
    Project = var.project_name
  }
}


##############################################################################
##############################################################################
##
## IAM permissions that we need to grant to CodeBuild.
##

resource "aws_iam_role" "codebuild_service" {
  name = "${var.project_name}-downloader-codebuild-service-role"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "codebuild.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "codebuild_service" {
  role = aws_iam_role.codebuild_service.name

  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Resource": [
          aws_cloudwatch_log_group.codebuild.arn,
          "${aws_cloudwatch_log_group.codebuild.arn}:*"
        ],
        "Action": [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
      },
      {
        "Effect": "Allow",
        "Resource": [
          "arn:aws:s3:::codepipeline-us-west-2-*"
        ],
        "Action": [
          "s3:PutObject",
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:GetBucketAcl",
          "s3:GetBucketLocation"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "codebuild_ecr_push" {
  role = aws_iam_role.codebuild_service.name
  policy_arn = aws_iam_policy.ecr_push.arn
}


#
# This one is needed because we're doing a batch build
#
resource "aws_iam_role" "codebuild_batch_service" {
  name = "${var.project_name}-downloader-codebuild-batch-service-role"
  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "Service": "codebuild.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "codebuild_batch_service" {
  role = aws_iam_role.codebuild_batch_service.name

  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Resource": [
          aws_codebuild_project.docker_image.arn
        ],
        "Action": [
          "codebuild:StartBuild",
          "codebuild:StopBuild",
          "codebuild:RetryBuild"
        ]
      }
    ]
  })
}


##############################################################################
##############################################################################
##
## Logging to CloudWatch
##

resource "aws_cloudwatch_log_group" "codebuild" {
  name = "${var.project_name}-codebuild"
  retention_in_days = 30
  tags = {
    Project = var.project_name
  }
}


