##############################################################################
##############################################################################
##
## Repo for the website application image
##

resource "aws_ecr_repository" "main_repo" {
  name = var.project_name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "main_repo_cleanup" {
  repository = aws_ecr_repository.main_repo.name

  policy = <<EOF
{
    "rules": [
        {
            "rulePriority": 1,
            "description": "Expire images older than 7 days",
            "selection": {
                "tagStatus": "untagged",
                "countType": "sinceImagePushed",
                "countUnit": "days",
                "countNumber": 7
            },
            "action": {
                "type": "expire"
            }
        }
    ]
}
EOF
}


##############################################################################
##############################################################################
##
## Repo for the DBT project image
##

resource "aws_ecr_repository" "dbt_repo" {
  name = "${var.project_name}-dbt"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "dbt_repo_cleanup" {
  repository = aws_ecr_repository.dbt_repo.name

  policy = <<EOF
{
    "rules": [
        {
            "rulePriority": 1,
            "description": "Expire images older than 7 days",
            "selection": {
                "tagStatus": "untagged",
                "countType": "sinceImagePushed",
                "countUnit": "days",
                "countNumber": 7
            },
            "action": {
                "type": "expire"
            }
        }
    ]
}
EOF
}


##############################################################################
##############################################################################
##
## Repo for the downloader image
##

resource "aws_ecr_repository" "downloader_repo" {
  name = "${var.project_name}-downloader"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

data "aws_ecr_image" "downloader" {
  repository_name = aws_ecr_repository.downloader_repo.name
  image_tag       = "latest"
}

resource "aws_ecr_lifecycle_policy" "downloader_repo_cleanup" {
  repository = aws_ecr_repository.downloader_repo.name

  policy = <<EOF
{
    "rules": [
        {
            "rulePriority": 1,
            "description": "Expire images older than 7 days",
            "selection": {
                "tagStatus": "untagged",
                "countType": "sinceImagePushed",
                "countUnit": "days",
                "countNumber": 7
            },
            "action": {
                "type": "expire"
            }
        }
    ]
}
EOF
}


##############################################################################
##############################################################################
##
## IAM setup for GitHub Actions and CodeBuild to push Docker images to ECR
##

resource "aws_iam_user" "ecr_push_user" {
  name = "covid-19-puerto-rico-github-ecr"
  path = "/"
  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_user_policy_attachment" "ecr_push_user_push" {
  user = aws_iam_user.ecr_push_user.name
  policy_arn = aws_iam_policy.ecr_push.arn
}

resource "aws_iam_policy" "ecr_push" {
  name = "${var.project_name}-downloader-ecr-push"
  description = "Grant ability to push to the ECR repo."
    policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "VisualEditor0",
        "Effect": "Allow",
        "Action": [
          "ecr:BatchCheckLayerAvailability",
          "ecr:BatchGetImage",
          "ecr:CompleteLayerUpload",
          "ecr:GetDownloadUrlForLayer",
          "ecr:InitiateLayerUpload",
          "ecr:PutImage",
          "ecr:UploadLayerPart"
        ],
        "Resource": [
          aws_ecr_repository.main_repo.arn,
          aws_ecr_repository.dbt_repo.arn,
          aws_ecr_repository.downloader_repo.arn
        ]
      },
      {
        "Sid": "VisualEditor1",
        "Effect": "Allow",
        "Action": "ecr:GetAuthorizationToken",
        "Resource": "*"
      }
    ]
  })
}
