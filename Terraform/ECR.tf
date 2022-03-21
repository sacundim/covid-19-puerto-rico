resource "aws_ecr_repository" "ecr_repo" {
  name = "${var.project_name}"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "cleanup" {
  repository = aws_ecr_repository.ecr_repo.name

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
## IAM setup for GitHub Actions to push Docker image to ECR
##

resource "aws_iam_user" "ecr_push_user" {
  name = "covid-19-puerto-rico-github-ecr"
  path = "/"
  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_user_policy" "ecr_push_user_policy" {
  name = "covid-19-puerto-rico-ecr-push"
  user = aws_iam_user.ecr_push_user.name

  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "VisualEditor0",
        "Effect": "Allow",
        "Action": [
          "ecr:CompleteLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:InitiateLayerUpload",
          "ecr:BatchCheckLayerAvailability",
          "ecr:PutImage"
        ],
        "Resource": aws_ecr_repository.ecr_repo.arn
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
