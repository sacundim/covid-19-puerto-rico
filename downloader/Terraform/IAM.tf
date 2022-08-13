resource "aws_iam_policy" "data_bucket_rw" {
  name        = "${var.project_name}-downloader-data-writer"
  description = "Grant list/read access to the S3 data bucket."

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          data.aws_s3_bucket.data_bucket.arn,
          "${data.aws_s3_bucket.data_bucket.arn}/*",
          data.aws_s3_bucket.testing_bucket.arn,
          "${data.aws_s3_bucket.testing_bucket.arn}/*"
        ]
      }
    ]
  })
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


##############################################################################
##############################################################################
##
## IAM setup for GitHub Actions to push Docker image to ECR
##

resource "aws_iam_policy" "ecr_push" {
  name = "${var.project_name}-downloader-ecr-push"
  description = "Grnt ability to push to the ECR repo."
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
        "Resource": aws_ecr_repository.downloader_repo.arn
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

resource "aws_iam_user" "ecr_push_user" {
  name = "covid-19-puerto-rico-downloader-github-ecr"
  path = "/"
  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_user_policy_attachment" "ecr_push_user_push" {
  user = aws_iam_user.ecr_push_user.name
  policy_arn = aws_iam_policy.ecr_push.arn
}