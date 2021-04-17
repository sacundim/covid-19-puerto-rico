#################################################################################
#################################################################################
##
## Policies
##

resource "aws_iam_policy" "data_bucket_ro" {
  name        = "${var.project_name}-data-reader"
  description = "Grant list/read access to the S3 data bucket."

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:GetObjectTagging",
          "s3:ListBucket"
        ],
        Resource = [
          aws_s3_bucket.data_bucket.arn,
          "${aws_s3_bucket.data_bucket.arn}/*"
        ]
      }
    ]
  })
}


resource "aws_iam_policy" "data_bucket_rw" {
  name        = "${var.project_name}-data-writer"
  description = "Grant list/read access to the S3 data bucket."

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:PutObjectTagging",
          "s3:GetObject",
          "s3:GetObjectTagging",
          "s3:ListBucket"
        ],
        Resource = [
          aws_s3_bucket.data_bucket.arn,
          "${aws_s3_bucket.data_bucket.arn}/*",
          aws_s3_bucket.testing_bucket.arn,
          "${aws_s3_bucket.testing_bucket.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "athena_bucket_rw" {
  name        = "${var.project_name}-athena-bucket"
  description = "Grant list/read/write access to the S3 Athena bucket."

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:PutObjectTagging",
          "s3:GetObject",
          "s3:GetObjectTagging",
          "s3:ListBucket"
        ],
        Resource = [
          aws_s3_bucket.athena_bucket.arn,
          "${aws_s3_bucket.athena_bucket.arn}/*"
        ]
      }
    ]
  })
}


#################################################################################
#################################################################################
##
## Roles
##

resource "aws_iam_role" "ecs_service_role" {
  name = "AWSServiceRoleForECS"
  path = "/aws-service-role/ecs.amazonaws.com/"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "ecs_service_role_attach" {
  role       = aws_iam_role.ecs_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/aws-service-role/AmazonECSServiceRolePolicy"
}



#################################################################################
#################################################################################
##
## Groups and users
##

resource "aws_iam_group" "athena" {
  name = "athena-users"
  path = "/"
}

resource "aws_iam_group_policy_attachment" "athena_data_bucket_ro" {
  group      = aws_iam_group.athena.name
  policy_arn = aws_iam_policy.data_bucket_ro.arn
}

resource "aws_iam_group_policy_attachment" "athena_athena_bucket_rw" {
  group      = aws_iam_group.athena.name
  policy_arn = aws_iam_policy.athena_bucket_rw.arn
}

resource "aws_iam_group_policy_attachment" "athena_full_access" {
  group      = aws_iam_group.athena.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
}


resource "aws_iam_group" "ecr" {
  name = "ecr-users"
  path = "/"
}

resource "aws_iam_group_policy_attachment" "ecr_ecr_power_user" {
  group      = aws_iam_group.ecr.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryPowerUser"
}


resource "aws_iam_group" "uploaders" {
  name = "uploaders"
  path = "/"
}

resource "aws_iam_group_policy_attachment" "uploaders_data_bucket_rw" {
  group      = aws_iam_group.uploaders.name
  policy_arn = aws_iam_policy.data_bucket_rw.arn
}


#################################################################################
#################################################################################
##
## Users
##

resource "aws_iam_user" "user" {
  name = "covid-19-puerto-rico"
  path = "/"
  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_user_group_membership" "user_athena_member" {
  user = aws_iam_user.user.name
  groups = [
    aws_iam_group.athena.name,
    aws_iam_group.ecr.name,
    aws_iam_group.uploaders.name,
  ]
}