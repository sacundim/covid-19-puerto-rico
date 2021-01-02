#################################################################################
#################################################################################
##
## Policies
##

resource "aws_iam_policy" "all_buckets_rw" {
  name        = "covid-19-puerto-rico-buckets-rw"
  description = "Grant list/read/write access to all project S3 buckets."

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::covid-19-puerto-rico",
                "arn:aws:s3:::covid-19-puerto-rico-athena",
                "arn:aws:s3:::covid-19-puerto-rico-data",
                "arn:aws:s3:::covid-19-puerto-rico/*",
                "arn:aws:s3:::covid-19-puerto-rico-athena/*",
                "arn:aws:s3:::covid-19-puerto-rico-data/*"
            ]
        }
    ]
}
EOF
}


#################################################################################
#################################################################################
##
## Roles
##

resource "aws_iam_role" "ecs_service_role" {
  name = "AWSServiceRoleForECS"
  path = "/aws-service-role/ecs.amazonaws.com/"
  tags = {
    Project = "covid-19-puerto-rico"
  }

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
## Users
##
