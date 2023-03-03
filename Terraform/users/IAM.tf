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
  policy_arn = data.aws_iam_policy.data_bucket_ro.arn
}

resource "aws_iam_group_policy_attachment" "athena_athena_bucket_rw" {
  group      = aws_iam_group.athena.name
  policy_arn = data.aws_iam_policy.athena_bucket_rw.arn
}

resource "aws_iam_group_policy_attachment" "athena_full_access" {
  group      = aws_iam_group.athena.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
}


resource "aws_iam_group" "uploaders" {
  name = "uploaders"
  path = "/"
}

resource "aws_iam_group_policy_attachment" "uploaders_data_bucket_rw" {
  group      = aws_iam_group.uploaders.name
  policy_arn = data.aws_iam_policy.data_bucket_rw.arn
}

resource "aws_iam_group_policy_attachment" "uploaders_main_bucket_rw" {
  group      = aws_iam_group.uploaders.name
  policy_arn = data.aws_iam_policy.main_bucket_rw.arn
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
    aws_iam_group.uploaders.name,
  ]
}