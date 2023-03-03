resource "aws_iam_policy" "main_bucket_ro" {
  name        = "${var.project_name}-main-reader"
  description = "Grant list/read access to the S3 main bucket."

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
          aws_s3_bucket.main_bucket.arn,
          "${aws_s3_bucket.main_bucket.arn}/*"
        ]
      }
    ]
  })
}


resource "aws_iam_policy" "main_bucket_rw" {
  name        = "${var.project_name}-main-writer"
  description = "Grant list/read access to the S3 main bucket."

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:PutObjectTagging",
          "s3:GetObject",
          "s3:GetObjectTagging",
          "s3:ListBucket",
          "s3:DescribeJob"
        ],
        Resource = [
          aws_s3_bucket.main_bucket.arn,
          "${aws_s3_bucket.main_bucket.arn}/*"
        ]
      }
    ]
  })
}


resource "aws_iam_policy" "logs_bucket_ro" {
  name        = "${var.project_name}-logs-reader"
  description = "Grant list/read access to the S3 logs bucket."

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
          aws_s3_bucket.logs_bucket.arn,
          "${aws_s3_bucket.logs_bucket.arn}/*"
        ]
      }
    ]
  })
}