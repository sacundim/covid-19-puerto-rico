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
          "s3:ListBucket",
          "s3:DescribeJob"
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
          "s3:ListBucket",
          "s3:DescribeJob"
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
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:DescribeJob"
        ],
        Resource = [
          aws_s3_bucket.athena_bucket.arn,
          "${aws_s3_bucket.athena_bucket.arn}/*"
        ]
      }
    ]
  })
}