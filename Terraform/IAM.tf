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

