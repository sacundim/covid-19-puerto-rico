resource "aws_s3_bucket" "main_bucket" {
  bucket = var.main_bucket_name

  tags = {
    Project = "covid-19-puerto-rico"
  }

  lifecycle_rule {
    id      = "Tiered storage"
    enabled = true

    transition {
      days          = 31
      storage_class = "INTELLIGENT_TIERING"
    }

    abort_incomplete_multipart_upload_days = 7
  }
}

resource "aws_s3_bucket_public_access_block" "block_main_bucket" {
  bucket = aws_s3_bucket.main_bucket.id
  block_public_acls   = true
  block_public_policy = true
}


resource "aws_s3_bucket" "athena_bucket" {
  bucket = var.athena_bucket_name

  tags = {
    Project = "covid-19-puerto-rico"
  }

  lifecycle_rule {
    id      = "Expire stale data"
    enabled = true

    expiration {
      days = 2
    }

    noncurrent_version_expiration {
      days = 2
    }

    abort_incomplete_multipart_upload_days = 2
  }
}

resource "aws_s3_bucket_public_access_block" "block_athena_bucket" {
  bucket = aws_s3_bucket.athena_bucket.id
  block_public_acls   = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}


resource "aws_s3_bucket" "data_bucket" {
  bucket = var.datalake_bucket_name

  tags = {
    Project = "covid-19-puerto-rico"
  }

  versioning {
    enabled = true
  }

  lifecycle_rule {
    id      = "Transition to Intelligent Tiering"
    enabled = true

    transition {
      days          = 31
      storage_class = "INTELLIGENT_TIERING"
    }

    expiration {
      expired_object_delete_marker = true
    }

    noncurrent_version_expiration {
      days = 31
    }

    abort_incomplete_multipart_upload_days = 7
  }
}

resource "aws_s3_bucket_public_access_block" "block_data_bucket" {
  bucket = aws_s3_bucket.data_bucket.id
  block_public_acls   = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}


resource "aws_s3_bucket" "logs_bucket" {
  bucket = var.logs_bucket_name
  tags = {
    Project = "covid-19-puerto-rico-logs"
  }

  lifecycle_rule {
    id      = "Transition to Intelligent Tiering"
    enabled = true

    transition {
      days          = 31
      storage_class = "INTELLIGENT_TIERING"
    }

    expiration {
      expired_object_delete_marker = true
    }

    noncurrent_version_expiration {
      days = 31
    }

    abort_incomplete_multipart_upload_days = 7
  }
}

resource "aws_s3_bucket_public_access_block" "block_logs_bucket" {
  bucket = aws_s3_bucket.logs_bucket.id
  block_public_acls   = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}


resource "aws_s3_bucket" "testing_bucket" {
  bucket = var.testing_bucket_name
  tags = {
    Project = "covid-19-puerto-rico"
  }

  lifecycle_rule {
    id      = "Expire stale data"
    enabled = true

    expiration {
      days = 3
    }

    noncurrent_version_expiration {
      days = 3
    }

    abort_incomplete_multipart_upload_days = 3
  }
}

resource "aws_s3_bucket_public_access_block" "block_testing_bucket" {
  bucket = aws_s3_bucket.testing_bucket.id
  block_public_acls   = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}
