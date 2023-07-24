#################################################################################
#################################################################################
##
## "Main" bucket (website static content)
##
resource "aws_s3_bucket" "main_bucket" {
  bucket = var.main_bucket_name
  tags = {
    Project = "covid-19-puerto-rico"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "main_bucket" {
  bucket = aws_s3_bucket.main_bucket.id

  rule {
    id = "Transition to Intelligent Tiering"
    status = "Enabled"

    transition {
      days = 0
      storage_class = "INTELLIGENT_TIERING"
    }

    expiration {
      expired_object_delete_marker = true
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 2
    }
  }
}

resource "aws_s3_bucket_public_access_block" "block_main_bucket" {
  bucket = aws_s3_bucket.main_bucket.id
  block_public_acls   = true
  block_public_policy = true
}


#################################################################################
#################################################################################
##
## "Athena" bucket (query set results)
##
resource "aws_s3_bucket" "athena_bucket" {
  bucket = var.athena_bucket_name
  tags = {
    Project = "covid-19-puerto-rico"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_bucket" {
  bucket = aws_s3_bucket.athena_bucket.id

  rule {
    id = "Expire stale data"
    status = "Enabled"

    expiration {
      days = 2
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 2
    }
  }
}

resource "aws_s3_bucket_public_access_block" "block_athena_bucket" {
  bucket = aws_s3_bucket.athena_bucket.id
  block_public_acls   = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}


#################################################################################
#################################################################################
##
## "Data" bucket (data source captures)
##
resource "aws_s3_bucket" "data_bucket" {
  bucket = var.datalake_bucket_name
  tags = {
    Project = "covid-19-puerto-rico"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_bucket" {
  bucket = aws_s3_bucket.data_bucket.id

  rule {
    id = "Transition to Intelligent Tiering"
    status = "Enabled"

    transition {
      days = 0
      storage_class = "INTELLIGENT_TIERING"
    }

    expiration {
      expired_object_delete_marker = true
    }

    noncurrent_version_expiration {
      noncurrent_days = 31
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 2
    }
  }
}

resource "aws_s3_bucket_public_access_block" "block_data_bucket" {
  bucket = aws_s3_bucket.data_bucket.id
  block_public_acls   = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}


#################################################################################
#################################################################################
##
## Iceberg bucket (incremental datamart)
##
resource "aws_s3_bucket" "iceberg_bucket" {
  bucket = var.iceberg_bucket_name
  tags = {
    Project = "covid-19-puerto-rico"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "iceberg_bucket" {
  bucket = aws_s3_bucket.iceberg_bucket.id

  rule {
    id = "Transition to Intelligent Tiering"
    status = "Enabled"

    transition {
      days = 0
      storage_class = "INTELLIGENT_TIERING"
    }

    expiration {
      expired_object_delete_marker = true
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 2
    }
  }
}

resource "aws_s3_bucket_public_access_block" "block_iceberg_bucket" {
  bucket = aws_s3_bucket.iceberg_bucket.id
  block_public_acls   = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}


#################################################################################
#################################################################################
##
## Logs bucket
##
resource "aws_s3_bucket" "logs_bucket" {
  bucket = var.logs_bucket_name
  tags = {
    Project = "covid-19-puerto-rico"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "logs_bucket" {
  bucket = aws_s3_bucket.logs_bucket.id

  rule {
    id = "Transition to Intelligent Tiering"
    status = "Enabled"

    transition {
      days = 0
      storage_class = "INTELLIGENT_TIERING"
    }

    expiration {
      expired_object_delete_marker = true
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 2
    }
  }
}

resource "aws_s3_bucket_ownership_controls" "logs_bucket" {
  bucket = aws_s3_bucket.logs_bucket.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "logs_bucket" {
  depends_on = [aws_s3_bucket_ownership_controls.logs_bucket]
  bucket = aws_s3_bucket.logs_bucket.id
  access_control_policy {
    owner {
      id = data.aws_canonical_user_id.current_user.id
    }

    grant {
      permission = "FULL_CONTROL"
      grantee {
        id          = data.aws_canonical_user_id.current_user.id
        type        = "CanonicalUser"
      }
    }

    grant {
      permission = "FULL_CONTROL"
      grantee {
        id          = data.aws_cloudfront_log_delivery_canonical_user_id.current_user.id
        type        = "CanonicalUser"
      }
    }
  }
}
data "aws_canonical_user_id" "current_user" {}
data "aws_cloudfront_log_delivery_canonical_user_id" "current_user" {}

resource "aws_s3_bucket_public_access_block" "block_logs_bucket" {
  bucket = aws_s3_bucket.logs_bucket.id
  block_public_acls   = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}


#################################################################################
#################################################################################
##
## Testing bucket
##
resource "aws_s3_bucket" "testing_bucket" {
  bucket = var.testing_bucket_name
  tags = {
    Project = "covid-19-puerto-rico"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "testing_bucket" {
  bucket = aws_s3_bucket.testing_bucket.id

  rule {
    id = "Expire stale data"
    status = "Enabled"

    expiration {
      days = 3
    }

    noncurrent_version_expiration {
      noncurrent_days = 3
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}

resource "aws_s3_bucket_public_access_block" "block_testing_bucket" {
  bucket = aws_s3_bucket.testing_bucket.id
  block_public_acls   = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}


#################################################################################
#################################################################################
##
## Backups bucket
##
resource "aws_s3_bucket" "backups_bucket" {
  bucket = var.backups_bucket_name
  tags = {
    Project = "covid-19-puerto-rico"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "backups_bucket" {
  bucket = aws_s3_bucket.backups_bucket.id

  rule {
    id = "Transition to Intelligent Tiering"
    status = "Enabled"

    transition {
      days = 0
      storage_class = "INTELLIGENT_TIERING"
    }

    expiration {
      expired_object_delete_marker = true
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 2
    }
  }
}

resource "aws_s3_bucket_public_access_block" "block_backups_bucket" {
  bucket = aws_s3_bucket.backups_bucket.id
  block_public_acls   = true
  block_public_policy = true
}
