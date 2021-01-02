resource "aws_s3_bucket" "main_bucket" {
  bucket = "covid-19-puerto-rico"

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

resource "aws_s3_bucket" "athena_bucket" {
  bucket = "covid-19-puerto-rico-athena"

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

resource "aws_s3_bucket" "data_bucket" {
  bucket = "covid-19-puerto-rico-data"

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
