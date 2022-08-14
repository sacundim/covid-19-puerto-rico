locals {
  s3_origin_id = "covid-19-puerto-rico"
}

resource "aws_cloudfront_origin_access_identity" "origin_access_identity" {
  comment = "covid-19-puerto-rico-web"
}

resource "aws_cloudfront_distribution" "s3_distribution" {
  tags = {
    Project = var.project_name
  }

  origin {
    domain_name = aws_s3_bucket.main_bucket.bucket_regional_domain_name
    origin_id   = local.s3_origin_id

    s3_origin_config {
      origin_access_identity = aws_cloudfront_origin_access_identity.origin_access_identity.cloudfront_access_identity_path
    }
  }

  enabled             = true
  is_ipv6_enabled     = true
  #default_root_object = "index.html"

  aliases = [var.dns_name]

  logging_config {
    bucket          = aws_s3_bucket.logs_bucket.bucket_domain_name
    prefix          = "www"
  }

  default_cache_behavior {
    allowed_methods  = ["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"]
    cached_methods   = ["GET", "HEAD"]
    target_origin_id = local.s3_origin_id

    forwarded_values {
      query_string = false

      cookies {
        forward = "none"
      }
    }

    viewer_protocol_policy = "redirect-to-https"
    min_ttl                = 0
    default_ttl            = 3600
    max_ttl                = 86400
    compress = true

    # Redirect requests that don't have a filename in the path to `index.html`
    function_association {
      event_type   = "viewer-request"
      function_arn = aws_cloudfront_function.redirect_to_index.arn
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    acm_certificate_arn = aws_acm_certificate_validation.cert_validation.certificate_arn
    ssl_support_method = "sni-only"
    minimum_protocol_version = "TLSv1.1_2016"
  }
}

# to get the Cloud front URL if doamin/alias is not configured
output "cloudfront_domain_name" {
  value = aws_cloudfront_distribution.s3_distribution.domain_name
}


data "aws_iam_policy_document" "cloudfront_access_to_main_bucket" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.main_bucket.arn}/*"]

    principals {
      type        = "AWS"
      identifiers = [aws_cloudfront_origin_access_identity.origin_access_identity.iam_arn]
    }
  }
}

resource "aws_s3_bucket_policy" "cloudfront_access_to_main_bucket" {
  bucket = aws_s3_bucket.main_bucket.id
  policy = data.aws_iam_policy_document.cloudfront_access_to_main_bucket.json
}


#############################################################
#############################################################
##
## Cloudfront function to redirect to index.html
##

resource "aws_cloudfront_function" "redirect_to_index" {
  name    = "redirect_to_index"
  runtime = "cloudfront-js-1.0"
  comment = "Function to redirect requests without a file to index.html"
  publish = true
  code    = file("${path.module}/redirect_to_index_html.js")
}


#############################################################
#############################################################
##
## Certificate Manager and Route 53
##

resource "aws_acm_certificate" "cert" {
  provider = aws.acm_provider
  domain_name       = var.dns_name
  subject_alternative_names = ["*.${var.dns_name}"]
  validation_method = "DNS"

  tags = {
    Project = var.project_name
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_acm_certificate_validation" "cert_validation" {
  provider = aws.acm_provider
  certificate_arn = aws_acm_certificate.cert.arn
#  validation_record_fqdns = [for record in aws_route53_record.root-a : record.fqdn]
}


#############################################################
#############################################################
##
## Route 53
##

resource "aws_route53_zone" "dns_zone" {
  name         = var.dns_name
  tags = {
    Project = var.project_name
  }
}

resource "aws_route53_record" "root-a" {
  zone_id = aws_route53_zone.dns_zone.zone_id
  name = var.dns_name
  type = "A"

  alias {
    name = aws_cloudfront_distribution.s3_distribution.domain_name
    zone_id = aws_cloudfront_distribution.s3_distribution.hosted_zone_id
    evaluate_target_health = false
  }
}
