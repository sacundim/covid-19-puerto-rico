terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.60"
    }
  }
}

provider "aws" {
  profile = "admin"
  region  = "us-west-2"
}

data "aws_region" "current" {}
data "aws_availability_zones" "available" {}