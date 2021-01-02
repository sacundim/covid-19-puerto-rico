terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 2.70"
    }
  }
}

provider "aws" {
  profile = "admin"
  region  = "us-west-2"
}

data "aws_region" "current" {}
