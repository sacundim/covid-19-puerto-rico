variable "project_name" {
  type = string
  description = "The project name, which will be used to construct various resource names."
  default = "covid-19-puerto-rico"
}

variable "downloader_github_url" {
  type = string
  description = "The URL to the downloader GitHub repo."
  default = "https://github.com/sacundim/covid-19-puerto-rico-downloader"
}

variable "downloader_github_branch" {
  type = string
  description = "The Git brach to build."
  default = "aws-codebuild"
}

variable "datalake_bucket_name" {
  type = string
  description = "The name of the datalake bucket to create/use. This is where downloads will be stored."
  default = "covid-19-puerto-rico-data"
}

variable "testing_bucket_name" {
  type = string
  description = "The name of the bucket to create/use for testing stuff."
  default = "covid-19-puerto-rico-testing"
}

variable "aws_region" {
  description = "The AWS region things are created in."
  default     = "us-west-2"
}

variable "az_count" {
  description = "Number of AZs to cover in a given region. Depends on the AWS region. Most regions have 3."
  default     = "4"
}

variable "cidr_block" {
  description = "Private IP address range to use."
  default = "172.32.128.0/22"
}
