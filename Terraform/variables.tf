variable "project_name" {
  type = string
  description = "The project name, which will be used to construct various resource names."
  default = "covid-19-puerto-rico"
}

variable "main_bucket_name" {
  type = string
  description = "The name of the base S3 bucket to create/use."
  default = "covid-19-puerto-rico"
}

variable "datalake_bucket_name" {
  type = string
  description = "The name of the datalake bucket to create/use. This is where downloads will be stored."
  default = "covid-19-puerto-rico-data"
}

variable "athena_bucket_name" {
  type = string
  description = "The name of the bucket to create/use for Athena query results and tables."
  default = "covid-19-puerto-rico-athena"
}

variable "logs_bucket_name" {
  type = string
  description = "The name of the bucket to create/use for logs."
  default = "covid-19-puerto-rico-logs"
}

variable "testing_bucket_name" {
  type = string
  description = "The name of the bucket to create/use for testing stuff."
  default = "covid-19-puerto-rico-testing"
}

variable "backups_bucket_name" {
  type = string
  description = "The name of the bucket to create/use for miscellaneous."
  default = "covid-19-puerto-rico-backups"
}

variable "aws_region" {
  description = "The AWS region things are created in."
  default     = "us-west-2"
}

variable "dns_name" {
  type = string
  description = "The DNS name of the project website."
  default = "covid-19-puerto-rico.org"
}

