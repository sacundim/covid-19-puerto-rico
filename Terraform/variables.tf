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

variable "github_url" {
  type = string
  description = "The URL to the GitHub repo."
  default = "https://github.com/sacundim/covid-19-puerto-rico"
}

variable "github_branch" {
  type = string
  description = "The Git branch to build."
  default = "master"
}

variable "az_count" {
  description = "Number of AZs to cover in a given region. Depends on the AWS region. Most regions have 3."
  default     = "4"
}

variable "cidr_block" {
  description = "Private IP address range to use."
  default = "172.32.128.0/22"
}

variable "bioportal_api_url" {
  description = "Bioportal API endpoint base URL"
  # The official one is this, but it's a load balancer or something that's slow as heck
#  default = "https://bioportal-apim.salud.pr.gov/bioportal"

  # This bypasses the HTTP front-end and goes straight to the hosts:
  default = "https://api-bioportal-prod-eastus2-01.azurewebsites.net"
}