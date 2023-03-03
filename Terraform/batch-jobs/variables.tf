variable "project_name" {
  type = string
  description = "The project name, which will be used to construct various resource names."
}

variable "aws_region" {
  type = string
  description = "The AWS region things are created in."
}

variable "main_bucket_name" {
  description = "The name of the base S3 bucket to create/use."
}

variable "datalake_bucket_name" {
  type = string
  description = "The name of the datalake bucket to create/use. This is where downloads will be stored."
}

variable "athena_bucket_name" {
  type = string
  description = "The name of the Athena query results/tables bucket."
}

variable "ec2_queue_name" {
  type = string
  description = "Name of the AWS Batch EC2 compute environment we use"
}

variable "fargate_queue_name" {
  type = string
  description = "Name of the AWS Batch Fargate compute environment we use"
}

variable "athena_workgroup_main_name" {
  type = string
  description = "Name of the Athena workgroup to use to query our datamart"
}

variable "athena_workgroup_dbt_name" {
  type = string
  description = "Name of the Athena workgroup to use for our DBT jobs"
}

variable "main_bucket_rw_arn" {
  type = string
  description = "ARN of the IAM policy that gives R/W access to the website bucket."
}

variable "data_bucket_ro_arn" {
  type = string
  description = "ARN of the IAM policy that gives R/O access to the data bucket."
}

variable "data_bucket_rw_arn" {
  type = string
  description = "ARN of the IAM policy that gives R/W access to the data bucket."
}

variable "athena_bucket_rw_arn" {
  type = string
  description = "ARN of the IAM policy that gives R/W access to the data bucket."
}

variable "bioportal_api_url" {
  type = string
  description = "Bioportal API endpoint base URL"
  # The official one is this, but it's a load balancer or something that's slow as heck
#  default = "https://bioportal-apim.salud.pr.gov/bioportal"

  # This bypasses the HTTP front-end and goes straight to the hosts:
  default = "https://api-bioportal-prod-eastus2-01.azurewebsites.net"
}
