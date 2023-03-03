variable "project_name" {
  type = string
  description = "The project name, which will be used to construct various resource names."
}

variable "main_bucket_name" {
  type = string
  description = "The name of the base S3 bucket to create/use."
}

variable "logs_bucket_name" {
  type = string
  description = "The name of the bucket to create/use for logs."
}

variable "dns_name" {
  type = string
  description = "The DNS name of the project website."
}
