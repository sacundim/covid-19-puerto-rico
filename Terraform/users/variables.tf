variable "project_name" {
  type = string
  description = "The project name, which will be used to construct various resource names."
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
