variable "project_name" {
  type = string
  description = "The project name, which will be used to construct various resource names."
}

variable "datalake_bucket_name" {
  type = string
  description = "The name of the datalake bucket to create/use. This is where downloads will be stored."
}

variable "athena_bucket_name" {
  type = string
  description = "The name of the bucket to create/use for Athena query results and tables."
}

variable "testing_bucket_name" {
  type = string
  description = "The name of the bucket to create/use for testing stuff."
}

variable "backups_bucket_name" {
  type = string
  description = "The name of the bucket to create/use for miscellaneous."
}
