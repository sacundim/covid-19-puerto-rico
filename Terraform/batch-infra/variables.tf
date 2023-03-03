variable "project_name" {
  type = string
  description = "The project name, which will be used to construct various resource names."
}

variable "az_count" {
  type = string
  description = "Number of AZs to cover in a given region. Depends on the AWS region. Most regions have 3."
}

variable "cidr_block" {
  type = string
  description = "Private IP address range to use."
}
