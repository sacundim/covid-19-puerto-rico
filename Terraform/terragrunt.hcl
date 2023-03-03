generate "provider" {
  path = "provider.tf"
  if_exists = "overwrite_terragrunt"
  contents = <<EOF
provider "aws" {
  profile = "admin"
  version = "~> 4.56.0"
  region = "us-west-2"
}
EOF
}

remote_state {
  backend = "s3"
  generate = {
    path      = "backend.tf"
    if_exists = "overwrite_terragrunt"
  }
  config = {
    bucket = "covid-19-puerto-rico-terraform"
    key = "${path_relative_to_include()}/terraform.tfstate"
    region = "us-west-2"
    profile = "admin"
    encrypt = true
  }
}

inputs = {
  project_name = "covid-19-puerto-rico"
  main_bucket_name = "covid-19-puerto-rico"
  datalake_bucket_name = "covid-19-puerto-rico-data"
  athena_bucket_name = "covid-19-puerto-rico-athena"
  logs_bucket_name = "covid-19-puerto-rico-logs"
  testing_bucket_name = "covid-19-puerto-rico-testing"
  backups_bucket_name = "covid-19-puerto-rico-backups"
  aws_region = "us-west-2"
  dns_name = "covid-19-puerto-rico.org"
  az_count = "4"
  cidr_block = "172.32.128.0/22"

  bioportal_api_url = "https://api-bioportal-prod-eastus2-01.azurewebsites.net"
  # The official one is this, but it's a load balancer or something that's slow as heck
#  bioportal_api_url = "https://bioportal-apim.salud.pr.gov/bioportal"
}