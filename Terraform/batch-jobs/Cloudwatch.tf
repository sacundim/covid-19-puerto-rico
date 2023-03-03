##############################################################################
##############################################################################
##
## Logging
##

resource "aws_cloudwatch_log_group" "covid_19_puerto_rico" {
  name = "covid-19-puerto-rico"
  retention_in_days = 30
  tags = {
    Project = var.project_name
  }
}
