##################################################################################
##################################################################################
##
## State machine to rebuild the DBT schema and website.
##

resource "aws_sfn_state_machine" "covid_19_puerto_rico_rebuild" {
  name     = "${var.project_name}-rebuild"
  tags = {
    Project = var.project_name
  }

  role_arn = aws_iam_role.sfn_workflows.arn

  definition = jsonencode({
    "Comment" : "Orchestrate the daily covid-19-puerto-rico.org website rebuild",
    "TimeoutSeconds": 3600,
    "StartAt" : "RunDBT",
    "States" : {
      "RunDBT" : {
        "Type" : "Task",
        "Resource" : "arn:aws:states:::batch:submitJob.sync",
        "Parameters": {
          "JobDefinition" : aws_batch_job_definition.dbt_run_models.arn,
          "JobName" : "dbt-run-models",
          "JobQueue" : aws_batch_job_queue.fargate_amd64.arn
        },
        "Next": "RunWebsite"
      },

      "RunWebsite": {
        "End" : true
        "Type": "Task"
        "Resource" : "arn:aws:states:::batch:submitJob.sync",
        "Parameters": {
          "JobDefinition" : aws_batch_job_definition.website_generator.arn,
          "JobName" : "website-generator",
          "JobQueue" : aws_batch_job_queue.fargate_amd64.arn
        }
      }
    }
  })
}


##################################################################################
##################################################################################
##
## State machine to schedule and run the source ingestions.
##

resource "aws_sfn_state_machine" "covid_19_puerto_rico_ingest" {
  name = "${var.project_name}-ingest"
  tags = {
    Project = var.project_name
  }

  role_arn   = aws_iam_role.sfn_workflows.arn
  definition = jsonencode({
    "Comment" : "Orchestrate the daily covid-19-puerto-rico.org data ingestion",
    "TimeoutSeconds": 86400,
    "StartAt" : "ComputeSchedule",
    "States": {
      "ComputeSchedule": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
          "FunctionName": aws_lambda_function.resolve_ingestion_schedule.function_name,
          "Payload.$": "$"
        },
        "ResultSelector": {
          "Payload.$": "$.Payload"
        }
        "OutputPath": "$.Payload",
        "Next": "Ingestions"
      }

      "Ingestions": {
        "Type": "Parallel",
        "Branches": [
          {
            "StartAt": "Schedule Biostatistics",
            "States": {
              "Schedule Biostatistics": {
                "Type": "Wait",
                "TimestampPath": "$.utcSchedule.biostatistics",
                "Next": "Run Biostatistics"
              },
              "Run Biostatistics": {
                "Type" : "Task",
                "Resource" : "arn:aws:states:::batch:submitJob.sync",
                "Parameters": {
                  "JobDefinition" : aws_batch_job_definition.biostatistics_download_and_sync.arn,
                  "JobName" : "biostatistics-download-and-sync",
                  "JobQueue" : aws_batch_job_queue.ec2_arm64.arn,
                  "Parameters": {
                    "rclone_destination": ":s3,provider=AWS,env_auth:${var.testing_bucket_name}/data"
                  }
                },
                "Catch": [{
                  # We swallow the errors because we don't want the state machine
                  # execution to cancel our other parallel tasks if one fails.
                  "ErrorEquals": [ "States.ALL" ],
                  "Next": "Handle Biostatistics errors"
                }],
                "End": true
              },
              "Handle Biostatistics errors": {
                "Type": "Pass",
                "End": true
              }
            }
          },

          {
            "StartAt": "Schedule Covid19DatosV2",
            "States": {
              "Schedule Covid19DatosV2": {
                "Type": "Wait",
                "TimestampPath": "$.utcSchedule.covid19datos_v2",
                "Next": "Run Covid19DatosV2"
              },
              "Run Covid19DatosV2": {
                "Type" : "Task",
                "Resource" : "arn:aws:states:::batch:submitJob.sync",
                "Parameters": {
                  "JobDefinition" : aws_batch_job_definition.covid19datos_v2_download_and_sync.arn,
                  "JobName" : "covid19datos-v2-download-and-sync",
                  "JobQueue" : aws_batch_job_queue.fargate_amd64.arn,
                  "Parameters": {
                    "rclone_destination": ":s3,provider=AWS,env_auth:${var.testing_bucket_name}/data"
                  }
                },
                "Catch": [{
                  "ErrorEquals": [ "States.ALL" ],
                  "Next": "Handle Covid19DatosV2 errors"
                }],
                "End": true
              },
              "Handle Covid19DatosV2 errors": {
                "Type": "Pass",
                "End": true
              }
            }
          },

          {
            "StartAt": "Schedule HHS",
            "States": {
              "Schedule HHS": {
                "Type": "Wait",
                "TimestampPath": "$.utcSchedule.hhs",
                "Next": "Run HHS"
              },
              "Run HHS": {
                "Type" : "Task",
                "Resource" : "arn:aws:states:::batch:submitJob.sync",
                "Parameters": {
                  "JobDefinition" : aws_batch_job_definition.hhs_download_and_sync.arn,
                  "JobName" : "hhs-download-and-sync",
                  "JobQueue" : aws_batch_job_queue.fargate_amd64.arn,
                  "Parameters": {
                    "rclone_destination": ":s3,provider=AWS,env_auth:${var.testing_bucket_name}/data"
                  }
                },
                "Catch": [{
                  "ErrorEquals": [ "States.ALL" ],
                  "Next": "Handle HHS errors"
                }],
                "End": true
              },
              "Handle HHS errors": {
                "Type": "Pass",
                "End": true
              }
            }
          }
        ],
        "Next": "Verify Ingestions"
      },

      "Verify Ingestions": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
          "FunctionName": aws_lambda_function.verify_ingestion_results.function_name,
          "Payload.$": "$"
        },
        "End": true
      }
    }
  })
}

resource "aws_lambda_function" "resolve_ingestion_schedule" {
  function_name = "${var.project_name}-resolve-ingestion-schedule"
  tags = {
    Project = var.project_name
  }
  runtime = "python3.9"
  filename = data.archive_file.resolve_ingestion_schedule.output_path
  source_code_hash = data.archive_file.resolve_ingestion_schedule.output_base64sha256
  handler = "resolve_ingestion_schedule.lambda_handler"
  role = aws_iam_role.iam_for_lambda.arn
}

data "archive_file" "resolve_ingestion_schedule" {
  type = "zip"
  source_file = "${path.module}/resolve_ingestion_schedule.py"
  output_path = "${path.module}/tmp/resolve_ingestion_schedule.zip"
}


resource "aws_lambda_function" "verify_ingestion_results" {
  function_name = "${var.project_name}-verify-ingestion-results"
  tags = {
    Project = var.project_name
  }
  runtime = "python3.9"
  filename = data.archive_file.verify_ingestion_results.output_path
  source_code_hash = data.archive_file.verify_ingestion_results.output_base64sha256
  handler = "verify_ingestion_results.lambda_handler"
  role = aws_iam_role.iam_for_lambda.arn
}

data "archive_file" "verify_ingestion_results" {
  type = "zip"
  source_file = "${path.module}/verify_ingestion_results.py"
  output_path = "${path.module}/tmp/verify_ingestion_results.zip"
}


resource "aws_iam_role" "iam_for_lambda" {
  name               = "${var.project_name}-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}


##################################################################################
##################################################################################
##
## Daily schedule
##

resource "aws_scheduler_schedule" "test_daily_ingestion" {
  name        = "${var.project_name}-test-daily-ingestion"
  description = "Run the daily data ingestions."

  schedule_expression_timezone = "America/Puerto_Rico"
  schedule_expression = "cron(55 14 * * ? *)"
  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn = aws_sfn_state_machine.covid_19_puerto_rico_ingest.arn
    role_arn = aws_iam_role.eventbridge_scheduler_role.arn
    input = jsonencode({
      "localSchedule": {
        "biostatistics": {
          "timezone": "America/Puerto_Rico",
#          "localTime": "05:55:00"
          "localTime": "17:55:00"
        },

        "covid19datos_v2": {
          "timezone": "America/Puerto_Rico",
#          "localTime": "12:25:00"
          "localTime": "00:25:00"
        },

        "hhs": {
          "timezone": "America/New_York",
#          "localTime": "13:25:00"
          "localTime": "01:25:00"
        }
      }
    })
  }
}


resource "aws_scheduler_schedule" "daily_rebuild" {
  name        = "${var.project_name}-daily-rebuild"
  description = "Run the daily website rebuild."

  schedule_expression_timezone = "America/Puerto_Rico"
  schedule_expression = "cron(05 14 * * ? *)"
  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn = aws_sfn_state_machine.covid_19_puerto_rico_rebuild.arn
    role_arn = aws_iam_role.eventbridge_scheduler_role.arn
    input = jsonencode({})
  }
}


##################################################################################
##################################################################################
##
## IAM
##

resource "aws_iam_role" "sfn_workflows" {
  name = "${var.project_name}-SFN-for-workflows"
  assume_role_policy = jsonencode({
    "Version":"2012-10-17",
    "Statement":[
      {
        "Effect":"Allow",
        "Principal":{
          "Service":[
            "states.amazonaws.com"
          ]
        },
        "Action":"sts:AssumeRole"
      }
    ]
  })
}


resource "aws_iam_role_policy_attachment" "sfn_execute_lambda" {
  role       = aws_iam_role.sfn_workflows.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaRole"
}


resource "aws_iam_policy" "sfn_run_batch_job_sync" {
  name = "${var.project_name}-sfn-run-batch-job-sync"
  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "batch:SubmitJob",
          "batch:DescribeJobs",
          "batch:TerminateJob"
        ],
        "Resource": "*"
      },
      {
        "Effect": "Allow",
        "Action": [
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule"
        ],
        "Resource": [
          "arn:aws:events:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:rule/StepFunctionsGetEventsForBatchJobsRule",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "sfn_batch_run_batch_job_sync" {
  role       = aws_iam_role.sfn_workflows.name
  policy_arn = aws_iam_policy.sfn_run_batch_job_sync.arn
}
