resource "aws_sfn_state_machine" "covid_19_puerto_rico_rebuild" {
  name     = "${var.project_name}-rebuild"
  tags = {
    Project = var.project_name
  }

  role_arn = aws_iam_role.sfn_batch.arn

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
## IAM
##

resource "aws_iam_role" "sfn_batch" {
  name = "SFNForWorkflows"
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

/*
resource "aws_iam_role_policy_attachment" "sfn_batch_lambda_role" {
  role       = aws_iam_role.sfn_batch.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaRole"
}
*/


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
  role       = aws_iam_role.sfn_batch.name
  policy_arn = aws_iam_policy.sfn_run_batch_job_sync.arn
}
