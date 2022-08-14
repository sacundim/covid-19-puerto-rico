#!/usr/bin/env bash
#
# Push the docker image for the download scripts to AWS ECR so that
# it's available for the Terraform infrastructure.
#
# Make sure to set the REPO_ENDPOINT environment variable to your
# ECR Docker registry hostname, e.g.:
#
#     <AWS account #>.dkr.ecr.us-west-2.amazonaws.com
#
set -e -o pipefail

IMAGE_NAME="${IMAGE_NAME-covid-19-puerto-rico-downloader}"
ECR_ENDPOINT="${ECR_ENDPOINT:?"ECR_ENDPOINT not set"}"

aws ecr get-login-password --region us-west-2 \
  | docker login --username AWS --password-stdin "${ECR_ENDPOINT}"

docker tag \
  "${IMAGE_NAME}":latest \
  "${ECR_ENDPOINT}"/"${IMAGE_NAME}":latest

docker push "${ECR_ENDPOINT}"/"${IMAGE_NAME}":latest
