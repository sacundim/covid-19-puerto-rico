#!/usr/bin/env bash
#
#
# Build the docker image for the download scripts and push it to
# AWS ECR so that it's available for the Terraform infrastructure.
#
# Make sure to set the REPO_ENDPOINT environment variable to your
# ECR Docker registry hostname, e.g.:
#
#     <AWS account #>.dkr.ecr.us-west-2.amazonaws.com
#
set -e -o pipefail

ECR_ENDPOINT="${ECR_ENDPOINT:?"ECR_ENDPOINT not set"}"
IMAGE_NAME="${IMAGE_NAME-covid-19-puerto-rico-scripts}"

cd "$(dirname $0)"
aws ecr get-login-password --region us-west-2 \
  | docker login --username AWS --password-stdin "${ECR_ENDPOINT}"

docker build -t "${IMAGE_NAME}" .

docker tag \
  "${IMAGE_NAME}":latest \
  "${ECR_ENDPOINT}"/"${IMAGE_NAME}":latest

docker push "${ECR_ENDPOINT}"/"${IMAGE_NAME}":latest