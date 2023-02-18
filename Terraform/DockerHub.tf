resource "aws_secretsmanager_secret" "docker_hub" {
  name = "${var.project_name}-docker-hub"
  description = "Our app token for Docker Hub. We get throttled without one."
}

resource "aws_iam_policy" "docker_hub_app_token" {
  name        = "${var.project_name}-docker-hub-app-token"
  description = "Grant read access to the Docker Hub app token secret."

  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "secretsmanager:GetSecretValue"
        ],
        "Resource": [
          aws_secretsmanager_secret.docker_hub.arn
        ]
      }
    ]
  })
}
