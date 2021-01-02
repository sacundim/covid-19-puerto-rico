resource "aws_ecr_repository" "script_image_repo" {
  name = "covid-19-puerto-rico-scripts"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

data "aws_ecr_image" "scripts" {
  repository_name = aws_ecr_repository.script_image_repo.name
  image_tag       = "latest"
}
