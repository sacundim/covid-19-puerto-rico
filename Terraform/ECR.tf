resource "aws_ecr_repository" "downloader_repo" {
  name = "${var.project_name}-downloader"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

data "aws_ecr_image" "downloader" {
  repository_name = aws_ecr_repository.downloader_repo.name
  image_tag       = "latest"
}
