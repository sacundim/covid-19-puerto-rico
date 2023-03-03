output "ec2_queue_name" {
  value = aws_batch_job_queue.ec2_amd64.name
}

output "fargate_queue_name" {
  value = aws_batch_job_queue.fargate_amd64.name
}
