output "autoscaling_group_name" {
  description = "Name of the client node Auto Scaling group"
  value       = aws_autoscaling_group.client.name
}

output "autoscaling_group_arn" {
  description = "ARN of the client node Auto Scaling group"
  value       = aws_autoscaling_group.client.arn
}
