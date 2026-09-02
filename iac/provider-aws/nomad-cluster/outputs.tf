output "client_autoscaling_group_name" {
  description = "Name of the sandbox client Auto Scaling group"
  value       = module.client.autoscaling_group_name
}

output "client_autoscaling_group_arn" {
  description = "ARN of the sandbox client Auto Scaling group"
  value       = module.client.autoscaling_group_arn
}
