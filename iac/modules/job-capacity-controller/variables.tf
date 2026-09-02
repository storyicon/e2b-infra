variable "node_pool" {
  type = string
}

variable "artifact_source" {
  type        = string
  description = "Full artifact URL for the capacity-controller binary"
}

variable "job_env_vars" {
  type      = map(string)
  default   = {}
  sensitive = true
}
