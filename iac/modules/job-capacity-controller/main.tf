locals {
  job_env_vars = {
    for key, value in var.job_env_vars : key => trimspace(value)
    if try(trimspace(value), "") != ""
  }
}

resource "nomad_job" "capacity_controller" {
  jobspec = templatefile("${path.module}/jobs/capacity-controller.hcl", {
    node_pool       = var.node_pool
    artifact_source = var.artifact_source
    job_env_vars    = local.job_env_vars
  })
}
