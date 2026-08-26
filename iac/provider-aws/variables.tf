variable "domain_name" {
  type = string
}

variable "allow_force_destroy" {
  default = false
}

variable "prefix" {
  type        = string
  description = "Name prefix for all resources"
}

variable "bucket_prefix" {
  type = string
}

variable "environment" {
  type = string
}

variable "docker_reverse_proxy_enabled" {
  type        = bool
  default     = true
  description = "Whether to create the docker-reverse-proxy ECR repository. The component is deprecated and no AWS Nomad job consumes this repository; set to false to stop creating it."
}

variable "redis_managed" {
  type    = bool
  default = false
}

variable "redis_instance_type" {
  type    = string
  default = "cache.t2.small"
}

variable "redis_replica_size" {
  type    = number
  default = 2
}

variable "api_cluster_size" {
  type    = number
  default = 1
}

variable "api_internal_grpc_port" {
  type    = number
  default = 5009
}

variable "api_env_vars" {
  type      = map(string)
  default   = {}
  sensitive = true
}

variable "api_db_migrator_env_vars" {
  type      = map(string)
  default   = {}
  sensitive = true
}

variable "client_proxy_env_vars" {
  type      = map(string)
  default   = {}
  sensitive = true
}

variable "orchestrator_env_vars" {
  type      = map(string)
  default   = {}
  sensitive = true
}

variable "template_manager_env_vars" {
  type      = map(string)
  default   = {}
  sensitive = true
}

variable "s3_use_path_style" {
  type        = bool
  default     = false
  description = "When true, use path-style S3 addressing (https://host/bucket/key). When false (default), use virtual-host-style (https://bucket.host/key). Set to true for S3-compatible backends (MinIO, Ceph, etc.) that don't support virtual-host addressing."
}

variable "api_server_machine_type" {
  type    = string
  default = "t3.xlarge"
}

variable "api_image_family_prefix" {
  type    = string
  default = ""
}

variable "ingress_count" {
  type    = number
  default = 1
}

variable "client_proxy_count" {
  type    = number
  default = 1
}

variable "clickhouse_cluster_size" {
  type    = number
  default = 1
}

variable "clickhouse_server_machine_type" {
  type    = string
  default = "t3.xlarge"
}

variable "clickhouse_image_family_prefix" {
  type    = string
  default = ""
}

variable "client_cluster_size" {
  type    = number
  default = 1
}

variable "client_cluster_min_size" {
  type        = number
  description = "Minimum number of sandbox client nodes; defaults to client_cluster_size"
  default     = null
  nullable    = true
}

variable "client_cluster_max_size" {
  type        = number
  description = "Maximum number of sandbox client nodes; defaults to client_cluster_size"
  default     = null
  nullable    = true
}

variable "capacity_autoscaler_enabled" {
  type        = bool
  description = "Deploy the AWS scale-out-only sandbox capacity controller"
  default     = false

  validation {
    condition = !var.capacity_autoscaler_enabled || contains([
      "legacy-failure-ledger:legacy-failure-ledger",
      "dual-write:legacy-failure-ledger",
      "dual-write:start-intent-v1",
      "start-intent-v1:start-intent-v1",
    ], "${var.capacity_api_demand_mode}:${var.capacity_controller_demand_mode}")
    error_message = "capacity API/controller demand modes must form a safe explicit migration pair."
  }

  validation {
    condition = !var.capacity_autoscaler_enabled || var.capacity_api_demand_mode == "legacy-failure-ledger" || (
      var.capacity_api_pool_vcpu != null && var.capacity_api_pool_memory_mib != null
    )
    error_message = "capacity_api_pool_vcpu and capacity_api_pool_memory_mib are required for dual-write or start-intent-v1."
  }
}

variable "capacity_controller_cluster_id" {
  type        = string
  description = "E2B cluster whose pending sandbox demand drives the AWS capacity controller"
  default     = "00000000-0000-0000-0000-000000000000"
}

variable "capacity_controller_slots_per_node" {
  type        = number
  description = "Safe sandbox capacity assigned to each client node"
  default     = 20

  validation {
    condition     = var.capacity_controller_slots_per_node > 0
    error_message = "capacity_controller_slots_per_node must be positive."
  }
}

variable "capacity_controller_max_starting_per_node" {
  type        = number
  description = "Maximum concurrent sandbox starts per client node; defaults to the node capacity"
  default     = null

  validation {
    condition     = var.capacity_controller_max_starting_per_node == null ? true : var.capacity_controller_max_starting_per_node > 0
    error_message = "capacity_controller_max_starting_per_node must be positive when set."
  }
}

variable "capacity_controller_reconcile_interval" {
  type        = string
  description = "Interval between capacity controller reconciliations"
  default     = "1s"
}

variable "capacity_api_demand_mode" {
  type        = string
  description = "Explicit API capacity demand write mode"
  default     = "legacy-failure-ledger"

  validation {
    condition = contains([
      "legacy-failure-ledger",
      "dual-write",
      "start-intent-v1",
    ], var.capacity_api_demand_mode)
    error_message = "capacity_api_demand_mode must be legacy-failure-ledger, dual-write, or start-intent-v1."
  }
}

variable "capacity_controller_demand_mode" {
  type        = string
  description = "Explicit capacity controller read mode"
  default     = "legacy-failure-ledger"

  validation {
    condition = contains([
      "legacy-failure-ledger",
      "start-intent-v1",
    ], var.capacity_controller_demand_mode)
    error_message = "capacity_controller_demand_mode must be legacy-failure-ledger or start-intent-v1."
  }
}

variable "capacity_api_wait_timeout" {
  type        = string
  description = "Maximum time Sandbox.create waits for autoscaled capacity when the capacity controller is enabled"
  default     = "120s"
}

variable "capacity_api_retry_interval" {
  type        = string
  description = "Interval between server-side placement retries while waiting for autoscaled capacity"
  default     = "500ms"
}

variable "capacity_api_pool_vcpu" {
  type        = number
  description = "Exact vCPU request accepted by the single autoscaled sandbox pool"
  default     = null
  nullable    = true

  validation {
    condition     = var.capacity_api_pool_vcpu == null ? true : var.capacity_api_pool_vcpu > 0
    error_message = "capacity_api_pool_vcpu must be positive when set."
  }
}

variable "capacity_api_pool_memory_mib" {
  type        = number
  description = "Exact memory request in MiB accepted by the single autoscaled sandbox pool"
  default     = null
  nullable    = true

  validation {
    condition     = var.capacity_api_pool_memory_mib == null ? true : var.capacity_api_pool_memory_mib > 0
    error_message = "capacity_api_pool_memory_mib must be positive when set."
  }
}

variable "capacity_ingress_idle_timeout_seconds" {
  type        = number
  description = "ALB idle timeout used while the capacity autoscaler is enabled; keep above the API capacity wait budget"
  default     = 180

  validation {
    condition     = var.capacity_ingress_idle_timeout_seconds >= 61 && var.capacity_ingress_idle_timeout_seconds <= 4000
    error_message = "capacity_ingress_idle_timeout_seconds must be between 61 and 4000 seconds."
  }
}

variable "capacity_controller_env_vars" {
  type        = map(string)
  description = "Additional environment variables for the sandbox capacity controller"
  default     = {}
  sensitive   = true
}

variable "client_server_machine_type" {
  type    = string
  default = "m8i.4xlarge"
}

variable "client_server_nested_virtualization" {
  type    = bool
  default = true
}

variable "client_node_labels" {
  description = "Labels to assign to client nodes for scheduling purposes"
  type        = list(string)
  default     = []
}

variable "client_image_family_prefix" {
  type    = string
  default = ""
}

variable "control_server_machine_type" {
  type    = string
  default = "t3.medium"
}

variable "control_server_image_family_prefix" {
  type    = string
  default = ""
}

variable "orchestrator_port" {
  type    = number
  default = 5008
}

variable "orchestrator_proxy_port" {
  type    = number
  default = 5007
}

variable "allow_sandbox_internal_cidrs" {
  type        = string
  description = "Comma-separated CIDRs to allow through the sandbox firewall deny list (e.g. 10.0.0.1/32,10.0.0.2/32)"
  default     = ""
}

variable "envd_timeout" {
  type    = string
  default = "40s"
}

variable "build_cluster_size" {
  type    = number
  default = 1
}

variable "build_server_machine_type" {
  type    = string
  default = "m8i.2xlarge"
}

variable "build_server_nested_virtualization" {
  type    = bool
  default = true
}

variable "build_node_labels" {
  description = "Labels to assign to build nodes for scheduling purposes"
  type        = list(string)
  default     = []
}

variable "control_server_cluster_size" {
  type    = number
  default = 3
}

variable "traefik_config_files" {
  type        = map(string)
  description = "Map of filename => content for additional Traefik dynamic configuration files"
  default     = {}
}

variable "db_max_open_connections" {
  type    = number
  default = 40
}

variable "db_min_idle_connections" {
  type    = number
  default = 5
}

variable "auth_db_max_open_connections" {
  type    = number
  default = 20
}

variable "auth_db_min_idle_connections" {
  type    = number
  default = 5
}

variable "enable_otel_router_logs" {
  type        = bool
  default     = false
  description = "Enable teeing non-internal customer logs from Vector to otel-router."
}

variable "otel_router_http_port" {
  type        = number
  default     = 4321
  description = "Local otel-router Vector-compatible logs port used by Vector when otel-router log teeing is enabled."
}

variable "enable_otel_router_metrics" {
  type        = bool
  default     = false
  description = "Enable teeing external customer metrics from otel-collector to otel-router."
}

variable "otel_router_grpc_port" {
  type        = number
  default     = 4320
  description = "Local otel-router OTLP gRPC port used by otel-collector when otel-router metric teeing is enabled."
}
