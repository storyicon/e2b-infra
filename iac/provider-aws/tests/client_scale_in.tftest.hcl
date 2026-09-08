# Plan-only tests with a mocked provider: no credentials or cloud resources.
mock_provider "aws" {
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}

variables {
  prefix                            = "test-"
  name                              = "client"
  aws_account_id                    = "123456789012"
  cluster_tag_name                  = "cluster"
  cluster_tag_value                 = "test"
  cluster_node_policy_arn           = "arn:aws:iam::123456789012:policy/test"
  cluster_node_ec2_policy_json      = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
  setup_bucket_name                 = "test-setup"
  setup_files_hash                  = { run-consul = "test", run-nomad = "test" }
  security_group_ids                = ["sg-0123456789abcdef0"]
  vpc_private_subnets               = ["subnet-0123456789abcdef0"]
  min_size                          = 1
  max_size                          = 500
  node_pool_name                    = "client"
  node_labels                       = []
  consul_acl_token                  = "test-only"
  consul_gossip_encryption_key      = "test-only"
  consul_dns_request_token          = "test-only"
  aws_ecr_account_repository_domain = "123456789012.dkr.ecr.us-west-2.amazonaws.com"
  fc_kernels_bucket_name            = "test-kernels"
  fc_versions_bucket_name           = "test-versions"
  fc_env_pipeline_bucket_name       = "test-pipeline"
  fc_busybox_bucket_name            = "test-busybox"
  fc_env_pipeline_bucket_arn        = "arn:aws:s3:::test-pipeline"
  fc_kernels_bucket_arn             = "arn:aws:s3:::test-kernels"
  fc_versions_bucket_arn            = "arn:aws:s3:::test-versions"
  fc_busybox_bucket_arn             = "arn:aws:s3:::test-busybox"
  templates_bucket_arn              = "arn:aws:s3:::test-templates"
  templates_build_cache_bucket_arn  = "arn:aws:s3:::test-cache"
  custom_environments_repo_arn      = "arn:aws:ecr:us-west-2:123456789012:repository/test"
}

run "default_keeps_asg_processes_enabled" {
  command = plan
  module {
    source = "./modules/nodepool-client"
  }
  assert {
    condition     = length(aws_autoscaling_group.client.suspended_processes) == 0
    error_message = "Default client/build pools must not suspend ASG processes."
  }
}

run "enforcement_suspends_only_az_rebalance" {
  command = plan
  module {
    source = "./modules/nodepool-client"
  }
  variables {
    protect_from_scale_in        = true
    scale_in_protection_required = true
    suspend_az_rebalance         = true
  }
  assert {
    condition     = aws_autoscaling_group.client.suspended_processes == toset(["AZRebalance"])
    error_message = "Only proactive AZ rebalancing may be suspended."
  }
  assert {
    condition     = aws_autoscaling_group.client.protect_from_scale_in
    error_message = "New worker protection must remain enabled."
  }
}

run "disabling_enforcement_keeps_protection" {
  command = plan
  module {
    source = "./modules/nodepool-client"
  }
  variables {
    protect_from_scale_in = true
    suspend_az_rebalance  = false
  }
  assert {
    condition     = length(aws_autoscaling_group.client.suspended_processes) == 0 && aws_autoscaling_group.client.protect_from_scale_in
    error_message = "Disabling the setting must resume processes without removing new worker protection."
  }
}
