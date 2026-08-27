resource "aws_security_group" "ingress" {
  name   = "${var.prefix}ingress-load-balancer"
  vpc_id = module.init.vpc_id

  ingress {
    from_port        = 80
    to_port          = 80
    protocol         = "TCP"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  ingress {
    from_port        = 443
    to_port          = 443
    protocol         = "TCP"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  // We are already limiting network access on ingress of specific instances
  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    description      = "Allow all outbound traffic"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Name = "${var.prefix}ingress-load-balancer"
  }
}

resource "aws_lb" "ingress" {
  name               = "${var.prefix}ingress"
  internal           = false
  load_balancer_type = "application"
  idle_timeout = var.capacity_autoscaler_enabled && var.capacity_controller_demand_mode == "start-intent-v1" ? var.capacity_ingress_idle_timeout_seconds : (
    var.capacity_autoscaler_enabled ? 180 : 60
  )
  subnets = module.init.vpc_public_subnet_ids
  security_groups = [
    aws_security_group.ingress.id
  ]

  lifecycle {
    precondition {
      condition = !var.capacity_autoscaler_enabled || contains([
        "legacy-failure-ledger:legacy-failure-ledger",
        "dual-write:legacy-failure-ledger",
        "dual-write:start-intent-v1",
        "start-intent-v1:start-intent-v1",
      ], "${var.capacity_api_demand_mode}:${var.capacity_controller_demand_mode}")
      error_message = "capacity API/controller demand modes must form a safe explicit migration pair."
    }

    precondition {
      condition = !var.capacity_autoscaler_enabled || var.capacity_api_demand_mode == "legacy-failure-ledger" || (
        var.capacity_api_pool_vcpu != null &&
        var.capacity_api_pool_memory_mib != null &&
        try(trimspace(var.capacity_api_pool_cpu_architecture), "") != "" &&
        try(trimspace(var.capacity_api_pool_cpu_family), "") != "" &&
        try(trimspace(var.capacity_api_pool_cpu_model), "") != ""
      )
      error_message = "capacity_api_pool_vcpu, capacity_api_pool_memory_mib, and the pool CPU architecture/family/model are required for dual-write or start-intent-v1."
    }

    precondition {
      condition = !var.capacity_autoscaler_enabled || var.capacity_controller_demand_mode != "start-intent-v1" || (
        var.capacity_ingress_idle_timeout_seconds >= ceil(local.capacity_api_wait_seconds) + 75
      )
      error_message = "capacity_ingress_idle_timeout_seconds must cover capacity_api_wait_timeout plus the API's 70-second normal request budget and 5 seconds of server grace."
    }

    precondition {
      condition = !var.capacity_autoscaler_enabled || var.capacity_controller_demand_mode != "start-intent-v1" || (
        local.capacity_controller_batch_idle_seconds > 0 &&
        local.capacity_controller_batch_max_seconds > 0 &&
        local.capacity_controller_batch_idle_seconds <= local.capacity_controller_batch_max_seconds &&
        local.capacity_controller_batch_max_seconds < local.capacity_api_wait_seconds
      )
      error_message = "start-intent batching requires batch idle duration <= batch max duration < capacity_api_wait_timeout."
    }
  }

  access_logs {
    bucket  = data.aws_s3_bucket.load_balancer_logs.id
    prefix  = local.ingress_logs_path_prefix
    enabled = true
  }
}

resource "aws_lb_listener" "ingress_redirect" {
  load_balancer_arn = aws_lb.ingress.arn

  port     = "80"
  protocol = "HTTP"

  default_action {
    type = "redirect"

    redirect {
      protocol    = "HTTPS"
      port        = "443"
      status_code = "HTTP_301"
    }
  }
}

resource "aws_lb_listener" "ingress_wildcard" {
  load_balancer_arn = aws_lb.ingress.arn

  port     = "443"
  protocol = "HTTPS"

  ssl_policy      = "ELBSecurityPolicy-2016-08"
  certificate_arn = aws_acm_certificate_validation.wildcard.certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.ingress.arn
  }
}

resource "aws_lb_listener_rule" "ingress_grpc" {
  listener_arn = aws_lb_listener.ingress_wildcard.arn
  priority     = 20

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.ingress_grpc.arn
  }

  condition {
    http_header {
      http_header_name = "content-type"
      values           = ["application/grpc*"]
    }
  }
}

resource "aws_lb_listener_rule" "nomad" {
  listener_arn = aws_lb_listener.ingress_wildcard.arn
  priority     = 10

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.nomad.arn
  }

  condition {
    host_header {
      values = [
        "nomad.${var.domain_name}"
      ]
    }
  }
}

resource "aws_lb_target_group" "ingress" {
  name   = "${var.prefix}ingress"
  port   = local.ingress_port
  vpc_id = module.init.vpc_id

  protocol         = "HTTP"
  protocol_version = "HTTP1"
  target_type      = "instance"

  deregistration_delay = 30

  health_check {
    path                = "/ping"
    protocol            = "HTTP"
    matcher             = "200"
    interval            = 5
    timeout             = 2
    healthy_threshold   = 2
    unhealthy_threshold = 2
  }
}

resource "aws_lb_target_group" "ingress_grpc" {
  name   = "${var.prefix}ingress-grpc"
  port   = local.ingress_port
  vpc_id = module.init.vpc_id

  protocol         = "HTTP"
  protocol_version = "GRPC"
  target_type      = "instance"

  deregistration_delay = 30

  health_check {
    path                = "/ping"
    protocol            = "HTTP"
    matcher             = "0"
    interval            = 5
    timeout             = 2
    healthy_threshold   = 2
    unhealthy_threshold = 2
  }
}

resource "aws_lb_target_group" "nomad" {
  name   = "${var.prefix}nomad"
  port   = local.nomad_port
  vpc_id = module.init.vpc_id

  protocol         = "HTTP"
  protocol_version = "HTTP1"
  target_type      = "instance"

  deregistration_delay = 30

  health_check {
    path                = "/v1/status/peers"
    protocol            = "HTTP"
    matcher             = "200"
    interval            = 5
    timeout             = 2
    healthy_threshold   = 2
    unhealthy_threshold = 2
  }
}
