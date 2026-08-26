job "capacity-controller" {
  type      = "service"
  node_pool = "${node_pool}"
  priority  = 92

  group "capacity-controller" {
    count = 1

    restart {
      interval = "5s"
      attempts = 1
      delay    = "5s"
      mode     = "delay"
    }

    task "capacity-controller" {
      driver       = "raw_exec"
      kill_signal  = "SIGTERM"
      kill_timeout = "30s"

      resources {
        cpu    = 256
        memory = 256
      }

      env {
%{ for key, value in job_env_vars ~}
        ${key} = "${value}"
%{ endfor ~}
      }

      config {
        command = "/bin/bash"
        args    = ["-c", "chmod +x local/capacity-controller && exec local/capacity-controller"]
      }

      artifact {
        source      = "${artifact_source}"
        destination = "local/capacity-controller"
        mode        = "file"
      }
    }
  }
}
