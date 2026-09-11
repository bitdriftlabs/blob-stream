resource "kubernetes_config_map" "blob_stream_config" {
  metadata {
    name      = "${local.app_name}-config"
    namespace = kubernetes_namespace.blob_stream.metadata[0].name
  }

  data = {
    "config.yaml" = yamlencode(local.broker_config)
  }
}

resource "kubernetes_config_map" "blob_stream_feature_flags" {
  metadata {
    name      = "${local.app_name}-feature-flags"
    namespace = kubernetes_namespace.blob_stream.metadata[0].name
  }

  data = {
    "feature_flags.yaml" = yamlencode({
      values = local.feature_flags
    })
  }
}

resource "kubernetes_deployment" "blob_stream" {
  wait_for_rollout = false

  lifecycle {
    ignore_changes = [spec[0].replicas]
  }

  metadata {
    name      = local.app_name
    namespace = kubernetes_namespace.blob_stream.metadata[0].name
    labels = {
      app = local.app_name
    }
  }

  spec {
    replicas = local.scaling_config.min_replicas

    selector {
      match_labels = {
        app = local.app_name
      }
    }

    template {
      metadata {
        labels = {
          app = local.app_name
        }
      }

      spec {
        service_account_name = kubernetes_service_account.blob_stream.metadata[0].name

        topology_spread_constraint {
          max_skew           = 1
          topology_key       = "kubernetes.io/hostname"
          when_unsatisfiable = "ScheduleAnyway"

          label_selector {
            match_labels = {
              app = local.app_name
            }
          }
        }

        container {
          name  = local.app_name
          image = "${var.account}.dkr.ecr.${local.aws_region}.amazonaws.com/${var.image_repository}:${var.revision}"

          port {
            container_port = 8080
          }

          env {
            name  = "BLOB_STREAM_CONFIG"
            value = "/etc/blob-stream/config.yaml"
          }

          readiness_probe {
            http_get {
              path = "/metrics"
              port = 8080
            }
          }

          liveness_probe {
            http_get {
              path = "/metrics"
              port = 8080
            }
          }

          resources {
            requests = {
              cpu    = local.scaling_config.cpu_request
              memory = local.scaling_config.memory_request
            }
            limits = {
              cpu    = local.scaling_config.cpu_limit
              memory = local.scaling_config.memory_limit
            }
          }

          volume_mount {
            name       = "config"
            mount_path = "/etc/blob-stream"
            read_only  = true
          }

          volume_mount {
            name       = "feature-flags"
            mount_path = "/etc/blob-stream/feature_flags"
            read_only  = true
          }
        }

        volume {
          name = "config"

          config_map {
            name = kubernetes_config_map.blob_stream_config.metadata[0].name
          }
        }

        volume {
          name = "feature-flags"

          config_map {
            name = kubernetes_config_map.blob_stream_feature_flags.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_horizontal_pod_autoscaler_v2" "blob_stream" {
  metadata {
    name      = local.app_name
    namespace = kubernetes_namespace.blob_stream.metadata[0].name
  }

  spec {
    min_replicas = local.scaling_config.min_replicas
    max_replicas = local.scaling_config.max_replicas

    scale_target_ref {
      api_version = "apps/v1"
      kind        = "Deployment"
      name        = kubernetes_deployment.blob_stream.metadata[0].name
    }

    metric {
      type = "Resource"

      resource {
        name = "cpu"

        target {
          type                = "Utilization"
          average_utilization = local.scaling_config.cpu_target
        }
      }
    }

    behavior {
      scale_up {
        select_policy = "Max"

        policy {
          type           = "Pods"
          value          = 1
          period_seconds = 60
        }
      }

      scale_down {
        stabilization_window_seconds = 1800
        select_policy                = "Max"

        policy {
          type           = "Pods"
          value          = 1
          period_seconds = 900
        }
      }
    }
  }
}

resource "kubernetes_pod_disruption_budget_v1" "blob_stream" {
  metadata {
    name      = local.app_name
    namespace = kubernetes_namespace.blob_stream.metadata[0].name
  }

  spec {
    max_unavailable = 1

    selector {
      match_labels = {
        app = local.app_name
      }
    }
  }
}
