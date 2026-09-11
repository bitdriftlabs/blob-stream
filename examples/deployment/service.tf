resource "kubernetes_service" "blob_stream" {
  metadata {
    name      = local.service_name
    namespace = kubernetes_namespace.blob_stream.metadata[0].name
    annotations = {
      "prometheus.io/scrape" = "true"
      "prometheus.io/port"   = "8080"
      "prometheus.io/path"   = "/metrics"
    }
    labels = {
      app = local.app_name
    }
  }

  spec {
    selector = {
      app = local.app_name
    }

    port {
      name        = "http"
      port        = 8080
      target_port = 8080
    }
  }
}
