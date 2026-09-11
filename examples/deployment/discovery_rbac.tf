resource "kubernetes_role" "blob_stream_endpoints_read" {
  metadata {
    name      = "${local.app_name}-endpoints-read"
    namespace = kubernetes_namespace.blob_stream.metadata[0].name
  }

  rule {
    api_groups = [""]
    resources  = ["endpoints"]
    verbs      = ["get", "list", "watch"]
  }
}

resource "kubernetes_role_binding" "blob_stream_endpoints_read" {
  metadata {
    name      = kubernetes_role.blob_stream_endpoints_read.metadata[0].name
    namespace = kubernetes_namespace.blob_stream.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.blob_stream.metadata[0].name
    namespace = kubernetes_namespace.blob_stream.metadata[0].name
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.blob_stream_endpoints_read.metadata[0].name
  }
}
