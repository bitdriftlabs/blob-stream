resource "kubernetes_service_account" "blob_stream" {
  metadata {
    name      = local.app_name
    namespace = kubernetes_namespace.blob_stream.metadata[0].name
    annotations = {
      "eks.amazonaws.com/role-arn" = local.target_role_arn
    }
  }
}
