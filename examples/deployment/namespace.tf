resource "kubernetes_namespace" "blob_stream" {
  metadata {
    name = local.namespace
  }
}
