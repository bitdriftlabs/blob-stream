variable "cluster_name" {
  type        = string
  description = "Name of the Kubernetes cluster; this example uses example-cluster"
}

variable "environment" {
  type        = string
  description = "Name of the deployment configuration; this example uses example"
}

variable "revision" {
  type        = string
  description = "Container image tag or SHA revision"
  default     = ""
}

variable "account" {
  type        = string
  description = "AWS account ID containing the IAM role and ECR registry"
}

variable "resource_prefix" {
  type        = string
  description = "Prefix for the S3 bucket, DynamoDB tables, and IAM role"
  default     = "example-blob-stream"
}

variable "image_repository" {
  type        = string
  description = "ECR repository containing the broker image"
  default     = "blob-stream-broker"
}
