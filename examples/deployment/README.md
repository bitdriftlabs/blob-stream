# Kubernetes Deployment Reference

This Terraform example deploys one Blob Stream broker writer into a Kubernetes cluster. It
includes the namespace, service account, ConfigMaps, Deployment, Service, autoscaler,
pod-disruption budget, and endpoint-discovery RBAC required by the broker. It is a per-cluster
reference module, not a complete infrastructure deployment.

The bundled configuration has one profile: `environment = "example"` and
`cluster_name = "example-cluster"`. That profile runs a single writer for a `telemetry` topic with
eight partitions. Update the profile when using a different environment or cluster name.

## Inputs

Set these values from the calling Terraform configuration:

```hcl
cluster_name     = "example-cluster"
environment      = "example"
revision         = "image-tag-or-sha"
account          = "123456789012"
resource_prefix  = "example-blob-stream"
image_repository = "blob-stream-broker"
```

`account` is used for both the ECR registry and the IAM role ARN. `resource_prefix` determines the
external resource names. With the values above, create the following resources before applying:

- S3 bucket: `example-blob-stream-example`
- IAM role: `example-blob-stream-example`
- DynamoDB tables: `example-blob-stream-segment-metadata-example`,
  `example-blob-stream-producer-partition-leases-example`,
  `example-blob-stream-consumer-group-leases-example`, and
  `example-blob-stream-consumer-group-membership-example`

The IAM role must be configured for the service account through IRSA and have access to the bucket
and tables. The caller must also configure the Kubernetes provider and publish the selected broker
image to the referenced ECR repository. This example does not create cloud resources, configure a
provider, or provide root Terraform configuration. See the
[infrastructure guide](../../docs/infrastructure.md) for the runtime and storage configuration
contract.
