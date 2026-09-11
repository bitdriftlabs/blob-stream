locals {
  aws_region      = "us-east-1"
  namespace       = "blob-stream"
  app_name        = "blob-stream-broker"
  service_name    = local.app_name
  target_role_arn = "arn:aws:iam::${var.account}:role/${var.resource_prefix}-${var.environment}"

  s3_bucket_name = "${var.resource_prefix}-${var.environment}"
  s3_prefix      = "blob-stream/"

  environment_configs = {
    example = {
      deployments = {
        "example-cluster" = {
          writer_id = 0
        }
      }
      feature_flags = {
        blob_stream_broker_fenced_metadata_writes = false
      }
      broker_leasing = {
        lease_duration     = "120s"
        heartbeat_interval = "40s"
      }
      scaling = {
        min_replicas   = 2
        max_replicas   = 4
        cpu_target     = 80
        cpu_request    = "1"
        memory_request = "2Gi"
        cpu_limit      = "2"
        memory_limit   = "4Gi"
      }
      topics = {
        telemetry = {
          partition_count = 8
          num_writers     = 1
          retention       = "604800s"
        }
      }
    }
  }

  environment_config = local.environment_configs[var.environment]
  deployment_config  = local.environment_config.deployments[var.cluster_name]
  feature_flags      = local.environment_config.feature_flags
  broker_leasing     = local.environment_config.broker_leasing
  scaling_config     = local.environment_config.scaling

  topic_configs = [
    for name, topic in local.environment_config.topics : merge(topic, { name = name })
  ]

  segment_metadata_table_name = "${var.resource_prefix}-segment-metadata-${var.environment}"

  producer_partition_lease_table_name = "${var.resource_prefix}-producer-partition-leases-${var.environment}"

  consumer_group_lease_table_name = "${var.resource_prefix}-consumer-group-leases-${var.environment}"

  consumer_group_membership_table_name = "${var.resource_prefix}-consumer-group-membership-${var.environment}"

  segment_ttl_buffer = "3600s"
  lease_ttl_buffer   = "3600s"

  broker_config = {
    broker = {
      bind_addr                 = "0.0.0.0:8080"
      flush_max_bytes           = 67108864
      flush_max_delay           = "1s"
      lease_duration            = local.broker_leasing.lease_duration
      heartbeat_interval        = local.broker_leasing.heartbeat_interval
      sequence_reservation_size = 10000
      feature_flags = {
        dir  = "/etc/blob-stream/feature_flags"
        file = "/etc/blob-stream/feature_flags/feature_flags.yaml"
      }
      node_identity = {
        hostname = {}
      }
      writer_id = local.deployment_config.writer_id
      discovery = {
        k8s_service = {
          namespace    = local.namespace
          service_name = local.service_name
        }
      }
    }
    topics = local.topic_configs
    blob_store = {
      s3 = {
        bucket = local.s3_bucket_name
        prefix = local.s3_prefix
        region = local.aws_region
      }
    }
    metadata_store = {
      dynamo = {
        segment_metadata_table_name          = local.segment_metadata_table_name
        producer_partition_lease_table_name  = local.producer_partition_lease_table_name
        consumer_group_lease_table_name      = local.consumer_group_lease_table_name
        consumer_group_membership_table_name = local.consumer_group_membership_table_name
        segment_ttl_buffer                   = local.segment_ttl_buffer
        lease_ttl_buffer                     = local.lease_ttl_buffer
        region                               = local.aws_region
      }
    }
  }
}
