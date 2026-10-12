use anyhow::{Result, anyhow};
use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_types::region::Region;
use bd_runtime_config::feature_flags::FeatureFlagsWatch;
use blob_stream_blob_store::{BlobStore, InMemoryBlobStore, S3BlobStore};
use blob_stream_metadata_store::{aws_retry_config, aws_timeout_config};
use blob_stream_proto::protos::blobstream::v1::config::{BlobStoreConfig, RuntimeConfig};
use log::debug;
use std::sync::Arc;

#[cfg(test)]
#[path = "./storage_test.rs"]
mod tests;

//
// BrokerBlobStore
//

/// Blob store and key prefix shared by broker runtime components.
pub struct BrokerBlobStore {
  pub blob_store: Arc<dyn BlobStore>,
  pub prefix: Option<String>,
}

/// Build the blob store configured for the broker runtime.
pub async fn build_runtime_blob_store(config: &RuntimeConfig) -> Result<BrokerBlobStore> {
  build_runtime_blob_store_with_feature_flags(config, None).await
}

pub async fn build_runtime_blob_store_with_feature_flags(
  config: &RuntimeConfig,
  feature_flags: Option<FeatureFlagsWatch>,
) -> Result<BrokerBlobStore> {
  let blob_store = config
    .blob_store
    .as_ref()
    .ok_or_else(|| anyhow!("runtime config missing blob_store config"))?;
  build_blob_store(blob_store, feature_flags).await
}

async fn build_blob_store(
  config: &BlobStoreConfig,
  feature_flags: Option<FeatureFlagsWatch>,
) -> Result<BrokerBlobStore> {
  if config.has_in_memory() {
    debug!("using in-memory blob store backend");
    return Ok(BrokerBlobStore {
      blob_store: Arc::new(InMemoryBlobStore::new()),
      prefix: None,
    });
  }

  if config.has_s3() {
    debug!("using s3 blob store backend");
    let s3 = config.s3();
    let region_provider = RegionProviderChain::first_try(Some(Region::new(s3.region.to_string())));
    let mut loader = aws_config::defaults(BehaviorVersion::latest())
      .region(region_provider)
      .retry_config(aws_retry_config())
      .timeout_config(aws_timeout_config());
    if !s3.endpoint.is_empty() {
      loader = loader.endpoint_url(s3.endpoint.to_string());
    }

    let shared = loader.load().await;
    let client = if s3.endpoint.is_empty() {
      aws_sdk_s3::Client::new(&shared)
    } else {
      // Local S3-compatible endpoints (e.g. LocalStack) require path-style requests.
      let config = aws_sdk_s3::config::Builder::from(&shared)
        .force_path_style(true)
        .build();
      aws_sdk_s3::Client::from_conf(config)
    };
    return Ok(broker_blob_store_from_s3_config(s3, client, feature_flags));
  }

  Err(anyhow!("blob_store backend not configured"))
}

fn broker_blob_store_from_s3_config(
  config: &blob_stream_proto::protos::blobstream::v1::config::S3BlobStoreConfig,
  client: aws_sdk_s3::Client,
  feature_flags: Option<FeatureFlagsWatch>,
) -> BrokerBlobStore {
  BrokerBlobStore {
    blob_store: Arc::new(S3BlobStore::new(
      client,
      config.bucket.to_string(),
      feature_flags,
    )),
    prefix: (!config.prefix.is_empty()).then(|| config.prefix.to_string()),
  }
}
