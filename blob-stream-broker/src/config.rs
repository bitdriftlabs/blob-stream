#[cfg(test)]
#[path = "./config_test.rs"]
mod tests;

use anyhow::{Context, Result, ensure};
use bd_log::otel::{LogConfig, OtelCollectorConfig};
use bd_pgv::proto_validate;
use blob_stream_proto::protos::blobstream::v1::config::{BrokerConfig, RuntimeConfig};
use log::{debug, trace};
use std::fs;
use std::path::Path;

pub fn broker_log_config(config: &BrokerConfig) -> Result<LogConfig> {
  let mut log_config = LogConfig::default();
  if let Some(hostname) = config.otlp_collector_hostname.as_ref() {
    ensure!(
      !hostname.trim().is_empty(),
      "OTLP collector hostname must not be empty"
    );
    let mut otel =
      OtelCollectorConfig::new("blob-stream-broker", format!("http://{hostname}:4317"));
    if let Ok(pod_name) = hostname::get().and_then(|hostname| {
      hostname
        .into_string()
        .map_err(|_| std::io::Error::other("invalid hostname"))
    }) {
      otel
        .resource_attributes
        .insert("k8s.pod.name".to_string(), pod_name);
    }
    if let Ok(cluster_name) = std::env::var("K8S_CLUSTER_NAME")
      && !cluster_name.is_empty()
    {
      otel
        .resource_attributes
        .insert("k8s.cluster.name".to_string(), cluster_name);
    }
    log_config.otel = Some(otel);
  }
  Ok(log_config)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigFormat {
  Json,
  Yaml,
}

impl ConfigFormat {
  #[must_use]
  pub fn from_path(path: &Path) -> Option<Self> {
    match path.extension().and_then(|ext| ext.to_str()) {
      Some("json") => Some(Self::Json),
      Some("yaml" | "yml") => Some(Self::Yaml),
      _ => None,
    }
  }
}

pub fn decode_runtime_config_str(input: &str, format: ConfigFormat) -> Result<RuntimeConfig> {
  trace!("decoding broker runtime config payload: format={format:?}");
  let json_payload = match format {
    ConfigFormat::Json => input.to_string(),
    ConfigFormat::Yaml => {
      let yaml_value: serde_json::Value =
        serde_yaml::from_str(input).context("failed to parse YAML config")?;
      serde_json::to_string(&yaml_value).context("failed to encode YAML as JSON")?
    },
  };

  let config = protobuf_json_mapping::parse_from_str::<RuntimeConfig>(&json_payload)
    .context("failed to decode runtime config from JSON")?;
  proto_validate::validate(&config).context("runtime config validation failed")?;
  Ok(config)
}

pub fn load_runtime_config(path: &Path) -> Result<RuntimeConfig> {
  debug!(
    "loading broker runtime config from path={path}",
    path = path.display()
  );
  let format = ConfigFormat::from_path(path).context("unsupported config file extension")?;
  let contents = fs::read_to_string(path).with_context(|| {
    format!(
      "failed to read runtime config from {path}",
      path = path.display()
    )
  })?;
  decode_runtime_config_str(&contents, format)
}
