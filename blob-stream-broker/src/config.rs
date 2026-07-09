use anyhow::{Context, Result};
use bd_pgv::proto_validate;
use blob_stream_proto::protos::blobstream::v1::config::RuntimeConfig;
use log::{debug, trace};
use std::fs;
use std::path::Path;

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
