//! Configuration data models.
//!
//! This module defines the structures for parsing YAML configuration
//! following the OpenTelemetry configuration schema.

use serde::Deserialize;
use std::collections::HashMap;

/// Top-level telemetry configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    /// Metrics configuration
    #[serde(default)]
    pub metrics: Option<MetricsConfig>,

    /// Traces configuration
    #[serde(default)]
    pub traces: Option<TracesConfig>,

    /// Logs configuration
    #[serde(default)]
    pub logs: Option<LogsConfig>,

    /// Resource attributes
    #[serde(default)]
    pub resource: ResourceConfig,
}

/// Metrics configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    /// Metric readers configuration
    pub readers: Vec<ReaderConfig>,
}

/// Traces configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TracesConfig {
    /// Span processors configuration
    #[serde(default)]
    pub processors: Vec<ProcessorConfig>,
}

/// Logs configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogsConfig {
    /// Log processors configuration
    #[serde(default)]
    pub processors: Vec<LogProcessorConfig>,
}

/// Reader configuration (periodic or pull).
#[derive(Debug)]
pub enum ReaderConfig {
    Periodic(PeriodicReaderConfig),
    Pull(PullReaderConfig),
}

impl<'de> Deserialize<'de> for ReaderConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let map: HashMap<String, serde_yaml::Value> = HashMap::deserialize(deserializer)?;

        if let Some((key, value)) = map.into_iter().next() {
            match key.as_str() {
                "periodic" => {
                    let config: PeriodicReaderConfig =
                        serde_yaml::from_value(value).map_err(serde::de::Error::custom)?;
                    Ok(ReaderConfig::Periodic(config))
                }
                "pull" => {
                    let config: PullReaderConfig =
                        serde_yaml::from_value(value).map_err(serde::de::Error::custom)?;
                    Ok(ReaderConfig::Pull(config))
                }
                _ => Err(serde::de::Error::unknown_variant(&key, &["periodic", "pull"])),
            }
        } else {
            Err(serde::de::Error::custom("Empty reader configuration"))
        }
    }
}

/// Periodic reader configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeriodicReaderConfig {
    /// Export interval in milliseconds
    #[serde(default)]
    pub interval_millis: Option<u64>,

    /// Export timeout in milliseconds
    #[serde(default)]
    pub timeout_millis: Option<u64>,

    /// Exporter configuration (name and settings)
    pub exporter: serde_yaml::Value,
}

/// Pull reader configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullReaderConfig {
    /// Exporter configuration (name and settings)
    #[serde(default)]
    pub exporter: Option<serde_yaml::Value>,
}

/// Span processor configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessorConfig {
    /// Batch processor configuration
    #[serde(default)]
    pub batch: Option<BatchProcessorConfig>,

    /// Simple processor configuration
    #[serde(default)]
    pub simple: Option<SimpleProcessorConfig>,
}

/// Batch span processor configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchProcessorConfig {
    /// Max queue size
    #[serde(default)]
    pub max_queue_size: Option<usize>,

    /// Max export batch size
    #[serde(default)]
    pub max_export_batch_size: Option<usize>,

    /// Schedule delay in milliseconds
    #[serde(default)]
    pub schedule_delay_millis: Option<u64>,

    /// Export timeout in milliseconds
    #[serde(default)]
    pub export_timeout_millis: Option<u64>,

    /// Exporter configuration
    pub exporter: serde_yaml::Value,
}

/// Simple span processor configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimpleProcessorConfig {
    /// Exporter configuration
    pub exporter: serde_yaml::Value,
}

/// Log processor configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogProcessorConfig {
    /// Batch processor configuration
    #[serde(default)]
    pub batch: Option<BatchLogProcessorConfig>,

    /// Simple processor configuration
    #[serde(default)]
    pub simple: Option<SimpleLogProcessorConfig>,
}

/// Batch log processor configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchLogProcessorConfig {
    /// Max queue size
    #[serde(default)]
    pub max_queue_size: Option<usize>,

    /// Max export batch size
    #[serde(default)]
    pub max_export_batch_size: Option<usize>,

    /// Schedule delay in milliseconds
    #[serde(default)]
    pub schedule_delay_millis: Option<u64>,

    /// Export timeout in milliseconds
    #[serde(default)]
    pub export_timeout_millis: Option<u64>,

    /// Exporter configuration
    pub exporter: serde_yaml::Value,
}

/// Simple log processor configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimpleLogProcessorConfig {
    /// Exporter configuration
    pub exporter: serde_yaml::Value,
}

/// Resource attributes configuration.
#[derive(Debug, Deserialize, Default)]
pub struct ResourceConfig {
    /// Resource attributes as key-value pairs
    #[serde(flatten)]
    pub attributes: HashMap<String, serde_yaml::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_telemetry_config() {
        let yaml = r#"
metrics:
  readers:
    - periodic:
        exporter:
          console:
            temporality: delta
resource:
  service.name: "test-service"
  service.version: "1.0.0"
"#;

        let config: TelemetryConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.metrics.is_some());
        assert_eq!(config.resource.attributes.len(), 2);
    }

    #[test]
    fn test_deserialize_periodic_reader() {
        let yaml = r#"
periodic:
  interval_millis: 5000
  timeout_millis: 1000
  exporter:
    console:
      temporality: cumulative
"#;

        let reader: ReaderConfig = serde_yaml::from_str(yaml).unwrap();
        match reader {
            ReaderConfig::Periodic(config) => {
                assert_eq!(config.interval_millis, Some(5000));
                assert_eq!(config.timeout_millis, Some(1000));
            }
            _ => panic!("Expected periodic reader"),
        }
    }

    #[test]
    fn test_deserialize_pull_reader() {
        let yaml = r#"
pull:
  exporter:
    prometheus:
      host: localhost
      port: 9090
"#;

        let reader: ReaderConfig = serde_yaml::from_str(yaml).unwrap();
        match reader {
            ReaderConfig::Pull(_) => {}
            _ => panic!("Expected pull reader"),
        }
    }

    #[test]
    fn test_invalid_reader_type() {
        let yaml = r#"
invalid:
  exporter:
    console: {}
"#;

        let result: Result<ReaderConfig, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_deny_unknown_fields() {
        let yaml = r#"
metrics:
  readers:
    - periodic:
        unknown_field: value
        exporter:
          console: {}
"#;

        let result: Result<TelemetryConfig, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
    }
}
