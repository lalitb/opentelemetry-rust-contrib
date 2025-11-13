//! # Custom Metrics Exporter for otel-config-2
//!
//! This crate demonstrates how external exporter implementations can integrate
//! with `otel-config-2` using the simple factory function pattern.
//!
//! ## Key Points
//!
//! - **No traits to implement** for registration
//! - **Just provide a factory function** that returns your concrete exporter type
//! - **Define your own configuration** structure
//! - **Type erasure happens automatically** at registration
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use otel_config_2::ConfigurationRegistry;
//! use otel_config_2_custom_exporter::create_custom_exporter;
//!
//! let mut registry = ConfigurationRegistry::new();
//! registry.register_metric_exporter("custom", create_custom_exporter);
//! ```

use otel_config_2::Error;
use opentelemetry_sdk::{
    error::OTelSdkResult,
    metrics::{data::ResourceMetrics, exporter::PushMetricExporter, Temporality},
};
use serde::Deserialize;
use std::fmt;

/// Configuration for the custom exporter.
///
/// This structure defines all the configuration options for your exporter.
/// It will be automatically deserialized from the YAML configuration.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CustomExporterConfig {
    /// Endpoint URL to send metrics to
    pub endpoint: String,

    /// Timeout in seconds for export operations
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,

    /// Batch size for exporting metrics
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Custom header to include in requests
    #[serde(default)]
    pub custom_header: Option<String>,

    /// Temporality preference (delta or cumulative)
    #[serde(default)]
    pub temporality: Option<CustomTemporality>,

    /// Enable debug logging
    #[serde(default)]
    pub debug: bool,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum CustomTemporality {
    Delta,
    Cumulative,
}

fn default_timeout() -> u64 {
    30
}

fn default_batch_size() -> usize {
    512
}

/// The actual custom exporter implementation.
///
/// This implements the standard OpenTelemetry SDK `PushMetricExporter` trait.
pub struct CustomMetricExporter {
    config: CustomExporterConfig,
}

impl CustomMetricExporter {
    /// Creates a new custom metric exporter with the given configuration.
    pub fn new(config: CustomExporterConfig) -> Self {
        if config.debug {
            println!(
                "[CustomExporter] Initialized with endpoint: {}",
                config.endpoint
            );
            println!(
                "[CustomExporter] Timeout: {}s, Batch size: {}",
                config.timeout_secs, config.batch_size
            );
        }

        Self { config }
    }

    /// Gets the configured endpoint.
    pub fn endpoint(&self) -> &str {
        &self.config.endpoint
    }
}

impl fmt::Debug for CustomMetricExporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CustomMetricExporter")
            .field("endpoint", &self.config.endpoint)
            .field("timeout_secs", &self.config.timeout_secs)
            .field("batch_size", &self.config.batch_size)
            .finish()
    }
}

impl PushMetricExporter for CustomMetricExporter {
    async fn export(&self, _metrics: &ResourceMetrics) -> OTelSdkResult {
        if self.config.debug {
            println!("[CustomExporter] Exporting metrics to {}", self.config.endpoint);
            println!("[CustomExporter] Exporting resource metrics");
        }

        // In a real implementation, you would:
        // 1. Serialize the metrics to your format
        // 2. Send HTTP request to self.config.endpoint
        // 3. Handle response and errors
        // 4. Apply timeout from self.config.timeout_secs

        println!(
            "[CustomExporter] ✓ Successfully exported metrics to {}",
            self.config.endpoint
        );
        
        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        if self.config.debug {
            println!("[CustomExporter] Force flush called");
        }
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: std::time::Duration) -> OTelSdkResult {
        println!("[CustomExporter] Shutting down");
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        match &self.config.temporality {
            Some(CustomTemporality::Delta) => Temporality::Delta,
            Some(CustomTemporality::Cumulative) => Temporality::Cumulative,
            None => Temporality::Cumulative, // Default
        }
    }
}

/// Factory function to create a custom exporter from configuration.
///
/// This is the **only function** you need to provide for otel-config-2 integration!
///
/// ## Key Points
///
/// 1. Takes `serde_yaml::Value` as input (raw configuration)
/// 2. Returns `Result<YourConcreteType, Error>` (NOT a trait object!)
/// 3. No traits to implement
/// 4. No boilerplate needed
///
/// ## Example YAML Configuration
///
/// ```yaml
/// metrics:
///   readers:
///     - periodic:
///         exporter:
///           custom:
///             endpoint: "http://localhost:8080/metrics"
///             timeout_secs: 30
///             batch_size: 512
///             custom_header: "X-API-Key: secret"
///             temporality: cumulative
///             debug: true
/// ```
///
/// ## Usage
///
/// ```rust,no_run
/// use otel_config_2::ConfigurationRegistry;
/// use otel_config_2_custom_exporter::create_custom_exporter;
///
/// let mut registry = ConfigurationRegistry::new();
///
/// // That's it! Type erasure happens automatically.
/// registry.register_metric_exporter("custom", create_custom_exporter);
/// ```
pub fn create_custom_exporter(
    config: serde_yaml::Value,
) -> Result<CustomMetricExporter, Error> {
    // Deserialize the configuration
    let exporter_config = serde_yaml::from_value::<CustomExporterConfig>(config).map_err(|e| {
        Error::InvalidExporterConfig {
            exporter: "custom".to_string(),
            reason: format!("Failed to deserialize custom exporter configuration: {}", e),
        }
    })?;

    // Validate configuration
    if exporter_config.endpoint.is_empty() {
        return Err(Error::InvalidExporterConfig {
            exporter: "custom".to_string(),
            reason: "endpoint cannot be empty".to_string(),
        });
    }

    if exporter_config.timeout_secs == 0 {
        return Err(Error::InvalidExporterConfig {
            exporter: "custom".to_string(),
            reason: "timeout_secs must be greater than 0".to_string(),
        });
    }

    // Create and return the exporter
    // Notice: Returns CONCRETE type, not Box<dyn PushMetricExporter>
    Ok(CustomMetricExporter::new(exporter_config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_deserialization_minimal() {
        let yaml = r#"
endpoint: "http://localhost:8080/metrics"
"#;

        let config: CustomExporterConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.endpoint, "http://localhost:8080/metrics");
        assert_eq!(config.timeout_secs, 30); // default
        assert_eq!(config.batch_size, 512); // default
        assert_eq!(config.debug, false); // default
    }

    #[test]
    fn test_config_deserialization_full() {
        let yaml = r#"
endpoint: "http://localhost:8080/metrics"
timeout_secs: 60
batch_size: 1024
custom_header: "X-API-Key: secret"
temporality: delta
debug: true
"#;

        let config: CustomExporterConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.endpoint, "http://localhost:8080/metrics");
        assert_eq!(config.timeout_secs, 60);
        assert_eq!(config.batch_size, 1024);
        assert_eq!(config.custom_header, Some("X-API-Key: secret".to_string()));
        assert!(matches!(
            config.temporality,
            Some(CustomTemporality::Delta)
        ));
        assert_eq!(config.debug, true);
    }

    #[test]
    fn test_factory_function() {
        let yaml = r#"
endpoint: "http://localhost:8080/metrics"
timeout_secs: 60
"#;
        let config: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();

        let exporter = create_custom_exporter(config).unwrap();
        assert_eq!(exporter.endpoint(), "http://localhost:8080/metrics");
    }

    #[test]
    fn test_factory_validation_empty_endpoint() {
        let yaml = r#"
endpoint: ""
"#;
        let config: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();

        let result = create_custom_exporter(config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("endpoint cannot be empty"));
    }

    #[test]
    fn test_factory_validation_zero_timeout() {
        let yaml = r#"
endpoint: "http://localhost:8080/metrics"
timeout_secs: 0
"#;
        let config: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();

        let result = create_custom_exporter(config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("timeout_secs must be greater than 0"));
    }

    #[test]
    fn test_exporter_temporality() {
        let yaml = r#"
endpoint: "http://localhost:8080/metrics"
temporality: delta
"#;
        let config: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();

        let exporter = create_custom_exporter(config).unwrap();
        assert_eq!(exporter.temporality(), Temporality::Delta);
    }
}
