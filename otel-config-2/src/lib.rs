//! # Simplified Declarative Configuration for OpenTelemetry
//!
//! This crate provides a simplified, factory-based approach to configuring
//! OpenTelemetry SDK providers from YAML configuration files.
//!
//! ## Key Features
//!
//! - **Simple factory functions**: Just write a function that returns a concrete exporter
//! - **No traits to implement**: Type erasure happens at registration time
//! - **Standard SDK types**: Returns concrete exporter types
//! - **No builder exposure**: SDK wiring is handled internally
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use otel_config_2::{ConfigurationRegistry, TelemetryProvider};
//!
//! // Just a simple function returning concrete type
//! fn create_console_exporter(
//!     config: serde_yaml::Value
//! ) -> Result<opentelemetry_stdout::MetricExporter, otel_config_2::Error> {
//!     // Parse config and create exporter
//!     Ok(opentelemetry_stdout::MetricExporter::default())
//! }
//!
//! // Register and build
//! let mut registry = ConfigurationRegistry::new();
//! registry.register_metric_exporter("console", create_console_exporter);
//!
//! let providers = TelemetryProvider::build_from_yaml_file(&registry, "config.yaml")?;
//! # Ok::<(), otel_config_2::Error>(())
//! ```

use std::collections::HashMap;
use std::path::Path;

use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::Resource;

pub mod error;
pub mod model;

pub use error::Error;

// Re-export common types
pub use opentelemetry_sdk::logs::SdkLoggerProvider;
pub use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
pub use opentelemetry_sdk::trace::{SpanExporter, SdkTracerProvider};

// ============================================================================
// Object-Safe Configurator Traits
// ============================================================================

/// Object-safe trait for configuring a meter provider with an exporter.
///
/// This trait is implemented automatically by the generic `MetricExporterFactory`
/// wrapper. Users don't implement this directly.
trait MetricExporterConfigurator: Send + Sync {
    fn configure(
        &self,
        builder: opentelemetry_sdk::metrics::MeterProviderBuilder,
        config: serde_yaml::Value,
    ) -> Result<opentelemetry_sdk::metrics::MeterProviderBuilder, Error>;
}

/// Object-safe trait for configuring a tracer provider with an exporter.
#[allow(dead_code)]
trait TraceExporterConfigurator: Send + Sync {
    fn configure(
        &self,
        builder: SdkTracerProvider,
        config: serde_yaml::Value,
    ) -> Result<SdkTracerProvider, Error>;
}

/// Object-safe trait for configuring a logger provider with an exporter.
#[allow(dead_code)]
trait LogExporterConfigurator: Send + Sync {
    fn configure(
        &self,
        builder: SdkLoggerProvider,
        config: serde_yaml::Value,
    ) -> Result<SdkLoggerProvider, Error>;
}

// ============================================================================
// Generic Factory Wrappers (Type Erasure Magic)
// ============================================================================

/// Generic wrapper that implements the object-safe configurator trait.
///
/// This wrapper captures a user's factory function at registration time
/// and erases its type. The factory returns a concrete exporter type `E`,
/// not a trait object.
struct MetricExporterFactory<F, E>
where
    F: Fn(serde_yaml::Value) -> Result<E, Error> + Send + Sync,
    E: PushMetricExporter + 'static,
{
    factory: F,
    _phantom: std::marker::PhantomData<E>,
}

impl<F, E> MetricExporterConfigurator for MetricExporterFactory<F, E>
where
    F: Fn(serde_yaml::Value) -> Result<E, Error> + Send + Sync,
    E: PushMetricExporter + 'static,
{
    fn configure(
        &self,
        builder: opentelemetry_sdk::metrics::MeterProviderBuilder,
        config: serde_yaml::Value,
    ) -> Result<opentelemetry_sdk::metrics::MeterProviderBuilder, Error> {
        // Call user's factory to create concrete exporter
        let exporter = (self.factory)(config)?;

        // Builder's with_reader is generic and accepts concrete type
        let reader = PeriodicReader::builder(exporter).build();

        Ok(builder.with_reader(reader))
    }
}

/// Generic wrapper for trace exporters.
#[allow(dead_code)]
struct TraceExporterFactory<F, E>
where
    F: Fn(serde_yaml::Value) -> Result<E, Error> + Send + Sync,
    E: SpanExporter + 'static,
{
    factory: F,
    _phantom: std::marker::PhantomData<E>,
}

impl<F, E> TraceExporterConfigurator for TraceExporterFactory<F, E>
where
    F: Fn(serde_yaml::Value) -> Result<E, Error> + Send + Sync,
    E: SpanExporter + 'static,
{
    fn configure(
        &self,
        builder: SdkTracerProvider,
        config: serde_yaml::Value,
    ) -> Result<SdkTracerProvider, Error> {
        let exporter = (self.factory)(config)?;

        // TODO: Configure batch processor with exporter
        // For now, just return builder as-is
        let _ = exporter;
        Ok(builder)
    }
}

/// Generic wrapper for log exporters.
#[allow(dead_code)]
struct LogExporterFactory<F, E>
where
    F: Fn(serde_yaml::Value) -> Result<E, Error> + Send + Sync,
    E: opentelemetry_sdk::logs::LogExporter + 'static,
{
    factory: F,
    _phantom: std::marker::PhantomData<E>,
}

impl<F, E> LogExporterConfigurator for LogExporterFactory<F, E>
where
    F: Fn(serde_yaml::Value) -> Result<E, Error> + Send + Sync,
    E: opentelemetry_sdk::logs::LogExporter + 'static,
{
    fn configure(
        &self,
        builder: SdkLoggerProvider,
        config: serde_yaml::Value,
    ) -> Result<SdkLoggerProvider, Error> {
        let exporter = (self.factory)(config)?;

        // TODO: Configure batch processor with exporter
        let _ = exporter;
        Ok(builder)
    }
}

// ============================================================================
// Configuration Registry
// ============================================================================

/// Registry for configuration provider factories.
///
/// This registry stores factory functions for creating exporters from configuration.
/// The magic happens at registration time: user functions that return concrete types
/// are wrapped in generic factories and type-erased into object-safe trait objects.
///
/// # Example
///
/// ```rust
/// use otel_config_2::ConfigurationRegistry;
///
/// // Simple function returning concrete type
/// fn create_my_exporter(
///     config: serde_yaml::Value
/// ) -> Result<MyExporter, otel_config_2::Error> {
///     // Create and return concrete exporter
///     # unimplemented!()
/// }
///
/// let mut registry = ConfigurationRegistry::new();
/// registry.register_metric_exporter("my-exporter", create_my_exporter);
/// # struct MyExporter;
/// # impl opentelemetry_sdk::metrics::exporter::PushMetricExporter for MyExporter {
/// #     fn export(&self, _: &mut opentelemetry_sdk::metrics::data::ResourceMetrics) -> opentelemetry_sdk::metrics::MetricResult<()> { Ok(()) }
/// #     fn force_flush(&self) -> opentelemetry_sdk::metrics::MetricResult<()> { Ok(()) }
/// #     fn shutdown(&self) -> opentelemetry_sdk::metrics::MetricResult<()> { Ok(()) }
/// # }
/// ```
pub struct ConfigurationRegistry {
    metric_exporters: HashMap<&'static str, Box<dyn MetricExporterConfigurator>>,
    trace_exporters: HashMap<&'static str, Box<dyn TraceExporterConfigurator>>,
    log_exporters: HashMap<&'static str, Box<dyn LogExporterConfigurator>>,
}

impl ConfigurationRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        Self {
            metric_exporters: HashMap::new(),
            trace_exporters: HashMap::new(),
            log_exporters: HashMap::new(),
        }
    }

    /// Registers a metric exporter factory.
    ///
    /// The factory function should take configuration and return a concrete exporter type.
    /// Type erasure happens automatically at registration time.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use otel_config_2::{ConfigurationRegistry, Error};
    /// # use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
    /// fn create_console(
    ///     config: serde_yaml::Value
    /// ) -> Result<opentelemetry_stdout::MetricExporter, Error> {
    ///     Ok(opentelemetry_stdout::MetricExporter::default())
    /// }
    ///
    /// let mut registry = ConfigurationRegistry::new();
    /// registry.register_metric_exporter("console", create_console);
    /// ```
    pub fn register_metric_exporter<F, E>(&mut self, name: &'static str, factory: F)
    where
        F: Fn(serde_yaml::Value) -> Result<E, Error> + Send + Sync + 'static,
        E: PushMetricExporter + 'static,
    {
        let wrapper = MetricExporterFactory {
            factory,
            _phantom: std::marker::PhantomData,
        };
        self.metric_exporters.insert(name, Box::new(wrapper));
    }

    /// Registers a trace exporter factory.
    pub fn register_trace_exporter<F, E>(&mut self, name: &'static str, factory: F)
    where
        F: Fn(serde_yaml::Value) -> Result<E, Error> + Send + Sync + 'static,
        E: SpanExporter + 'static,
    {
        let wrapper = TraceExporterFactory {
            factory,
            _phantom: std::marker::PhantomData,
        };
        self.trace_exporters.insert(name, Box::new(wrapper));
    }

    /// Registers a log exporter factory.
    pub fn register_log_exporter<F, E>(&mut self, name: &'static str, factory: F)
    where
        F: Fn(serde_yaml::Value) -> Result<E, Error> + Send + Sync + 'static,
        E: opentelemetry_sdk::logs::LogExporter + 'static,
    {
        let wrapper = LogExporterFactory {
            factory,
            _phantom: std::marker::PhantomData,
        };
        self.log_exporters.insert(name, Box::new(wrapper));
    }

    /// Internal method to configure a meter provider with a registered exporter.
    fn configure_metric_exporter(
        &self,
        name: &str,
        builder: opentelemetry_sdk::metrics::MeterProviderBuilder,
        config: serde_yaml::Value,
    ) -> Result<opentelemetry_sdk::metrics::MeterProviderBuilder, Error> {
        let configurator = self.metric_exporters.get(name).ok_or_else(|| {
            Error::UnknownExporter {
                signal: "metrics",
                name: name.to_string(),
            }
        })?;

        configurator.configure(builder, config)
    }

    /// Internal method to configure a tracer provider with a registered exporter.
    #[allow(dead_code)]
    fn configure_trace_exporter(
        &self,
        name: &str,
        builder: SdkTracerProvider,
        config: serde_yaml::Value,
    ) -> Result<SdkTracerProvider, Error> {
        let configurator = self.trace_exporters.get(name).ok_or_else(|| {
            Error::UnknownExporter {
                signal: "traces",
                name: name.to_string(),
            }
        })?;

        configurator.configure(builder, config)
    }
}

impl Default for ConfigurationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Telemetry Providers
// ============================================================================

/// Configured SDK providers ready to use.
#[derive(Default)]
pub struct TelemetryProviders {
    pub meter_provider: Option<SdkMeterProvider>,
    pub tracer_provider: Option<SdkTracerProvider>,
    pub logger_provider: Option<SdkLoggerProvider>,
}

impl TelemetryProviders {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Provider for building telemetry providers from configuration.
pub struct TelemetryProvider;

impl TelemetryProvider {
    /// Builds providers from a YAML configuration file.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use otel_config_2::{ConfigurationRegistry, TelemetryProvider};
    /// let registry = ConfigurationRegistry::new();
    /// let providers = TelemetryProvider::build_from_yaml_file(
    ///     &registry,
    ///     "telemetry.yaml",
    /// )?;
    /// # Ok::<(), otel_config_2::Error>(())
    /// ```
    pub fn build_from_yaml_file(
        registry: &ConfigurationRegistry,
        path: impl AsRef<Path>,
    ) -> Result<TelemetryProviders, Error> {
        let yaml = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            Error::InvalidConfiguration(format!(
                "Failed to read file '{}': {}",
                path.as_ref().display(),
                e
            ))
        })?;

        Self::build_from_yaml(registry, &yaml)
    }

    /// Builds providers from a YAML configuration string.
    pub fn build_from_yaml(
        registry: &ConfigurationRegistry,
        yaml: &str,
    ) -> Result<TelemetryProviders, Error> {
        let config: model::TelemetryConfig = serde_yaml::from_str(yaml)
            .map_err(|e| Error::InvalidConfiguration(format!("Invalid YAML: {}", e)))?;

        Self::build(registry, config)
    }

    /// Builds providers from a parsed configuration.
    fn build(
        registry: &ConfigurationRegistry,
        config: model::TelemetryConfig,
    ) -> Result<TelemetryProviders, Error> {
        let mut providers = TelemetryProviders::new();

        // Build resource (shared across all signals)
        let resource = build_resource(&config.resource);

        // Build meter provider if configured
        if let Some(metrics_config) = config.metrics {
            let meter_provider = build_meter_provider(registry, metrics_config, resource.clone())?;
            providers.meter_provider = Some(meter_provider);
        }

        // Build tracer provider if configured
        if let Some(traces_config) = config.traces {
            let tracer_provider = build_tracer_provider(registry, traces_config, resource.clone())?;
            providers.tracer_provider = Some(tracer_provider);
        }

        // Build logger provider if configured
        if let Some(logs_config) = config.logs {
            let logger_provider = build_logger_provider(registry, logs_config, resource.clone())?;
            providers.logger_provider = Some(logger_provider);
        }

        Ok(providers)
    }
}

// ============================================================================
// Internal Builder Functions
// ============================================================================

/// Builds a meter provider from metrics configuration.
fn build_meter_provider(
    registry: &ConfigurationRegistry,
    config: model::MetricsConfig,
    resource: Resource,
) -> Result<SdkMeterProvider, Error> {
    let mut builder = SdkMeterProvider::builder().with_resource(resource);

    for reader_config in config.readers {
        match reader_config {
            model::ReaderConfig::Periodic(periodic) => {
                // Extract exporter name and config from the YAML structure
                let (exporter_name, exporter_config) =
                    extract_exporter_name_and_config(&periodic.exporter)?;

                // Configure the builder with the exporter
                builder = registry.configure_metric_exporter(
                    exporter_name,
                    builder,
                    exporter_config,
                )?;
            }
            model::ReaderConfig::Pull(_) => {
                return Err(Error::UnsupportedFeature(
                    "Pull readers are not yet supported".to_string(),
                ));
            }
        }
    }

    Ok(builder.build())
}

/// Builds a tracer provider from traces configuration.
fn build_tracer_provider(
    _registry: &ConfigurationRegistry,
    _config: model::TracesConfig,
    _resource: Resource,
) -> Result<SdkTracerProvider, Error> {
    Err(Error::UnsupportedFeature(
        "Traces configuration is not yet implemented".to_string(),
    ))
}

/// Builds a logger provider from logs configuration.
fn build_logger_provider(
    _registry: &ConfigurationRegistry,
    _config: model::LogsConfig,
    _resource: Resource,
) -> Result<SdkLoggerProvider, Error> {
    Err(Error::UnsupportedFeature(
        "Logs configuration is not yet implemented".to_string(),
    ))
}

/// Builds a resource from resource configuration.
fn build_resource(config: &model::ResourceConfig) -> Resource {
    let mut builder = Resource::builder();

    for (key, value) in &config.attributes {
        let key_value = match value {
            serde_yaml::Value::String(s) => KeyValue::new(key.clone(), s.clone()),
            serde_yaml::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    KeyValue::new(key.clone(), i)
                } else if let Some(f) = n.as_f64() {
                    KeyValue::new(key.clone(), f)
                } else {
                    KeyValue::new(key.clone(), n.to_string())
                }
            }
            serde_yaml::Value::Bool(b) => KeyValue::new(key.clone(), *b),
            _ => KeyValue::new(key.clone(), format!("{:?}", value)),
        };

        builder = builder.with_attribute(key_value);
    }

    builder.build()
}

/// Extracts exporter name and configuration from YAML structure.
///
/// Expected structure:
/// ```yaml
/// exporter:
///   console:
///     temporality: delta
/// ```
///
/// Returns: ("console", { "temporality": "delta" })
fn extract_exporter_name_and_config(
    exporter_value: &serde_yaml::Value,
) -> Result<(&str, serde_yaml::Value), Error> {
    let mapping = exporter_value.as_mapping().ok_or_else(|| {
        Error::InvalidConfiguration("Exporter configuration must be an object".to_string())
    })?;

    if mapping.len() != 1 {
        return Err(Error::InvalidConfiguration(format!(
            "Expected exactly one exporter, found {}",
            mapping.len()
        )));
    }

    let (name_key, config_value) = mapping.iter().next().unwrap();

    let name = name_key.as_str().ok_or_else(|| {
        Error::InvalidConfiguration("Exporter name must be a string".to_string())
    })?;

    Ok((name, config_value.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_creation() {
        let registry = ConfigurationRegistry::new();
        assert_eq!(registry.metric_exporters.len(), 0);
    }

    #[test]
    fn test_extract_exporter_name_and_config() {
        let yaml = r#"
console:
  temporality: delta
"#;
        let value: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();

        let (name, config) = extract_exporter_name_and_config(&value).unwrap();

        assert_eq!(name, "console");
        assert!(config.get("temporality").is_some());
    }

    #[test]
    fn test_build_resource() {
        let yaml = r#"
service.name: "test-service"
service.version: "1.0.0"
"#;
        let mut attributes = std::collections::HashMap::new();
        let yaml_value: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();

        if let serde_yaml::Value::Mapping(map) = yaml_value {
            for (k, v) in map {
                if let serde_yaml::Value::String(key) = k {
                    attributes.insert(key, v);
                }
            }
        }

        let config = model::ResourceConfig { attributes };
        let resource = build_resource(&config);

        assert!(!resource.is_empty());
    }
}
