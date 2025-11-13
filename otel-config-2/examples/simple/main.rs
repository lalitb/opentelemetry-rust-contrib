//! Simple example demonstrating otel-config-2 with type erasure pattern.
//!
//! This example shows:
//! 1. Creating a simple factory function that returns a concrete exporter type
//! 2. Registering it with the configuration registry (type erasure happens automatically)
//! 3. Building providers from YAML configuration
//! 4. Using the configured providers

use otel_config_2::{ConfigurationRegistry, Error, TelemetryProvider};
use serde::Deserialize;
use std::env;

/// Configuration for the console exporter
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ConsoleExporterConfig {
    #[serde(default)]
    pub temporality: Option<Temporality>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Temporality {
    Delta,
    Cumulative,
}

/// Simple factory function that creates a console exporter.
///
/// This is what external exporter crates would provide.
/// Notice: It returns a CONCRETE type (opentelemetry_stdout::MetricExporter),
/// not Box<dyn PushMetricExporter>!
pub fn create_console_exporter(
    config: serde_yaml::Value,
) -> Result<opentelemetry_stdout::MetricExporter, Error> {
    println!("Creating console exporter from config...");

    // Deserialize the configuration
    let exporter_config = serde_yaml::from_value::<ConsoleExporterConfig>(config).map_err(|e| {
        Error::InvalidExporterConfig {
            exporter: "console".to_string(),
            reason: format!("Failed to deserialize console exporter configuration: {}", e),
        }
    })?;

    println!("  Config: {:?}", exporter_config);

    // Build the exporter with the configured temporality
    let mut exporter_builder = opentelemetry_stdout::MetricExporter::builder();

    if let Some(temporality) = exporter_config.temporality {
        let sdk_temporality = match temporality {
            Temporality::Delta => opentelemetry_sdk::metrics::Temporality::Delta,
            Temporality::Cumulative => opentelemetry_sdk::metrics::Temporality::Cumulative,
        };
        exporter_builder = exporter_builder.with_temporality(sdk_temporality);
    }

    let exporter = exporter_builder.build();
    println!("  ✓ Console exporter created successfully\n");

    // Return concrete type - no Box<dyn ...>!
    Ok(exporter)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    // Show usage if no file provided
    if args.len() < 2 {
        println!("Usage: cargo run --example simple <config.yaml>");
        println!("\nExample config file content:");
        println!(
            r#"
metrics:
  readers:
    - periodic:
        interval_millis: 5000
        exporter:
          console:
            temporality: cumulative
resource:
  service.name: "example-service"
  service.version: "1.0.0"
"#
        );
        return Ok(());
    }

    let config_file = &args[1];

    println!("=== otel-config-2 Type Erasure Pattern Example ===\n");

    // Step 1: Create registry and register exporters
    println!("Step 1: Creating registry and registering exporters...");
    let mut registry = ConfigurationRegistry::new();

    // Register console exporter factory
    // Type erasure happens here automatically!
    // The generic wrapper captures the concrete type (opentelemetry_stdout::MetricExporter)
    // and stores it as Box<dyn MetricExporterConfigurator>
    registry.register_metric_exporter("console", create_console_exporter);
    println!("  ✓ Registered 'console' metric exporter\n");

    // Step 2: Build providers from YAML configuration
    println!("Step 2: Building providers from '{}'...", config_file);
    let providers = TelemetryProvider::build_from_yaml_file(&registry, config_file)?;
    println!("  ✓ Providers built successfully\n");

    // Step 3: Use the configured providers
    println!("Step 3: Installing providers...");
    if let Some(meter_provider) = providers.meter_provider {
        println!("  ✓ Meter provider configured");

        // Install globally
        opentelemetry::global::set_meter_provider(meter_provider.clone());

        // Create a meter and emit some metrics
        let meter = opentelemetry::global::meter("example");
        let counter = meter.u64_counter("example_counter").build();

        println!("\nStep 4: Emitting example metrics...");
        for i in 1..=5 {
            counter.add(
                1,
                &[opentelemetry::KeyValue::new("iteration", i as i64)],
            );
            println!("  ✓ Emitted metric: example_counter = {} (iteration={})", 1, i);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        println!("\nStep 5: Shutting down...");
        meter_provider.shutdown()?;
        println!("  ✓ Meter provider shut down\n");
    } else {
        println!("  ✗ No meter provider configured\n");
    }

    if let Some(tracer_provider) = providers.tracer_provider {
        println!("  ✓ Tracer provider configured");
        tracer_provider.shutdown()?;
    }

    if let Some(logger_provider) = providers.logger_provider {
        println!("  ✓ Logger provider configured");
        logger_provider.shutdown()?;
    }

    println!("=== Example completed successfully ===");
    println!("\nKey Takeaway:");
    println!("  - create_console_exporter() returns opentelemetry_stdout::MetricExporter (concrete)");
    println!("  - NOT Box<dyn PushMetricExporter> (which would fail to compile)");
    println!("  - Type erasure happens at registration time via generic wrapper");
    println!("  - Simple API for exporter authors, no traits to implement!");

    Ok(())
}
