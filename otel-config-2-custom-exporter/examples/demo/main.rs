//! Demonstration of using multiple exporters with otel-config-2.
//!
//! This example shows:
//! 1. Registering the built-in console exporter
//! 2. Registering a custom exporter from an external crate
//! 3. Loading configuration that uses both exporters
//! 4. Emitting metrics that go to both exporters

use otel_config_2::{ConfigurationRegistry, TelemetryProvider};
use std::env;

// Factory function for console exporter (could be from opentelemetry-stdout crate)
fn create_console_exporter(
    config: serde_yaml::Value,
) -> Result<opentelemetry_stdout::MetricExporter, otel_config_2::Error> {
    use otel_config_2::Error;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct ConsoleConfig {
        #[serde(default)]
        temporality: Option<String>,
    }

    let cfg: ConsoleConfig = serde_yaml::from_value(config).map_err(|e| {
        Error::InvalidExporterConfig {
            exporter: "console".to_string(),
            reason: e.to_string(),
        }
    })?;

    let mut builder = opentelemetry_stdout::MetricExporter::builder();

    if let Some(temp) = cfg.temporality {
        let sdk_temp = match temp.as_str() {
            "delta" => opentelemetry_sdk::metrics::Temporality::Delta,
            "cumulative" => opentelemetry_sdk::metrics::Temporality::Cumulative,
            _ => opentelemetry_sdk::metrics::Temporality::Cumulative,
        };
        builder = builder.with_temporality(sdk_temp);
    }

    Ok(builder.build())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("=== Custom Exporter Demo ===\n");
        println!("Usage: cargo run <config.yaml>\n");
        println!("This example demonstrates:");
        println!("  1. Registering multiple exporters (console + custom)");
        println!("  2. Loading YAML configuration");
        println!("  3. Metrics being sent to both exporters\n");
        println!("Example config.yaml:");
        println!(
            r#"
metrics:
  readers:
    - periodic:
        interval_millis: 3000
        exporter:
          console:
            temporality: cumulative
    - periodic:
        interval_millis: 5000
        exporter:
          custom:
            endpoint: "http://localhost:8080/metrics"
            timeout_secs: 30
            batch_size: 512
            temporality: delta
            debug: true
resource:
  service.name: "custom-exporter-demo"
  service.version: "1.0.0"
  environment: "demo"
"#
        );
        return Ok(());
    }

    let config_file = &args[1];

    println!("\n=== otel-config-2: Custom Exporter Integration Demo ===\n");

    // Step 1: Create registry
    println!("Step 1: Creating configuration registry...");
    let mut registry = ConfigurationRegistry::new();
    println!("  ✓ Registry created\n");

    // Step 2: Register exporters
    println!("Step 2: Registering exporters...");

    // Register console exporter
    println!("  → Registering 'console' exporter");
    registry.register_metric_exporter("console", create_console_exporter);
    println!("    ✓ Console exporter registered (built-in)");

    // Register custom exporter from external crate
    println!("  → Registering 'custom' exporter");
    registry.register_metric_exporter("custom", otel_config_2_custom_exporter::create_custom_exporter);
    println!("    ✓ Custom exporter registered (from otel-config-2-custom-exporter crate)");
    println!("    ℹ Type erasure happened automatically for both!\n");

    // Step 3: Load configuration
    println!("Step 3: Loading configuration from '{}'...", config_file);
    let providers = TelemetryProvider::build_from_yaml_file(&registry, config_file)?;
    println!("  ✓ Configuration loaded and providers built\n");

    // Step 4: Use the providers
    println!("Step 4: Installing and using providers...");
    if let Some(meter_provider) = providers.meter_provider {
        println!("  ✓ Meter provider configured with both exporters");

        // Set globally
        opentelemetry::global::set_meter_provider(meter_provider.clone());

        // Create meter and instruments
        let meter = opentelemetry::global::meter("demo-app");
        let counter = meter.u64_counter("requests").build();
        let histogram = meter.f64_histogram("request_duration").build();

        println!("\nStep 5: Emitting metrics...");
        println!("  (Metrics will go to BOTH console and custom exporters)\n");

        for i in 1..=10 {
            // Emit counter
            counter.add(
                1,
                &[
                    opentelemetry::KeyValue::new("endpoint", "/api/users"),
                    opentelemetry::KeyValue::new("method", "GET"),
                    opentelemetry::KeyValue::new("iteration", i as i64),
                ],
            );

            // Emit histogram
            histogram.record(
                (100 + i * 10) as f64,
                &[
                    opentelemetry::KeyValue::new("endpoint", "/api/users"),
                    opentelemetry::KeyValue::new("method", "GET"),
                ],
            );

            println!("  ✓ Iteration {}: Emitted counter=1, histogram={}", i, 100 + i * 10);

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        println!("\nStep 6: Shutting down...");
        meter_provider.shutdown()?;
        println!("  ✓ Meter provider shut down");
        println!("  ✓ All exporters flushed and shut down\n");
    }

    println!("=== Demo Complete ===\n");
    println!("Key Takeaways:");
    println!("  • External crates just provide a factory function");
    println!("  • No traits to implement");
    println!("  • Type erasure is automatic and invisible");
    println!("  • Multiple exporters work seamlessly");
    println!("  • Configuration is clean and declarative\n");

    Ok(())
}
