//! Basic example showing minimal setup with custom exporter.
//!
//! This example demonstrates:
//! - Creating a simple exporter factory function
//! - Registering it with the configuration registry
//! - Loading configuration from YAML
//! - Using the exporter to send metrics
//!
//! Run with:
//! ```bash
//! cargo run --example basic-example config-basic.yaml
//! ```

use otel_config_2::{ConfigurationRegistry, TelemetryProvider};
use otel_config_2_custom_exporter::create_custom_exporter;
use opentelemetry::KeyValue;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <config.yaml>", args[0]);
        eprintln!("\nExample config:");
        eprintln!(
            r#"
metrics:
  readers:
    - periodic:
        interval_millis: 5000
        exporter:
          custom:
            endpoint: "http://localhost:8080/metrics"
            timeout_secs: 30

resource:
  service.name: "basic-example"
"#
        );
        return Ok(());
    }

    println!("=== Basic Custom Exporter Example ===\n");

    // Step 1: Create registry
    println!("1. Creating configuration registry...");
    let mut registry = ConfigurationRegistry::new();

    // Step 2: Register the custom exporter
    println!("2. Registering custom exporter...");
    registry.register_metric_exporter("custom", create_custom_exporter);
    println!("   ✓ Registered 'custom' exporter\n");

    // Step 3: Load configuration from YAML file
    let config_file = &args[1];
    println!("3. Loading configuration from '{}'...", config_file);
    let providers = TelemetryProvider::build_from_yaml_file(&registry, config_file)?;
    println!("   ✓ Configuration loaded\n");

    // Step 4: Install the meter provider
    println!("4. Installing meter provider...");
    if let Some(meter_provider) = providers.meter_provider {
        opentelemetry::global::set_meter_provider(meter_provider.clone());
        println!("   ✓ Meter provider installed globally\n");

        // Step 5: Create instruments and emit metrics
        println!("5. Creating instruments and emitting metrics...\n");
        let meter = opentelemetry::global::meter("basic-example");
        
        let counter = meter
            .u64_counter("http_requests_total")
            .with_description("Total number of HTTP requests")
            .build();

        let histogram = meter
            .f64_histogram("http_request_duration_seconds")
            .with_description("HTTP request duration in seconds")
            .build();

        // Emit some sample metrics
        for i in 1..=5 {
            counter.add(
                1,
                &[
                    KeyValue::new("method", "GET"),
                    KeyValue::new("endpoint", "/api/users"),
                    KeyValue::new("status", "200"),
                ],
            );

            histogram.record(
                0.1 + (i as f64 * 0.05),
                &[
                    KeyValue::new("method", "GET"),
                    KeyValue::new("endpoint", "/api/users"),
                ],
            );

            println!("   → Emitted metrics (iteration {})", i);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        println!("\n6. Shutting down...");
        meter_provider.shutdown()?;
        println!("   ✓ Meter provider shut down cleanly\n");
    }

    println!("=== Example Complete ===");
    Ok(())
}
