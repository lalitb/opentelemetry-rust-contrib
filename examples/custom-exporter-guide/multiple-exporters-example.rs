//! Example showing how to use multiple exporters together.
//!
//! This example demonstrates:
//! - Registering multiple exporters (console + custom)
//! - Configuring different intervals for each reader
//! - Metrics being sent to both exporters
//! - Different temporality settings per exporter
//!
//! Run with:
//! ```bash
//! cargo run --example multiple-exporters-example config-multiple.yaml
//! ```

use otel_config_2::{ConfigurationRegistry, Error, TelemetryProvider};
use otel_config_2_custom_exporter::create_custom_exporter;
use opentelemetry::KeyValue;
use serde::Deserialize;
use std::env;

// Factory function for console exporter
fn create_console_exporter(
    config: serde_yaml::Value,
) -> Result<opentelemetry_stdout::MetricExporter, Error> {
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
        eprintln!("Usage: {} <config.yaml>", args[0]);
        eprintln!("\nExample config with multiple exporters:");
        eprintln!(
            r#"
metrics:
  readers:
    # Console exporter for debugging
    - periodic:
        interval_millis: 3000
        exporter:
          console:
            temporality: cumulative
    
    # Custom exporter for production
    - periodic:
        interval_millis: 5000
        exporter:
          custom:
            endpoint: "http://localhost:8080/metrics"
            timeout_secs: 30
            temporality: delta

resource:
  service.name: "multiple-exporters-example"
"#
        );
        return Ok(());
    }

    println!("=== Multiple Exporters Example ===\n");

    // Step 1: Create registry
    println!("1. Creating configuration registry...");
    let mut registry = ConfigurationRegistry::new();

    // Step 2: Register both exporters
    println!("2. Registering exporters...");
    registry.register_metric_exporter("console", create_console_exporter);
    println!("   ✓ Registered 'console' exporter");
    
    registry.register_metric_exporter("custom", create_custom_exporter);
    println!("   ✓ Registered 'custom' exporter");
    println!("   ℹ Both exporters use the same registration pattern!\n");

    // Step 3: Load configuration
    let config_file = &args[1];
    println!("3. Loading configuration from '{}'...", config_file);
    let providers = TelemetryProvider::build_from_yaml_file(&registry, config_file)?;
    println!("   ✓ Configuration loaded with multiple readers\n");

    // Step 4: Install and use providers
    println!("4. Installing meter provider...");
    if let Some(meter_provider) = providers.meter_provider {
        opentelemetry::global::set_meter_provider(meter_provider.clone());
        println!("   ✓ Meter provider installed with TWO readers\n");

        // Step 5: Create instruments
        println!("5. Creating instruments...");
        let meter = opentelemetry::global::meter("multiple-exporters-example");
        
        let request_counter = meter
            .u64_counter("api_requests_total")
            .with_description("Total API requests")
            .build();

        let response_time = meter
            .f64_histogram("api_response_time_seconds")
            .with_description("API response time")
            .build();

        let active_connections = meter
            .u64_up_down_counter("active_connections")
            .with_description("Number of active connections")
            .build();

        println!("   ✓ Created counter, histogram, and up-down counter\n");

        // Step 6: Emit metrics
        println!("6. Emitting metrics...");
        println!("   (Metrics will be exported to BOTH console and custom exporters)\n");

        for i in 1..=10 {
            // Increment request counter
            request_counter.add(
                1,
                &[
                    KeyValue::new("method", "GET"),
                    KeyValue::new("endpoint", "/api/data"),
                    KeyValue::new("status", "200"),
                ],
            );

            // Record response time
            response_time.record(
                0.05 + (i as f64 * 0.01),
                &[
                    KeyValue::new("method", "GET"),
                    KeyValue::new("endpoint", "/api/data"),
                ],
            );

            // Update active connections
            if i % 3 == 0 {
                active_connections.add(1, &[]);
            } else if i % 2 == 0 {
                active_connections.add(-1, &[]);
            }

            println!("   → Iteration {}: Emitted metrics", i);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        println!("\n7. Waiting for final export cycles...");
        tokio::time::sleep(tokio::time::Duration::from_secs(6)).await;

        println!("\n8. Shutting down...");
        meter_provider.shutdown()?;
        println!("   ✓ Both exporters shut down cleanly\n");
    }

    println!("=== Example Complete ===\n");
    println!("Key Observations:");
    println!("  • Console exporter exports every 3 seconds (cumulative)");
    println!("  • Custom exporter exports every 5 seconds (delta)");
    println!("  • Same metrics go to both exporters");
    println!("  • Different temporality per exporter is supported");
    println!("  • Clean shutdown ensures all metrics are flushed\n");

    Ok(())
}
