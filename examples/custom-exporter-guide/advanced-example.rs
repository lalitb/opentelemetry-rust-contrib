//! Advanced example showing production-ready patterns.
//!
//! This example demonstrates:
//! - Production-grade error handling
//! - Rich resource attributes
//! - Multiple metric types (counter, histogram, gauge)
//! - Proper shutdown handling
//! - Configuration validation
//!
//! Run with:
//! ```bash
//! cargo run --example advanced-example config-advanced.yaml
//! ```

use otel_config_2::{ConfigurationRegistry, Error, TelemetryProvider};
use otel_config_2_custom_exporter::create_custom_exporter;
use opentelemetry::{
    metrics::{Counter, Histogram, UpDownCounter},
    KeyValue,
};
use serde::Deserialize;
use std::{env, time::Duration};

// Console exporter factory with validation
fn create_console_exporter(
    config: serde_yaml::Value,
) -> Result<opentelemetry_stdout::MetricExporter, Error> {
    #[derive(Deserialize)]
    struct ConsoleConfig {
        #[serde(default)]
        temporality: Option<String>,
        #[serde(default)]
        pretty_print: bool,
    }

    let cfg: ConsoleConfig = serde_yaml::from_value(config).map_err(|e| {
        Error::InvalidExporterConfig {
            exporter: "console".to_string(),
            reason: format!("Invalid configuration: {}", e),
        }
    })?;

    let mut builder = opentelemetry_stdout::MetricExporter::builder();

    if let Some(temp) = cfg.temporality {
        let sdk_temp = match temp.as_str() {
            "delta" => opentelemetry_sdk::metrics::Temporality::Delta,
            "cumulative" => opentelemetry_sdk::metrics::Temporality::Cumulative,
            _ => {
                return Err(Error::InvalidExporterConfig {
                    exporter: "console".to_string(),
                    reason: format!(
                        "Invalid temporality '{}', must be 'delta' or 'cumulative'",
                        temp
                    ),
                })
            }
        };
        builder = builder.with_temporality(sdk_temp);
    }

    Ok(builder.build())
}

/// Simulate a web application handling requests
struct WebApplication {
    request_counter: Counter<u64>,
    request_duration: Histogram<f64>,
    active_connections: UpDownCounter<i64>,
    error_counter: Counter<u64>,
}

impl WebApplication {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            request_counter: meter
                .u64_counter("http.server.requests")
                .with_description("Total number of HTTP requests received")
                .with_unit("requests")
                .build(),
            request_duration: meter
                .f64_histogram("http.server.request.duration")
                .with_description("HTTP request duration")
                .with_unit("seconds")
                .build(),
            active_connections: meter
                .i64_up_down_counter("http.server.active_connections")
                .with_description("Number of active HTTP connections")
                .with_unit("connections")
                .build(),
            error_counter: meter
                .u64_counter("http.server.errors")
                .with_description("Total number of HTTP errors")
                .with_unit("errors")
                .build(),
        }
    }

    async fn handle_request(
        &self,
        method: &str,
        endpoint: &str,
        status: u16,
        duration_ms: u64,
    ) {
        let attributes = vec![
            KeyValue::new("http.method", method.to_string()),
            KeyValue::new("http.route", endpoint.to_string()),
            KeyValue::new("http.status_code", status as i64),
        ];

        // Increment request counter
        self.request_counter.add(1, &attributes);

        // Record request duration
        self.request_duration
            .record(duration_ms as f64 / 1000.0, &attributes);

        // Track errors
        if status >= 400 {
            let error_attrs = vec![
                KeyValue::new("http.method", method.to_string()),
                KeyValue::new("http.route", endpoint.to_string()),
                KeyValue::new("http.status_code", status as i64),
                KeyValue::new(
                    "error.type",
                    if status >= 500 { "server" } else { "client" },
                ),
            ];
            self.error_counter.add(1, &error_attrs);
        }

        // Simulate connection lifecycle
        tokio::time::sleep(Duration::from_millis(duration_ms)).await;
    }

    fn add_connection(&self) {
        self.active_connections.add(1, &[]);
    }

    fn remove_connection(&self) {
        self.active_connections.add(-1, &[]);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <config.yaml>", args[0]);
        eprintln!("\nThis example demonstrates production-ready patterns:");
        eprintln!("  • Multiple metric types (counter, histogram, gauge)");
        eprintln!("  • Rich resource attributes");
        eprintln!("  • Proper error handling");
        eprintln!("  • Multiple exporters with different intervals");
        eprintln!("\nSee config-advanced.yaml for example configuration.");
        return Ok(());
    }

    println!("=== Advanced Custom Exporter Example ===\n");

    // Step 1: Create and configure registry
    println!("1. Setting up configuration registry...");
    let mut registry = ConfigurationRegistry::new();

    registry.register_metric_exporter("console", create_console_exporter);
    registry.register_metric_exporter("custom", create_custom_exporter);
    println!("   ✓ Registered 2 exporters (console, custom)\n");

    // Step 2: Load and validate configuration
    let config_file = &args[1];
    println!("2. Loading configuration from '{}'...", config_file);
    
    let providers = match TelemetryProvider::build_from_yaml_file(&registry, config_file) {
        Ok(p) => {
            println!("   ✓ Configuration loaded and validated\n");
            p
        }
        Err(e) => {
            eprintln!("   ✗ Configuration error: {}", e);
            return Err(Box::new(e));
        }
    };

    // Step 3: Install meter provider
    println!("3. Installing telemetry providers...");
    let meter_provider = providers.meter_provider.ok_or("No meter provider configured")?;
    
    opentelemetry::global::set_meter_provider(meter_provider.clone());
    println!("   ✓ Meter provider installed globally\n");

    // Step 4: Create application with instrumentation
    println!("4. Initializing instrumented application...");
    let meter = opentelemetry::global::meter("advanced-example");
    let app = WebApplication::new(&meter);
    println!("   ✓ Application instrumented with 4 metric types\n");

    // Step 5: Simulate application workload
    println!("5. Simulating application workload...\n");

    let endpoints = ["/api/users", "/api/products", "/api/orders", "/health"];
    let methods = ["GET", "POST", "PUT", "DELETE"];

    for iteration in 1..=15 {
        println!("   Iteration {}:", iteration);

        // Simulate multiple concurrent requests
        let num_requests = 3 + (iteration % 4);
        
        for req in 0..num_requests {
            app.add_connection();

            let endpoint = endpoints[req % endpoints.len()];
            let method = methods[req % methods.len()];
            
            // Simulate various response scenarios
            let (status, duration_ms) = match (iteration + req) % 10 {
                0 => (500, 150), // Server error
                1 => (404, 50),  // Not found
                2 => (400, 30),  // Bad request
                _ => (200, 80 + (req * 20) as u64), // Success
            };

            println!(
                "      → {} {} - {} ({}ms)",
                method, endpoint, status, duration_ms
            );

            app.handle_request(method, endpoint, status, duration_ms).await;
            app.remove_connection();
        }

        println!();
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Step 6: Wait for final exports
    println!("6. Waiting for final metric exports...");
    tokio::time::sleep(Duration::from_secs(6)).await;
    println!("   ✓ All metrics exported\n");

    // Step 7: Clean shutdown
    println!("7. Shutting down gracefully...");
    match meter_provider.shutdown() {
        Ok(_) => {
            println!("   ✓ Meter provider shut down cleanly");
            println!("   ✓ All exporters flushed and closed\n");
        }
        Err(e) => {
            eprintln!("   ✗ Error during shutdown: {}", e);
        }
    }

    println!("=== Example Complete ===\n");
    println!("Metrics Collected:");
    println!("  • http.server.requests - Request counts by method, route, and status");
    println!("  • http.server.request.duration - Response time distribution");
    println!("  • http.server.active_connections - Active connection gauge");
    println!("  • http.server.errors - Error counts by type\n");
    println!("Export Behavior:");
    println!("  • Console exporter: exports every 3 seconds (for debugging)");
    println!("  • Custom exporter: exports every 10 seconds (production-like)\n");

    Ok(())
}
