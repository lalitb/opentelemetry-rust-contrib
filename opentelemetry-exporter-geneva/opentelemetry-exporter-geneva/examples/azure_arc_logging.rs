// Complete Azure Arc MSI Logging Example for Geneva
//
// This example demonstrates end-to-end logging to Geneva using Azure Arc
// Managed Identity authentication. It shows real-world usage patterns
// including application logging, structured data, and error handling.

use opentelemetry::{
    logs::{LogRecord, Logger, LoggerProvider, Severity},
    Key, Value,
};
use opentelemetry_exporter_geneva::{GenevaExporter, GenevaExporterBuilder};
use opentelemetry_sdk::logs::{LoggerProvider as SdkLoggerProvider, BatchProcessor};
use std::{collections::HashMap, time::Duration};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Geneva Azure Arc MSI Logging Example");
    println!("========================================\n");

    // Step 1: Environment Configuration
    println!("📋 Step 1: Reading configuration...");
    let config = read_configuration();
    print_configuration(&config);

    // Step 2: Create Geneva Exporter with Azure Arc MSI
    println!("\n🔧 Step 2: Setting up Geneva exporter with Azure Arc MSI...");
    let exporter = create_geneva_exporter(&config).await?;
    println!("✅ Geneva exporter created successfully");

    // Step 3: Set up OpenTelemetry logging pipeline
    println!("\n📝 Step 3: Setting up OpenTelemetry logging pipeline...");
    let logger_provider = SdkLoggerProvider::builder()
        .with_batch_processor(
            BatchProcessor::builder(exporter, opentelemetry_sdk::runtime::Tokio)
                .with_max_export_batch_size(100)
                .with_scheduled_delay(Duration::from_millis(1000))
                .build()?,
        )
        .build();

    let logger = logger_provider.logger("azure-arc-example");
    println!("✅ Logging pipeline configured");

    // Step 4: Demonstrate various logging scenarios
    println!("\n📊 Step 4: Sending logs to Geneva...");

    // Application startup log
    send_startup_log(&logger);

    // Business logic events
    send_business_events(&logger).await;

    // Error scenarios
    send_error_logs(&logger);

    // Performance metrics
    send_performance_logs(&logger);

    // Structured data logging
    send_structured_logs(&logger);

    println!("\n⏳ Step 5: Waiting for logs to be exported...");
    sleep(Duration::from_secs(3)).await;

    // Step 6: Graceful shutdown
    println!("\n🔄 Step 6: Shutting down logger provider...");
    drop(logger_provider);
    println!("✅ Shutdown complete");

    println!("\n🎉 Example completed successfully!");
    println!("💡 Check your Geneva dashboard for the exported logs");

    Ok(())
}

/// Configuration structure for the example
#[derive(Debug, Clone)]
struct ExampleConfig {
    pub geneva_endpoint: String,
    pub geneva_environment: String,
    pub geneva_account: String,
    pub geneva_namespace: String,
    pub geneva_region: String,
    pub geneva_config_version: u32,
}

/// Read configuration from environment variables with sensible defaults
fn read_configuration() -> ExampleConfig {
    ExampleConfig {
        geneva_endpoint: std::env::var("GENEVA_ENDPOINT")
            .unwrap_or_else(|_| "https://your-geneva-config-endpoint.com".to_string()),
        geneva_environment: std::env::var("GENEVA_ENVIRONMENT")
            .unwrap_or_else(|_| "prod".to_string()),
        geneva_account: std::env::var("GENEVA_ACCOUNT")
            .unwrap_or_else(|_| "your-account".to_string()),
        geneva_namespace: std::env::var("GENEVA_NAMESPACE")
            .unwrap_or_else(|_| "your-namespace".to_string()),
        geneva_region: std::env::var("GENEVA_REGION")
            .unwrap_or_else(|_| "westus2".to_string()),
        geneva_config_version: std::env::var("GENEVA_CONFIG_MAJOR_VERSION")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1),
    }
}

/// Print configuration for debugging
fn print_configuration(config: &ExampleConfig) {
    println!("  📍 Endpoint: {}", config.geneva_endpoint);
    println!("  🌍 Environment: {}", config.geneva_environment);
    println!("  👤 Account: {}", config.geneva_account);
    println!("  📦 Namespace: {}", config.geneva_namespace);
    println!("  🗺️  Region: {}", config.geneva_region);
    println!("  📊 Config Version: {}", config.geneva_config_version);
}

/// Create Geneva exporter with Azure Arc MSI authentication
async fn create_geneva_exporter(
    config: &ExampleConfig,
) -> Result<GenevaExporter, Box<dyn std::error::Error>> {
    match GenevaExporterBuilder::new()
        .with_endpoint(&config.geneva_endpoint)
        .with_environment(&config.geneva_environment)
        .with_account(&config.geneva_account)
        .with_namespace(&config.geneva_namespace)
        .with_region(&config.geneva_region)
        .with_config_version(config.geneva_config_version)
        .with_azure_arc_msi_auth() // Enable Azure Arc MSI authentication
        .build()
        .await
    {
        Ok(exporter) => {
            println!("  ✅ Successfully connected to Geneva Config Service");
            println!("  🔑 Using Azure Arc Managed Identity authentication");
            Ok(exporter)
        }
        Err(e) => {
            println!("  ❌ Failed to create Geneva exporter: {}", e);
            println!("  💡 Make sure you're running on an Azure Arc-enabled machine");
            println!("  💡 Verify the Arc identity is registered in Geneva portal:");
            println!("      - Tenant ID: Check with 'az connectedmachine show --query identity.tenantId'");
            println!("      - Object ID: Check with 'az connectedmachine show --query identity.principalId'");
            Err(e)
        }
    }
}

/// Send application startup log
fn send_startup_log(logger: &dyn Logger) {
    println!("  📱 Application startup event");

    let mut log_record = LogRecord::default();
    log_record
        .set_severity_number(Severity::Info)
        .set_severity_text("INFO")
        .set_body("Application started successfully".into())
        .add_attribute(Key::new("event.type"), Value::from("application.startup"))
        .add_attribute(Key::new("app.name"), Value::from("azure-arc-logging-example"))
        .add_attribute(Key::new("app.version"), Value::from("1.0.0"))
        .add_attribute(Key::new("auth.method"), Value::from("azure-arc-msi"));

    logger.emit(log_record);
}

/// Send business logic events
async fn send_business_events(logger: &dyn Logger) {
    println!("  💼 Business logic events");

    // Simulate processing multiple orders
    for order_id in 1001..=1003 {
        let mut log_record = LogRecord::default();
        log_record
            .set_severity_number(Severity::Info)
            .set_severity_text("INFO")
            .set_body(format!("Order {} processed successfully", order_id).into())
            .add_attribute(Key::new("event.type"), Value::from("order.processed"))
            .add_attribute(Key::new("order.id"), Value::from(order_id as i64))
            .add_attribute(Key::new("order.amount"), Value::from(99.99 * order_id as f64))
            .add_attribute(Key::new("customer.region"), Value::from("west-us"));

        logger.emit(log_record);

        // Small delay to simulate processing
        sleep(Duration::from_millis(100)).await;
    }
}

/// Send error scenarios
fn send_error_logs(logger: &dyn Logger) {
    println!("  ⚠️  Error scenarios");

    // Database connection error
    let mut error_record = LogRecord::default();
    error_record
        .set_severity_number(Severity::Error)
        .set_severity_text("ERROR")
        .set_body("Failed to connect to database".into())
        .add_attribute(Key::new("event.type"), Value::from("database.connection.failed"))
        .add_attribute(Key::new("error.type"), Value::from("ConnectionTimeout"))
        .add_attribute(Key::new("database.host"), Value::from("prod-db-cluster.internal"))
        .add_attribute(Key::new("retry.count"), Value::from(3))
        .add_attribute(Key::new("error.recoverable"), Value::from(true));

    logger.emit(error_record);

    // Validation warning
    let mut warning_record = LogRecord::default();
    warning_record
        .set_severity_number(Severity::Warn)
        .set_severity_text("WARN")
        .set_body("Invalid email format in user registration".into())
        .add_attribute(Key::new("event.type"), Value::from("validation.warning"))
        .add_attribute(Key::new("field.name"), Value::from("email"))
        .add_attribute(Key::new("user.id"), Value::from("user_12345"))
        .add_attribute(Key::new("validation.rule"), Value::from("email_format"));

    logger.emit(warning_record);
}

/// Send performance metrics
fn send_performance_logs(logger: &dyn Logger) {
    println!("  📈 Performance metrics");

    let mut perf_record = LogRecord::default();
    perf_record
        .set_severity_number(Severity::Info)
        .set_severity_text("INFO")
        .set_body("API endpoint performance metrics".into())
        .add_attribute(Key::new("event.type"), Value::from("performance.metrics"))
        .add_attribute(Key::new("http.method"), Value::from("GET"))
        .add_attribute(Key::new("http.route"), Value::from("/api/v1/orders"))
        .add_attribute(Key::new("http.status_code"), Value::from(200))
        .add_attribute(Key::new("duration.ms"), Value::from(245))
        .add_attribute(Key::new("memory.used.mb"), Value::from(128))
        .add_attribute(Key::new("cpu.usage.percent"), Value::from(15.7));

    logger.emit(perf_record);
}

/// Send structured data logs
fn send_structured_logs(logger: &dyn Logger) {
    println!("  🏗️  Structured data");

    // Complex business event with nested data
    let mut structured_record = LogRecord::default();
    structured_record
        .set_severity_number(Severity::Info)
        .set_severity_text("INFO")
        .set_body("User session activity".into())
        .add_attribute(Key::new("event.type"), Value::from("user.session"))
        .add_attribute(Key::new("user.id"), Value::from("user_67890"))
        .add_attribute(Key::new("session.id"), Value::from("sess_abcdef123456"))
        .add_attribute(Key::new("session.duration.minutes"), Value::from(23))
        .add_attribute(Key::new("pages.viewed"), Value::from(12))
        .add_attribute(Key::new("actions.performed"), Value::from(8))
        .add_attribute(Key::new("device.type"), Value::from("desktop"))
        .add_attribute(Key::new("browser"), Value::from("Chrome/118.0"))
        .add_attribute(Key::new("location.country"), Value::from("US"))
        .add_attribute(Key::new("location.state"), Value::from("WA"));

    logger.emit(structured_record);

    // Configuration change event
    let mut config_record = LogRecord::default();
    config_record
        .set_severity_number(Severity::Info)
        .set_severity_text("INFO")
        .set_body("Application configuration updated".into())
        .add_attribute(Key::new("event.type"), Value::from("config.change"))
        .add_attribute(Key::new("config.section"), Value::from("logging"))
        .add_attribute(Key::new("config.key"), Value::from("log_level"))
        .add_attribute(Key::new("config.old_value"), Value::from("INFO"))
        .add_attribute(Key::new("config.new_value"), Value::from("DEBUG"))
        .add_attribute(Key::new("changed_by"), Value::from("admin@company.com"))
        .add_attribute(Key::new("change.reason"), Value::from("troubleshooting"));

    logger.emit(config_record);
}

// Example helper functions for different log types that applications might use

/// Helper function for application logging
pub fn log_application_event(logger: &dyn Logger, event: &str, details: HashMap<String, String>) {
    let mut log_record = LogRecord::default();
    log_record
        .set_severity_number(Severity::Info)
        .set_severity_text("INFO")
        .set_body(event.into())
        .add_attribute(Key::new("event.type"), Value::from("application"));

    // Add all details as attributes
    for (key, value) in details {
        log_record.add_attribute(Key::new(key), Value::from(value));
    }

    logger.emit(log_record);
}

/// Helper function for error logging
pub fn log_error(logger: &dyn Logger, error_msg: &str, error_type: &str, context: HashMap<String, String>) {
    let mut log_record = LogRecord::default();
    log_record
        .set_severity_number(Severity::Error)
        .set_severity_text("ERROR")
        .set_body(error_msg.into())
        .add_attribute(Key::new("event.type"), Value::from("error"))
        .add_attribute(Key::new("error.type"), Value::from(error_type));

    for (key, value) in context {
        log_record.add_attribute(Key::new(key), Value::from(value));
    }

    logger.emit(log_record);
}

/// Helper function for performance logging
pub fn log_performance(logger: &dyn Logger, operation: &str, duration_ms: u64, additional_metrics: HashMap<String, f64>) {
    let mut log_record = LogRecord::default();
    log_record
        .set_severity_number(Severity::Info)
        .set_severity_text("INFO")
        .set_body(format!("Performance metrics for {}", operation).into())
        .add_attribute(Key::new("event.type"), Value::from("performance"))
        .add_attribute(Key::new("operation"), Value::from(operation))
        .add_attribute(Key::new("duration.ms"), Value::from(duration_ms as i64));

    for (metric, value) in additional_metrics {
        log_record.add_attribute(Key::new(metric), Value::from(value));
    }

    logger.emit(log_record);
}