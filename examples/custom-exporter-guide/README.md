# Custom Exporter Configuration Guide

This guide provides comprehensive examples showing how to use custom exporter configurations with `otel-config-2`.

## Overview

The `otel-config-2` framework makes it incredibly simple to integrate custom exporters. You only need to provide **one factory function** - no traits, no boilerplate!

## What's Included

This guide contains multiple examples demonstrating:

1. **Basic Usage** - Minimal setup with a custom exporter
2. **Multiple Exporters** - Using console and custom exporters together
3. **Advanced Configuration** - Complex configurations with multiple readers
4. **Error Handling** - Proper validation and error handling
5. **Real-World Scenarios** - Production-like examples

## Quick Start

### Step 1: Create Your Exporter Factory

```rust
use otel_config_2::Error;
use opentelemetry_sdk::metrics::PushMetricExporter;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct MyExporterConfig {
    pub endpoint: String,
    pub timeout_secs: u64,
}

pub fn create_my_exporter(
    config: serde_yaml::Value,
) -> Result<MyExporter, Error> {
    let cfg: MyExporterConfig = serde_yaml::from_value(config)
        .map_err(|e| Error::InvalidExporterConfig {
            exporter: "my-exporter".to_string(),
            reason: e.to_string(),
        })?;
    
    Ok(MyExporter::new(cfg))
}
```

### Step 2: Register Your Exporter

```rust
use otel_config_2::ConfigurationRegistry;

let mut registry = ConfigurationRegistry::new();
registry.register_metric_exporter("my-exporter", create_my_exporter);
```

### Step 3: Configure in YAML

```yaml
metrics:
  readers:
    - periodic:
        interval_millis: 5000
        exporter:
          my-exporter:
            endpoint: "http://localhost:8080/metrics"
            timeout_secs: 30
```

### Step 4: Build and Use

```rust
let providers = TelemetryProvider::build_from_yaml_file(&registry, "config.yaml")?;
opentelemetry::global::set_meter_provider(providers.meter_provider.unwrap());
```

## Running the Examples

Each example can be run with:

```bash
cargo run --example <example-name> -- <config-file>
```

For example:

```bash
cargo run --example basic config-basic.yaml
cargo run --example multiple-exporters config-multiple.yaml
cargo run --example advanced config-advanced.yaml
```

## Example Configurations

### Minimal Configuration

```yaml
metrics:
  readers:
    - periodic:
        exporter:
          custom:
            endpoint: "http://localhost:8080/metrics"
```

### With Debugging

```yaml
metrics:
  readers:
    - periodic:
        exporter:
          custom:
            endpoint: "http://localhost:8080/metrics"
            debug: true
```

### With All Options

```yaml
metrics:
  readers:
    - periodic:
        interval_millis: 10000
        exporter:
          custom:
            endpoint: "http://production.example.com/api/v1/metrics"
            timeout_secs: 60
            batch_size: 1024
            custom_header: "X-API-Key: your-secret-key"
            temporality: delta
            debug: false
```

### Multiple Exporters

```yaml
metrics:
  readers:
    # Console for local debugging
    - periodic:
        interval_millis: 3000
        exporter:
          console:
            temporality: cumulative
    
    # Custom for production
    - periodic:
        interval_millis: 5000
        exporter:
          custom:
            endpoint: "http://localhost:8080/metrics"
            timeout_secs: 30
            temporality: delta

resource:
  service.name: "my-service"
  service.version: "1.0.0"
```

## Key Concepts

### 1. Factory Function Pattern

The core of the design is the factory function:

```rust
pub fn create_exporter(
    config: serde_yaml::Value
) -> Result<YourExporter, Error>
```

**Why this works:**
- Takes raw YAML configuration
- Returns concrete exporter type (not trait object!)
- Type erasure happens automatically during registration
- No traits to implement

### 2. Configuration Structure

Define your config as a Rust struct:

```rust
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]  // Catch typos!
pub struct MyConfig {
    pub endpoint: String,
    
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    
    #[serde(default)]
    pub debug: bool,
}

fn default_timeout() -> u64 { 30 }
```

### 3. Validation

Always validate your configuration:

```rust
pub fn create_exporter(config: serde_yaml::Value) -> Result<MyExporter, Error> {
    let cfg: MyConfig = serde_yaml::from_value(config)?;
    
    // Validate
    if cfg.endpoint.is_empty() {
        return Err(Error::InvalidExporterConfig {
            exporter: "my-exporter".to_string(),
            reason: "endpoint cannot be empty".to_string(),
        });
    }
    
    Ok(MyExporter::new(cfg))
}
```

### 4. Type Safety

Exporter names are `&'static str`:

```rust
// ✅ Good - compile-time constant
registry.register_metric_exporter("my-exporter", create_exporter);

// ❌ Bad - won't compile
let name = String::from("my-exporter");
registry.register_metric_exporter(&name, create_exporter);
```

## Advanced Topics

### Custom Temporality

```rust
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MyTemporality {
    Delta,
    Cumulative,
}

impl PushMetricExporter for MyExporter {
    fn temporality(&self) -> Temporality {
        match self.config.temporality {
            MyTemporality::Delta => Temporality::Delta,
            MyTemporality::Cumulative => Temporality::Cumulative,
        }
    }
}
```

### Authentication Headers

```rust
#[derive(Deserialize)]
pub struct MyConfig {
    pub endpoint: String,
    pub api_key: Option<String>,
    pub bearer_token: Option<String>,
}

impl MyExporter {
    fn build_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(key) = &self.config.api_key {
            headers.insert("X-API-Key", key.parse().unwrap());
        }
        if let Some(token) = &self.config.bearer_token {
            headers.insert("Authorization", format!("Bearer {}", token).parse().unwrap());
        }
        headers
    }
}
```

### Retry Logic

```rust
impl PushMetricExporter for MyExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let mut retries = 0;
        loop {
            match self.send_metrics(metrics).await {
                Ok(_) => return Ok(()),
                Err(e) if retries < 3 => {
                    retries += 1;
                    tokio::time::sleep(Duration::from_secs(1 << retries)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}
```

## Comparison with Old Approach

### Before (opentelemetry-config)

```rust
// ~80 lines of boilerplate
pub struct MyProvider {}

impl MetricsReaderPeriodicExporterProvider for MyProvider {
    fn provide(&self, mut builder: MeterProviderBuilder, config: &Value) 
        -> MeterProviderBuilder {
        let cfg = serde_yaml::from_value(config.clone()).expect("...");
        let exporter = MyExporter::new(cfg);
        builder.with_periodic_exporter(exporter)
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl MyProvider {
    pub fn register_into(registry: &mut Registry) {
        let key = ExporterId::PeriodicExporter.qualified_name("my-exporter");
        registry.register_provider(key, Box::new(Self::new()));
    }
}

// Usage
MyProvider::register_into(&mut registry);
```

### After (otel-config-2)

```rust
// ~10 lines, no boilerplate
pub fn create_exporter(config: serde_yaml::Value) 
    -> Result<MyExporter, Error> {
    let cfg: MyConfig = serde_yaml::from_value(config)?;
    Ok(MyExporter::new(cfg))
}

// Usage
registry.register_metric_exporter("my-exporter", create_exporter);
```

**Result:** 88% less code!

## Best Practices

1. **Always validate configuration** - Return proper errors, don't panic
2. **Use #[serde(deny_unknown_fields)]** - Catch typos in configs
3. **Provide sensible defaults** - Use `#[serde(default)]` attributes
4. **Document your config** - Users need to know what fields exist
5. **Test your factory function** - It's just a function, easy to test!
6. **Return concrete types** - Not `Box<dyn Trait>`
7. **Handle errors gracefully** - Map serde errors to `otel_config_2::Error`

## Troubleshooting

### "Failed to deserialize" Error

```
Error: InvalidExporterConfig { exporter: "custom", reason: "missing field `endpoint`" }
```

**Solution:** Make sure your YAML includes all required fields.

### "Unknown field" Error

```
Error: InvalidExporterConfig { exporter: "custom", reason: "unknown field `endpont`" }
```

**Solution:** Check for typos in your YAML. The `#[serde(deny_unknown_fields)]` catches this.

### "Exporter not registered" Error

```
Error: ExporterNotRegistered { exporter_type: "custom" }
```

**Solution:** Make sure you registered the exporter before loading the config:
```rust
registry.register_metric_exporter("custom", create_custom_exporter);
```

## Additional Resources

- [otel-config-2 README](../../otel-config-2/README.md)
- [otel-config-2-custom-exporter source](../../otel-config-2-custom-exporter/)
- [OpenTelemetry Rust SDK](https://github.com/open-telemetry/opentelemetry-rust)

## License

Apache-2.0
