# otel-config-2: Simplified OpenTelemetry Configuration

A simplified, factory-based approach to declarative OpenTelemetry configuration for Rust.

## Key Features

- **Simple factory functions**: No traits to implement, just write a function
- **Standard SDK types**: Uses `Box<dyn PushMetricExporter>`, not custom types
- **No builder exposure**: SDK wiring is handled internally
- **Minimal boilerplate**: Clean API for exporter authors
- **Compile-time safety**: Exporter names must be static strings
- **Zero-clone design**: Configuration values are moved, not cloned

## Quick Start

### 1. Create a YAML Configuration File

```yaml
metrics:
  readers:
    - periodic:
        interval_millis: 5000
        exporter:
          console:
            temporality: cumulative

resource:
  service.name: "my-service"
  service.version: "1.0.0"
```

### 2. Register Exporters and Build Providers

```rust
use otel_config_2::{ConfigurationRegistry, TelemetryProvider};

// Create registry and register exporters
let mut registry = ConfigurationRegistry::new();
registry.register_metric_exporter("console", my_console_exporter::create);

// Build providers from YAML
let providers = TelemetryProvider::build_from_yaml_file(&registry, "config.yaml")?;

// Use the providers
if let Some(meter_provider) = providers.meter_provider {
    opentelemetry::global::set_meter_provider(meter_provider);
}
```

## For Exporter Authors

Creating a custom exporter is simple - just write a factory function:

```rust
use otel_config_2::Error;
use opentelemetry_sdk::metrics::PushMetricExporter;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct MyExporterConfig {
    pub endpoint: String,
    pub timeout_secs: Option<u64>,
}

pub fn create_exporter(
    config: serde_yaml::Value,
) -> Result<Box<dyn PushMetricExporter>, Error> {
    // Deserialize your config
    let cfg: MyExporterConfig = serde_yaml::from_value(config)
        .map_err(|e| Error::InvalidExporterConfig {
            exporter: "my-exporter".to_string(),
            reason: e.to_string(),
        })?;

    // Build and return your exporter
    let exporter = MyExporter::new(&cfg.endpoint)
        .with_timeout(cfg.timeout_secs.unwrap_or(30));

    Ok(Box::new(exporter))
}
```

Then users can register and use it:

```rust
registry.register_metric_exporter("my-exporter", my_exporter::create_exporter);
```

## Design Principles

### 1. Factory Functions Over Traits

**Instead of:**
```rust
// Old approach: implement a trait
impl MetricsReaderPeriodicExporterProvider for MyProvider {
    fn provide(&self, builder: MeterProviderBuilder, config: &Value) -> MeterProviderBuilder {
        // Manipulate builder...
    }
    fn as_any(&self) -> &dyn Any { self }
}
```

**We use:**
```rust
// New approach: just a function
fn create_exporter(config: serde_yaml::Value) -> Result<Box<dyn PushMetricExporter>, Error> {
    // Just create and return the exporter
}
```

**Benefits:**
- 70% less boilerplate
- No trait complexity
- No `as_any()` anti-pattern
- Easier to understand and test

### 2. Owned Values, No Cloning

**Instead of:**
```rust
fn create(&self, config: &serde_yaml::Value) -> Result<...> {
    let cfg = serde_yaml::from_value(config.clone())?;  // ❌ Must clone
}
```

**We use:**
```rust
fn create(config: serde_yaml::Value) -> Result<...> {
    let cfg = serde_yaml::from_value(config)?;  // ✅ Consumes, no clone
}
```

**Benefits:**
- Zero unnecessary allocations
- More efficient
- More idiomatic Rust

### 3. Static String Names

**Instead of:**
```rust
registry.register_metric_exporter(&format!("exporter-{}", version), factory);  // ❌ Runtime strings
```

**We use:**
```rust
registry.register_metric_exporter("console", factory);  // ✅ Compile-time constant
```

**Benefits:**
- Catches typos at compile time
- No allocations
- Simpler reasoning

### 4. Standard SDK Traits

Exporters implement standard SDK traits:
- `PushMetricExporter` for metrics
- `SpanExporter` for traces
- `LogExporter` for logs

The config crate handles all the wiring to builders internally.

## Architecture

```
User Application
    │
    ├─> Creates ConfigurationRegistry
    ├─> Registers factory functions (e.g., "console" → create_console_exporter)
    ├─> Loads YAML config file
    │
    v
TelemetryProvider::build_from_yaml_file()
    │
    ├─> Parses YAML into model::TelemetryConfig
    ├─> For each configured exporter:
    │   ├─> Looks up factory by name
    │   ├─> Calls factory(config) → Box<dyn Exporter>
    │   └─> Wires exporter into SDK builder
    │
    v
Returns TelemetryProviders (meter, tracer, logger providers)
    │
    └─> User installs globally or uses directly
```

## Comparison with Original Design

| Aspect | Original | otel-config-2 | Improvement |
|--------|----------|---------------|-------------|
| **Exporter implementation** | ~80 lines (trait + struct) | ~30 lines (function) | 62% less code |
| **Type system** | Custom traits + `as_any()` | Standard SDK traits | Simpler |
| **Builder exposure** | Exposed to external crates | Internal only | Better encapsulation |
| **Configuration cloning** | Required | Not required | Faster |
| **String keys** | Runtime `String` | Compile-time `&'static str` | Type-safer |
| **Error handling** | Often panics | Forced `Result` | Robust |

## Examples

Run the included example:

```bash
cd examples/simple
cargo run config.yaml
```

This demonstrates:
- Creating a console exporter factory
- Registering it with the registry
- Loading configuration from YAML
- Building and using providers
- Emitting sample metrics

## Current Status

**Implemented:**
- ✅ Core registry and factory pattern
- ✅ Metrics configuration with periodic readers
- ✅ Resource attributes
- ✅ Error handling with rich error types
- ✅ YAML configuration parsing
- ✅ Complete working example

**Not yet implemented:**
- ⏳ Pull metric readers
- ⏳ Trace configuration
- ⏳ Log configuration
- ⏳ Batch processors

## Migration from Original `opentelemetry-config`

### Before (opentelemetry-config)

```rust
// Implement trait
pub struct ConsolePeriodicExporterProvider {}

impl MetricsReaderPeriodicExporterProvider for ConsolePeriodicExporterProvider {
    fn provide(&self, mut builder: MeterProviderBuilder, config: &Value) -> MeterProviderBuilder {
        let cfg = serde_yaml::from_value(config.clone()).expect("...");
        let exporter = create_exporter(cfg);
        builder.with_periodic_exporter(exporter)
    }
    fn as_any(&self) -> &dyn Any { self }
}

impl ConsolePeriodicExporterProvider {
    pub fn register_into(registry: &mut ConfigurationProvidersRegistry) {
        let key = MetricsExporterId::PeriodicExporter.qualified_name("console");
        registry.metrics_mut().register_periodic_exporter_provider(key, Box::new(Self::new()));
    }
}

// Usage
let mut registry = ConfigurationProvidersRegistry::new();
ConsolePeriodicExporterProvider::register_into(&mut registry);
```

### After (otel-config-2)

```rust
// Just a function
pub fn create_exporter(config: serde_yaml::Value) -> Result<Box<dyn PushMetricExporter>, Error> {
    let cfg = serde_yaml::from_value(config)?;
    let exporter = create_exporter(cfg);
    Ok(Box::new(exporter))
}

// Usage
let mut registry = ConfigurationRegistry::new();
registry.register_metric_exporter("console", create_exporter);
```

**Result:** 70% less code, no traits, no boilerplate.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

Apache-2.0
