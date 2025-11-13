# otel-config-2-custom-exporter

Example custom metrics exporter demonstrating how external crates can integrate with `otel-config-2`.

## Purpose

This crate shows how **incredibly simple** it is to create a custom OpenTelemetry exporter that works with `otel-config-2`'s configuration system.

**Key Insight:** You only need to provide **ONE function** - no traits to implement, no boilerplate!

## What This Demonstrates

1. ✅ **Custom exporter implementation** - A working metrics exporter with custom config
2. ✅ **Simple factory function** - The ONLY thing external crates need to provide
3. ✅ **Configuration deserialization** - Custom YAML config structure
4. ✅ **Validation** - Config validation with helpful error messages
5. ✅ **Integration example** - How to use it alongside other exporters

## The Entire Integration Code

This is ALL you need to integrate with `otel-config-2`:

```rust
/// Your custom exporter configuration
#[derive(Deserialize)]
pub struct CustomExporterConfig {
    pub endpoint: String,
    pub timeout_secs: u64,
    // ... your custom fields
}

/// Your exporter implementation
pub struct CustomMetricExporter {
    config: CustomExporterConfig,
}

impl PushMetricExporter for CustomMetricExporter {
    fn export(&self, metrics: &mut ResourceMetrics) -> MetricResult<()> {
        // Your export logic
        Ok(())
    }
    // ... other required methods
}

/// THE ONLY FUNCTION YOU NEED FOR INTEGRATION! 🎉
pub fn create_custom_exporter(
    config: serde_yaml::Value
) -> Result<CustomMetricExporter, Error> {
    let cfg: CustomExporterConfig = serde_yaml::from_value(config)?;

    // Validate and create your exporter
    Ok(CustomMetricExporter::new(cfg))
}
```

**That's it!** No traits, no wrappers, no `register_into()` methods!

## Usage

### 1. Add Dependencies

```toml
[dependencies]
otel-config-2 = "0.1"
otel-config-2-custom-exporter = "0.1"
```

### 2. Register Your Exporter

```rust
use otel_config_2::ConfigurationRegistry;
use otel_config_2_custom_exporter::create_custom_exporter;

let mut registry = ConfigurationRegistry::new();

// One line registration - type erasure happens automatically!
registry.register_metric_exporter("custom", create_custom_exporter);
```

### 3. Configure in YAML

```yaml
metrics:
  readers:
    - periodic:
        exporter:
          custom:
            endpoint: "http://localhost:8080/metrics"
            timeout_secs: 30
            batch_size: 512
            temporality: delta
            debug: true
```

### 4. Build and Use

```rust
let providers = TelemetryProvider::build_from_yaml_file(&registry, "config.yaml")?;
opentelemetry::global::set_meter_provider(providers.meter_provider.unwrap());
```

## Running the Example

```bash
cd examples/demo
cargo run config.yaml
```

This will:
1. Register both console and custom exporters
2. Load configuration with TWO readers (one for each exporter)
3. Emit metrics that go to BOTH exporters
4. Show the custom exporter receiving and "sending" metrics

## Configuration Options

The custom exporter supports these configuration options:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `endpoint` | string | Yes | - | URL to send metrics to |
| `timeout_secs` | number | No | 30 | Timeout for export operations |
| `batch_size` | number | No | 512 | Number of metrics to batch |
| `custom_header` | string | No | - | Custom header to include in requests |
| `temporality` | string | No | cumulative | Either "delta" or "cumulative" |
| `debug` | boolean | No | false | Enable debug logging |

### Example Configurations

#### Minimal Configuration

```yaml
exporter:
  custom:
    endpoint: "http://localhost:8080/metrics"
```

#### Full Configuration

```yaml
exporter:
  custom:
    endpoint: "http://production-metrics.example.com/api/v1/metrics"
    timeout_secs: 60
    batch_size: 1024
    custom_header: "X-API-Key: your-secret-key"
    temporality: delta
    debug: false
```

## Comparison with Old Approach

### ❌ Old Approach (opentelemetry-config)

```rust
// Step 1: Define a struct
pub struct CustomPeriodicExporterProvider {}

// Step 2: Implement a trait with multiple methods
impl MetricsReaderPeriodicExporterProvider for CustomPeriodicExporterProvider {
    fn provide(
        &self,
        mut builder: MeterProviderBuilder,
        config: &Value,
    ) -> MeterProviderBuilder {
        let config = serde_yaml::from_value(config.clone())
            .expect("Failed to deserialize");
        // ... setup logic
        builder.with_periodic_exporter(exporter)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// Step 3: Create a registration helper
impl CustomPeriodicExporterProvider {
    pub fn register_into(registry: &mut ConfigurationProvidersRegistry) {
        let key = MetricsExporterId::PeriodicExporter.qualified_name("custom");
        registry
            .metrics_mut()
            .register_periodic_exporter_provider(key, Box::new(Self::new()));
    }
}

// Step 4: User calls the registration helper
CustomPeriodicExporterProvider::register_into(&mut registry);
```

**Total: ~80 lines of boilerplate**

### ✅ New Approach (otel-config-2)

```rust
// Step 1: Write a function
pub fn create_custom_exporter(
    config: serde_yaml::Value
) -> Result<CustomMetricExporter, Error> {
    let cfg: CustomExporterConfig = serde_yaml::from_value(config)?;
    Ok(CustomMetricExporter::new(cfg))
}

// Step 2: User registers it
registry.register_metric_exporter("custom", create_custom_exporter);
```

**Total: ~10 lines**

**88% less code! 🎉**

## Benefits for Exporter Authors

1. **Simple**: Just write a factory function
2. **No boilerplate**: No traits, no wrapper structs
3. **Type-safe**: Return concrete types, not trait objects
4. **Flexible**: Full control over config structure
5. **Testable**: Easy to unit test the factory function
6. **Clear errors**: Proper Result types, no panics
7. **No magic**: What you see is what you get

## Implementation Details

### How Type Erasure Works

When you call:

```rust
registry.register_metric_exporter("custom", create_custom_exporter);
```

Behind the scenes:

1. Your function signature is captured as generic parameters `F` and `E`
2. A wrapper `MetricExporterFactory<F, E>` is created
3. The wrapper implements an object-safe trait
4. The wrapper is stored as `Box<dyn MetricExporterConfigurator>`

**Type erasure happens once at registration**, not at every use!

### Why This Works

- ✅ Your function returns `CustomMetricExporter` (concrete type)
- ✅ SDK's `PeriodicReader::builder()` accepts concrete types
- ✅ No need for `Box<dyn PushMetricExporter>` (which would fail!)
- ✅ Type erasure is hidden in the registration machinery

## For Exporter Library Authors

If you're creating an exporter library (like `opentelemetry-otlp`, `opentelemetry-jaeger`, etc.):

1. **Export a factory function**:
   ```rust
   pub fn create_exporter(config: serde_yaml::Value)
       -> Result<YourExporter, otel_config_2::Error>
   ```

2. **Document your config structure**:
   ```rust
   #[derive(Deserialize)]
   pub struct YourExporterConfig {
       // Your fields with docs
   }
   ```

3. **That's it!** Users can now register and use your exporter.

## Testing

Run tests:

```bash
cargo test
```

Tests cover:
- Configuration deserialization
- Config validation
- Factory function behavior
- Exporter functionality

## Real-World Usage

For a production exporter, you would:

1. **Implement actual export logic** (HTTP POST, gRPC, etc.)
2. **Handle retries and backpressure**
3. **Add proper logging**
4. **Implement batch aggregation**
5. **Handle authentication**
6. **Add metrics about the exporter itself**

But the **integration with otel-config-2 stays the same** - just one factory function!

## See Also

- [otel-config-2](../otel-config-2/) - The configuration framework
- [otel-config-2 examples](../otel-config-2/examples/) - More examples
- [OpenTelemetry Rust SDK](https://github.com/open-telemetry/opentelemetry-rust) - The SDK

## License

Apache-2.0
