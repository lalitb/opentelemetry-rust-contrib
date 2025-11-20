# OpenTelemetry Config API Reference

This document details the **Public API** exposed by `opentelemetry-config`.

## Feature Flags

The library provides two distinct API experiences controlled by a feature flag:

### Default Mode (No Feature Flags)
**Cargo.toml**: `opentelemetry-config = "0.1"`

- **Purpose**: Minimal, YAML-focused API for maximum flexibility
- **Use Case**: When you want simple configuration from YAML files without compile-time type checking
- **API Surface**: Only YAML parsing methods and generic exporter registration
- **Pros**: 
  - Simple API
  - No need to understand internal model structures
  - Flexible - works with any YAML structure
- **Cons**: 
  - No compile-time validation of configuration structure
  - Manual parsing of YAML values in factory functions

### Typed API Mode
**Cargo.toml**: `opentelemetry-config = { version = "0.1", features = ["typed-api"] }`

- **Purpose**: Strongly-typed configuration with compile-time safety
- **Use Case**: When you want type-safe configuration and programmatic access to config models
- **API Surface**: Full model module exposed + separate periodic/pull registration methods
- **Pros**: 
  - Compile-time type checking
  - IDE autocomplete for configuration structures
  - Pattern matching on typed enums
- **Cons**: 
  - Larger API surface
  - More types to understand

---

## Common API
These types and methods are available in **all** modes.

### Registry
The entry point for registering custom component factories.

```rust
pub struct ConfigurationProviderRegistry {
    pub fn metrics(&mut self) -> &mut MeterProviderRegistry;
}
```

### Errors
Error types returned by the library.

```rust
pub enum ConfigurationError {
  // TODO
}
```

---

## Default API (YAML-based)
**Feature Flag**: None (Default)

In this mode, the API is minimal. You interact with the library primarily through YAML parsing methods and a single unified factory registration. **Internal configuration models are hidden.**

### Parsing Configuration
Methods to load configuration.

```rust
impl TelemetryProviders {
    /// Configures providers from a YAML string
    pub fn configure_from_yaml(
        registry: &ConfigurationProviderRegistry,
        yaml: &str
    ) -> Result<TelemetryProviders, ProviderError>

    /// Configures providers from a YAML file path
    pub fn configure_from_yaml_file(
        registry: &ConfigurationProviderRegistry,
        path: &str
    ) -> Result<TelemetryProviders, ProviderError>
}
```

### Factory Registration
A single method to register exporters (both push and pull).

```rust
pub struct MeterProviderRegistry {
    /// Register an exporter (works for both periodic and pull readers)
    pub fn register_config(
        &mut self,
        name: &'static str,
        factory: impl Fn(MeterProviderBuilder, &serde_yaml::Value) 
            -> Result<MeterProviderBuilder, ConfigurationError> + 'static
    );
}
```

### Examples

#### Custom Push Exporter
```rust
fn my_push_factory(
    builder: MeterProviderBuilder,
    config: &serde_yaml::Value
) -> Result<MeterProviderBuilder, ConfigurationError> {
    // Parse config manually
    if let Some(map) = config.as_mapping() {
        // ... initialize exporter ...
        return Ok(builder);
    }
    Err(ConfigurationError::InvalidConfiguration("Invalid config".into()))
}

registry.metrics().register_config("my_exporter", my_push_factory);
```

#### Custom Pull Exporter
```rust
fn my_pull_factory(
    builder: MeterProviderBuilder,
    config: &serde_yaml::Value
) -> Result<MeterProviderBuilder, ConfigurationError> {
    // ... initialize pull exporter ...
    Ok(builder)
}

registry.metrics().register_config("my_pull_exporter", my_pull_factory);
```

---

## Typed API
**Feature Flag**: `typed-api`

Enabling this feature exposes the full strongly-typed configuration model and allows for programmatic configuration. Reader registration is split into separate methods for periodic and pull readers.

### Programmatic Configuration
Allows passing a constructed `Telemetry` struct directly.

```rust
impl TelemetryProviders {
    pub fn configure(
        registry: &ConfigurationProviderRegistry,
        config: &model::Telemetry
    ) -> Result<TelemetryProviders, ProviderError>
}
```

### Configuration Models
The `model` module becomes public.

```rust
pub mod model {
    pub struct Telemetry {
        pub metrics: Option<metrics::Metrics>,
        pub resource: HashMap<String, serde_yaml::Value>,
    }

    pub mod metrics {
        pub struct Metrics {
            pub readers: Vec<reader::Reader>,
        }

        pub mod reader {
            pub enum Reader {
                Periodic(Periodic),
                Pull(Pull),
            }
            
            pub struct Periodic {
                pub interval: u64,
                pub timeout: u64,
                pub exporter: Exporter, // Typed Exporter
            }

            pub struct Pull {
                pub exporter: PullExporter, // Typed PullExporter
            }

            // Typed Exporter Configuration
            pub enum Exporter {
                Console(Console),
                Otlp(Otlp),
                Pull(PullExporter),
                Custom(String, serde_yaml::Value),
            }

            pub enum PullExporter {
                Prometheus(PullExporterPrometheus),
                Custom(String, serde_yaml::Value),
            }
            
            // ... specific config structs (Console, Otlp, etc.)
        }
    }
}
```

### Factory Registration
Separate methods for periodic and pull reader factories.

```rust
pub struct MeterProviderRegistry {
    /// Register a periodic reader factory
    pub fn register_periodic_reader_factory(
        &mut self,
        name: &'static str,
        factory: impl Fn(MeterProviderBuilder, &Exporter) 
            -> Result<MeterProviderBuilder, ConfigurationError> + 'static
    );
    
    /// Register a pull reader factory
    pub fn register_pull_reader_factory(
        &mut self,
        name: &'static str,
        factory: impl Fn(MeterProviderBuilder, &PullExporter) 
            -> Result<MeterProviderBuilder, ConfigurationError> + 'static
    );
    
    /// Check if a periodic reader factory is registered
    pub fn has_periodic_reader_factory(&self, name: &str) -> bool;
    
    /// Check if a pull reader factory is registered
    pub fn has_pull_reader_factory(&self, name: &str) -> bool;
}
```

### Examples

#### Custom Push Exporter
```rust
fn my_typed_push_factory(
    builder: MeterProviderBuilder,
    config: &Exporter
) -> Result<MeterProviderBuilder, ConfigurationError> {
    match config {
        Exporter::Custom(name, value) if name == "my_exporter" => {
            // ... initialize exporter using value ...
            Ok(builder)
        }
        _ => Err(ConfigurationError::InvalidConfiguration("Expected my_exporter".into())),
    }
}

registry.metrics().register_periodic_reader_factory("my_exporter", my_typed_push_factory);
```

#### Custom Pull Exporter
```rust
fn my_typed_pull_factory(
    builder: MeterProviderBuilder,
    config: &PullExporter
) -> Result<MeterProviderBuilder, ConfigurationError> {
    match config {
        PullExporter::Custom(name, value) if name == "my_pull_exporter" => {
            // ... initialize pull exporter ...
            Ok(builder)
        }
        _ => Err(ConfigurationError::InvalidConfiguration("Expected my_pull_exporter".into())),
    }
}

registry.metrics().register_pull_reader_factory("my_pull_exporter", my_typed_pull_factory);
```

---

## Complete Usage Examples

### Default Mode Example

```rust
use opentelemetry_config::{
    ConfigurationProviderRegistry, ConfigurationError, TelemetryProviders,
};
use opentelemetry_sdk::metrics::MeterProviderBuilder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create registry
    let mut registry = ConfigurationProviderRegistry::default();
    
    // 2. Register custom exporter factory
    registry.metrics().register_config(
        "my_custom_exporter",
        |builder, config| {
            // Parse YAML config manually
            println!("Configuring with: {:?}", config);
            // ... create and configure your exporter ...
            Ok(builder)
        },
    );
    
    // 3. Load configuration from YAML
    let yaml_config = r#"
        metrics:
          readers:
            - periodic:
                interval: 60000
                timeout: 5000
                exporter:
                  my_custom_exporter:
                    endpoint: "http://localhost:4317"
                    compression: true
        resource:
          service.name: "my-service"
          service.version: "1.0.0"
    "#;
    
    // 4. Configure providers from YAML
    let providers = TelemetryProviders::configure_from_yaml(
        &registry,
        yaml_config,
    )?;
    
    // 5. Get the meter provider
    if let Some(meter_provider) = providers.meter_provider() {
        // Use the meter provider
        let meter = meter_provider.meter("my-app");
        let counter = meter.u64_counter("requests").build();
        counter.add(1, &[]);
        println!("Meter provider configured successfully!");
    }
    
    Ok(())
}
```

### Typed Mode Example

```rust
#[cfg(feature = "typed-api")]
use opentelemetry_config::{
    ConfigurationProviderRegistry, ConfigurationError, TelemetryProviders,
    model::metrics::reader::Exporter,
};
use opentelemetry_sdk::metrics::MeterProviderBuilder;

#[cfg(feature = "typed-api")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create registry
    let mut registry = ConfigurationProviderRegistry::default();
    
    // 2. Register custom periodic exporter factory
    registry.metrics().register_periodic_reader_factory(
        "my_custom_exporter",
        |builder, exporter| {
            match exporter {
                Exporter::Custom(name, config) if name == "my_custom_exporter" => {
                    println!("Configuring {} with: {:?}", name, config);
                    // ... create and configure your exporter ...
                    Ok(builder)
                }
                _ => Err(ConfigurationError::InvalidConfiguration(
                    "Expected my_custom_exporter".into()
                )),
            }
        },
    );
    
    // 3. Load configuration from YAML
    let yaml_config = r#"
        metrics:
          readers:
            - periodic:
                interval: 60000
                timeout: 5000
                exporter:
                  my_custom_exporter:
                    endpoint: "http://localhost:4317"
                    compression: true
        resource:
          service.name: "my-service"
          service.version: "1.0.0"
    "#;
    
    // 4. Configure providers from YAML (still works in typed mode)
    let providers = TelemetryProviders::configure_from_yaml(
        &registry,
        yaml_config,
    )?;
    
    // 5. Get the meter provider
    if let Some(meter_provider) = providers.meter_provider() {
        // Use the meter provider
        let meter = meter_provider.meter("my-app");
        let counter = meter.u64_counter("requests").build();
        counter.add(1, &[]);
        println!("Meter provider configured successfully!");
    }
    
    Ok(())
}
```

### YAML Configuration Example

```yaml
metrics:
  readers:
    # Periodic reader with custom exporter
    - periodic:
        interval: 60000
        timeout: 5000
        exporter:
          my_custom_exporter:
            endpoint: "http://localhost:4317"
            protocol: "grpc"
    
    # Pull reader (e.g., Prometheus)
    - pull:
        exporter:
          prometheus:
            host: "0.0.0.0"
            port: 9090

resource:
  service.name: "my-service"
  service.version: "1.0.0"
  deployment.environment: "production"
```
