# How otel-config-2 Solves the Trait Object Safety Problem

## The Problem

OpenTelemetry exporter traits like `PushMetricExporter` are **not object-safe**, so you cannot create `Box<dyn PushMetricExporter>`:

```rust
// ❌ This doesn't work - PushMetricExporter is not object-safe
let exporter: Box<dyn PushMetricExporter> = Box::new(MyExporter::new());
```

## The Solution: Two-Layer Type Erasure

### Step 1: Define an Object-Safe Configurator Trait

```rust
// This trait IS object-safe (no generics, no Self in problematic positions)
trait MetricExporterConfigurator: Send + Sync {
    fn configure(
        &self,
        builder: MeterProviderBuilder,
        config: serde_yaml::Value,
    ) -> Result<MeterProviderBuilder, Error>;
}
```

### Step 2: Create a Generic Wrapper

```rust
// Generic wrapper that captures the concrete exporter type E
struct MetricExporterFactory<F, E>
where
    F: Fn(serde_yaml::Value) -> Result<E, Error>,
    E: PushMetricExporter + 'static,  // E is the concrete type!
{
    factory: F,
    _phantom: PhantomData<E>,
}

// Implement the object-safe trait
impl<F, E> MetricExporterConfigurator for MetricExporterFactory<F, E>
where
    F: Fn(serde_yaml::Value) -> Result<E, Error>,
    E: PushMetricExporter + 'static,
{
    fn configure(
        &self,
        builder: MeterProviderBuilder,
        config: serde_yaml::Value,
    ) -> Result<MeterProviderBuilder, Error> {
        // Call factory to get CONCRETE type E
        let exporter: E = (self.factory)(config)?;
        
        // Pass concrete type directly to SDK builder (which is generic)
        // No trait object needed!
        let reader = PeriodicReader::builder(exporter).build();
        Ok(builder.with_reader(reader))
    }
}
```

### Step 3: Registration (Type Erasure Happens Here)

```rust
pub fn register_metric_exporter<F, E>(&mut self, name: &'static str, factory: F)
where
    F: Fn(serde_yaml::Value) -> Result<E, Error> + 'static,
    E: PushMetricExporter + 'static,
{
    // Wrap user's factory in our generic wrapper
    let wrapper = MetricExporterFactory {
        factory,
        _phantom: PhantomData,
    };
    
    // Store as Box<dyn MetricExporterConfigurator> (object-safe!)
    self.exporters.insert(name, Box::new(wrapper));
}
```

## Usage Example

```rust
// User writes a simple factory returning concrete type
fn create_my_exporter(config: serde_yaml::Value) -> Result<MyExporter, Error> {
    Ok(MyExporter::new(config))
}

// Register it
registry.register_metric_exporter("my-exporter", create_my_exporter);

// At runtime, when building:
// 1. Look up Box<dyn MetricExporterConfigurator>
// 2. Call configurator.configure()
// 3. Inside configure(), factory returns MyExporter (concrete)
// 4. Pass MyExporter directly to builder.with_reader()
// 5. No trait object of PushMetricExporter ever created!
```

## The Key Insight

The SDK's builder accepts **concrete types** via generics:

```rust
impl MeterProviderBuilder {
    // Generic method - accepts ANY type that implements PushMetricExporter
    pub fn with_reader<E: PushMetricExporter + 'static>(self, exporter: E) -> Self {
        // ...
    }
}
```

So we:
1. ✅ Store `Box<dyn MetricExporterConfigurator>` (object-safe)
2. ✅ Factory returns concrete type `E`
3. ✅ Pass concrete `E` to generic builder method
4. ❌ Never create `Box<dyn PushMetricExporter>` (not object-safe)

## Why This Works

```
User Factory Function: serde_yaml::Value -> Result<ConcreteExporter, Error>
                                    ↓
      Wrapped in: MetricExporterFactory<F, ConcreteExporter>
                                    ↓
          Stored as: Box<dyn MetricExporterConfigurator>  ← Object-safe!
                                    ↓
         Called at runtime: configurator.configure(builder, config)
                                    ↓
        Factory returns: ConcreteExporter (not Box<dyn ...>)
                                    ↓
           Passed to: builder.with_reader(concrete_exporter)  ← Generic method!
```

**Result:** No trait object of non-object-safe trait ever created!
