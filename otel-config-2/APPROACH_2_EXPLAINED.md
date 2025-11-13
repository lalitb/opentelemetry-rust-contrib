# Approach 2: Type Erasure Pattern - Explained

## ✅ This Implementation WORKS!

Unlike the naive factory approach, this implementation **compiles and runs successfully** by using type erasure at registration time.

## The Key Insight

**Problem:** SDK traits like `PushMetricExporter` are not object-safe (cannot use `Box<dyn PushMetricExporter>`)

**Solution:** Erase the type at registration time, not at return time!

## How It Works

### Step 1: User Writes a Simple Factory Function

```rust
// Just a function returning a concrete type
fn create_console_exporter(
    config: serde_yaml::Value
) -> Result<opentelemetry_stdout::MetricExporter, Error> {
    //                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Concrete type!
    let cfg: ConsoleConfig = serde_yaml::from_value(config)?;
    Ok(opentelemetry_stdout::MetricExporter::builder()
        .with_temporality(cfg.temporality)
        .build())
}
```

**Notice:** Returns `opentelemetry_stdout::MetricExporter` (concrete), not `Box<dyn PushMetricExporter>` (which would fail)

### Step 2: Registration with Automatic Type Erasure

```rust
registry.register_metric_exporter("console", create_console_exporter);
```

**What happens internally:**

```rust
pub fn register_metric_exporter<F, E>(&mut self, name: &'static str, factory: F)
where
    F: Fn(serde_yaml::Value) -> Result<E, Error> + Send + Sync + 'static,
    E: PushMetricExporter + 'static,
    //  ^^^ Generic type parameter captures the concrete exporter type
{
    // Wrap the factory in a generic wrapper
    let wrapper = MetricExporterFactory {
        factory,  // Captures F and E at compile time
        _phantom: PhantomData,
    };

    // Store as object-safe trait object
    self.metric_exporters.insert(name, Box::new(wrapper));
    //                                 ^^^^^^^^^^^^^^^^^^^ Type erased here!
}
```

### Step 3: Object-Safe Wrapper Trait

```rust
// Object-safe trait (no generics, no async, no impl Future)
trait MetricExporterConfigurator: Send + Sync {
    fn configure(
        &self,
        builder: MeterProviderBuilder,
        config: serde_yaml::Value,
    ) -> Result<MeterProviderBuilder, Error>;
}
```

### Step 4: Generic Implementation

```rust
// Generic wrapper implements the object-safe trait
struct MetricExporterFactory<F, E> {
    factory: F,
    _phantom: PhantomData<E>,
}

impl<F, E> MetricExporterConfigurator for MetricExporterFactory<F, E>
where
    F: Fn(serde_yaml::Value) -> Result<E, Error> + Send + Sync,
    E: PushMetricExporter + 'static,
{
    fn configure(
        &self,
        builder: MeterProviderBuilder,
        config: serde_yaml::Value,
    ) -> Result<MeterProviderBuilder, Error> {
        // Call user's factory to get concrete exporter
        let exporter = (self.factory)(config)?;
        //             ^^^^^^^^^^^^^^^^^^^^^^^^^^^ Returns concrete E

        // Builder's with_reader() is generic and accepts concrete E
        let reader = PeriodicReader::builder(exporter).build();
        //                                   ^^^^^^^^ Concrete type

        Ok(builder.with_reader(reader))
    }
}
```

### Step 5: Runtime Usage

```rust
// At runtime, look up the wrapper
let configurator: &dyn MetricExporterConfigurator =
    registry.metric_exporters.get("console")?;
    //       ^^^^^^^^^^^^^^^^ Box<dyn MetricExporterConfigurator>

// Call the object-safe method
builder = configurator.configure(builder, config)?;
//        ^^^^^^^^^^^ Internally calls the user's factory with concrete types
```

## The Magic

```
User Function          Generic Wrapper            Object-Safe Storage
━━━━━━━━━━━━━━         ━━━━━━━━━━━━━━━━━         ━━━━━━━━━━━━━━━━━━━

create_console    ->   MetricExporterFactory<   ->   Box<dyn MetricExporterConfigurator>
returns                  F: Fn(...) -> Console,
ConsoleExporter           E: Console
                        >

Type: Concrete         Type: Generic              Type: Trait Object
                       (captures concrete)        (object-safe)
```

## Why This Works

1. **User's factory returns concrete type** → No object safety issue
2. **Generic wrapper captures the concrete type** → Type known at compile time for that wrapper
3. **Wrapper implements object-safe trait** → Can be stored as `Box<dyn Trait>`
4. **Builder methods are generic** → Accept concrete types, not trait objects
5. **Type erasure happens once at registration** → Not at every use

## Comparison with Naive Approach

### ❌ Naive Approach (Fails)

```rust
// Try to return trait object directly
pub type MetricExporterFactory =
    dyn Fn(Value) -> Result<Box<dyn PushMetricExporter>, Error>;
    //                       ^^^^^^^^^^^^^^^^^^^^^^^^^ ❌ NOT object-safe
```

**Error:** `trait PushMetricExporter is not dyn compatible`

### ✅ Type Erasure Approach (Works)

```rust
// Return concrete type, wrap it generically
pub fn register_metric_exporter<F, E>(&mut self, name: &'static str, factory: F)
where
    F: Fn(Value) -> Result<E, Error>,  // ✅ E is concrete
    E: PushMetricExporter,
{
    let wrapper = MetricExporterFactory { factory, _phantom: PhantomData };
    //            ^^^^^^^^^^^^^^^^^^^^^^^^^ Generic wrapper
    self.metric_exporters.insert(name, Box::new(wrapper));
    //                                 ^^^^^^^^^^^^^^^^^^^ Type-erased wrapper
}
```

**Works because:** Type erasure happens at the wrapper level, not the exporter level

## User Experience

### For Exporter Authors

```rust
// Before (old approach): Implement trait, ~80 lines
impl MetricsReaderPeriodicExporterProvider for ConsoleProvider {
    fn provide(&self, builder: MeterProviderBuilder, config: &Value) -> MeterProviderBuilder {
        // ...lots of boilerplate...
    }
    fn as_any(&self) -> &dyn Any { self }
}

// After (type erasure): Just a function, ~30 lines
fn create_console_exporter(
    config: serde_yaml::Value
) -> Result<opentelemetry_stdout::MetricExporter, Error> {
    let cfg: ConsoleConfig = serde_yaml::from_value(config)?;
    Ok(opentelemetry_stdout::MetricExporter::builder()
        .with_temporality(cfg.temporality)
        .build())
}
```

**Reduction: 62% less code**

### For Application Developers

```rust
// Register exporters (type erasure is invisible)
let mut registry = ConfigurationRegistry::new();
registry.register_metric_exporter("console", create_console_exporter);
registry.register_metric_exporter("otlp", create_otlp_exporter);

// Build from YAML
let providers = TelemetryProvider::build_from_yaml_file(&registry, "config.yaml")?;

// Use providers
opentelemetry::global::set_meter_provider(providers.meter_provider.unwrap());
```

**Simple, clean API with no visible complexity**

## Technical Benefits

1. **No trait implementation required** → Simpler for exporter authors
2. **Returns concrete types** → Works with non-object-safe SDK traits
3. **Type-safe** → Compile-time guarantees
4. **Zero-cost abstraction** → Generic code is monomorphized
5. **Flexible** → Can support any exporter type that implements the SDK trait

## Limitations

1. **Type erasure pattern has some "magic"** → Uses generics and PhantomData
2. **Slightly more complex implementation** → But invisible to users
3. **Still need separate wrapper for each signal type** → Because SDK traits differ

## When to Use This Pattern

Use type erasure pattern when:
- ✅ You want simple function-based APIs
- ✅ The underlying traits are not object-safe
- ✅ You need to store different implementations in a collection
- ✅ You control the registration API

Don't use when:
- ❌ Traits are already object-safe
- ❌ You don't need runtime plugin selection
- ❌ Users need fine-grained builder control

## Conclusion

The type erasure pattern solves the object-safety problem by:
1. Letting users return concrete types (object-safe or not)
2. Wrapping them in generic structs at registration time
3. Storing the wrappers as object-safe trait objects
4. Using the builders' generic methods with concrete types

**Result:** Simple API for users, works with SDK constraints, compiles and runs successfully!

## Run The Example

```bash
cd otel-config-2/examples/simple
cargo run config.yaml
```

You'll see it:
1. Register the console exporter (type erasure happens here)
2. Load YAML configuration
3. Create the concrete exporter (no trait objects involved)
4. Wire it into the meter provider
5. Emit metrics successfully
