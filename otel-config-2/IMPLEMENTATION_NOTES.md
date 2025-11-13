# Implementation Notes

## Critical Issue Discovered

During implementation, I discovered a **fundamental blocker** with the simplified factory approach:

### The Problem

The OpenTelemetry SDK traits (`PushMetricExporter`, `SpanExporter`, `LogExporter`) are **not dyn-compatible** (not object-safe).

This means we CANNOT use:
```rust
Box<dyn PushMetricExporter>  // ❌ Compile error: trait is not dyn compatible
```

### Why This Matters

The suggested simplified approach relies on factories returning boxed trait objects:

```rust
pub type MetricExporterFactory =
    dyn Fn(serde_yaml::Value) -> Result<Box<dyn PushMetricExporter>, Error>;
    //                                    ^^^^^^^^^^^^^^^^^^^^^^^^^ NOT POSSIBLE
```

### Why Are These Traits Not Dyn-Compatible?

The SDK traits likely have:
1. Generic methods
2. Associated types (other than Self)
3. `where Self: Sized` bounds
4. Or async methods without `async_trait`

### What This Means

**The simplified factory pattern as suggested cannot work** with the current OpenTelemetry Rust SDK design.

## Possible Solutions

### Option 1: Keep the Current Trait-Based Approach

The existing `opentelemetry-config` design with `MetricsReaderPeriodicExporterProvider` trait exists precisely because of this limitation:

```rust
pub trait MetricsReaderPeriodicExporterProvider {
    fn provide(
        &self,
        meter_provider_builder: MeterProviderBuilder,
        config: &Value,
    ) -> MeterProviderBuilder;
}
```

This works because:
- The provider manipulates the builder directly
- The builder methods accept concrete exporter types (not trait objects)
- No boxing of non-object-safe traits needed

**Improvements to make:**
1. Change `config: &Value` to `config: Value` (no clone)
2. Change registration to use `&'static str` for names
3. Return `Result` instead of panicking
4. Remove the `as_any()` method

### Option 2: Wait for SDK Changes

The OpenTelemetry Rust SDK could potentially:
1. Provide object-safe wrapper traits
2. Provide `BoxedExporter` types that are object-safe
3. Change the trait designs to be object-safe

This would require SDK changes and is not under our control.

### Option 3: Type Erasure Pattern

Create our own object-safe wrapper:

```rust
// Our own object-safe trait
trait ExporterFactory: Send + Sync {
    fn create_and_configure(
        &self,
        builder: MeterProviderBuilder,
        config: serde_yaml::Value,
    ) -> Result<MeterProviderBuilder, Error>;
}

// Implement for any closure that captures a specific exporter type
struct FactoryWrapper<F>(F);

impl<F, E> ExporterFactory for FactoryWrapper<F>
where
    F: Fn(serde_yaml::Value) -> Result<E, Error> + Send + Sync,
    E: PushMetricExporter + 'static,
{
    fn create_and_configure(
        &self,
        builder: MeterProviderBuilder,
        config: serde_yaml::Value,
    ) -> Result<MeterProviderBuilder, Error> {
        let exporter = (self.0)(config)?;
        Ok(builder.with_periodic_exporter(exporter))
    }
}
```

This is essentially what the current design does, just with different naming.

## Recommendation

**For now, improve the existing design** rather than the simplified factory approach:

### Concrete Improvements to `opentelemetry-config`:

1. **Remove `&` from config parameter** (no clone needed):
   ```rust
   fn provide(&self, builder: MeterProviderBuilder, config: serde_yaml::Value) -> ...
   ```

2. **Return `Result`** (proper error handling):
   ```rust
   fn provide(...) -> Result<MeterProviderBuilder, ProviderError>
   ```

3. **Use `&'static str` for registration**:
   ```rust
   pub fn register_periodic_exporter_provider(&mut self, name: &'static str, provider: Box<dyn ...>)
   ```

4. **Remove `as_any()` method** (use better testing patterns)

5. **Rename for clarity**:
   - `MetricsReaderPeriodicExporterProvider` → `MetricExporterProvider`
   - `provide` → keep it (it's fine)

These changes give us 80% of the benefits of the simplified approach while working within the SDK's constraints.

## Future Work

If/when the OpenTelemetry Rust SDK provides object-safe exporter traits or wrappers, we can migrate to the simpler factory pattern.

Until then, the trait-based provider pattern is the correct design given the constraints.

## Lessons Learned

1. **Always check trait object-safety** when designing plugin systems
2. **SDK constraints drive architecture** - we can't always use the "ideal" pattern
3. **The existing design wasn't over-engineered** - it was solving a real problem
4. **Small improvements matter** - even without a full redesign, fixing the clone/Result issues helps

## Status

This implementation (`otel-config-2`) demonstrates the limitations of the simplified approach and why it cannot be completed as designed.

The code is left in a non-compiling state to document the issue for future reference.

**Action Item**: Apply the recommended improvements to the existing `opentelemetry-config` crate instead.
