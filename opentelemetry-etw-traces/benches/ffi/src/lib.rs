/*
    Preliminary standalone results, 2026-11-03 (microseconds per complete span).
    Exporter: 2a34861319802fc33865f1825385f698a50f3f17; rustc 1.99.0, MSVC /O2.
    AMD EPYC 7763 VM: 8 guest-visible cores / 16 logical CPUs, 63.95 GiB RAM;
    Windows 11 Enterprise x64 10.0.26300. Inventory captured before/after each run.
    File-mode ETW, PID-filtered, perf clock, 64 KiB buffers (min 2048 / max 4096).
    Affinity mask 1; six processes/case, seven batches of 10,000 spans/process,
    1,000 warm-up spans/batch. Values are pooled medians of batch averages.

    | Attributes | Native Rust | Rust via wrapper | C++ via DLL | C++ vs native |
    |------------|-------------|------------------|-------------|---------------|
    | 0          | 2.932       | 3.093            | 3.067       | +4.6%         |
    | 3          | 3.200       | 3.621            | 3.641       | +13.8%        |

    Native Rust enters the DLL once per batch; C++ makes three API calls/span.
    Wrapper/C++ allocations: 8/13 versus native 6/7; all retain 512 bytes/thread.
    2,790,036 latency/memory events decoded, zero capture loss.
    C++ versus the same wrapper called from Rust: -0.84% / +0.56% pooled;
    paired differences varied both ways. This is not an equivalence proof.
    These measurements preceded repository packaging; see README.md for details.
*/

//! Experimental benchmark ABI. Non-null pointers must reference valid, correctly aligned
//! caller-owned memory for the call. Span handles are thread-affine and consumed
//! by end; providers must outlive their spans. No Rust allocations cross ownership.

use opentelemetry::trace::{SpanKind, Status, TraceContextExt, Tracer, TracerProvider};
use opentelemetry::{Context, ContextGuard, KeyValue};
use opentelemetry_etw_traces::Processor;
use opentelemetry_sdk::trace::{Sampler, SdkTracer, SdkTracerProvider};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::ptr;
use std::thread::ThreadId;
use tracelogging_dynamic::{Guid, Level, Provider};

const PROVIDER: &str = "EtwRustFfiCompare";
const NAME: &str = "BenchmarkOperation";
const BAD_ARGUMENT: u32 = 1;
const WRONG_THREAD: u32 = 2;
const PANIC: u32 = 3;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Text {
    data: *const u8,
    len: usize,
}

impl Text {
    fn borrowed(value: &str) -> Self {
        Self {
            data: value.as_ptr(),
            len: value.len(),
        }
    }

    unsafe fn read<'a>(self) -> Result<&'a str, u32> {
        if self.len > isize::MAX as usize || (self.len != 0 && self.data.is_null()) {
            return Err(BAD_ARGUMENT);
        }
        let bytes = if self.len == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(self.data, self.len)
        };
        std::str::from_utf8(bytes).map_err(|_| BAD_ARGUMENT)
    }
}

#[repr(C)]
pub struct Attribute {
    key: Text,
    kind: u32,
    string_value: Text,
    integer_value: i64,
}

pub struct ProviderHandle {
    provider: SdkTracerProvider,
    tracer: SdkTracer,
    probe: Pin<Box<Provider>>,
    attributes: Vec<KeyValue>,
}

pub struct SpanHandle {
    context: Context,
    guard: Option<ContextGuard>,
    owner: ThreadId,
}

impl Drop for SpanHandle {
    fn drop(&mut self) {
        self.context.span().end();
        drop(self.guard.take());
    }
}

fn boundary(action: impl FnOnce() -> Result<(), u32>) -> u32 {
    match catch_unwind(AssertUnwindSafe(action)) {
        Ok(Ok(())) => 0,
        Ok(Err(code)) => code,
        Err(_) => PANIC,
    }
}

unsafe fn span_ref<'a>(span: *mut SpanHandle) -> Result<&'a SpanHandle, u32> {
    let span = span.as_ref().ok_or(BAD_ARGUMENT)?;
    if span.owner != std::thread::current().id() {
        return Err(WRONG_THREAD);
    }
    Ok(span)
}

#[no_mangle]
/// # Safety
/// `output` must be a writable pointer slot. Destroy the returned handle once,
/// after all spans and calls using it have finished.
pub unsafe extern "C" fn etw_provider_create(
    attribute_count: usize,
    output: *mut *mut ProviderHandle,
) -> u32 {
    boundary(|| {
        let output = output.as_mut().ok_or(BAD_ARGUMENT)?;
        *output = ptr::null_mut();
        if attribute_count != 0 && attribute_count != 3 {
            return Err(BAD_ARGUMENT);
        }
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_resource(opentelemetry_sdk::Resource::builder_empty().build())
            .with_span_processor(
                Processor::builder(PROVIDER)
                    .build()
                    .map_err(|_| BAD_ARGUMENT)?,
            )
            .build();
        let tracer = provider.tracer("ffi-benchmark");
        let probe = Box::pin(Provider::new(PROVIDER, &Provider::options()));
        if probe.as_ref().register() != 0 {
            return Err(BAD_ARGUMENT);
        }
        let attributes = if attribute_count == 3 {
            vec![
                KeyValue::new("string_key", "benchmark-value"),
                KeyValue::new("integer_key", 42_i64),
                KeyValue::new("bool_key", true),
            ]
        } else {
            vec![]
        };
        *output = Box::into_raw(Box::new(ProviderHandle {
            provider,
            tracer,
            probe,
            attributes,
        }));
        Ok(())
    })
}

#[no_mangle]
/// # Safety
/// `provider` must point to a live provider handle with no outstanding spans/calls.
pub unsafe extern "C" fn etw_provider_destroy(provider: *mut *mut ProviderHandle) -> u32 {
    boundary(|| {
        let slot = provider.as_mut().ok_or(BAD_ARGUMENT)?;
        if slot.is_null() {
            return Err(BAD_ARGUMENT);
        }
        let owned = Box::from_raw(std::mem::replace(slot, ptr::null_mut()));
        owned.provider.shutdown().map_err(|_| BAD_ARGUMENT)
    })
}

#[no_mangle]
/// # Safety
/// `provider` must be a live handle.
pub unsafe extern "C" fn etw_provider_enabled(provider: *const ProviderHandle) -> u32 {
    provider
        .as_ref()
        .map_or(0, |p| u32::from(p.probe.enabled(Level::Verbose, 1)))
}

#[no_mangle]
/// # Safety
/// `output` must reference at least 36 writable bytes.
pub unsafe extern "C" fn etw_provider_guid(output: *mut u8) {
    ptr::copy_nonoverlapping(
        Guid::from_name(PROVIDER).to_utf8_bytes().as_ptr(),
        output,
        36,
    );
}

#[no_mangle]
#[inline(never)]
/// # Safety
/// Provider and input buffers must be valid for this call; output must be writable.
/// A successful call owns copies of its inputs. End the handle on this same thread.
pub unsafe extern "C" fn etw_span_begin(
    provider: *const ProviderHandle,
    name: Text,
    attributes: *const Attribute,
    count: usize,
    output: *mut *mut SpanHandle,
) -> u32 {
    boundary(|| {
        let output = output.as_mut().ok_or(BAD_ARGUMENT)?;
        *output = ptr::null_mut();
        let provider = provider.as_ref().ok_or(BAD_ARGUMENT)?;
        let name = name.read()?.to_owned();
        if count > isize::MAX as usize / std::mem::size_of::<Attribute>()
            || (count != 0 && attributes.is_null())
        {
            return Err(BAD_ARGUMENT);
        }
        let input = if count == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(attributes, count)
        };
        let mut owned = Vec::with_capacity(count);
        for attribute in input {
            let key = attribute.key.read()?.to_owned();
            let value = match attribute.kind {
                1 => KeyValue::new(key, attribute.string_value.read()?.to_owned()),
                2 => KeyValue::new(key, attribute.integer_value),
                3 if matches!(attribute.integer_value, 0 | 1) => {
                    KeyValue::new(key, attribute.integer_value != 0)
                }
                _ => return Err(BAD_ARGUMENT),
            };
            owned.push(value);
        }
        let span = provider
            .tracer
            .span_builder(name)
            .with_kind(SpanKind::Server)
            .with_attributes(owned)
            .start(&provider.tracer);
        let context = Context::current_with_span(span);
        let guard = context.clone().attach();
        *output = Box::into_raw(Box::new(SpanHandle {
            context,
            guard: Some(guard),
            owner: std::thread::current().id(),
        }));
        Ok(())
    })
}

#[no_mangle]
#[inline(never)]
/// # Safety
/// `span` must be a live handle with no concurrent use. Non-owner threads are rejected.
pub unsafe extern "C" fn etw_span_status(span: *mut SpanHandle, status: u32) -> u32 {
    boundary(|| {
        let span = span_ref(span)?;
        let status = match status {
            0 => Status::Unset,
            1 => Status::Ok,
            2 => Status::error(""),
            _ => return Err(BAD_ARGUMENT),
        };
        span.context.span().set_status(status);
        Ok(())
    })
}

#[no_mangle]
#[inline(never)]
/// # Safety
/// `span` must be a writable slot holding a live handle with no concurrent use.
/// Non-owner threads are rejected. Success consumes the handle and clears the slot.
pub unsafe extern "C" fn etw_span_end(span: *mut *mut SpanHandle) -> u32 {
    boundary(|| {
        let slot = span.as_mut().ok_or(BAD_ARGUMENT)?;
        span_ref(*slot)?;
        drop(Box::from_raw(std::mem::replace(slot, ptr::null_mut())));
        Ok(())
    })
}

#[inline(never)]
fn native_emit(provider: &ProviderHandle) {
    let span = provider
        .tracer
        .span_builder(NAME)
        .with_kind(SpanKind::Server)
        .with_attributes(provider.attributes.iter().cloned())
        .start(&provider.tracer);
    let context = Context::current_with_span(span);
    let guard = context.clone().attach();
    context.span().set_status(Status::Ok);
    context.span().end();
    drop(guard);
    drop(context);
}

#[no_mangle]
/// # Safety
/// `provider` must be a live handle; calls must not overlap provider destruction.
pub unsafe extern "C" fn etw_rust_batch(
    provider: *const ProviderHandle,
    through_wrapper: u32,
    iterations: usize,
) -> u32 {
    boundary(|| {
        let provider = provider.as_ref().ok_or(BAD_ARGUMENT)?;
        if through_wrapper == 0 {
            for _ in 0..iterations {
                native_emit(provider);
            }
        } else {
            let attributes = [
                Attribute {
                    key: Text::borrowed("string_key"),
                    kind: 1,
                    string_value: Text::borrowed("benchmark-value"),
                    integer_value: 0,
                },
                Attribute {
                    key: Text::borrowed("integer_key"),
                    kind: 2,
                    string_value: Text::borrowed(""),
                    integer_value: 42,
                },
                Attribute {
                    key: Text::borrowed("bool_key"),
                    kind: 3,
                    string_value: Text::borrowed(""),
                    integer_value: 1,
                },
            ];
            for _ in 0..iterations {
                let mut span = ptr::null_mut();
                let code = etw_span_begin(
                    provider,
                    Text::borrowed(NAME),
                    attributes.as_ptr(),
                    provider.attributes.len(),
                    &mut span,
                );
                if code != 0 {
                    return Err(code);
                }
                let status = etw_span_status(span, 1);
                let end = etw_span_end(&mut span);
                if status != 0 || end != 0 {
                    return Err(BAD_ARGUMENT);
                }
            }
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn etw_prepare_thread() {
    drop(Context::new().attach());
    let _ = std::thread::current().id();
}

#[no_mangle]
pub extern "C" fn etw_current_span_valid() -> u32 {
    u32::from(Context::current().span().span_context().is_valid())
}

#[no_mangle]
#[inline(never)]
pub extern "C" fn etw_empty_call(value: u64) -> u64 {
    std::hint::black_box(value).wrapping_add(1)
}

#[no_mangle]
pub extern "C" fn etw_allocator_self_test() {
    use std::alloc::{alloc, dealloc, realloc, Layout};
    unsafe {
        let a_layout = Layout::from_size_align(73, 1).unwrap();
        let b_layout = Layout::from_size_align(257, 1).unwrap();
        let c_layout = Layout::from_size_align(89, 1).unwrap();
        let a = std::hint::black_box(alloc(a_layout));
        let b = std::hint::black_box(alloc(b_layout));
        assert!(!a.is_null() && !b.is_null());
        let b = std::hint::black_box(realloc(b, b_layout, 401));
        let c = std::hint::black_box(alloc(c_layout));
        assert!(!b.is_null() && !c.is_null());
        dealloc(a, a_layout);
        dealloc(b, Layout::from_size_align(401, 1).unwrap());
        dealloc(c, c_layout);
    }
}
