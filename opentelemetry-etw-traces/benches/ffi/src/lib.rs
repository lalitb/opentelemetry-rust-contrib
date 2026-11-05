//! Experimental benchmark ABI. Non-null pointers must reference valid, correctly aligned
//! memory with no concurrent handle access. Handles are fixed to their creating thread.
//! End spans in LIFO order; providers must outlive active spans and provider calls.
//! The original API copies strings and consumes handles. Reusable handles have explicit
//! destruction, and only `etw_span_begin_static` opts into process-lifetime string borrowing.

use opentelemetry::trace::{SpanKind, Status, TraceContextExt, Tracer, TracerProvider};
use opentelemetry::{Context, ContextGuard, KeyValue};
use opentelemetry_etw_traces::Processor;
use opentelemetry_sdk::trace::{Sampler, SdkTracer, SdkTracerProvider};
use std::borrow::Cow;
use std::cell::Cell;
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
const INVALID_STATE: u32 = 4;
const OUT_OF_ORDER: u32 = 5;

thread_local! {
    // Intrusive stack: stable Rust-owned handle addresses, not an allocated stack.
    static CURRENT_HANDLE: Cell<*const SpanHandle> = const { Cell::new(ptr::null()) };
}

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

    /// The caller must keep the referenced bytes readable and immutable for `'a`.
    /// Null is permitted only for an empty string. UTF-8 validation does not allocate.
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

struct Attachment {
    guard: Option<ContextGuard>,
    previous: *const SpanHandle,
}

impl Drop for Attachment {
    fn drop(&mut self) {
        drop(self.guard.take());
        CURRENT_HANDLE.with(|current| current.set(self.previous));
    }
}

struct ActiveSpan {
    context: Context,
    _attachment: Attachment,
}

impl Drop for ActiveSpan {
    fn drop(&mut self) {
        // Attachment also unwinds on an exporter panic, restoring both stacks.
        self.context.span().end();
    }
}

pub struct SpanHandle {
    owner: ThreadId,
    reusable: bool,
    active: Option<ActiveSpan>,
}

impl SpanHandle {
    fn new(reusable: bool) -> Self {
        Self {
            owner: std::thread::current().id(),
            reusable,
            active: None,
        }
    }

    fn begin(&mut self, provider: &ProviderHandle, name: Cow<'static, str>, attrs: Vec<KeyValue>) {
        let mut builder = provider
            .tracer
            .span_builder(name)
            .with_kind(SpanKind::Server);
        // Move this one buffer into the SDK; do not collect or deep-clone it again.
        builder.attributes = Some(attrs);
        let span = builder.start(&provider.tracer);
        let context = Context::current_with_span(span);
        let guard = context.clone().attach();
        let previous = CURRENT_HANDLE.with(|current| current.replace(self));
        self.active = Some(ActiveSpan {
            context,
            _attachment: Attachment {
                guard: Some(guard),
                previous,
            },
        });
    }

    fn can_end(&self) -> Result<(), u32> {
        self.active.as_ref().ok_or(INVALID_STATE)?;
        if !CURRENT_HANDLE.with(|current| ptr::eq(current.get(), self)) {
            return Err(OUT_OF_ORDER);
        }
        Ok(())
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

#[derive(Clone, Copy)]
enum Strings {
    Copy,
    Static,
}

impl Strings {
    /// All bytes must be valid for this call. For `Static`, they must additionally
    /// remain immutable and valid until process exit, including module residency.
    unsafe fn read(self, text: Text) -> Result<Cow<'static, str>, u32> {
        match self {
            Self::Copy => Ok(Cow::Owned(text.read()?.to_owned())),
            // This is the only foreign-to-'static lifetime conversion. The explicit
            // static entrypoint's caller guarantees the above lifetime, not the pointer
            // value, descriptor lifetime, or successful UTF-8 validation.
            Self::Static => Ok(Cow::Borrowed(text.read::<'static>()?)),
        }
    }
}

/// Descriptors need only live for this call. String lifetimes follow `strings`.
unsafe fn inputs(
    name: Text,
    attributes: *const Attribute,
    count: usize,
    strings: Strings,
) -> Result<(Cow<'static, str>, Vec<KeyValue>), u32> {
    if count > isize::MAX as usize / std::mem::size_of::<Attribute>()
        || (count != 0 && attributes.is_null())
    {
        return Err(BAD_ARGUMENT);
    }
    let name = strings.read(name)?;
    let input = if count == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(attributes, count)
    };
    let mut owned = Vec::with_capacity(count);
    for attribute in input {
        let key = strings.read(attribute.key)?;
        let value = match attribute.kind {
            1 => KeyValue::new(key, strings.read(attribute.string_value)?),
            2 => KeyValue::new(key, attribute.integer_value),
            3 if matches!(attribute.integer_value, 0 | 1) => {
                KeyValue::new(key, attribute.integer_value != 0)
            }
            _ => return Err(BAD_ARGUMENT),
        };
        owned.push(value);
    }
    Ok((name, owned))
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
/// The output slot must not own a live handle. A successful call owns copies of all
/// strings/descriptors. End on this same thread, in LIFO order, using `etw_span_end`.
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
        let (name, owned) = inputs(name, attributes, count, Strings::Copy)?;
        let mut handle = Box::new(SpanHandle::new(false));
        handle.begin(provider, name, owned);
        *output = Box::into_raw(handle);
        Ok(())
    })
}

#[no_mangle]
/// # Safety
/// `output` must be a writable slot not owning a live handle. Destroy this handle
/// with `etw_span_handle_destroy` on this thread, while inactive. Storage is Rust-owned.
pub unsafe extern "C" fn etw_span_handle_create(output: *mut *mut SpanHandle) -> u32 {
    boundary(|| {
        let output = output.as_mut().ok_or(BAD_ARGUMENT)?;
        *output = ptr::null_mut();
        *output = Box::into_raw(Box::new(SpanHandle::new(true)));
        Ok(())
    })
}

/// Pointers and lifetimes must satisfy the selected public begin entrypoint.
unsafe fn begin_reuse(
    provider: *const ProviderHandle,
    name: Text,
    attributes: *const Attribute,
    count: usize,
    span: *mut SpanHandle,
    strings: Strings,
) -> Result<(), u32> {
    let handle = span_ref(span)?;
    if !handle.reusable || handle.active.is_some() {
        return Err(INVALID_STATE);
    }
    let provider = provider.as_ref().ok_or(BAD_ARGUMENT)?;
    let (name, owned) = inputs(name, attributes, count, strings)?;
    // No mutable access until the owner-thread and lifecycle checks have passed.
    (*span).begin(provider, name, owned);
    Ok(())
}

#[no_mangle]
#[inline(never)]
/// # Safety
/// `span` must be a live reusable handle with no concurrent use. Provider and inputs
/// must be valid during this call; the provider must outlive the active span.
/// All strings are copied. Descriptors and bytes may be overwritten/freed on return.
pub unsafe extern "C" fn etw_span_begin_reuse(
    provider: *const ProviderHandle,
    name: Text,
    attributes: *const Attribute,
    count: usize,
    span: *mut SpanHandle,
) -> u32 {
    boundary(|| begin_reuse(provider, name, attributes, count, span, Strings::Copy))
}

#[no_mangle]
#[inline(never)]
/// # Safety
/// As for `etw_span_begin_reuse`, EXCEPT all name/key/string-value bytes must remain
/// immutable and valid until process exit, even after end/destroy/provider shutdown.
/// Keep their containing modules loaded. Only the descriptor array is call-scoped.
/// This explicit promise permits `Strings::read` to produce `&'static str`; checking
/// UTF-8 cannot verify that promise. Never use temporary or unloadable-module strings.
pub unsafe extern "C" fn etw_span_begin_static(
    provider: *const ProviderHandle,
    name: Text,
    attributes: *const Attribute,
    count: usize,
    span: *mut SpanHandle,
) -> u32 {
    boundary(|| begin_reuse(provider, name, attributes, count, span, Strings::Static))
}

#[no_mangle]
#[inline(never)]
/// # Safety
/// `span` must be a live handle with no concurrent use. Non-owner threads are rejected.
pub unsafe extern "C" fn etw_span_status(span: *mut SpanHandle, status: u32) -> u32 {
    boundary(|| {
        let span = span_ref(span)?;
        let active = span.active.as_ref().ok_or(INVALID_STATE)?;
        let status = match status {
            0 => Status::Unset,
            1 => Status::Ok,
            2 => Status::error(""),
            _ => return Err(BAD_ARGUMENT),
        };
        active.context.span().set_status(status);
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
        let handle = span_ref(*slot)?;
        if handle.reusable {
            return Err(INVALID_STATE);
        }
        handle.can_end()?;
        drop(Box::from_raw(std::mem::replace(slot, ptr::null_mut())));
        Ok(())
    })
}

#[no_mangle]
#[inline(never)]
/// # Safety
/// `span` must be a live reusable handle with no concurrent use. End on its owner
/// thread in LIFO order, without interfering with the attached context.
/// This resets but never frees storage. Even on a caught export panic it is inactive;
/// export may be incomplete. Exporter I/O failures are not conveyed by the SDK's end.
pub unsafe extern "C" fn etw_span_end_reuse(span: *mut SpanHandle) -> u32 {
    boundary(|| {
        let handle = span_ref(span)?;
        if !handle.reusable {
            return Err(INVALID_STATE);
        }
        handle.can_end()?;
        drop((*span).active.take());
        Ok(())
    })
}

#[no_mangle]
/// # Safety
/// `span` must be a writable slot holding a live reusable handle, with no concurrent
/// use. Only its owner thread may destroy it, and only while inactive. Active destroy
/// is rejected without ending the span. Success frees in Rust and nulls the slot.
pub unsafe extern "C" fn etw_span_handle_destroy(span: *mut *mut SpanHandle) -> u32 {
    boundary(|| {
        let slot = span.as_mut().ok_or(BAD_ARGUMENT)?;
        let handle = span_ref(*slot)?;
        if !handle.reusable || handle.active.is_some() {
            return Err(INVALID_STATE);
        }
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
            let attributes = benchmark_attributes();
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
/// # Safety
/// Provider and reusable handle must be live, on the owner thread, without concurrent
/// use. The caller creates/destroys the handle outside timing. All borrowed strings
/// below are Rust literals in this DLL, which must remain loaded until process exit.
pub unsafe extern "C" fn etw_rust_batch_reuse(
    provider: *const ProviderHandle,
    span: *mut SpanHandle,
    static_inputs: u32,
    iterations: usize,
) -> u32 {
    boundary(|| {
        let provider = provider.as_ref().ok_or(BAD_ARGUMENT)?;
        let begin = match static_inputs {
            0 => etw_span_begin_reuse,
            1 => etw_span_begin_static,
            _ => return Err(BAD_ARGUMENT),
        };
        let attributes = benchmark_attributes();
        for _ in 0..iterations {
            let code = begin(
                provider,
                Text::borrowed(NAME),
                attributes.as_ptr(),
                provider.attributes.len(),
                span,
            );
            if code != 0 {
                return Err(code);
            }
            let status = etw_span_status(span, 1);
            let end = etw_span_end_reuse(span);
            if status != 0 || end != 0 {
                return Err(if status != 0 { status } else { end });
            }
        }
        Ok(())
    })
}

fn benchmark_attributes() -> [Attribute; 3] {
    [
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
    ]
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

#[cfg(test)]
mod tests;
