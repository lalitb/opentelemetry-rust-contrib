use super::*;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{Span, SpanData, SpanProcessor};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

#[derive(Clone, Debug, Default)]
struct Capture {
    spans: Arc<Mutex<Vec<SpanData>>>,
    panic_on_end: Arc<AtomicBool>,
}

impl SpanProcessor for Capture {
    fn on_start(&self, _: &mut Span, _: &Context) {}
    fn on_end(&self, mut span: SpanData) {
        if self.panic_on_end.swap(false, Ordering::Relaxed) {
            panic!("contract-test exporter panic");
        }
        // The test observer may retain owned snapshots, never borrowed FFI input.
        span.name = Cow::Owned(span.name.into_owned());
        span.attributes = span
            .attributes
            .into_iter()
            .map(|attribute| {
                let key = attribute.key.as_str().to_owned();
                let value = match attribute.value {
                    opentelemetry::Value::String(value) => {
                        opentelemetry::Value::from(value.as_str().to_owned())
                    }
                    value => value,
                };
                KeyValue::new(key, value)
            })
            .collect();
        self.spans.lock().unwrap().push(span);
    }
    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }
    fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
        Ok(())
    }
}

fn provider(count: usize) -> (ProviderHandle, Capture) {
    let capture = Capture::default();
    (provider_with_processor(count, capture.clone()), capture)
}

fn provider_with_processor(
    count: usize,
    processor: impl SpanProcessor + 'static,
) -> ProviderHandle {
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .with_resource(opentelemetry_sdk::Resource::builder_empty().build())
        .with_span_processor(processor)
        .build();
    let tracer = provider.tracer("ffi-benchmark");
    let attributes = if count == 0 {
        vec![]
    } else {
        vec![
            KeyValue::new("string_key", "benchmark-value"),
            KeyValue::new("integer_key", 42_i64),
            KeyValue::new("bool_key", true),
        ]
    };
    ProviderHandle {
        provider,
        tracer,
        probe: Box::pin(Provider::new(PROVIDER, &Provider::options())),
        attributes,
    }
}

#[derive(Debug)]
struct BorrowObservation {
    // Owned snapshots for assertions after foreign buffers are freed. These
    // test-only copies are made by the observer, never by the FFI under test.
    name: String,
    attributes: Vec<KeyValue>,
    addresses: Vec<usize>,
}

#[derive(Clone, Debug, Default)]
struct SynchronousCapture {
    observations: Arc<Mutex<Vec<BorrowObservation>>>,
    panic_on_end: Arc<AtomicBool>,
}

impl SpanProcessor for SynchronousCapture {
    fn on_start(&self, _: &mut Span, _: &Context) {}

    fn on_end(&self, span: SpanData) {
        assert!(matches!(span.name, Cow::Borrowed(_)));
        let mut addresses = vec![span.name.as_ptr() as usize];
        let attributes = span
            .attributes
            .iter()
            .map(|attribute| {
                addresses.push(attribute.key.as_str().as_ptr() as usize);
                let value = match &attribute.value {
                    opentelemetry::Value::String(value) => {
                        addresses.push(value.as_str().as_ptr() as usize);
                        opentelemetry::Value::from(value.as_str().to_owned())
                    }
                    value => value.clone(),
                };
                KeyValue::new(attribute.key.as_str().to_owned(), value)
            })
            .collect();
        self.observations.lock().unwrap().push(BorrowObservation {
            name: span.name.to_string(),
            attributes,
            addresses,
        });
        if self.panic_on_end.swap(false, Ordering::Relaxed) {
            panic!("synchronous export panic");
        }
        // SpanData and all borrowed references die here, also during unwinding.
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
        Ok(())
    }
}

fn reusable() -> *mut SpanHandle {
    let mut handle = ptr::null_mut();
    assert_eq!(unsafe { etw_span_handle_create(&mut handle) }, 0);
    handle
}

#[test]
fn borrowed_dynamic_bytes_reach_export_unchanged_and_are_released_at_end() {
    let capture = SynchronousCapture::default();
    let provider = provider_with_processor(0, capture.clone());
    unsafe {
        let mut handle = reusable();
        let address = handle;
        for (cycle, panic) in [false, true, false].into_iter().enumerate() {
            let mut name = format!("temporary-operation-{cycle}");
            let mut key = format!("temporary-key-{cycle}");
            let mut value = format!("temporary-value-{cycle}");
            let expected_addresses = vec![
                name.as_ptr() as usize,
                key.as_ptr() as usize,
                value.as_ptr() as usize,
            ];
            {
                let attribute = Attribute {
                    key: Text::borrowed(&key),
                    kind: 1,
                    string_value: Text::borrowed(&value),
                    integer_value: 0,
                };
                assert_eq!(
                    etw_span_begin_borrowed(
                        &provider,
                        Text::borrowed(&name),
                        &attribute,
                        1,
                        handle,
                    ),
                    0
                );
            } // Descriptor storage is not borrowed.
            {
                let rejected = String::from("rejected-input");
                assert_eq!(
                    etw_span_begin_borrowed(
                        &provider,
                        Text::borrowed(&rejected),
                        ptr::null(),
                        0,
                        handle,
                    ),
                    INVALID_STATE
                );
            } // Rejected begin does not retain its inputs.
            assert_eq!(etw_span_handle_destroy(&mut handle), INVALID_STATE);
            assert_eq!(etw_span_status(handle, 1), 0);
            capture.panic_on_end.store(panic, Ordering::Relaxed);
            assert_eq!(etw_span_end_reuse(handle), if panic { PANIC } else { 0 });
            assert_eq!(handle, address);
            assert!((*handle).active.is_none());
            assert_eq!(etw_current_span_valid(), 0);

            // End has released ALL borrowed bytes, even after an export panic.
            name.clear();
            key.clear();
            value.clear();
            drop((name, key, value));
            let observations = capture.observations.lock().unwrap();
            let observed = &observations[cycle];
            assert_eq!(observed.addresses, expected_addresses);
            assert_eq!(observed.name, format!("temporary-operation-{cycle}"));
            assert_eq!(
                observed.attributes,
                vec![KeyValue::new(
                    format!("temporary-key-{cycle}"),
                    format!("temporary-value-{cycle}")
                )]
            );
        }
        assert_eq!(etw_span_handle_destroy(&mut handle), 0);
    }
}

#[test]
fn rejected_borrowed_begin_retains_nothing_and_rejected_end_keeps_borrow_active() {
    let capture = SynchronousCapture::default();
    let provider = provider_with_processor(0, capture.clone());
    unsafe {
        let mut outer = reusable();
        let mut inner = reusable();
        {
            let name = String::from("failed-begin");
            let bad_attribute = Attribute {
                key: Text::borrowed(&name),
                kind: 99,
                string_value: Text::borrowed(&name),
                integer_value: 0,
            };
            assert_eq!(
                etw_span_begin_borrowed(&provider, Text::borrowed(&name), &bad_attribute, 1, outer),
                BAD_ARGUMENT
            );
        }
        assert!((*outer).active.is_none());
        assert_eq!(etw_current_span_valid(), 0);
        let outer_name = String::from("outer-borrow");
        assert_eq!(
            etw_span_begin_borrowed(
                &provider,
                Text::borrowed(&outer_name),
                ptr::null(),
                0,
                outer
            ),
            0
        );
        {
            let inner_name = String::from("inner-borrow");
            assert_eq!(
                etw_span_begin_borrowed(
                    &provider,
                    Text::borrowed(&inner_name),
                    ptr::null(),
                    0,
                    inner
                ),
                0
            );
            assert_eq!(etw_span_end_reuse(outer), OUT_OF_ORDER);
            assert!((*outer).active.is_some());
            assert!(capture.observations.lock().unwrap().is_empty());
            assert_eq!(etw_span_end_reuse(inner), 0);
        }
        // The outer name remained alive through its rejected end and inner end.
        assert_eq!(etw_span_end_reuse(outer), 0);
        drop(outer_name);
        assert_eq!(etw_current_span_valid(), 0);
        assert_eq!(etw_span_handle_destroy(&mut inner), 0);
        assert_eq!(etw_span_handle_destroy(&mut outer), 0);
        let observations = capture.observations.lock().unwrap();
        assert_eq!(observations.len(), 2);
        assert_eq!(observations[0].name, "inner-borrow");
        assert_eq!(observations[1].name, "outer-borrow");
    }
}

#[test]
fn reuse_and_invalid_lifecycle_preserve_handle_and_context() {
    let (provider, capture) = provider(0);
    unsafe {
        let mut handle = reusable();
        let address = handle;
        assert_eq!(etw_span_end_reuse(handle), INVALID_STATE);
        assert_eq!(etw_span_status(handle, 1), INVALID_STATE);
        assert_eq!(etw_span_end(&mut handle), INVALID_STATE);
        for _ in 0..3 {
            assert_eq!(
                etw_span_begin_reuse(&provider, Text::borrowed(NAME), ptr::null(), 0, handle),
                0
            );
            let context = Context::current().span().span_context().clone();
            for begin in [etw_span_begin_reuse, etw_span_begin_borrowed] {
                assert_eq!(
                    begin(&provider, Text::borrowed(NAME), ptr::null(), 0, handle),
                    INVALID_STATE
                );
            }
            assert_eq!(etw_span_handle_destroy(&mut handle), INVALID_STATE);
            assert_eq!(Context::current().span().span_context(), &context);
            assert_eq!(etw_span_status(handle, 99), BAD_ARGUMENT);
            assert_eq!(etw_span_status(handle, 1), 0);
            assert_eq!(etw_span_end_reuse(handle), 0);
            assert_eq!(handle, address);
            assert_eq!(etw_current_span_valid(), 0);
            assert_eq!(etw_span_end_reuse(handle), INVALID_STATE);
        }
        assert_eq!(etw_span_handle_destroy(&mut handle), 0);
        assert!(handle.is_null());
        assert_eq!(etw_span_handle_destroy(&mut handle), BAD_ARGUMENT);
        assert_eq!(capture.spans.lock().unwrap().len(), 3);
    }
}

#[test]
fn copying_inputs_can_be_overwritten_and_freed_before_end() {
    for reuse in [false, true] {
        let (provider, capture) = provider(3);
        unsafe {
            let mut handle = if reuse { reusable() } else { ptr::null_mut() };
            {
                let mut name = NAME.as_bytes().to_vec();
                let mut key = b"string_key".to_vec();
                let mut value = b"benchmark-value".to_vec();
                let mut attributes = benchmark_attributes();
                attributes[0].key = Text {
                    data: key.as_ptr(),
                    len: key.len(),
                };
                attributes[0].string_value = Text {
                    data: value.as_ptr(),
                    len: value.len(),
                };
                let name_input = Text {
                    data: name.as_ptr(),
                    len: name.len(),
                };
                let code = if reuse {
                    etw_span_begin_reuse(&provider, name_input, attributes.as_ptr(), 3, handle)
                } else {
                    etw_span_begin(&provider, name_input, attributes.as_ptr(), 3, &mut handle)
                };
                assert_eq!(code, 0);
                name.fill(b'X');
                key.fill(b'X');
                value.fill(b'X');
            }
            assert_eq!(etw_span_status(handle, 1), 0);
            if reuse {
                assert_eq!(etw_span_end_reuse(handle), 0);
                assert_eq!(etw_span_handle_destroy(&mut handle), 0);
            } else {
                assert_eq!(etw_span_end(&mut handle), 0);
                assert_eq!(etw_span_end(&mut handle), BAD_ARGUMENT);
            }
            let spans = capture.spans.lock().unwrap();
            assert_eq!(spans[0].name, NAME);
            assert_eq!(spans[0].attributes, provider.attributes);
        }
    }
}

#[test]
fn literal_bytes_are_borrowed_but_descriptors_are_call_scoped() {
    let capture = SynchronousCapture::default();
    let provider = provider_with_processor(3, capture.clone());
    let keys;
    let string_value;
    unsafe {
        let mut handle = reusable();
        {
            let attributes = benchmark_attributes();
            keys = attributes.each_ref().map(|a| a.key.data);
            string_value = attributes[0].string_value.data;
            assert_eq!(
                etw_span_begin_borrowed(
                    &provider,
                    Text::borrowed(NAME),
                    attributes.as_ptr(),
                    3,
                    handle
                ),
                0
            );
        }
        assert_eq!(etw_span_end_reuse(handle), 0);
        assert_eq!(etw_span_handle_destroy(&mut handle), 0);
    }
    let observations = capture.observations.lock().unwrap();
    let observed = &observations[0];
    assert_eq!(observed.name, NAME);
    assert_eq!(observed.attributes, provider.attributes);
    assert_eq!(
        observed.addresses,
        vec![
            NAME.as_ptr() as usize,
            keys[0] as usize,
            string_value as usize,
            keys[1] as usize,
            keys[2] as usize,
        ]
    );
}

#[test]
fn invalid_inputs_do_not_activate_or_consume_reusable_storage() {
    static INVALID_UTF8: [u8; 1] = [0xff];
    let invalid = Text {
        data: INVALID_UTF8.as_ptr(),
        len: 1,
    };
    let (provider, capture) = provider(3);
    unsafe {
        let mut handle = reusable();
        for begin in [etw_span_begin_reuse, etw_span_begin_borrowed] {
            assert_eq!(
                begin(&provider, invalid, ptr::null(), 0, handle),
                BAD_ARGUMENT
            );
            assert_eq!(
                begin(ptr::null(), Text::borrowed(NAME), ptr::null(), 0, handle),
                BAD_ARGUMENT
            );
            assert_eq!(
                begin(&provider, Text::borrowed(NAME), ptr::null(), 3, handle),
                BAD_ARGUMENT
            );
            for bad_field in 0..4 {
                let mut attributes = benchmark_attributes();
                match bad_field {
                    0 => attributes[0].key = invalid,
                    1 => attributes[0].string_value = invalid,
                    2 => attributes[0].kind = 99,
                    _ => attributes[2].integer_value = 2,
                }
                assert_eq!(
                    begin(
                        &provider,
                        Text::borrowed(NAME),
                        attributes.as_ptr(),
                        3,
                        handle
                    ),
                    BAD_ARGUMENT
                );
                assert_eq!(etw_span_end_reuse(handle), INVALID_STATE);
                assert_eq!(etw_current_span_valid(), 0);
            }
            assert_eq!(
                begin(&provider, Text::borrowed(NAME), ptr::null(), 0, handle),
                0
            );
            assert_eq!(etw_span_end_reuse(handle), 0);
        }
        assert_eq!(etw_span_handle_destroy(&mut handle), 0);
        assert_eq!(capture.spans.lock().unwrap().len(), 2);
    }
}

#[test]
fn mixed_nested_spans_restore_the_exact_previous_context() {
    let (provider, capture) = provider(0);
    let parent = provider.tracer.start("external-parent");
    let external = Context::current_with_span(parent);
    let external_id = external.span().span_context().clone();
    let guard = external.clone().attach();
    unsafe {
        let mut outer = ptr::null_mut();
        let mut inner = reusable();
        assert_eq!(
            etw_span_begin(
                &provider,
                Text::borrowed("outer"),
                ptr::null(),
                0,
                &mut outer
            ),
            0
        );
        assert_eq!(etw_span_end_reuse(outer), INVALID_STATE);
        assert_eq!(etw_span_handle_destroy(&mut outer), INVALID_STATE);
        let outer_id = Context::current().span().span_context().clone();
        assert_eq!(
            etw_span_begin_borrowed(&provider, Text::borrowed("inner"), ptr::null(), 0, inner),
            0
        );
        let inner_id = Context::current().span().span_context().clone();
        assert_eq!(etw_span_end(&mut outer), OUT_OF_ORDER);
        assert_eq!(Context::current().span().span_context(), &inner_id);
        assert_eq!(etw_span_end_reuse(inner), 0);
        assert_eq!(Context::current().span().span_context(), &outer_id);
        assert_eq!(etw_span_end(&mut outer), 0);
        assert_eq!(Context::current().span().span_context(), &external_id);
        assert_eq!(etw_span_handle_destroy(&mut inner), 0);
        let spans = capture.spans.lock().unwrap();
        assert_eq!(spans[0].name, "inner");
        assert_eq!(spans[0].parent_span_id, outer_id.span_id());
        assert_eq!(spans[1].name, "outer");
        assert_eq!(spans[1].parent_span_id, external_id.span_id());
        assert_eq!(spans[0].span_context.trace_id(), external_id.trace_id());
    }
    external.span().end();
    drop(guard);
    assert_eq!(etw_current_span_valid(), 0);
}

#[test]
fn wrong_thread_rejects_all_reusable_operations_without_mutating_state() {
    let (provider, _) = provider(0);
    unsafe {
        let mut handle = reusable();
        for active in [false, true] {
            if active {
                assert_eq!(
                    etw_span_begin_reuse(&provider, Text::borrowed(NAME), ptr::null(), 0, handle),
                    0
                );
            }
            let address = handle as usize;
            let provider_address = &provider as *const ProviderHandle as usize;
            // Join before touching the handle again: this is a sequential wrong-thread
            // call, not simultaneous access to the non-Send ContextGuard.
            std::thread::spawn(move || {
                let mut alias = address as *mut SpanHandle;
                for begin in [etw_span_begin_reuse, etw_span_begin_borrowed] {
                    assert_eq!(
                        begin(
                            provider_address as *const ProviderHandle,
                            Text::borrowed(NAME),
                            ptr::null(),
                            0,
                            alias
                        ),
                        WRONG_THREAD
                    );
                }
                assert_eq!(etw_span_status(alias, 1), WRONG_THREAD);
                assert_eq!(etw_span_end_reuse(alias), WRONG_THREAD);
                assert_eq!(etw_span_end(&mut alias), WRONG_THREAD);
                assert_eq!(etw_span_handle_destroy(&mut alias), WRONG_THREAD);
                assert_eq!(alias as usize, address);
                assert_eq!(etw_current_span_valid(), 0);
            })
            .join()
            .unwrap();
            assert_eq!(etw_current_span_valid(), u32::from(active));
            if active {
                assert_eq!(etw_span_end_reuse(handle), 0);
            }
        }
        assert_eq!(etw_span_handle_destroy(&mut handle), 0);
    }
}

#[test]
fn panic_at_export_resets_handle_and_restores_parent() {
    let (provider, capture) = provider(0);
    unsafe {
        let mut handle = reusable();
        for panic in [true, false] {
            assert_eq!(
                etw_span_begin_reuse(&provider, Text::borrowed(NAME), ptr::null(), 0, handle),
                0
            );
            capture.panic_on_end.store(panic, Ordering::Relaxed);
            assert_eq!(etw_span_end_reuse(handle), if panic { PANIC } else { 0 });
            assert_eq!(etw_current_span_valid(), 0);
            assert_eq!(etw_span_end_reuse(handle), INVALID_STATE);
        }
        assert_eq!(etw_span_handle_destroy(&mut handle), 0);
        assert_eq!(capture.spans.lock().unwrap().len(), 1);
    }
}

#[test]
fn inactive_handle_can_outlive_and_switch_providers() {
    unsafe {
        let mut handle = reusable();
        for _ in 0..2 {
            let (provider, capture) = provider(0);
            assert_eq!(
                etw_span_begin_reuse(&provider, Text::borrowed(NAME), ptr::null(), 0, handle),
                0
            );
            assert_eq!(etw_span_end_reuse(handle), 0);
            assert_eq!(capture.spans.lock().unwrap().len(), 1);
            provider.provider.shutdown().unwrap();
        }
        assert_eq!(etw_span_handle_destroy(&mut handle), 0);
    }
}

#[test]
fn benchmark_variants_export_equivalent_sdk_content() {
    for count in [0, 3] {
        let (provider, capture) = provider(count);
        unsafe {
            assert_eq!(etw_rust_batch(&provider, 0, 2), 0);
            assert_eq!(etw_rust_batch(&provider, 1, 2), 0);
            let mut handle = reusable();
            assert_eq!(etw_rust_batch_reuse(&provider, handle, 0, 2), 0);
            assert_eq!(etw_rust_batch_reuse(&provider, handle, 1, 2), 0);
            assert_eq!(etw_span_handle_destroy(&mut handle), 0);
        }
        let spans = capture.spans.lock().unwrap();
        assert_eq!(spans.len(), 8);
        for span in spans.iter() {
            assert_eq!(span.name, NAME);
            assert_eq!(span.span_kind, SpanKind::Server);
            assert_eq!(span.status, Status::Ok);
            assert_eq!(span.attributes, provider.attributes);
            assert_eq!(span.parent_span_id, opentelemetry::trace::SpanId::INVALID);
            assert!(span.span_context.is_valid());
            assert!(span.span_context.is_sampled());
            assert!(span.end_time >= span.start_time);
            assert_eq!(span.instrumentation_scope.name(), "ffi-benchmark");
            assert!(span.events.events.is_empty());
            assert!(span.links.links.is_empty());
            assert_eq!(span.dropped_attributes_count, 0);
        }
    }
}
