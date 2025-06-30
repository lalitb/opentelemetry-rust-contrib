#[cfg(test)]
mod benchmarks {
    use crate::payload_encoder::otlp_encoder::OtlpEncoder;
    use crate::payload_encoder::otlp_encoder_simplified::SimplifiedOtlpEncoder;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::logs::v1::LogRecord;
    use std::time::Instant;

    fn create_test_logs(count: usize, vary_attributes: bool) -> Vec<LogRecord> {
        let mut logs = Vec::with_capacity(count);
        
        for i in 0..count {
            let mut log = LogRecord {
                observed_time_unix_nano: 1_700_000_000_000_000_000 + i as u64,
                event_name: format!("event_{}", i % 5), // 5 different event names
                severity_number: (i % 16) as i32,
                severity_text: match i % 4 {
                    0 => "DEBUG",
                    1 => "INFO",
                    2 => "WARN",
                    _ => "ERROR",
                }.to_string(),
                ..Default::default()
            };

            // Add trace context for some logs
            if i % 3 == 0 {
                log.trace_id = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
                log.span_id = vec![1, 2, 3, 4, 5, 6, 7, 8];
                log.flags = 1;
            }

            // Add body for some logs
            if i % 2 == 0 {
                log.body = Some(AnyValue {
                    value: Some(Value::StringValue(format!("Log message {}", i))),
                });
            }

            // Add varying or consistent attributes
            if vary_attributes {
                // Different attributes for different logs
                for j in 0..(i % 5 + 1) {
                    log.attributes.push(KeyValue {
                        key: format!("attr_{}", j),
                        value: Some(AnyValue {
                            value: match j % 3 {
                                0 => Some(Value::StringValue(format!("value_{}", j))),
                                1 => Some(Value::IntValue(j as i64 * 100)),
                                _ => Some(Value::DoubleValue(j as f64 * 1.5)),
                            },
                        }),
                    });
                }
            } else {
                // Same attributes for all logs
                log.attributes.push(KeyValue {
                    key: "service".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("test-service".to_string())),
                    }),
                });
                log.attributes.push(KeyValue {
                    key: "version".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("1.0.0".to_string())),
                    }),
                });
            }

            logs.push(log);
        }

        logs
    }

    fn benchmark_encoder<F>(name: &str, encoder_fn: F, logs: &[LogRecord], iterations: usize)
    where
        F: Fn(&[LogRecord]) -> Vec<(u64, String, Vec<u8>, usize)>,
    {
        // Warm up
        for _ in 0..10 {
            let _ = encoder_fn(logs);
        }

        // Actual benchmark
        let start = Instant::now();
        let mut total_bytes = 0;
        let mut total_blobs = 0;

        for _ in 0..iterations {
            let result = encoder_fn(logs);
            for (_, _, bytes, _) in &result {
                total_bytes += bytes.len();
            }
            total_blobs += result.len();
        }

        let duration = start.elapsed();
        let avg_duration = duration / iterations as u32;
        let logs_per_second = (logs.len() as f64 * iterations as f64) / duration.as_secs_f64();
        let avg_blob_size = if total_blobs > 0 { total_bytes / total_blobs } else { 0 };

        println!("{}:", name);
        println!("  Total time: {:?}", duration);
        println!("  Avg time per iteration: {:?}", avg_duration);
        println!("  Logs per second: {:.0}", logs_per_second);
        println!("  Total bytes: {}", total_bytes);
        println!("  Total blobs: {}", total_blobs);
        println!("  Avg blob size: {} bytes", avg_blob_size);
        println!("  Avg blobs per iteration: {:.2}", total_blobs as f64 / iterations as f64);
        println!();
    }

    #[test]
    fn compare_encoders_consistent_schema() {
        println!("\n=== Benchmark: Logs with consistent attributes ===");
        let logs = create_test_logs(1000, false);
        let metadata = "namespace=test/eventVersion=Ver1v0";
        let iterations = 100;

        let original_encoder = OtlpEncoder::new();
        benchmark_encoder(
            "Original Encoder",
            |logs| original_encoder.encode_log_batch(logs.iter(), metadata),
            &logs,
            iterations,
        );

        let simplified_encoder = SimplifiedOtlpEncoder::new();
        benchmark_encoder(
            "Simplified Encoder",
            |logs| simplified_encoder.encode_log_batch(logs.iter(), metadata),
            &logs,
            iterations,
        );
    }

    #[test]
    fn compare_encoders_varying_schema() {
        println!("\n=== Benchmark: Logs with varying attributes ===");
        let logs = create_test_logs(1000, true);
        let metadata = "namespace=test/eventVersion=Ver1v0";
        let iterations = 100;

        let original_encoder = OtlpEncoder::new();
        benchmark_encoder(
            "Original Encoder",
            |logs| original_encoder.encode_log_batch(logs.iter(), metadata),
            &logs,
            iterations,
        );

        let simplified_encoder = SimplifiedOtlpEncoder::new();
        benchmark_encoder(
            "Simplified Encoder",
            |logs| simplified_encoder.encode_log_batch(logs.iter(), metadata),
            &logs,
            iterations,
        );
    }

    #[test]
    fn memory_usage_comparison() {
        println!("\n=== Memory Usage Comparison ===");
        
        // Original encoder with schema caching
        let original_encoder = OtlpEncoder::new();
        let logs_varying = create_test_logs(100, true);
        let metadata = "namespace=test/eventVersion=Ver1v0";
        
        // Generate different schemas
        for i in 0..10 {
            let mut log = LogRecord {
                observed_time_unix_nano: 1_700_000_000_000_000_000,
                event_name: "test".to_string(),
                severity_number: 9,
                ..Default::default()
            };
            
            // Add different attributes to create different schemas
            for j in 0..i {
                log.attributes.push(KeyValue {
                    key: format!("field_{}", j),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(format!("value_{}", j))),
                    }),
                });
            }
            
            let _ = original_encoder.encode_log_batch([log].iter(), metadata);
        }
        
        // The simplified encoder doesn't cache schemas
        let simplified_encoder = SimplifiedOtlpEncoder::new();
        let _ = simplified_encoder.encode_log_batch(logs_varying.iter(), metadata);
        
        println!("Original encoder maintains a schema cache that grows with unique field combinations");
        println!("Simplified encoder uses a fixed universal schema, no caching required");
    }

    #[test]
    fn schema_id_consistency() {
        println!("\n=== Schema ID Consistency Test ===");
        
        let original_encoder = OtlpEncoder::new();
        let simplified_encoder = SimplifiedOtlpEncoder::new();
        let metadata = "namespace=test/eventVersion=Ver1v0";
        
        // Create logs with different attributes
        let log1 = LogRecord {
            observed_time_unix_nano: 1_700_000_000_000_000_000,
            event_name: "test_event".to_string(),
            severity_number: 9,
            ..Default::default()
        };
        
        let mut log2 = log1.clone();
        log2.attributes.push(KeyValue {
            key: "extra_field".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("extra_value".to_string())),
            }),
        });
        
        // Original encoder should create different schema IDs
        let result1_orig = original_encoder.encode_log_batch([log1.clone()].iter(), metadata);
        let result2_orig = original_encoder.encode_log_batch([log2.clone()].iter(), metadata);
        
        let schema_id_1_orig = result1_orig[0].0;
        let schema_id_2_orig = result2_orig[0].0;
        
        // Simplified encoder should use the same schema ID
        let result1_simp = simplified_encoder.encode_log_batch([log1].iter(), metadata);
        let result2_simp = simplified_encoder.encode_log_batch([log2].iter(), metadata);
        
        let schema_id_1_simp = result1_simp[0].0;
        let schema_id_2_simp = result2_simp[0].0;
        
        println!("Original encoder:");
        println!("  Schema ID for log without extra field: {:#x}", schema_id_1_orig);
        println!("  Schema ID for log with extra field: {:#x}", schema_id_2_orig);
        println!("  Different schemas: {}", schema_id_1_orig != schema_id_2_orig);
        
        println!("\nSimplified encoder:");
        println!("  Schema ID for log without extra field: {:#x}", schema_id_1_simp);
        println!("  Schema ID for log with extra field: {:#x}", schema_id_2_simp);
        println!("  Same schema: {}", schema_id_1_simp == schema_id_2_simp);
    }
}
